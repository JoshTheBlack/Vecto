"""
Episode-edit lifecycle helpers. Called from submit_episode_edit (view) and
testable in isolation without HTTP machinery.
"""
import logging

logger = logging.getLogger(__name__)


def parse_chapter_payload(raw) -> dict:
    """Sanitizes raw chapter input (list or v1.2 dict) into the canonical
    {"version": "1.2.0", "chapters": [...]} format. Invalid chapters are
    silently dropped so a single bad entry never rejects the whole submission."""
    is_dict = isinstance(raw, dict)
    raw_list = raw.get('chapters', []) if is_dict else (raw or [])
    waypoints = raw.get('waypoints', False) if is_dict else False

    clean = []
    for chap in raw_list:
        if 'startTime' not in chap or 'title' not in chap:
            continue
        try:
            start_val = float(chap['startTime'])
            c = {
                'startTime': int(start_val) if start_val.is_integer() else start_val,
                'title': str(chap['title']).strip(),
            }
            if 'endTime' in chap and chap['endTime'] not in (None, ''):
                end_val = float(chap['endTime'])
                c['endTime'] = int(end_val) if end_val.is_integer() else end_val
            if 'url' in chap and str(chap['url']).startswith('http'):
                c['url'] = str(chap['url']).strip()
            if 'img' in chap and str(chap['img']).startswith('http'):
                c['img'] = str(chap['img']).strip()
            if 'toc' in chap and chap['toc'] is False:
                c['toc'] = False
            if 'location' in chap and isinstance(chap['location'], dict):
                loc = chap['location']
                if 'name' in loc and 'geo' in loc:
                    c_loc = {'name': str(loc['name']).strip(), 'geo': str(loc['geo']).strip()}
                    if loc.get('osm'):
                        c_loc['osm'] = str(loc['osm']).strip()
                    c['location'] = c_loc
            clean.append(c)
        except ValueError:
            pass

    result = {'version': '1.2.0', 'chapters': clean}
    if waypoints:
        result['waypoints'] = True
    return result


def chapter_items(value):
    """Chapters are stored either as a bare list (legacy) or as the v1.2
    {'version': ..., 'chapters': [...]} dict. Comparisons must always use the
    inner list, or an empty dict-format submission against a legacy empty
    list looks like a change (and pollutes stats/audit with phantom edits)."""
    if isinstance(value, dict):
        return value.get('chapters') or []
    return value or []


def snapshot_episode(ep) -> dict:
    """Returns a dict of the episode fields that submit_episode_edit can modify,
    used as original_data on the EpisodeEditSuggestion row."""
    return {
        'title': ep.title,
        'description': ep.clean_description,
        'tags': ep.tags or [],
        # Capture the EFFECTIVE chapters (what the editor saw + what approve will
        # overwrite onto both columns), so rollback restores the meaningful
        # pre-edit chapters instead of the usually-blank public column.
        'chapters': ep.chapters_private or ep.chapters_public or [],
        'season_number': ep.season_number,
        'episode_number': ep.episode_number,
        'episode_type': ep.episode_type,
        'cross_publish_podcast_ids': sorted(ep.cross_publications.values_list('podcast_id', flat=True)),
    }


def apply_approved_edit(ep, suggested_data, user=None):
    """Writes an auto-approved edit directly onto the episode and saves it."""
    # Speaker label edits only touch transcript files, not Episode fields. Replay
    # recomputes state from the speaker_id base + the approved chain, so the edit
    # row must already be APPROVED before this runs (it is — submit_speaker_labels
    # creates it APPROVED on the trusted path). The mappings dict is no longer
    # passed: apply_speaker_labels folds the chain itself (user_edit_rollback.md §3.3).
    if 'speaker_mappings' in suggested_data:
        from pod_manager.services.transcription import apply_speaker_labels
        apply_speaker_labels(ep.id)
        return

    ep.title = suggested_data.get('title', ep.title)
    ep.clean_description = suggested_data.get('description', ep.clean_description)
    ep.tags = suggested_data.get('tags', ep.tags)
    new_chapters = suggested_data.get('chapters', ep.chapters_public)
    ep.chapters_public = new_chapters
    ep.chapters_private = new_chapters
    if 'season_number' in suggested_data:
        ep.season_number = suggested_data['season_number']
    if 'episode_number' in suggested_data:
        ep.episode_number = suggested_data['episode_number']
    if 'episode_type' in suggested_data:
        ep.episode_type = str(suggested_data['episode_type'])[:50]
    if 'cross_publish_podcast_ids' in suggested_data:
        from pod_manager.services.cross_publish import sync_cross_publications, validate_cross_targets
        targets = validate_cross_targets(
            ep, suggested_data['cross_publish_podcast_ids'], ep.podcast.network
        )
        sync_cross_publications(ep, targets, added_by=user)
    ep.is_metadata_locked = True
    ep.save()


# iTunes / Sequence Metadata fields (§8a) — grouped into one edits_sequence counter.
SEQUENCE_FIELDS = ('season_number', 'episode_number', 'episode_type')
# Fields 1-7 that count toward the multi-field bonus. Cross-publish (#8) is never
# scored; speaker labels (#9) are always a separate single-field edit.
SWEEP_CORE_FIELDS = ('title', 'description', 'tags', 'chapters',
                     'season_number', 'episode_number', 'episode_type')

# Multi-field + first-responder bonus values — the SINGLE source of truth. The
# approve-desk live-points preview reads these via scoring_config() so the JS
# never hardcodes the bonus math: change a number here and the desk follows.
SWEEP_PARTIAL_MIN = 3
SWEEP_PARTIAL_BONUS = 2
SWEEP_FULL_BONUS = 4
FIRST_RESPONDER_BONUS = 1
# Trust penalty for a rejection (explicit reject, or an approval with no sections
# selected, which converts to a rejection).
REJECT_PENALTY = 2


def scoring_config() -> dict:
    """Front-end scoring parameters for the inbox live-points preview."""
    return {
        'sweep_full_count': len(SWEEP_CORE_FIELDS),
        'sweep_full_bonus': SWEEP_FULL_BONUS,
        'sweep_partial_min': SWEEP_PARTIAL_MIN,
        'sweep_partial_bonus': SWEEP_PARTIAL_BONUS,
        'first_responder_bonus': FIRST_RESPONDER_BONUS,
        'reject_penalty': REJECT_PENALTY,
    }


def score_contribution(changes, *, is_first=False):
    """Single source of truth for the trust + counter award of one approved edit
    (user_edit_rollback.md trust model). Returns ``(points, counter_deltas)``.

    ``changes`` carries the fields actually APPLIED; a key's PRESENCE means the
    field changed, its value carries the quantity:

        title / description / season_number / episode_number / episode_type -> any
        tags     -> # tags added              (point is flat +1 if the key is present)
        chapters -> # chapters                (points = that count)
        speaker  -> # speaker points (already computed upstream)

    Cross-publish is intentionally never a key here — it is not scored. ``points``
    is the trust delta (banked on ``edit.points``); ``counter_deltas`` are the exact
    NetworkMembership counter increments (banked on ``edit.counter_deltas``), so
    rollback reverses both exactly. first_responder rides in ``counter_deltas`` too.
    """
    points = 0
    deltas = {}
    applied = set()  # fields 1-7 that landed, for the bonus

    if 'title' in changes:
        points += 1; deltas['edits_title'] = 1; applied.add('title')
    if 'description' in changes:
        points += 1; deltas['edits_descriptions'] = 1; applied.add('description')
    if 'tags' in changes:                          # +1 pt flat, +N counter
        points += 1; applied.add('tags')
        n = int(changes.get('tags') or 0)
        if n:
            deltas['edits_tags'] = n
    if 'chapters' in changes:                      # +N pt, +N counter
        n = int(changes.get('chapters') or 0)
        points += n; applied.add('chapters')
        if n:
            deltas['edits_chapters'] = n
    seq = [k for k in SEQUENCE_FIELDS if k in changes]
    if seq:
        points += len(seq); deltas['edits_sequence'] = len(seq); applied.update(seq)
    if 'speaker' in changes:                        # +N pt, +N counter
        n = int(changes.get('speaker') or 0)
        points += n
        if n:
            deltas['edits_speakers'] = n

    # Multi-field bonus (no counter): all of fields 1-7 -> +4, else 3+ -> +2.
    # Banked into points so rollback removes it exactly.
    if set(SWEEP_CORE_FIELDS) <= applied:
        points += SWEEP_FULL_BONUS
    elif len(applied) >= SWEEP_PARTIAL_MIN:
        points += SWEEP_PARTIAL_BONUS

    # First responder: +1 trust + its own counter (reversed via counter_deltas).
    if is_first:
        points += FIRST_RESPONDER_BONUS; deltas['first_responder_count'] = 1

    return points, deltas


def _chapter_count(value):
    items = chapter_items(value)
    return len(items)


def metadata_changes(suggested_data, original_data):
    """Build the score_contribution `changes` map for a metadata edit by comparing
    the applied (suggested) values against the pre-edit snapshot (original).
    A key is present only when it actually differs; tags carry the ADDED count and
    chapters the chapter count. Cross-publish is intentionally never scored. Shared
    by the trusted-submit award and the legacy edit-points backfill.
    """
    changes = {}
    if 'title' in suggested_data and suggested_data['title'] != original_data.get('title'):
        changes['title'] = True
    if 'description' in suggested_data and suggested_data['description'] != original_data.get('description'):
        changes['description'] = True
    if 'tags' in suggested_data and set(suggested_data.get('tags') or []) != set(original_data.get('tags') or []):
        # Counter credits tags ADDED (removals score the point but add no counter).
        changes['tags'] = len(set(suggested_data.get('tags') or []) - set(original_data.get('tags') or []))
    if 'chapters' in suggested_data and chapter_items(suggested_data.get('chapters')) != chapter_items(original_data.get('chapters')):
        changes['chapters'] = _chapter_count(suggested_data.get('chapters'))
    for k in SEQUENCE_FIELDS:
        if k in suggested_data and suggested_data[k] != original_data.get(k):
            changes[k] = True
    return changes


def update_contribution_stats(membership, suggested_data, original_data, *, is_first: bool):
    """Apply the trust + counter award for an auto-approved submission, using the
    SAME scorer as the inbox approve handler (auto == manual). Returns
    ``(points, counter_deltas)`` so the caller banks them on the edit for an exact
    rollback wash. ``suggested_data`` already holds only the changed keys (the view
    drops unchanged ones), so a present key means a real change.
    """
    changes = metadata_changes(suggested_data, original_data)
    points, deltas = score_contribution(changes, is_first=is_first)
    membership.trust_score += points
    for attr, amt in deltas.items():
        setattr(membership, attr, getattr(membership, attr) + amt)
    membership.save()
    return points, deltas
