"""
Field-level merge primitive — the one true merge engine the Merge Desk
redesign will run on (planned_migration_match_suggestions.txt §3.6).

merge_pair_with_choices(survivor, deleted, field_choices, *, actor) collapses
an episode pair into one surviving row: sanitized field choices are written
onto the survivor, every related object is preserved per the §3.6 (a)-(f)
rules by the individually-testable handlers below, and the losing row is
deleted — all inside one transaction.atomic. Every side effect (fragment /
feed-shell cache busts, R2 audio rekey, legacy transcript file removal,
transcription dispatch) is deferred to transaction.on_commit, so a mid-merge
failure rolls back to exactly the pre-merge state with ZERO file deletions
and zero half-moved relations.

Caller contract (like move_episodes): callers own network scoping — validate
both episodes belong to their network before calling. The primitive takes NO
EpisodeMatchSuggestion; the suggested-pair wrapper (Chat 3) resolves its
suggestion row itself, so plain manual merges and future bulk merges (a
Celery loop over pairs) reuse this unchanged.

The survivor rule (§3.6) is also the caller's job: the transcript-owning row
must be passed as ``survivor`` — no transcript repoint/rekey path exists by
design (transcript R2 keys embed the episode id). If the deleted row
nonetheless owns a transcript (the both-transcripts edge), it is SUPERSEDED,
never repointed: its R2 files go to the orphan queue under
MERGE_SUPERSEDED_TRANSCRIPT (30-day retention via r2_cleanup_orphans), its
legacy local files are unlinked after commit, and its speaker edits are
marked SUPERSEDED.

(f) is intentionally absent: R2OrphanedObject.episode is a SET_NULL audit
pointer that nulls harmlessly when the loser dies, and LiveSchedulePost /
NotFoundEntry / mixes carry no Episode FKs.
"""
import logging

from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

from ..models import (
    CalendarEntry,
    EpisodeCrossPublication,
    EpisodeEditSuggestion,
    Podcast,
    R2OrphanedObject,
    Transcript,
)
from ..utils import sanitize_user_html
from .edits import parse_chapter_payload

logger = logging.getLogger(__name__)

# Protected match_reason the ingester never overwrites (same string
# handle_merge_episodes stamps today).
MANUAL_MERGE_REASON = "Manual Merge (Merge Desk)"

# Every key merge_pair_with_choices accepts in field_choices. Content keys are
# Episode field names ('description' is accepted as an alias for
# clean_description, matching the edit-approval path's vocabulary); the flag
# keys are consumed by apply_lock_and_pin_choices (§3.6e).
CONTENT_CHOICE_KEYS = frozenset({
    'title', 'description', 'clean_description', 'raw_description', 'tags',
    'chapters_public', 'chapters_private', 'pub_date', 'link', 'duration',
    'season_number', 'episode_number', 'episode_type', 'explicit',
    'audio_url_public', 'audio_url_subscriber', 'guid_public', 'guid_private',
    'podcast',
})
FLAG_CHOICE_KEYS = frozenset({'is_metadata_locked', 'audio_locked', 'keep_pin'})
ALLOWED_CHOICE_KEYS = CONTENT_CHOICE_KEYS | FLAG_CHOICE_KEYS


def apply_field_choices(survivor, deleted, field_choices):
    """Write the owner's per-field picks onto the survivor (in memory — the
    primitive saves). Sanitization mirrors the edit-approval path:
    sanitize_user_html for descriptions, parse_chapter_payload for chapters,
    the submit_episode_edit int coercion for season/episode numbers.

    The R2 mirror trio (r2_url / r2_uploaded_at / r2_source_signature) is
    NEVER independently choosable — it travels with the audio_url_subscriber
    pick automatically whenever that pick is the deleted row's value, so the
    only backup of the subscriber audio stays referenced (Episode.r2_url is
    the orphan-GC's source of truth and the mirror signal only fires on row
    creation — it would not self-heal)."""
    choices = dict(field_choices or {})

    if 'description' in choices and 'clean_description' not in choices:
        choices['clean_description'] = choices.pop('description')

    if 'title' in choices:
        survivor.title = str(choices['title'])
    if 'clean_description' in choices:
        survivor.clean_description = sanitize_user_html(choices['clean_description'] or '')
    if 'raw_description' in choices:
        survivor.raw_description = sanitize_user_html(choices['raw_description'] or '')
    if 'tags' in choices:
        survivor.tags = list(choices['tags'] or [])
    for field in ('chapters_public', 'chapters_private'):
        if field in choices:
            raw = choices[field]
            parsed = parse_chapter_payload(raw) if raw else None
            if parsed and not parsed.get('chapters') and not parsed.get('waypoints'):
                parsed = None
            setattr(survivor, field, parsed)
    if 'pub_date' in choices:
        survivor.pub_date = choices['pub_date']
    if 'link' in choices:
        survivor.link = choices['link'] or None
    if 'duration' in choices:
        survivor.duration = str(choices['duration'] or '')
    for int_field in ('season_number', 'episode_number'):
        if int_field in choices:
            try:
                val = choices[int_field]
                setattr(survivor, int_field, int(val) if val not in (None, '', 0) else None)
            except (ValueError, TypeError):
                pass  # unparseable — keep the survivor's current value
    if 'episode_type' in choices:
        survivor.episode_type = str(choices['episode_type'] or '')[:50]
    if 'explicit' in choices:
        val = choices['explicit']
        survivor.explicit = None if val in (None, '') else bool(val)
    if 'audio_url_public' in choices:
        survivor.audio_url_public = choices['audio_url_public'] or None
    for guid_field in ('guid_public', 'guid_private'):
        if guid_field in choices:
            setattr(survivor, guid_field, choices[guid_field] or None)

    if 'audio_url_subscriber' in choices:
        original_audio = survivor.audio_url_subscriber
        val = choices['audio_url_subscriber'] or None
        survivor.audio_url_subscriber = val
        # The trio rides the pick: chosen value came from the deleted row (or
        # matches both rows and only the deleted row holds a mirror).
        if val and val == deleted.audio_url_subscriber and deleted.r2_url and (
                val != original_audio or not survivor.r2_url):
            survivor.r2_url = deleted.r2_url
            survivor.r2_uploaded_at = deleted.r2_uploaded_at
            survivor.r2_source_signature = deleted.r2_source_signature

    if 'podcast' in choices:
        parent = choices['podcast']
        if isinstance(parent, Podcast):
            survivor.podcast = parent
        else:
            survivor.podcast_id = int(parent)


def apply_lock_and_pin_choices(survivor, deleted, field_choices, *, actor=None):
    """(e) Locks and pin from the editor's explicit picks (§3.5/§3.6e).

    'is_metadata_locked' / 'audio_locked': applied as booleans when present.
    'keep_pin' truthy -> the survivor ends pinned: its own stamp wins, else
    the deleted row's stamp carries over (a manual decision must not be
    lost), else a fresh stamp by ``actor``. Falsy -> pin cleared. Absent ->
    survivor untouched. In-memory only; the primitive saves."""
    choices = field_choices or {}
    for flag in ('is_metadata_locked', 'audio_locked'):
        if flag in choices:
            setattr(survivor, flag, bool(choices[flag]))
    if 'keep_pin' in choices:
        if choices['keep_pin']:
            if not survivor.podcast_pinned_at:
                if deleted.podcast_pinned_at:
                    survivor.podcast_pinned_at = deleted.podcast_pinned_at
                    survivor.podcast_pinned_by = deleted.podcast_pinned_by
                else:
                    survivor.podcast_pinned_at = timezone.now()
                    survivor.podcast_pinned_by = actor
        else:
            survivor.podcast_pinned_at = None
            survivor.podcast_pinned_by = None


def merge_transcript(survivor, deleted):
    """(a) The survivor keeps its own transcript; nothing ever moves or
    rekeys. A transcript on the deleted row (both-transcripts edge) is
    superseded: its R2 object keys become MERGE_SUPERSEDED_TRANSCRIPT orphan
    rows (rows only — the objects are untouched inside the transaction and
    die in r2_cleanup_orphans after 30 days), its legacy local files are
    returned for on_commit deletion, and the Transcript row is deleted with
    auto_delete_transcript_files bypassed so the CASCADE-equivalent delete
    performs no inline file I/O.

    Returns {'superseded': bool, 'orphaned_keys': [...], 'local_paths': [...]}."""
    from pathlib import Path
    from .transcription import transcript_r2_key
    from .r2_maintenance import _transcript_marker_exts

    result = {'superseded': False, 'orphaned_keys': [], 'local_paths': []}
    transcript = Transcript.objects.filter(episode=deleted).first()
    if transcript is None:
        return result

    # R2-backed files -> orphan queue (mirrors the signal's R2 gate). The
    # audit pointer targets the SURVIVOR: the loser's row is about to die and
    # SET_NULL would erase a loser pointer anyway.
    if settings.R2_MEDIA_ENABLED and (transcript.version or 0) >= 1:
        for ext in _transcript_marker_exts(transcript):
            key = transcript_r2_key(deleted.id, ext, transcript.r2_key_token)
            R2OrphanedObject.objects.get_or_create(
                key=key,
                defaults={
                    'reason': R2OrphanedObject.Reason.MERGE_SUPERSEDED_TRANSCRIPT,
                    'episode': survivor,
                },
            )
            result['orphaned_keys'].append(key)

    # Legacy local files (version 0, or leftover MEDIA_ROOT markers) —
    # collected here, unlinked only after commit.
    media_root = Path(settings.MEDIA_ROOT)
    for field in ('vtt_file', 'json_file', 'srt_file', 'html_file', 'words_json_file'):
        rel = getattr(transcript, field, None)
        if rel:
            result['local_paths'].append(media_root / rel)

    transcript._defer_file_deletion = True
    transcript.delete()
    result['superseded'] = True
    return result


def merge_edit_suggestions(survivor, deleted):
    """(b) Rekey the deleted row's edit suggestions to the survivor so trust
    history and rollback survive the merge. Metadata edits repoint with their
    banked points / counter_deltas and created_at untouched (the replay fold
    keeps its exact ordering; rollback still reverses exactly). Speaker edits
    additionally flip to SUPERSEDED (§3.6a): they reference the superseded
    transcript's diarization and can never replay, but stay for audit and
    trust history — which is exactly why they repoint instead of riding the
    CASCADE into deletion.

    Returns {'metadata': n, 'speaker_superseded': n}."""
    counts = {'metadata': 0, 'speaker_superseded': 0}
    for edit in EpisodeEditSuggestion.objects.filter(episode=deleted):
        edit.episode = survivor
        if 'speaker_mappings' in (edit.suggested_data or {}):
            edit.status = EpisodeEditSuggestion.Status.SUPERSEDED
            edit.save(update_fields=['episode', 'status'])
            counts['speaker_superseded'] += 1
        else:
            edit.save(update_fields=['episode'])
            counts['metadata'] += 1
    return counts


def merge_calendar_entry(survivor, deleted):
    """(c) Relink-BEFORE-delete: when only the deleted row has a calendar
    entry, repoint it to the survivor now, so the Episode pre_delete signal
    (delete_auto_created_calendar_entry) later sees nothing linked. When both
    rows have one, the survivor's wins and the loser's rides the normal
    pre_delete path (auto-created dies; pre-planned returns to the unlinked
    pool). Queries by queryset — never the reverse descriptor — so no stale
    relation is cached on ``deleted`` for the signal to trip over.

    Returns 'none' | 'kept_survivor' | 'relinked'."""
    entry = CalendarEntry.objects.filter(episode=deleted).first()
    if entry is None:
        return 'none'
    if CalendarEntry.objects.filter(episode=survivor).exists():
        return 'kept_survivor'
    entry.episode = survivor
    entry.save(update_fields=['episode'])
    return 'relinked'


def merge_cross_publications(survivor, deleted):
    """(d) Manual cross-publications (auto_created=False) on the deleted row
    repoint to the survivor, deduped against uniq_episode_crosspub_target and
    skipping any link into the survivor's (post-choice) parent podcast — the
    owner can prune the rest in the existing cross-publish UI. Auto links are
    left for the CASCADE; the survivor re-derives its own from its parent's
    auto_crosspublish_targets.

    Returns {'repointed': n, 'skipped': n}."""
    existing_targets = set(
        EpisodeCrossPublication.objects.filter(episode=survivor)
        .values_list('podcast_id', flat=True))
    counts = {'repointed': 0, 'skipped': 0}
    for link in EpisodeCrossPublication.objects.filter(episode=deleted, auto_created=False):
        if link.podcast_id == survivor.podcast_id or link.podcast_id in existing_targets:
            counts['skipped'] += 1  # dies with the loser via CASCADE
            continue
        link.episode = survivor
        link.save(update_fields=['episode'])
        existing_targets.add(link.podcast_id)
        counts['repointed'] += 1
    return counts


def merge_pair_with_choices(survivor, deleted, field_choices, *, actor, base_url=None):
    """Merge ``deleted`` into ``survivor`` applying the owner's per-field
    picks. See the module docstring for the caller contract. ``actor`` is the
    acting user (pin stamp + audit log). ``base_url`` is optional: when given,
    the survivor's feed fragments are also pre-warmed after commit via
    task_rebuild_episode_fragments (the cache busts happen either way, so the
    next feed serve rebuilds lazily without it).

    Returns the refreshed survivor."""
    if survivor.pk == deleted.pk:
        raise ValueError("survivor and deleted are the same episode")
    unknown = set(field_choices or {}) - ALLOWED_CHOICE_KEYS
    if unknown:
        raise ValueError(f"Unknown field_choices keys: {sorted(unknown)}")

    with transaction.atomic():
        original_parent_id = survivor.podcast_id
        # Feeds whose shells may change membership: both parents (before and
        # after the parent pick) plus every feed either row is placed into.
        shell_podcast_ids = {
            original_parent_id, deleted.podcast_id,
        } | set(
            EpisodeCrossPublication.objects
            .filter(episode__in=[survivor, deleted])
            .values_list('podcast_id', flat=True))

        apply_field_choices(survivor, deleted, field_choices)
        apply_lock_and_pin_choices(survivor, deleted, field_choices, actor=actor)
        survivor.match_reason = MANUAL_MERGE_REASON
        survivor.save()

        parent_changed = survivor.podcast_id != original_parent_id
        shell_podcast_ids.add(survivor.podcast_id)
        if parent_changed:
            # Same invariants move_episodes owns for a parent change: a link
            # into the new parent would now self-reference, and the auto
            # cross-publish links re-derive from the new parent's targets.
            from .cross_publish import reeval_auto_cross_publish
            EpisodeCrossPublication.objects.filter(
                episode=survivor, podcast_id=survivor.podcast_id).delete()
            shell_podcast_ids |= set(
                reeval_auto_cross_publish([survivor.pk], survivor.podcast))

        transcript_result = merge_transcript(survivor, deleted)
        merge_edit_suggestions(survivor, deleted)
        merge_calendar_entry(survivor, deleted)
        merge_cross_publications(survivor, deleted)

        # The relink above rewrote the DB; drop any stale cached reverse
        # relation so delete_auto_created_calendar_entry (pre_delete) re-reads
        # it fresh and sees nothing linked.
        deleted._state.fields_cache.pop('calendar_entry', None)
        deleted_id = deleted.pk
        deleted.delete()

        # Computed after the loser (and any superseded transcript) is gone:
        # only the survivor's own transcript can remain.
        needs_transcription = bool(
            survivor.audio_url_subscriber
        ) and not Transcript.objects.filter(episode=survivor).exists()

        survivor_id = survivor.pk
        rekey_survivor = parent_changed and bool(survivor.r2_url)
        local_paths = transcript_result['local_paths']

        def _after_commit():
            for ep_id in (survivor_id, deleted_id):
                cache.delete(f"ep_frag_public_{ep_id}")
                cache.delete(f"ep_frag_private_{ep_id}")
            for pod_id in shell_podcast_ids:
                cache.delete(f"feed_shell_public_{pod_id}")
                cache.delete(f"feed_shell_private_{pod_id}")
            if base_url:
                from ..tasks import task_rebuild_episode_fragments
                task_rebuild_episode_fragments.delay(survivor_id, base_url.rstrip('/'))
            if rekey_survivor and getattr(settings, 'R2_MIRROR_ENABLED', True):
                from ..tasks import task_rekey_episode_audio
                task_rekey_episode_audio.delay(survivor_id)
            _delete_local_transcript_files(local_paths)
            if needs_transcription and getattr(settings, 'WHISPER_ENABLED', False):
                # The post_save signal only queues on row creation, so the
                # merged episode would otherwise never transcribe. Mirror the
                # signal: PENDING row for immediate UI feedback, then dispatch.
                from .transcription import dispatch_transcription
                Transcript.objects.update_or_create(
                    episode_id=survivor_id,
                    defaults={
                        'status': Transcript.Status.PENDING,
                        'requested_at': timezone.now(),
                        'error_message': None,
                    },
                )
                dispatch_transcription(survivor_id)

        transaction.on_commit(_after_commit)

    actor_name = getattr(actor, 'username', actor) or 'system'
    logger.info(
        "[merge] Episode %d merged into %d by %s (transcript_superseded=%s, "
        "parent_changed=%s)",
        deleted_id, survivor_id, actor_name,
        transcript_result['superseded'], parent_changed,
    )
    return survivor


def _delete_local_transcript_files(paths):
    """Unlink legacy MEDIA_ROOT transcript files (and their bucket dir if it
    emptied) — the on_commit half of the superseded-transcript path, mirroring
    auto_delete_transcript_files' local branch. Best-effort: the DB merge has
    already committed, so a filesystem hiccup only logs."""
    deleted_dir = None
    for path in paths:
        try:
            if path.exists():
                path.unlink()
                deleted_dir = path.parent
        except Exception as e:
            logger.error("Failed to delete superseded transcript file %s: %s", path, e)
    if deleted_dir and deleted_dir.exists() and not any(deleted_dir.iterdir()):
        try:
            deleted_dir.rmdir()
        except Exception:
            pass
