"""
Field-level merge editor support (planned_migration_match_suggestions.txt §3.5/§3.6).

The GET partial (creator_match_editor) renders both rows of an
EpisodeMatchSuggestion column-by-column; the POST action (handle_commit_match_merge)
resolves the owner's picks into the field_choices vocabulary the merge primitive
(services/episode_merge.py) accepts. This module owns the mapping so the two
endpoints — and the tests — agree on it.

Roles are fixed as detected (never re-derived): ``public_episode`` matched the
incoming public GUID, ``private_episode`` the incoming private GUID. The SURVIVOR
is a separate, transcript-driven decision (default_survivor) — the primitive
writes role-based field values onto whichever row survives.
"""
import json

# (episode attr, field_choices key, label, kind). The key is exactly a member of
# episode_merge.ALLOWED_CHOICE_KEYS — anything else the primitive rejects.
# kinds drive both rendering and edit-parsing:
#   text  int  datetime  html  tags  chapters  explicit
FIELD_SPECS = [
    ('title',                'title',                'Title',            'text'),
    ('clean_description',    'clean_description',    'Description',      'html'),
    ('raw_description',      'raw_description',       'Raw Description',  'html'),
    ('tags',                 'tags',                 'Tags',             'tags'),
    ('chapters_public',      'chapters_public',       'Public Chapters',  'chapters'),
    ('chapters_private',     'chapters_private',      'Premium Chapters', 'chapters'),
    ('pub_date',             'pub_date',             'Publish Date',     'datetime'),
    ('link',                 'link',                 'Link',             'text'),
    ('duration',             'duration',             'Duration',         'text'),
    ('season_number',        'season_number',         'Season #',         'int'),
    ('episode_number',       'episode_number',        'Episode #',        'int'),
    ('episode_type',         'episode_type',          'Episode Type',     'text'),
    ('explicit',             'explicit',             'Explicit',         'explicit'),
    ('audio_url_public',     'audio_url_public',      'Public Audio URL', 'text'),
    ('audio_url_subscriber', 'audio_url_subscriber',  'Premium Audio URL', 'text'),
    ('guid_public',          'guid_public',           'Public GUID',      'text'),
    ('guid_private',         'guid_private',          'Private GUID',     'text'),
]

# A/B tie-break: which side a field defaults to when BOTH rows carry a value.
# GUIDs and each audio tier default to their own tier so the survivor carries
# both correct GUIDs (§3.5); everything else defaults to the private row (the
# richer "Premium Exclusive" side in the common case).
_TIE_DEFAULT = {
    'guid_public': 'public',
    'guid_private': 'private',
    'audio_url_public': 'public',
    'audio_url_subscriber': 'private',
}


def _has_transcript(episode):
    from ..models import Transcript
    return Transcript.objects.filter(episode=episode).exists()


def default_survivor(public_episode, private_episode):
    """§3.6 survivor rule (deterministic, no transcript repoint path exists):

      - exactly one row owns a Transcript  -> that row survives (not editable);
      - BOTH own one (the pre-fix-corruption norm, §4b) -> owner picks, default
        the private-GUID row, editable;
      - neither -> the subscriber-audio row, else the public-GUID row.

    Returns (survivor, deleted, both_transcripts, survivor_editable)."""
    pub_tx = _has_transcript(public_episode)
    priv_tx = _has_transcript(private_episode)
    if pub_tx and priv_tx:
        return private_episode, public_episode, True, True
    if pub_tx:
        return public_episode, private_episode, False, False
    if priv_tx:
        return private_episode, public_episode, False, False
    if private_episode.audio_url_subscriber:
        return private_episode, public_episode, False, False
    if public_episode.audio_url_subscriber:
        return public_episode, private_episode, False, False
    return public_episode, private_episode, False, False


def _is_empty(val):
    if val is None:
        return True
    if isinstance(val, str):
        return val.strip() == ''
    if isinstance(val, (list, dict)):
        return len(val) == 0
    return False


def _default_choice(key, a_val, b_val, survivor_side):
    """Which side ('public'/'private') a field defaults to: the non-empty / more
    complete side; on a tie the field's own preference, else the survivor's side
    (§3.5 'defaulting to the non-empty / more complete side')."""
    a_empty, b_empty = _is_empty(a_val), _is_empty(b_val)
    if a_empty and not b_empty:
        return 'private'
    if b_empty and not a_empty:
        return 'public'
    return _TIE_DEFAULT.get(key, survivor_side)


def build_editor_fields(public_episode, private_episode, survivor):
    """Per-field descriptors for the GET template: both values plus the default
    A/B pick. Chapters carry their JSON for the shared editor seed."""
    survivor_side = 'private' if survivor.pk == private_episode.pk else 'public'
    fields = []
    for attr, key, label, kind in FIELD_SPECS:
        a_val = getattr(public_episode, attr)
        b_val = getattr(private_episode, attr)
        fields.append({
            'attr': attr,
            'key': key,
            'label': label,
            'kind': kind,
            'a_value': a_val,
            'b_value': b_val,
            'a_json': json.dumps(a_val) if kind in ('tags', 'chapters') else None,
            'b_json': json.dumps(b_val) if kind in ('tags', 'chapters') else None,
            'default_choice': _default_choice(key, a_val, b_val, survivor_side),
        })
    return fields


def _parse_edit(kind, raw):
    """Turn an inline-edit POST string into the value the primitive expects for
    its kind. Malformed tags/chapters JSON falls back to a safe empty payload."""
    if kind == 'tags':
        try:
            parsed = json.loads(raw) if raw.strip() else []
            return [str(t) for t in parsed] if isinstance(parsed, list) else []
        except (ValueError, TypeError):
            # Comma-separated fallback.
            return [t.strip() for t in raw.split(',') if t.strip()]
    if kind == 'chapters':
        try:
            return json.loads(raw) if raw.strip() else None
        except (ValueError, TypeError):
            return None
    if kind == 'explicit':
        low = raw.strip().lower()
        if low in ('true', 'yes', '1'):
            return True
        if low in ('false', 'no', '0'):
            return False
        return None
    # text / int — the primitive coerces ints and normalizes ''->None itself.
    return raw


def resolve_field_choices(post, public_episode, private_episode):
    """Build the field_choices dict for merge_pair_with_choices from the editor
    form. Each field carries choice_<key> in {public, private, edit}; 'edit' also
    carries edit_<key>. A missing/blank choice leaves the survivor's own value
    (the key is omitted). datetime fields are A/B only (no inline edit).

    Locks/pin (§3.6e) come from their own checkboxes, not FIELD_SPECS."""
    choices = {}
    for attr, key, label, kind in FIELD_SPECS:
        sel = post.get('choice_' + key, '')
        if sel == 'public':
            choices[key] = getattr(public_episode, attr)
        elif sel == 'private':
            choices[key] = getattr(private_episode, attr)
        elif sel == 'edit' and kind != 'datetime':
            choices[key] = _parse_edit(kind, post.get('edit_' + key, ''))
        # else: not chosen -> keep the survivor's current value.

    # Flags (§3.6e): plain checkbox presence.
    choices['is_metadata_locked'] = bool(post.get('is_metadata_locked'))
    choices['audio_locked'] = bool(post.get('audio_locked'))
    choices['keep_pin'] = bool(post.get('keep_pin'))
    return choices
