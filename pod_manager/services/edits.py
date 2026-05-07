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


def snapshot_episode(ep) -> dict:
    """Returns a dict of the episode fields that submit_episode_edit can modify,
    used as original_data on the EpisodeEditSuggestion row."""
    return {
        'title': ep.title,
        'description': ep.clean_description,
        'tags': ep.tags or [],
        'chapters': ep.chapters_public or [],
        'season_number': ep.season_number,
        'episode_number': ep.episode_number,
        'episode_type': ep.episode_type,
    }


def apply_approved_edit(ep, suggested_data):
    """Writes an auto-approved edit directly onto the episode and saves it."""
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
    ep.is_metadata_locked = True
    ep.save()


def update_contribution_stats(membership, suggested_data, original_data, *, is_first: bool):
    """Awards +5 trust and increments the appropriate edit counters on the
    NetworkMembership for an auto-approved submission."""
    membership.trust_score += 5

    if suggested_data.get('title') != original_data.get('title'):
        membership.edits_title += 1

    # Count each individual tag added or removed, not just whether tags changed.
    orig_tags = set(original_data.get('tags') or [])
    new_tags = set(suggested_data.get('tags') or [])
    tag_delta = len(new_tags.symmetric_difference(orig_tags))
    if tag_delta:
        membership.edits_tags += tag_delta

    if suggested_data.get('chapters') != original_data.get('chapters'):
        chap_data = suggested_data.get('chapters', [])
        membership.edits_chapters += (
            len(chap_data.get('chapters', [])) if isinstance(chap_data, dict) else len(chap_data)
        )

    if suggested_data.get('description') != original_data.get('description'):
        membership.edits_descriptions += 1

    if is_first:
        membership.first_responder_count += 1

    membership.save()
