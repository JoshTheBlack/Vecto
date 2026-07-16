"""
GET-side data-gather functions for creator_settings tabs.

Each function is decorated with @diagnostic_timer and returns a partial
context dict. creator_settings merges them with ** unpacking.
"""
import logging
import os

from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import Q, Case, When, CharField, Max, Count, F
from django.db.models.functions import Substr, Lower

from ...models import EpisodeEditSuggestion, NetworkMembership, Episode
from ...services.edits import chapter_items, score_contribution, scoring_config, FIRST_RESPONDER_BONUS, REJECT_PENALTY
from ...utils import diagnostic_timer

logger = logging.getLogger(__name__)

# Manage Podcasts renders one window of collapsed headers per request; "Load
# more" fetches the next. Filters/sort still run over the FULL set server-side.
SHOW_PAGE_SIZE = 25


def _annotate_edit_changes(edit):
    """Per-field "did the suggester actually change this?" flags plus
    normalized chapter lists. The inbox and audit log both hide untouched
    fields, so reviewers only ever see real deltas."""
    sugg = edit.suggested_data or {}
    orig = edit.original_data or {}
    edit.title_changed = 'title' in sugg and (sugg.get('title') or '') != (orig.get('title') or '')
    edit.desc_changed = 'description' in sugg and (sugg.get('description') or '') != (orig.get('description') or '')
    edit.tags_changed = 'tags' in sugg and set(sugg.get('tags') or []) != set(orig.get('tags') or [])
    edit.chapters_changed = 'chapters' in sugg and chapter_items(sugg.get('chapters')) != chapter_items(orig.get('chapters'))
    edit.season_changed = 'season_number' in sugg and sugg.get('season_number') != orig.get('season_number')
    edit.epnum_changed = 'episode_number' in sugg and sugg.get('episode_number') != orig.get('episode_number')
    edit.eptype_changed = 'episode_type' in sugg and (sugg.get('episode_type') or '') != (orig.get('episode_type') or '')
    edit.cross_publish_changed = (
        'cross_publish_podcast_ids' in sugg
        and sorted(sugg.get('cross_publish_podcast_ids') or []) != sorted(orig.get('cross_publish_podcast_ids') or [])
    )
    # Structured speaker diff (§8b): [(speaker_id, before, after)] so both the
    # inbox and audit log render a consistent before→after instead of reading the
    # raw dicts. The before-state is original_data.speaker_mappings (the submit-time
    # fold); an unmentioned speaker_id resolves to its own raw label. gather_inbox
    # overrides this with the *current* fold for pending edits.
    sugg_speakers = sugg.get('speaker_mappings') or {}
    orig_speakers = orig.get('speaker_mappings') or {}
    edit.speaker_diff = [
        (sid, orig_speakers.get(sid, sid), after) for sid, after in sugg_speakers.items()
    ]
    edit.speaker_changed = any(before != after for _, before, after in edit.speaker_diff)
    edit.has_changes = any([
        edit.title_changed, edit.desc_changed, edit.tags_changed, edit.chapters_changed,
        edit.season_changed, edit.epnum_changed, edit.eptype_changed,
        edit.cross_publish_changed, edit.speaker_changed,
    ])
    # Always lists of chapter dicts, never the raw v1.2 wrapper dict —
    # iterating the wrapper in a template yields its KEYS, and `key.title`
    # resolves to str.title() ("Version", "Chapters" at 0s).
    edit.orig_chapter_list = chapter_items(orig.get('chapters'))
    edit.sugg_chapter_list = chapter_items(sugg.get('chapters'))

    # Per-section trust points (mirrors score_contribution's per-field values) so
    # the inbox preview and the audit-log tally render the same badges. Speaker
    # points are context-specific (preview vs banked) and set by each gather_*.
    edit.pts_title = 1 if edit.title_changed else 0
    edit.pts_desc = 1 if edit.desc_changed else 0
    edit.pts_tags = 1 if edit.tags_changed else 0          # flat +1 trust (+N is a counter)
    edit.pts_chapters = len(edit.sugg_chapter_list) if edit.chapters_changed else 0
    edit.pts_season = 1 if edit.season_changed else 0
    edit.pts_epnum = 1 if edit.epnum_changed else 0
    edit.pts_eptype = 1 if edit.eptype_changed else 0


def _section_base(edit):
    """Sum of the per-section trust points (the badge values), excluding the
    multi-field / first-responder bonus."""
    return (edit.pts_title + edit.pts_desc + edit.pts_tags + edit.pts_chapters
            + edit.pts_season + edit.pts_epnum + edit.pts_eptype + edit.pts_speaker)


def network_podcast_list(current_network):
    """Alphabetized (ignoring a leading "The ") list of every feed in the
    network, annotated with episode_count + latest_episode_date (for the chip
    selector's selected-rows) and s3_episode_count (for GDrive Recovery). Feeds
    the cross-publish picker, merge-desk selector, mix editor, transcript-backfill
    picker, and GDrive Recovery — all of which show the full roster regardless of
    any active filter."""
    return list(current_network.podcasts.annotate(
        episode_count=Count('episodes', distinct=True),
        latest_episode_date=Max('episodes__pub_date'),
        s3_episode_count=Count(
            'episodes',
            filter=Q(episodes__audio_url_subscriber__icontains='s3.amazonaws.com'),
            distinct=True,
        ),
    ).order_by(
        Lower(Case(When(title__istartswith='The ', then=Substr('title', 5)),
                   default='title', output_field=CharField()))
    ))


@diagnostic_timer("1. Gather Manage Podcasts")
def gather_manage_podcasts(request, current_network):
    show_q = request.GET.get('show_q', '').strip()
    show_sort = request.GET.get('show_sort', 'alpha')
    show_mix = request.GET.get('show_mix', '')

    manage_podcasts = current_network.podcasts.annotate(
        clean_title=Case(When(title__istartswith='The ', then=Substr('title', 5)), default='title', output_field=CharField()),
        latest_episode_date=Max('episodes__pub_date'),
        episode_count=Count('episodes', distinct=True),
        s3_episode_count=Count(
            'episodes',
            filter=Q(episodes__audio_url_subscriber__icontains='s3.amazonaws.com'),
            distinct=True,
        ),
    )

    if show_q:
        manage_podcasts = manage_podcasts.filter(
            Q(title__icontains=show_q) | Q(slug__icontains=show_q)
        )

    if show_mix:
        try:
            mix = current_network.mixes.get(id=show_mix)
            manage_podcasts = manage_podcasts.filter(id__in=mix.selected_podcasts.all())
        except Exception:
            pass

    # Every sort gets 'id' as a tiebreaker: latest_episode_date and
    # episode_count tie freely across 88 shows, and an unstable order makes a
    # paginated window drop or repeat rows between pages.
    if show_sort == 'recent':
        manage_podcasts = manage_podcasts.order_by(F('latest_episode_date').desc(nulls_last=True), 'id')
    elif show_sort == 'oldest':
        manage_podcasts = manage_podcasts.order_by(F('latest_episode_date').asc(nulls_last=True), 'id')
    elif show_sort == 'count_desc':
        manage_podcasts = manage_podcasts.order_by('-episode_count', 'id')
    else:
        manage_podcasts = manage_podcasts.order_by(Lower('clean_title'), 'id')

    # Only the collapsed accordion headers render here — title, tier, counts.
    # Each show's full form (and its cross-publish selection) is fetched lazily
    # on expand via creator_show_form, so no auto_crosspublish prefetch needed.
    # Even collapsed, 88 headers + their Data-Ingestion panels were most of this
    # tab's weight, so only SHOW_PAGE_SIZE of them render per request.
    paginator = Paginator(manage_podcasts.select_related('required_tier'), SHOW_PAGE_SIZE)
    show_page_obj = paginator.get_page(request.GET.get('show_page', 1))
    # Page 2+ is a "load more" append: it returns just the next slice of rows,
    # so it needs none of the surrounding tab chrome.
    show_append = show_page_obj.number > 1

    context = {
        'manage_podcasts': show_page_obj.object_list,
        'show_page_obj': show_page_obj,
        'show_remaining': paginator.count - show_page_obj.end_index(),
        'show_append': show_append,
        'show_q': show_q,
        'show_sort': show_sort,
        'show_mix': show_mix,
    }
    if not show_append:
        # The lazily-loaded forms' cross-publish pickers clone their options from
        # a single page-level <template>, which needs the full network roster.
        # It renders once with the tab, so an append never pays for it.
        context['network_podcasts'] = network_podcast_list(current_network)
    return context


@diagnostic_timer("1b. Gather Network Mixes")
def gather_mixes(current_network):
    """Mix CARDS only — name, tier, artwork, feed URL and a show count. Each
    mix's edit form (and its show picker) loads on modal open via
    creator_mix_form, so this no longer prefetches every mix's podcasts just to
    build the selected-id set the form needed: a Count does the card's job in
    the query."""
    return {'network_mixes': list(
        current_network.mixes
        .select_related('required_tier')
        .annotate(selected_count=Count('selected_podcasts', distinct=True))
    )}


@diagnostic_timer("2. Gather Inbox")
def gather_inbox(current_network):
    pending_edits = EpisodeEditSuggestion.objects.filter(
        episode__podcast__network=current_network, status=EpisodeEditSuggestion.Status.PENDING
    ).select_related('episode', 'episode__podcast', 'user')

    user_ids = [e.user_id for e in pending_edits]
    memberships = {m.user_id: m for m in NetworkMembership.objects.filter(user_id__in=user_ids, network=current_network)}
    network_podcast_titles = dict(current_network.podcasts.values_list('id', 'title'))

    for edit in pending_edits:
        edit.membership = memberships.get(edit.user_id)
        ep = edit.episode
        orig = edit.original_data or {}

        current_title = ep.title or ''
        edit.title_conflict = current_title != (orig.get('title') or '')
        edit.current_title = current_title

        current_tags = ep.tags or []
        edit.tags_conflict = set(current_tags) != set(orig.get('tags') or [])
        edit.current_tags = current_tags

        current_chapters = ep.chapters_public or []
        edit.chapters_conflict = chapter_items(current_chapters) != chapter_items(orig.get('chapters'))
        edit.current_chapters = current_chapters

        current_desc = ep.clean_description or ''
        edit.desc_conflict = current_desc != (orig.get('description') or '')
        edit.current_description = current_desc

        edit.current_season_number = ep.season_number
        edit.current_episode_number = ep.episode_number
        edit.current_episode_type = ep.episode_type or ''

        _annotate_edit_changes(edit)

        # For a pending speaker edit the live resolved name may differ from the
        # submit-time snapshot, so rebuild the before→after against the *current*
        # fold (§8b). Falls back to the raw speaker_id when unmapped.
        sugg_speakers = (edit.suggested_data or {}).get('speaker_mappings') or {}
        edit.pts_speaker = 0
        if sugg_speakers:
            from ...services.transcription import fold_speaker_mappings, speaker_edit_points
            current_map = fold_speaker_mappings(ep.id)
            edit.speaker_diff = [
                (sid, current_map.get(sid, sid), after) for sid, after in sugg_speakers.items()
            ]
            edit.speaker_changed = any(before != after for _, before, after in edit.speaker_diff)
            edit.pts_speaker, _ = speaker_edit_points(sugg_speakers, current_map)

        # All-checked default the button shows on load; the JS recomputes the same
        # way live as sections are toggled (driven by scoring_config, below).
        preview_changes = {}
        if edit.title_changed: preview_changes['title'] = True
        if edit.desc_changed: preview_changes['description'] = True
        if edit.tags_changed: preview_changes['tags'] = 1
        if edit.chapters_changed: preview_changes['chapters'] = edit.pts_chapters
        if edit.season_changed: preview_changes['season_number'] = True
        if edit.epnum_changed: preview_changes['episode_number'] = True
        if edit.eptype_changed: preview_changes['episode_type'] = True
        if edit.pts_speaker: preview_changes['speaker'] = edit.pts_speaker
        total, _ = score_contribution(preview_changes, is_first=edit.is_first_responder)
        edit.base_points = _section_base(edit)
        edit.fr_bonus = FIRST_RESPONDER_BONUS if edit.is_first_responder else 0
        edit.sweep_bonus = max(0, total - edit.base_points - edit.fr_bonus)
        edit.total_points_preview = total

        if 'cross_publish_podcast_ids' in (edit.suggested_data or {}):
            current_cross_ids = sorted(ep.cross_publications.values_list('podcast_id', flat=True))
            edit.current_cross_publish_ids = current_cross_ids
            edit.cross_publish_conflict = current_cross_ids != sorted(orig.get('cross_publish_podcast_ids') or [])
            _titles = lambda ids: [network_podcast_titles.get(i, f'#{i}') for i in ids]
            edit.cross_publish_current_titles = _titles(current_cross_ids)
            edit.cross_publish_original_titles = _titles(orig.get('cross_publish_podcast_ids') or [])
            edit.cross_publish_suggested_titles = _titles(edit.suggested_data.get('cross_publish_podcast_ids') or [])

    return {
        'pending_edits': pending_edits,
        'network_podcast_titles': network_podcast_titles,
        'scoring_config': scoring_config(),
    }


@diagnostic_timer("3. Gather Merge Desk")
def gather_merge_desk(request, current_network):
    merge_view = request.GET.get('merge_view', 'orphans')
    merge_podcast_id = request.GET.get('merge_podcast_id', '')
    merge_q = request.GET.get('merge_q', '').strip()
    merge_reason = request.GET.get('merge_reason', '').strip()

    base_episodes = Episode.objects.filter(podcast__network=current_network).select_related('podcast')
    if merge_podcast_id:
        base_episodes = base_episodes.filter(podcast_id=merge_podcast_id)
    if merge_q:
        base_episodes = base_episodes.filter(
            Q(title__icontains=merge_q) | Q(guid_public__icontains=merge_q) | Q(guid_private__icontains=merge_q)
        )

    public_orphans = private_orphans = matched_episodes = None
    match_reasons = []

    if merge_view == 'orphans':
        pub_qs = base_episodes.filter(
            Q(guid_private__isnull=True) | Q(guid_private__exact='')
        ).exclude(
            Q(audio_url_public__isnull=True) | Q(audio_url_public__exact='')
        ).order_by('-pub_date')
        public_orphans = Paginator(pub_qs, 20).get_page(request.GET.get('pub_page', 1))

        priv_qs = base_episodes.filter(
            Q(guid_public__isnull=True) | Q(guid_public__exact='')
        ).exclude(
            Q(audio_url_subscriber__isnull=True) | Q(audio_url_subscriber__exact='')
        ).order_by('-pub_date')
        private_orphans = Paginator(priv_qs, 20).get_page(request.GET.get('priv_page', 1))

    elif merge_view == 'matched':
        matched_qs = base_episodes.exclude(
            Q(guid_public__isnull=True) | Q(guid_public__exact='')
        ).exclude(
            Q(guid_private__isnull=True) | Q(guid_private__exact='')
        )
        match_reasons = (
            Episode.objects.filter(podcast__network=current_network)
            .exclude(match_reason__isnull=True).exclude(match_reason__exact='')
            .values_list('match_reason', flat=True).distinct()
        )
        if merge_reason:
            matched_qs = matched_qs.filter(match_reason=merge_reason)
        matched_episodes = Paginator(matched_qs.order_by('-pub_date'), 20).get_page(request.GET.get('match_page', 1))

    return {
        'merge_view': merge_view,
        'merge_podcast_id': merge_podcast_id,
        'merge_q': merge_q,
        'merge_reason': merge_reason,
        'public_orphans': public_orphans,
        'private_orphans': private_orphans,
        'matched_episodes': matched_episodes,
        'match_reasons': match_reasons,
    }


def _audit_points(edit):
    """Net trust impact shown on the collapsed audit row: a rollback washes to
    0, a rejection is the flat penalty, approved/superseded keep their banked
    points. Reads only status + points, so a summary never has to touch
    suggested_data."""
    if edit.status == EpisodeEditSuggestion.Status.ROLLED_BACK:
        return 0
    if edit.status == EpisodeEditSuggestion.Status.REJECTED:
        return -REJECT_PENALTY
    return edit.points or 0


def annotate_audit_edit(edit, current_network):
    """Full annotation for ONE audit edit's before/after diff body, which the
    audit accordion loads on expand via creator_audit_edit. gather_audit_log
    deliberately does none of this: rendering every edit's diff (chapters, tags,
    description, speakers) up front was the entire weight of that tab."""
    edit.membership = NetworkMembership.objects.filter(
        user_id=edit.user_id, network=current_network
    ).first()
    # Trust after an exact-wash rollback (clamped at 0, mirroring _reverse_award),
    # shown right where the reviewer acts.
    if edit.membership is not None:
        edit.trust_after_revert = max(0, (edit.membership.trust_score or 0) - (edit.points or 0))

    _annotate_edit_changes(edit)

    # Section + bonus tally derived from the BANKED award (counter_deltas +
    # points) so it reconciles exactly with what Revert reverses (edit.points).
    cd = edit.counter_deltas or {}
    edit.pts_speaker = cd.get('edits_speakers') or 0
    edit.total_points = edit.points or 0
    if edit.total_points <= 0:
        # Legacy/unbanked edits reverse nothing, so don't attribute phantom
        # per-section points or a bonus (badges + breakdown are suppressed).
        edit.pts_title = edit.pts_desc = edit.pts_tags = edit.pts_chapters = 0
        edit.pts_season = edit.pts_epnum = edit.pts_eptype = edit.pts_speaker = 0
        edit.base_points = edit.sweep_bonus = edit.fr_bonus = 0
    else:
        edit.base_points = _section_base(edit)
        edit.fr_bonus = FIRST_RESPONDER_BONUS if 'first_responder_count' in cd else 0
        edit.sweep_bonus = max(0, edit.total_points - edit.base_points - edit.fr_bonus)
    edit.audit_points = _audit_points(edit)

    if edit.cross_publish_changed:
        titles = dict(current_network.podcasts.values_list('id', 'title'))
        _titles = lambda ids: [titles.get(i, f'#{i}') for i in ids]
        edit.cross_publish_original_titles = _titles((edit.original_data or {}).get('cross_publish_podcast_ids') or [])
        edit.cross_publish_suggested_titles = _titles(edit.suggested_data.get('cross_publish_podcast_ids') or [])
    return edit


@diagnostic_timer("4. Gather Audit Log")
def gather_audit_log(request, current_network):
    audit_query = EpisodeEditSuggestion.objects.filter(
        episode__podcast__network=current_network
    ).exclude(status=EpisodeEditSuggestion.Status.PENDING).select_related('episode', 'episode__podcast', 'user')

    audit_q = request.GET.get('audit_q', '').strip()
    audit_status = request.GET.get('audit_status', '').strip()
    audit_user = request.GET.get('audit_user', '').strip()

    if audit_q:
        audit_query = audit_query.filter(Q(episode__title__icontains=audit_q) | Q(episode__podcast__title__icontains=audit_q))
    if audit_status:
        audit_query = audit_query.filter(status=audit_status)
    if audit_user:
        audit_query = audit_query.filter(user__username__icontains=audit_user)

    # Ordering tiebreaker: resolved_at is null for legacy rows and can tie, and
    # an unstable order shuffles edits between pages.
    audit_page_obj = Paginator(
        audit_query.order_by('-resolved_at', '-id'), 20
    ).get_page(request.GET.get('audit_page', 1))
    # SUMMARIES ONLY — who / what / when / status / net trust. Each edit's
    # before/after diff loads on expand via creator_audit_edit (annotate_audit_edit),
    # so this render never touches suggested_data or queries memberships.
    for edit in audit_page_obj:
        edit.audit_points = _audit_points(edit)
    return {'audit_page_obj': audit_page_obj}


@diagnostic_timer("5. Gather Bulk Move Context")
def gather_move_context(request, current_network):
    source_pod_id = request.GET.get('source_pod_id', '')
    move_episodes = []
    if source_pod_id and source_pod_id.isdigit():
        move_episodes = Episode.objects.filter(
            podcast_id=source_pod_id, podcast__network=current_network
        ).order_by('-pub_date')
    return {'source_pod_id': source_pod_id, 'move_episodes': move_episodes}


@diagnostic_timer("5b. Gather Cross-Publish Context")
def gather_cross_publish_context(request, current_network):
    cross_source_id = request.GET.get('cross_source_id', '')
    cross_episodes = []
    if cross_source_id and cross_source_id.isdigit():
        cross_episodes = Episode.objects.filter(
            podcast_id=cross_source_id, podcast__network=current_network
        ).prefetch_related('cross_publications__podcast').order_by('-pub_date')
    cross_target_podcasts = current_network.podcasts.order_by('title')
    if cross_source_id and cross_source_id.isdigit():
        cross_target_podcasts = cross_target_podcasts.exclude(id=int(cross_source_id))
    return {
        'cross_source_id': cross_source_id,
        'cross_episodes': cross_episodes,
        'cross_target_podcasts': cross_target_podcasts,
    }


@diagnostic_timer("5c. Gather 404 Page Context")
def gather_notfound_context(current_network):
    return {'notfound_entries': current_network.notfound_entries.order_by('-created_at')}


@diagnostic_timer("6. Gather S3 Reports")
def gather_reports_data():
    txt_path = os.path.join(settings.MEDIA_ROOT, 's3_hosting_report.txt')
    csv_path = os.path.join(settings.MEDIA_ROOT, 's3_hosted_episodes.csv')
    return {
        'reports': {
            'txt_exists': os.path.exists(txt_path),
            'csv_exists': os.path.exists(csv_path),
            'txt_url': f"{settings.MEDIA_URL}s3_hosting_report.txt",
            'csv_url': f"{settings.MEDIA_URL}s3_hosted_episodes.csv",
        }
    }
