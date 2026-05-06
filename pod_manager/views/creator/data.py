"""
GET-side data-gather functions for creator_settings tabs.

Each function is decorated with @diagnostic_timer and returns a partial
context dict. creator_settings merges them with ** unpacking.
"""
import logging
import os

from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import Q, Case, When, CharField, Max, Count
from django.db.models.functions import Substr, Lower

from ...models import EpisodeEditSuggestion, NetworkMembership, Episode
from ...utils import diagnostic_timer

logger = logging.getLogger(__name__)


@diagnostic_timer("1. Gather Manage Podcasts")
def gather_manage_podcasts(current_network):
    podcasts = current_network.podcasts.annotate(
        clean_title=Case(When(title__istartswith='The ', then=Substr('title', 5)), default='title', output_field=CharField()),
        latest_episode_date=Max('episodes__pub_date'),
        episode_count=Count('episodes', distinct=True),
    ).order_by(Lower('clean_title'))
    return {'manage_podcasts': podcasts, 'network_podcasts': podcasts}


@diagnostic_timer("2. Gather Inbox")
def gather_inbox(current_network):
    pending_edits = EpisodeEditSuggestion.objects.filter(
        episode__podcast__network=current_network, status=EpisodeEditSuggestion.Status.PENDING
    ).select_related('episode', 'episode__podcast', 'user')

    user_ids = [e.user_id for e in pending_edits]
    memberships = {m.user_id: m for m in NetworkMembership.objects.filter(user_id__in=user_ids, network=current_network)}

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
        edit.chapters_conflict = current_chapters != (orig.get('chapters') or [])
        edit.current_chapters = current_chapters

        current_desc = ep.clean_description or ''
        edit.desc_conflict = current_desc != (orig.get('description') or '')
        edit.current_description = current_desc

    return {'pending_edits': pending_edits}


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

    audit_page_obj = Paginator(audit_query.order_by('-resolved_at'), 20).get_page(request.GET.get('audit_page', 1))
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
