"""
Listener-facing views: home/episode browser, feed listing, episode detail,
and the per-tenant user profile dashboard.
"""
import logging

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.core.paginator import Paginator
from django.db.models import Q
from django.http import Http404
from django.shortcuts import redirect, get_object_or_404, render
from django.urls import reverse
from django.utils import timezone

from ..models import (
    PatronProfile, NetworkMembership, Podcast, Episode, NetworkMix, UserMix,
    EpisodeEditSuggestion,
)
from ..services.access import _evaluate_access, _build_episode_description, _evaluate_mix_access
from ..services.analytics import get_live_user_stats
from ..utils import validate_public_url

logger = logging.getLogger(__name__)


def _get_user_networks(user):
    """Returns all networks the user has access to via membership or ownership, deduplicated."""
    if not user.is_authenticated:
        return []
    membership_networks = [m.network for m in user.network_memberships.select_related('network')]
    seen = {n.id for n in membership_networks}
    owned = [n for n in user.owned_networks.all() if n.id not in seen]
    return membership_networks + owned


def _build_feed_base_url(podcast, request):
    """Returns the base URL for a podcast's feed endpoint.

    Uses the current request's domain for podcasts on request.network.
    Falls back to the podcast network's custom_domain for cross-network podcasts.
    Returns None if no domain is resolvable.
    """
    if podcast.network_id == request.network.id:
        return request.build_absolute_uri('/')[:-1]
    if podcast.network.custom_domain:
        return f"{request.scheme}://{podcast.network.custom_domain}"
    return None


def home(request):
    show_slug = request.GET.get('show')
    search_query = request.GET.get('q', '').strip()
    older_than = request.GET.get('older_than', '').strip()
    newer_than = request.GET.get('newer_than', '').strip()

    tenant_profile = getattr(request, 'tenant_profile', None)

    user_networks = _get_user_networks(request.user)
    selected_networks = request.GET.getlist('network')

    # Determine which networks to query based on UI selection and user access
    if 'all' in selected_networks:
        target_network_slugs = [n.slug for n in user_networks] if user_networks else [request.network.slug]
    elif selected_networks:
        valid_slugs = {n.slug for n in user_networks}
        target_network_slugs = [slug for slug in selected_networks if slug in valid_slugs] or [request.network.slug]
    else:
        # Default to the current network only
        target_network_slugs = [request.network.slug]
        selected_networks = [request.network.slug]

    query = Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier').filter(podcast__network__slug__in=target_network_slugs)
    podcasts = Podcast.objects.filter(network__slug__in=target_network_slugs).order_by('title')

    # Apply Podcast & Text Filters
    if show_slug:
        query = query.filter(podcast__slug=show_slug)
    if search_query:
        query = query.filter(Q(title__icontains=search_query) | Q(clean_description__icontains=search_query))

    # Apply Date Filters (Stackable and Optional)
    from django.utils.dateparse import parse_date
    if newer_than:
        parsed_newer = parse_date(newer_than)
        if parsed_newer:
            query = query.filter(pub_date__gt=parsed_newer)

    if older_than:
        parsed_older = parse_date(older_than)
        if parsed_older:
            query = query.filter(pub_date__lt=parsed_older)

    page_obj = Paginator(query.order_by('-pub_date'), 20).get_page(request.GET.get('page', 1))

    page_number = page_obj.number
    total_pages = page_obj.paginator.num_pages
    start_page = max(1, page_number - 3)
    end_page = min(total_pages, page_number + 3)
    custom_page_range = range(start_page, end_page + 1)

    # Pre-fetch access data once for all episodes on this page
    if request.user.is_authenticated:
        page_network_ids = {ep.podcast.network_id for ep in page_obj}
        home_memberships = {
            m.network_id: m
            for m in request.user.network_memberships.filter(network_id__in=page_network_ids)
        }
        home_owned_ids = set(request.user.owned_networks.values_list('id', flat=True))
    else:
        home_memberships = {}
        home_owned_ids = set()

    for ep in page_obj:
        ep.user_has_access, _ = _evaluate_access(
            request.user, ep.podcast, ep.podcast.network,
            membership=home_memberships.get(ep.podcast.network_id),
            is_owner=ep.podcast.network_id in home_owned_ids,
        )

    context = {
        'episodes': page_obj, 'page_obj': page_obj, 'podcasts': podcasts,
        'current_filter': show_slug, 'current_network': request.network,
        'search_query': search_query, 'tenant_profile': tenant_profile,
        'older_than': older_than,
        'newer_than': newer_than,
        'custom_page_range': custom_page_range,
        'user_networks': user_networks,
        'selected_networks': selected_networks,
    }
    return render(request, 'pod_manager/home.html', context)


def user_feeds(request):
    tenant_profile = getattr(request, 'tenant_profile', None)
    profile = getattr(request.user, 'patron_profile', None) if request.user.is_authenticated else None

    # --- 1. HANDLE POST ACTIONS (CREATE, EDIT, DELETE MIX) ---
    if request.method == 'POST':
        if not request.user.is_authenticated:
            return redirect('patreon_login')

        if request.POST.get('create_mix'):
            mix_name = request.POST.get('mix_name', '').strip() or f"{request.user.first_name}'s Custom Mix"
            raw_image_url = request.POST.get('mix_image', '').strip()
            if raw_image_url:
                ok, reason = validate_public_url(raw_image_url)
                if not ok:
                    messages.warning(request, f"Mix image URL ignored: {reason}")
                    raw_image_url = ''
            mix = UserMix.objects.create(
                user=request.user,
                network=request.network,
                name=mix_name,
                image_url=raw_image_url,
            )
            if 'mix_image_upload' in request.FILES:
                mix.image_upload = request.FILES['mix_image_upload']

            mix.selected_podcasts.set(request.POST.getlist('podcasts'))
            mix.save()
            messages.success(request, f"Mix '{mix.name}' created successfully!")

        elif request.POST.get('edit_mix'):
            mix = get_object_or_404(UserMix, id=request.POST.get('mix_id'), user=request.user, network=request.network)
            mix.name = request.POST.get('mix_name', '').strip() or mix.name

            if 'mix_image_upload' in request.FILES and request.FILES['mix_image_upload']:
                if mix.image_upload:
                    mix.image_upload.delete(save=False)
                mix.image_upload = request.FILES['mix_image_upload']
                mix.image_url = ""
            elif request.POST.get('mix_image'):
                raw_image_url = request.POST.get('mix_image').strip()
                ok, reason = validate_public_url(raw_image_url)
                if not ok:
                    messages.warning(request, f"Mix image URL ignored: {reason}")
                else:
                    mix.image_url = raw_image_url
                    if mix.image_upload:
                        mix.image_upload.delete(save=False)

            cache.delete(f"shell_user_mix_{mix.id}")
            mix.selected_podcasts.set(request.POST.getlist('podcasts'))
            mix.save()

            messages.success(request, "Mix updated successfully!")

        elif request.POST.get('delete_mix'):
            mix = get_object_or_404(UserMix, id=request.POST.get('mix_id'), user=request.user, network=request.network)
            cache.delete(f"shell_user_mix_{mix.id}")

            if mix.image_upload:
                mix.image_upload.delete(save=False)

            mix.delete()
            messages.warning(request, "Custom mix deleted.")

        network_qs = '&'.join(f'network={s}' for s in request.GET.getlist('network'))
        redirect_url = reverse('user_feeds')
        if network_qs:
            redirect_url += f'?{network_qs}'
        return redirect(redirect_url)

    # --- 2. GENERATE GET DATA ---
    feed_data = []
    available_podcasts = []

    user_networks = _get_user_networks(request.user)
    selected_networks = request.GET.getlist('network')

    if 'all' in selected_networks:
        target_network_slugs = [n.slug for n in user_networks] if user_networks else [request.network.slug]
    elif selected_networks:
        valid_slugs = {n.slug for n in user_networks}
        target_network_slugs = [slug for slug in selected_networks if slug in valid_slugs] or [request.network.slug]
    else:
        # Default to the current network only
        target_network_slugs = [request.network.slug]
        selected_networks = [request.network.slug]

    # Pre-fetch access data to avoid N+1 in the mix and podcast loops
    if request.user.is_authenticated:
        memberships_by_network = {
            m.network_id: m
            for m in request.user.network_memberships.filter(
                network__slug__in=target_network_slugs
            ).select_related('network')
        }
        owned_network_ids = set(request.user.owned_networks.values_list('id', flat=True))
    else:
        memberships_by_network = {}
        owned_network_ids = set()

    # PROCESS NETWORK MIXES FIRST (So they group at the top of the UI)
    network_mixes = NetworkMix.objects.filter(network__slug__in=target_network_slugs).select_related('network', 'required_tier')
    for mix in network_mixes:
        mix.has_access = _evaluate_mix_access(
            request.user, mix,
            membership=memberships_by_network.get(mix.network_id),
            is_owner=mix.network_id in owned_network_ids,
        )
        mix.feed_url = (
            request.build_absolute_uri(reverse('network_mix_feed', args=[mix.network.slug, mix.slug]))
            + (f"?auth={profile.feed_token}" if profile else "")
        )
        feed_data.append({
            'is_network_mix': True,
            'mix': mix,
            'has_access': mix.has_access,
            'feed_url': mix.feed_url,
        })

    # PROCESS STANDARD PODCASTS
    for podcast in Podcast.objects.filter(network__slug__in=target_network_slugs).select_related('network', 'required_tier'):
        has_access, _ = _evaluate_access(
            request.user, podcast, podcast.network,
            membership=memberships_by_network.get(podcast.network_id),
            is_owner=podcast.network_id in owned_network_ids,
        )
        available_podcasts.append({'podcast': podcast, 'has_access': has_access})

        feed_base = _build_feed_base_url(podcast, request)
        if feed_base is None:
            continue

        if profile is not None:
            raw_url = reverse('custom_feed') + f"?auth={profile.feed_token}&show={podcast.slug}"
            feed_data.append({'is_network_mix': False, 'podcast': podcast, 'has_access': has_access, 'feed_url': feed_base + raw_url})
        elif not podcast.required_tier or podcast.public_feed_url:
            raw_url = reverse('public_feed', args=[podcast.slug])
            feed_data.append({'is_network_mix': False, 'podcast': podcast, 'has_access': False, 'feed_url': feed_base + raw_url})

    user_mixes = UserMix.objects.filter(user=request.user, network__slug__in=target_network_slugs, is_active=True).prefetch_related('selected_podcasts') if request.user.is_authenticated else []

    context = {
        'profile': profile,
        'tenant_profile': tenant_profile,
        'feed_data': feed_data,
        'user_mixes': user_mixes,
        'current_network': request.network,
        'available_podcasts': available_podcasts,
        'user_networks': user_networks,
        'selected_networks': selected_networks,
    }
    return render(request, 'pod_manager/user_feeds.html', context)


def episode_detail(request, episode_id):
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network'), id=episode_id)
    ep.user_has_access, _ = _evaluate_access(request.user, ep.podcast, ep.podcast.network)

    if ep.podcast.network != request.network and not ep.user_has_access:
        raise Http404("No Episode matches the given query.")

    ep.display_description = _build_episode_description(ep, ep.user_has_access)
    ep.raw_audio_url = ep.audio_url_subscriber if (ep.user_has_access and ep.audio_url_subscriber) else ep.audio_url_public
    return render(request, 'pod_manager/episode_detail.html', {'ep': ep})


@login_required(login_url='/login/')
def user_profile(request):
    tenant_profile = getattr(request, 'tenant_profile', None)

    current_membership = NetworkMembership.objects.filter(user=request.user, network=request.network).first()

    has_active_totp = request.user.totpdevice_set.filter(confirmed=True).exists()

    patron_profile = getattr(request.user, 'patron_profile', None)
    totp_mode = patron_profile.totp_mode if patron_profile else 'replace'

    if not tenant_profile:
        return render(request, 'pod_manager/user_profile.html', {
            'level': 0, 'title': "Commoner", 'progress_percent': 0,
            'total_approved': 0, 'account_vintage': None,
            'has_active_totp': has_active_totp,
            'totp_mode': totp_mode,
            'membership': current_membership,
            'live_stats': {'playback_hits': 0, 'hours_accessed': 0.0, 'streak_days': 0, 'streak_weeks': 0, 'obsession_title': "Wandering Adventurer"}
        })

    account_vintage = tenant_profile.patreon_join_date
    joined_after_launch_days = None
    account_age_years = None

    if account_vintage:
        account_age_years = (timezone.now() - account_vintage).days / 365.25
        if request.network.patreon_campaign_created_at:
            delta = account_vintage - request.network.patreon_campaign_created_at
            joined_after_launch_days = max(0, delta.days)

    total_approved = EpisodeEditSuggestion.objects.filter(user=request.user, episode__podcast__network=request.network, status='approved').count()

    level, title, next_level_goal, progress_percent = 0, "Commoner", 1, 0
    if total_approved >= 1000:
        level, title, next_level_goal, progress_percent = 5, "Keeper of the Tome", 1000, 100
    elif total_approved >= 500:
        level, title, next_level_goal, progress_percent = 4, "Grand Archivist", 1000, (total_approved / 1000) * 100
    elif total_approved >= 100:
        level, title, next_level_goal, progress_percent = 3, "Archivist", 500, (total_approved / 500) * 100
    elif total_approved >= 25:
        level, title, next_level_goal, progress_percent = 2, "Scout", 100, (total_approved / 100) * 100
    elif total_approved >= 1:
        level, title, next_level_goal, progress_percent = 1, "Initiate", 25, (total_approved / 25) * 100

    context = {
        'profile': tenant_profile, 'total_approved': total_approved,
        'level': level, 'title': title, 'next_level_goal': next_level_goal,
        'progress_percent': min(progress_percent, 100),
        'live_stats': get_live_user_stats(tenant_profile),
        'account_vintage': account_vintage,
        'joined_after_launch_days': joined_after_launch_days,
        'account_age_years': account_age_years,
        'has_active_totp': has_active_totp,
        'totp_mode': totp_mode,
        'membership': current_membership,
    }
    return render(request, 'pod_manager/user_profile.html', context)
