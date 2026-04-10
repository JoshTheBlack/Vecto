"""
This module contains all the view logic for the pod_manager application.
It handles user authentication via Patreon, serves user-facing pages,
generates dynamic RSS feeds, processes Patreon webhooks, and manages
a background task queue for importing podcast feeds.
"""
import hashlib
import hmac
import io
import json
import queue
import threading
import time
import urllib.parse
from datetime import timedelta

import requests
from django.conf import settings
from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.management import call_command
from django.core.paginator import Paginator
from django.db.models import Max
from django.http import JsonResponse, HttpResponse, HttpResponseForbidden, StreamingHttpResponse, Http404
from django.shortcuts import redirect, get_object_or_404, render
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
from podgen import Podcast as PodgenPodcast, Episode as PodgenEpisode, Media

from .models import PatronProfile, Podcast, Episode, Network, PatreonTier, UserMix

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def parse_duration(duration_str: str) -> timedelta | None:
    if not duration_str:
        return None
    try:
        if ':' in duration_str:
            parts = duration_str.split(':')
            if len(parts) == 3:
                return timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=int(parts[2]))
            elif len(parts) == 2:
                return timedelta(minutes=int(parts[0]), seconds=int(parts[1]))
        else:
            return timedelta(seconds=int(duration_str))
    except ValueError:
        return None
    
def get_active_pledge_amount(patreon_json: dict, campaign_id: str) -> int:
    if 'included' not in patreon_json or not campaign_id:
        return 0
        
    for item in patreon_json['included']:
        if item.get('type') == 'member':
            relationships = item.get('relationships', {})
            campaign_data = relationships.get('campaign', {}).get('data', {})
            
            if campaign_data and str(campaign_data.get('id')) == str(campaign_id):
                attributes = item.get('attributes', {})
                if attributes.get('patron_status') == 'active_patron':
                    return attributes.get('currently_entitled_amount_cents', 0)
    return 0

def invalidate_show_cache(show_id: int):
    version_key = f"podcast_cache_version_{show_id}"
    try:
        cache.incr(version_key)
    except ValueError:
        cache.set(version_key, 1, timeout=None)

# ==========================================
# OAUTH AUTHENTICATION
# ==========================================

def patreon_login(request):
    params = {
        'response_type': 'code',
        'client_id': settings.PATREON_CLIENT_ID,
        'redirect_uri': settings.PATREON_REDIRECT_URI,
        'scope': 'identity identity[email]', 
    }
    url = f"https://www.patreon.com/oauth2/authorize?{urllib.parse.urlencode(params, quote_via=urllib.parse.quote)}"
    return redirect(url)

def patreon_callback(request):
    code = request.GET.get('code')
    if not code:
        return JsonResponse({"error": "No code provided by Patreon."}, status=400)

    token_url = "https://www.patreon.com/api/oauth2/token"
    token_data = {
        'code': code,
        'grant_type': 'authorization_code',
        'client_id': settings.PATREON_CLIENT_ID,
        'client_secret': settings.PATREON_CLIENT_SECRET,
        'redirect_uri': settings.PATREON_REDIRECT_URI,
    }
    
    token_res = requests.post(token_url, data=token_data)
    tokens = token_res.json()

    if 'access_token' not in tokens:
        return JsonResponse({"error": "Failed to trade code for token.", "details": tokens}, status=400)

    access_token = tokens['access_token']

    user_url = (
        "https://www.patreon.com/api/oauth2/v2/identity"
        "?include=memberships,memberships.campaign"
        "&fields[user]=full_name,email"
        "&fields[member]=patron_status,currently_entitled_amount_cents"
    )
    headers = {'Authorization': f'Bearer {access_token}'}
    user_res = requests.get(user_url, headers=headers)
    user_data = user_res.json()

    user_info = user_data.get('data', {})
    patreon_id = user_info.get('id')
    if not patreon_id:
        return JsonResponse({"error": "Could not retrieve Patreon user ID from API response."}, status=400)

    attributes = user_info.get('attributes', {})
    raw_email = attributes.get('email')
    safe_username = raw_email if raw_email else patreon_id
    safe_email = raw_email if raw_email else ''
    full_name = attributes.get('full_name', '')

    # MULTI-NETWORK PLEDGE EXTRACTION
    active_pledges = {}
    for net in Network.objects.all():
        if net.patreon_campaign_id:
            cents = get_active_pledge_amount(user_data, str(net.patreon_campaign_id))
            active_pledges[str(net.patreon_campaign_id)] = cents

    user, created = User.objects.get_or_create(
        username=safe_username, 
        defaults={'email': safe_email, 'first_name': full_name}
    )

    profile, p_created = PatronProfile.objects.get_or_create(
        user=user, 
        defaults={'patreon_id': patreon_id}
    )
    profile.active_pledges = active_pledges
    profile.save()

    login(request, user)
    return redirect('user_feeds')

# ==========================================
# USER-FACING VIEWS
# ==========================================

@login_required(login_url='/login/')
def creator_settings(request):
    if request.user.is_superuser:
        allowed_networks = Network.objects.all()
    else:
        allowed_networks = Network.objects.filter(owners=request.user)
        
    if not allowed_networks.exists():
        return HttpResponseForbidden("You do not have creator access to any networks.")

    current_network_slug = request.GET.get('network')
    if current_network_slug:
        current_network = get_object_or_404(allowed_networks, slug=current_network_slug)
    else:
        current_network = allowed_networks.first()

    if request.method == 'POST':
        action = request.POST.get('action')

        if action == 'update_network':
            network_id = request.POST.get('network_id')
            network = get_object_or_404(allowed_networks, id=network_id)
            
            theme_config_str = request.POST.get('theme_config', '{}')
            footer_public = request.POST.get('footer_public', '')
            footer_private = request.POST.get('footer_private', '')

            try:
                clean_json_str = theme_config_str.replace("'", '"')
                network.theme_config = json.loads(clean_json_str)
            except json.JSONDecodeError:
                messages.error(request, f"Invalid JSON format for {network.name}. Settings not saved.")
                return redirect(f"{reverse('creator_settings')}?network={network.slug}")

            network.patreon_campaign_id = request.POST.get('patreon_campaign_id', '')
            network.website_url = request.POST.get('website_url', '')
            network.default_image_url = request.POST.get('default_image_url', '')
            network.ignored_title_tags = request.POST.get('ignored_title_tags', '')
            network.description_cut_triggers = request.POST.get('description_cut_triggers', '')

            network.url_patreon = request.POST.get('url_patreon', '')
            network.url_youtube = request.POST.get('url_youtube', '')
            network.url_twitch = request.POST.get('url_twitch', '')
            network.url_bluesky = request.POST.get('url_bluesky', '')
            network.url_twitter = request.POST.get('url_twitter', '')
            
            network.global_footer_public = footer_public
            network.global_footer_private = footer_private
            network.save()

            for show in network.podcasts.all():
                invalidate_show_cache(show.id)

            messages.success(request, f"{network.name} settings saved successfully! All related feed caches invalidated.")
            return redirect(f"{reverse('creator_settings')}?network={network.slug}")

        elif action == 'update_show':
            show_id = request.POST.get('show_id')
            show = get_object_or_404(Podcast, id=show_id, network__in=allowed_networks)
            
            # Capture the new URL fields
            show.public_feed_url = request.POST.get('public_feed_url', show.public_feed_url)
            show.subscriber_feed_url = request.POST.get('subscriber_feed_url', show.subscriber_feed_url)
            
            tier_id = request.POST.get('tier_id')
            show.show_footer_public = request.POST.get('show_footer_public', '')
            show.show_footer_private = request.POST.get('show_footer_private', '')

            if tier_id:
                show.required_tier_id = tier_id
            else:
                show.required_tier = None
                
            show.save()
            invalidate_show_cache(show.id)
            messages.success(request, f"{show.title} updated successfully! Feed cache invalidated.")
            return redirect(f"{reverse('creator_settings')}?network={show.network.slug}")

        elif action == 'add_show':
            network_id = request.POST.get('network_id')
            network = get_object_or_404(allowed_networks, id=network_id)
            title = request.POST.get('title')
            slug = request.POST.get('slug')
            public_feed_url = request.POST.get('public_feed_url')
            subscriber_feed_url = request.POST.get('subscriber_feed_url')
            tier_id = request.POST.get('tier_id')

            try:
                new_show = Podcast(
                    network=network, title=title, slug=slug,
                    public_feed_url=public_feed_url, subscriber_feed_url=subscriber_feed_url,
                )
                if tier_id: new_show.required_tier_id = tier_id
                new_show.save()
                
                out = io.StringIO()
                try:
                    call_command('ingest_feed', new_show.id, stdout=out, stderr=out)
                    messages.success(request, f"Show '{title}' added and feed successfully imported!")
                    messages.info(request, out.getvalue(), extra_tags="log")
                except Exception as e:
                    messages.warning(request, f"Show '{title}' added, but automatic import failed: {str(e)}")
                    if out.getvalue():
                        messages.error(request, out.getvalue(), extra_tags="log")
            except Exception as e:
                messages.error(request, f"Error adding show: {str(e)}")
                
            return redirect(f"{reverse('creator_settings')}?network={network.slug}")

    allowed_networks = allowed_networks.prefetch_related('podcasts', 'podcasts__required_tier')
    total_patrons = PatronProfile.objects.filter(pledge_amount_cents__gt=0).count()
    tiers = PatreonTier.objects.filter(network__in=allowed_networks).order_by('minimum_cents')
    
    context = {
        'networks': allowed_networks,
        'current_network': current_network,
        'total_patrons': total_patrons,
        'tiers': tiers,
        'theme_config_json': json.dumps(current_network.theme_config, indent=2) if current_network else "{}"
    }
    return render(request, 'pod_manager/creator_settings.html', context)

def home(request):
    podcasts = Podcast.objects.all().order_by('title')
    show_slug = request.GET.get('show')
    selected_network = request.GET.get('network', None)
    
    user_networks = Network.objects.filter(podcasts__in=podcasts).distinct()
    query = Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier')
    
    if selected_network:
        podcasts = podcasts.filter(network__slug=selected_network)
        query = query.filter(podcast__network__slug=selected_network)
    if show_slug:
        query = query.filter(podcast__slug=show_slug)
        
    all_episodes = query.order_by('-pub_date')
    paginator = Paginator(all_episodes, 20)
    
    try:
        page_number = int(request.GET.get('page', 1))
    except ValueError:
        page_number = 1
        
    page_obj = paginator.get_page(page_number)
    start_index = max(1, page_obj.number - 5)
    end_index = min(paginator.num_pages, page_obj.number + 5)
    custom_page_range = range(start_index, end_index + 1)
    
    user_active_pledges = {}
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        user_active_pledges = request.user.patron_profile.active_pledges or {}
        
    for ep in page_obj:
        req_cents = ep.podcast.required_tier.minimum_cents if ep.podcast.required_tier else 0
        camp_id = str(ep.podcast.network.patreon_campaign_id)
        user_cents = user_active_pledges.get(camp_id, 0)
        ep.user_has_access = (user_cents >= req_cents)

    current_network = Network.objects.filter(slug=selected_network).first() if selected_network else request.network

    context = {
        'episodes': page_obj,          
        'page_obj': page_obj,          
        'custom_page_range': custom_page_range, 
        'podcasts': podcasts,          
        'current_filter': show_slug,   
        'current_network': current_network,
        'user_networks': user_networks,
        'selected_network': selected_network,
    }
    return render(request, 'pod_manager/home.html', context)

def episode_detail(request, episode_id):
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier'), id=episode_id)

    user_active_pledges = {}
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        user_active_pledges = request.user.patron_profile.active_pledges or {}
        
    req_cents = ep.podcast.required_tier.minimum_cents if ep.podcast.required_tier else 0
    camp_id = str(ep.podcast.network.patreon_campaign_id)
    user_cents = user_active_pledges.get(camp_id, 0)
    ep.user_has_access = (user_cents >= req_cents)
    
    footer_parts = []
    if ep.user_has_access:
        if ep.podcast.show_footer_private: footer_parts.append(ep.podcast.show_footer_private)
        if ep.podcast.network.global_footer_private: footer_parts.append(ep.podcast.network.global_footer_private)
    else:
        if ep.podcast.show_footer_public: footer_parts.append(ep.podcast.show_footer_public)
        if ep.podcast.network.global_footer_public: footer_parts.append(ep.podcast.network.global_footer_public)

    ep.display_description = ep.clean_description
    if footer_parts:
        ep.display_description += "<br><br>" + "<br><br>".join(footer_parts)

    return render(request, 'pod_manager/episode_detail.html', {'ep': ep})

def user_feeds(request):
    network = request.network
    active_pledges = {}
    profile = None
    total_dollars = 0

    # Only fetch the profile and allow Mix manipulation if the user is logged in
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        profile = request.user.patron_profile
        active_pledges = profile.active_pledges or {}
        total_dollars = sum(active_pledges.values()) / 100 if active_pledges else 0

        # === HANDLE MIX CREATION / EDIT / DELETE (AUTHENTICATED ONLY) ===
        if request.method == 'POST' and 'create_mix' in request.POST:
            mix_name = request.POST.get('mix_name', '').strip()
            if not mix_name:
                mix_name = f"UserMix {UserMix.objects.filter(user=request.user).count() + 1}"
                
            selected_podcast_ids = request.POST.getlist('podcasts')
            if selected_podcast_ids:
                user_mix = UserMix.objects.create(
                    user=request.user, network=network, name=mix_name,
                    image_url=request.POST.get('mix_image', '').strip(),
                    image_upload=request.FILES.get('mix_image_upload') 
                )
                user_mix.selected_podcasts.set(selected_podcast_ids)
                messages.success(request, f"Mix '{mix_name}' created successfully!")
            else:
                messages.warning(request, "You must select at least one show to create a mix.")
            return redirect('user_feeds')

        if request.method == 'POST' and 'edit_mix' in request.POST:
            user_mix = UserMix.objects.filter(id=request.POST.get('mix_id'), user=request.user).first()
            if user_mix:
                mix_name = request.POST.get('mix_name', '').strip()
                user_mix.name = mix_name if mix_name else f"UserMix {UserMix.objects.filter(user=request.user).count() + 1}"
                user_mix.image_url = request.POST.get('mix_image', '').strip()
                
                if 'mix_image_upload' in request.FILES:
                    user_mix.image_upload = request.FILES['mix_image_upload']
                
                selected_podcast_ids = request.POST.getlist('podcasts')
                if selected_podcast_ids:
                    user_mix.selected_podcasts.set(selected_podcast_ids)
                    user_mix.save()
                    cache.delete(f"mix_feed_{user_mix.unique_id}")
                    messages.success(request, "Mix updated successfully!")
                else:
                    messages.warning(request, "You must select at least one show. Changes were not saved.")
            return redirect('user_feeds')

        if request.method == 'POST' and 'delete_mix' in request.POST:
            user_mix = UserMix.objects.filter(id=request.POST.get('mix_id'), user=request.user).first()
            if user_mix:
                cache.delete(f"mix_feed_{user_mix.unique_id}")
                user_mix.delete()
                messages.success(request, "Custom mix deleted.")
            return redirect('user_feeds')

    # === BUILD FEED DATA FOR ALL USERS ===
    feed_data = []
    available_podcasts = []
    
    for podcast in Podcast.objects.select_related('network', 'required_tier').all():
        req_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
        camp_id = str(podcast.network.patreon_campaign_id)
        user_cents = active_pledges.get(camp_id, 0)
        
        # They only get the private feed if they are logged in AND meet the threshold
        has_premium_access = (user_cents >= req_cents) and (req_cents > 0) and (profile is not None)
        
        if has_premium_access:
            base_feed_url = reverse('custom_feed', args=[podcast.network.slug])
            raw_url = f"{base_feed_url}?auth={profile.feed_token}&show={podcast.slug}"
            ui_has_access = True
        elif req_cents == 0:
            # It's a completely free show
            raw_url = reverse('public_feed', args=[podcast.slug])
            ui_has_access = True
        else:
            # They don't have access, give them the public unauthenticated feed
            raw_url = reverse('public_feed', args=[podcast.slug])
            ui_has_access = False
            
        feed_data.append({
            'podcast': podcast,
            'has_access': ui_has_access,
            'req_dollars': req_cents / 100,
            'feed_url': request.build_absolute_uri(raw_url)
        })
        
        available_podcasts.append({
            'podcast': podcast,
            'has_access': ui_has_access
        })

    # === GET CUSTOM MIXES (ONLY IF LOGGED IN) ===
    user_mixes = UserMix.objects.filter(user=request.user, network=network).prefetch_related('selected_podcasts') if request.user.is_authenticated else []
    mix_data = []
    for mix in user_mixes:
        raw_mix_url = f"/feed/{network.slug}/mix/{mix.unique_id}"
        mix_data.append({
            'mix': mix,
            'feed_url': request.build_absolute_uri(raw_mix_url)
        })

    context = {
        'profile': profile,
        'dollars': total_dollars,
        'feed_data': feed_data,
        'available_podcasts': available_podcasts,
        'mix_data': mix_data,
    }
    return render(request, 'pod_manager/user_feeds.html', context)


def generate_custom_feed(request, network_slug):
    feed_token = request.GET.get('auth')
    podcast_slug = request.GET.get('show')

    if not feed_token or not podcast_slug:
        return HttpResponseForbidden("Missing authentication or show parameters.")

    podcast = get_object_or_404(Podcast, slug=podcast_slug, network=request.network)

    try:
        profile = get_object_or_404(PatronProfile, feed_token=feed_token)
    except ValueError:
        return HttpResponseForbidden("Invalid authentication token format.")

    active_pledges = profile.active_pledges or {}
    required_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
    camp_id = str(podcast.network.patreon_campaign_id)
    user_cents = active_pledges.get(camp_id, 0)
    
    if user_cents < required_cents:
        return HttpResponseForbidden("Your current Patreon pledge does not grant access to this feed.")

    version = cache.get(f"podcast_cache_version_{podcast.id}", 1)
    cache_key = f"xml_feed_{version}_{profile.feed_token}_{podcast.slug}"
    xml_output = cache.get(cache_key)

    if not xml_output:
        episodes = podcast.episodes.all().order_by('-pub_date')
        
        p = PodgenPodcast(
            name=f"{podcast.title} (Custom Feed)",
            description=f"Premium ad-free feed for {profile.user.first_name}.",
            website=podcast.network.website_url or "https://example.com",
            explicit=True,
            image=podcast.image_url or podcast.network.default_image_url or "https://example.com/logo.png",
        )

        for ep in episodes:
            assembled_desc = ep.clean_description
            if podcast.show_footer_private:
                assembled_desc += f"<br><br>{podcast.show_footer_private}"
            if podcast.network.global_footer_private:
                assembled_desc += f"<br><br>{podcast.network.global_footer_private}"

            p.episodes.append(PodgenEpisode(
                title=ep.title,
                media=Media(ep.audio_url_subscriber,duration=parse_duration(ep.duration)),
                id=ep.guid,
                publication_date=ep.pub_date,
                summary=assembled_desc, 
            ))
            
        xml_output = p.rss_str()
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)

    return HttpResponse(xml_output, content_type='application/rss+xml')

def generate_public_feed(request, podcast_slug):
    podcast = get_object_or_404(Podcast, slug=podcast_slug)

    version = cache.get(f"podcast_cache_version_{podcast.id}", 1)
    cache_key = f"xml_feed_public_{version}_{podcast.slug}"
    xml_output = cache.get(cache_key)

    if not xml_output:
        episodes = podcast.episodes.all().order_by('-pub_date')
        
        p = PodgenPodcast(
            name=podcast.title,
            description=f"Public feed for {podcast.title}.",
            website=podcast.network.website_url or "https://example.com",
            explicit=True,
            image=podcast.image_url or podcast.network.default_image_url or "https://example.com/logo.png",
        )

        for ep in episodes:
            assembled_desc = ep.clean_description
            
            if podcast.show_footer_public:
                assembled_desc += f"<br><br>{podcast.show_footer_public}"
            if podcast.network.global_footer_public:
                assembled_desc += f"<br><br>{podcast.network.global_footer_public}"

            p.episodes.append(PodgenEpisode(
                title=ep.title,
                media=Media(ep.audio_url_public,duration=parse_duration(ep.duration)),
                id=ep.guid,
                publication_date=ep.pub_date,
                summary=assembled_desc, 
            ))
            
        xml_output = p.rss_str()
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)

    return HttpResponse(xml_output, content_type='application/rss+xml')

def generate_mix_feed(request, unique_id):
    cache_key = f"mix_feed_{unique_id}"
    feed_xml = cache.get(cache_key)

    if not feed_xml:
        user_mix = get_object_or_404(UserMix, unique_id=unique_id, network=request.network, is_active=True)
        
        profile = user_mix.user.patron_profile
        active_pledges = profile.active_pledges or {}
        
        selected_podcasts = user_mix.selected_podcasts.select_related('required_tier', 'network').all()
        
        access_map = {}
        for podcast in selected_podcasts:
            req_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
            camp_id = str(podcast.network.patreon_campaign_id)
            access_map[podcast.id] = (active_pledges.get(camp_id, 0) >= req_cents)
                
        episodes = Episode.objects.filter(podcast__in=selected_podcasts).select_related('podcast', 'podcast__network').order_by('-pub_date')[:100]
        
        if user_mix.display_image: image_url = request.build_absolute_uri(user_mix.display_image)
        else: image_url = request.build_absolute_uri(user_mix.network.default_image_url)

        feed = PodgenPodcast(
            name=user_mix.name,
            description=f"A custom blended podcast feed generated by Vecto for {user_mix.user.first_name}.",
            website=request.build_absolute_uri('/'),
            explicit=False,
            image=image_url,
            withhold_from_itunes=True
        )
        
        for ep in episodes:
            has_access = access_map.get(ep.podcast_id, False)
            
            if has_access and ep.audio_url_subscriber:
                audio_url = ep.audio_url_subscriber
            elif ep.audio_url_public:
                audio_url = ep.audio_url_public
            else:
                continue

            display_title = f"[{ep.podcast.title}] {ep.title}"
            
            assembled_desc = ep.clean_description or ep.raw_description
            footer_parts = []
            
            if has_access:
                if ep.podcast.show_footer_private: footer_parts.append(ep.podcast.show_footer_private)
                if ep.podcast.network.global_footer_private: footer_parts.append(ep.podcast.network.global_footer_private)
            else:
                if ep.podcast.show_footer_public: footer_parts.append(ep.podcast.show_footer_public)
                if ep.podcast.network.global_footer_public: footer_parts.append(ep.podcast.network.global_footer_public)
            
            if footer_parts:
                assembled_desc += "<br><br>" + "<br><br>".join(footer_parts)

            feed.episodes.append(PodgenEpisode(
                title=display_title,
                summary=assembled_desc, 
                publication_date=ep.pub_date,
                media=Media(url=audio_url, size=0, type="audio/mpeg")
            ))
            
        feed_xml = feed.rss_str()
        cache.set(cache_key, feed_xml, 300)
        
    return HttpResponse(feed_xml, content_type='application/rss+xml')

@csrf_exempt
def patreon_webhook(request):
    signature = request.headers.get('X-Patreon-Signature')
    if not signature:
        return HttpResponseForbidden("Missing signature")

    secret = settings.PATREON_WEBHOOK_SECRET.encode('utf-8')
    expected_sig = hmac.new(secret, request.body, hashlib.md5).hexdigest()

    if not hmac.compare_digest(expected_sig, signature):
        return HttpResponseForbidden("Invalid signature")

    try:
        data = json.loads(request.body)
        member_data = data.get('data', {})
        
        user_relationship = member_data.get('relationships', {}).get('user', {}).get('data', {})
        if not user_relationship or user_relationship.get('type') != 'user':
            return HttpResponse("Could not find user relationship in webhook.", status=400)
            
        patreon_user_id = user_relationship.get('id')
        if not patreon_user_id:
            return HttpResponse("Missing user ID in webhook.", status=400)

        profile = PatronProfile.objects.get(patreon_id=patreon_user_id)

        attributes = member_data.get('attributes', {})
        new_cents = attributes.get('currently_entitled_amount_cents', 0)
        status = attributes.get('patron_status') 

        final_amount = new_cents if status == 'active_patron' else 0
        
        campaign_relationship = member_data.get('relationships', {}).get('campaign', {}).get('data', {})
        campaign_id = str(campaign_relationship.get('id', ''))
        
        if campaign_id:
            active_pledges = profile.active_pledges or {}
            active_pledges[campaign_id] = final_amount
            profile.active_pledges = active_pledges
            profile.save()
            print(f"Webhook Success: Updated {profile.user.email} on Campaign {campaign_id} to {final_amount} cents.")
        
        return HttpResponse("Success", status=200)
        
    except PatronProfile.DoesNotExist:
        return HttpResponse("User has not logged in via OAuth yet.", status=200)

    except (ValueError, KeyError, AttributeError) as e:
        return HttpResponse("Error processing webhook payload.", status=400)

class CacheLogStream:
    def __init__(self, task_id):
        self.task_id = task_id
        cache.set(self.task_id, "data: Initiating Background Import...\n\n", timeout=3600)
        
    def write(self, text):
        if not text.strip(): return 
        current_log = cache.get(self.task_id, "")
        clean_line = text.replace('\n', '')
        cache.set(self.task_id, current_log + f"data: {clean_line}\n\n", timeout=3600)
        
    def flush(self):
        pass

feed_import_queue = queue.Queue()

def feed_import_worker():
    while True:
        show_id, task_id = feed_import_queue.get()
        stream = CacheLogStream(task_id)
        
        try:
            stream.write("\n[SYSTEM] Worker acquired task. Starting ingestion...\n")
            call_command('ingest_feed', show_id, stdout=stream, stderr=stream, no_color=True)
            invalidate_show_cache(show_id) 
        except Exception as e:
            stream.write(f"\n[ERROR] {str(e)}\n")
        finally:
            stream.write("[DONE]")
            feed_import_queue.task_done()

threading.Thread(target=feed_import_worker, daemon=True).start()

@login_required(login_url='/login/')
def stream_feed_import(request, show_id):
    task_id = f"import_logs_{show_id}"
    
    if not cache.get(task_id):
        cache.set(task_id, "data: [QUEUED] Waiting for database availability...\n\n", timeout=3600)
        feed_import_queue.put((show_id, task_id))

    def event_stream():
        last_length = 0
        while True:
            logs = cache.get(task_id, "")
            
            if len(logs) > last_length:
                new_logs = logs[last_length:]
                yield new_logs
                last_length = len(logs)
                
                if "[DONE]" in new_logs:
                    cache.delete(task_id)
                    break
                    
            time.sleep(0.5)

    return StreamingHttpResponse(event_stream(), content_type='text/event-stream')