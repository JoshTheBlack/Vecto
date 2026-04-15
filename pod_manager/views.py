"""
This module contains all the view logic for the pod_manager application.
It handles user authentication via Patreon, serves user-facing pages,
generates dynamic RSS feeds, processes Patreon webhooks, and manages
a background task queue for importing podcast feeds.
"""
import logging
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
from django.core.files.base import ContentFile
from django.core.management import call_command
from django.core.paginator import Paginator
from django.db.models import Max, Q
from django.http import JsonResponse, HttpResponse, HttpResponseForbidden, StreamingHttpResponse, Http404
from django.shortcuts import redirect, get_object_or_404, render
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
from podgen import Podcast as PodgenPodcast, Episode as PodgenEpisode, Media
from lxml import etree

from .models import PatronProfile, Podcast, Episode, Network, PatreonTier, UserMix

logger = logging.getLogger(__name__)
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

def inject_rss_categories(rss_str, episodes):
    """
    Takes the podgen generated RSS string and injects standard <category> tags 
    with CDATA blocks into the <item> elements for any episodes that have tags.
    """
    # Create a mapping of guid -> tags for fast lookup
    tag_map = {str(ep.guid): ep.tags for ep in episodes if ep.tags}
    
    if not tag_map:
        return rss_str

    # Parse the XML string using lxml
    root = etree.fromstring(rss_str.encode('utf-8'))
    
    # Iterate over all <item> elements
    for item in root.findall('.//item'):
        guid_elem = item.find('guid')
        if guid_elem is not None and guid_elem.text in tag_map:
            # Inject each tag as a standard <category> element with CDATA
            for tag in tag_map[guid_elem.text]:
                cat_elem = etree.SubElement(item, 'category')
                # Wrap the tag string in a CDATA block
                cat_elem.text = etree.CDATA(str(tag))
                
    # Return the modified XML as a string with the declaration intact
    return etree.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')

def process_mix_image_url(image_url, user_mix_instance):
    """Safely attempts to download an image from a URL and attach it to the model."""
    if not image_url:
        return None
    try:
        # 5 second timeout so the user isn't left hanging forever
        response = requests.get(image_url, timeout=5) 
        if response.status_code == 200:
            import os
            temp_name = os.path.basename(image_url).split('?')[0] or "cover.jpg"
            user_mix_instance.image_upload.save(temp_name, ContentFile(response.content), save=False)
            user_mix_instance.image_url = "" # Clear the URL so we rely on the uploaded file
            return None # None means no errors!
        else:
            return f"Could not download image. Server returned status {response.status_code}."
    except requests.exceptions.RequestException as e:
        return "Could not download image. The URL might be invalid or unreachable."

def refresh_patreon_token(network):
    """
    Trades a saved refresh_token for a new access_token.
    Returns True if successful, False if the refresh token is also invalid/expired.
    """
    logger.debug(f"--- [AUTO-HEAL] Triggering Token Refresh for {network.name} ---")
    
    if not network.patreon_creator_refresh_token:
        logger.error("[AUTO-HEAL] FAILED: No refresh token found in the database!")
        return False

    token_url = "https://www.patreon.com/api/oauth2/token"
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': network.patreon_creator_refresh_token,
        'client_id': settings.PATREON_CLIENT_ID,
        'client_secret': settings.PATREON_CLIENT_SECRET,
    }
    
    try:
        logger.debug("[AUTO-HEAL] Sending POST request to Patreon /api/oauth2/token...")
        res = requests.post(token_url, data=data, timeout=10)
        logger.debug(f"[AUTO-HEAL] Patreon Refresh Response Status: {res.status_code}")
        
        if res.status_code == 200:
            tokens = res.json()
            network.patreon_creator_access_token = tokens['access_token']
            
            if 'refresh_token' in tokens:
                network.patreon_creator_refresh_token = tokens['refresh_token']
                logger.debug("[AUTO-HEAL] Received both new access AND refresh tokens.")
            else:
                logger.warning("[AUTO-HEAL] Received new access token, but NO new refresh token.")
                
            network.save()
            logger.debug("[AUTO-HEAL] SUCCESS. Tokens saved to database.")
            return True
        else:
            logger.error(f"[AUTO-HEAL] FAILED. Patreon rejected the refresh request: {res.text}")
            return False
            
    except Exception as e:
        logger.error(f"[AUTO-HEAL] CRITICAL ERROR during refresh request: {str(e)}")
        return False
    
# ==========================================
# OAUTH AUTHENTICATION
# ==========================================

def patreon_login(request):
    network_id = request.GET.get('network_id')
    redirect_uri = request.build_absolute_uri(reverse('patreon_callback'))
    
    if network_id:
        scopes = 'identity identity[email] campaigns campaigns.members'
        state = f"network_{network_id}"
    else:
        scopes = 'identity identity[email]'
        state = "listener"
    
    params = {
        'response_type': 'code',
        'client_id': settings.PATREON_CLIENT_ID,
        'redirect_uri': redirect_uri,
        'scope': scopes,
        'state': state
    }
    url = f"https://www.patreon.com/oauth2/authorize?{urllib.parse.urlencode(params, quote_via=urllib.parse.quote)}"
    
    logger.info(f"[EVIDENCE] Generating Patreon Auth URL. State: {state} | Scopes: {scopes}")
    return redirect(url)

def patreon_callback(request):
    code = request.GET.get('code')
    state = request.GET.get('state', 'listener')
    if not code:
        return JsonResponse({"error": "No code provided by Patreon."}, status=400)
    
    redirect_uri = request.build_absolute_uri(request.path)
    token_url = "https://www.patreon.com/api/oauth2/token"
    token_data = {
        'code': code,
        'grant_type': 'authorization_code',
        'client_id': settings.PATREON_CLIENT_ID,
        'client_secret': settings.PATREON_CLIENT_SECRET,
        'redirect_uri': redirect_uri,
    }
    
    token_res = requests.post(token_url, data=token_data)
    tokens = token_res.json()

    if 'access_token' not in tokens:
        logger.error(f"[EVIDENCE] Token Exchange Failed: {tokens}")
        return JsonResponse({"error": "Failed to trade code for token.", "details": tokens}, status=400)

    # CRITICAL EVIDENCE: What scopes did we actually get?
    granted_scopes = tokens.get('scope', 'No scope field returned')
    logger.info(f"[EVIDENCE] Token Granted. Scopes: {granted_scopes}")

    access_token = tokens['access_token']
    refresh_token = tokens.get('refresh_token')

    # --- HANDLE CREATOR NETWORK LINKING ---
    if state.startswith("network_"):
        try:
            network_id = state.split("_")[1]
            network = get_object_or_404(Network, id=network_id)
            
            # Fetch campaign info to set the Campaign ID and save tokens
            camp_url = "https://www.patreon.com/api/oauth2/v2/campaigns"
            headers = {'Authorization': f'Bearer {access_token}'}
            camp_res = requests.get(camp_url, headers=headers).json()
            logger.info(f"[EVIDENCE] Campaign Discovery Response: {json.dumps(camp_res)[:500]}")
            
            if 'data' in camp_res and len(camp_res['data']) > 0:
                network.patreon_campaign_id = camp_res['data'][0]['id']
                network.patreon_creator_access_token = access_token
                network.patreon_creator_refresh_token = refresh_token
                network.patreon_sync_enabled = True
                network.save()
                logger.info(f"[EVIDENCE] Network {network.name} saved with Campaign ID {network.patreon_campaign_id}")
                messages.success(request, f"Successfully linked Patreon Campaign for {network.name}!")
            else:
                logger.warning("[EVIDENCE] No campaigns found in creator account.")
                messages.error(request, "No Patreon campaigns found for this account.")
                
            return redirect(f"{reverse('creator_settings')}?network={network.slug}")
        except Exception as e:
            return JsonResponse({"error": f"Failed to link network: {str(e)}"}, status=500)

    # --- REMAINING LISTENER LOGIN LOGIC ---
    user_url = (
        "https://www.patreon.com/api/oauth2/v2/identity"
        "?include=memberships,memberships.campaign"
        "&fields[user]=full_name,email"
        "&fields[member]=patron_status,currently_entitled_amount_cents"
    )
    headers = {'Authorization': f'Bearer {access_token}'}
    user_data = requests.get(user_url, headers=headers).json()

    user_info = user_data.get('data', {})
    patreon_id = user_info.get('id')
    
    attributes = user_info.get('attributes', {})
    raw_email = attributes.get('email')
    safe_username = raw_email if raw_email else patreon_id
    safe_email = raw_email if raw_email else ''
    full_name = attributes.get('full_name', '')

    user, _ = User.objects.get_or_create(
        username=safe_username, 
        defaults={'email': safe_email, 'first_name': full_name}
    )

    profile, _ = PatronProfile.objects.get_or_create(user=user, defaults={'patreon_id': patreon_id})
    
    active_pledges = {}
    for net in Network.objects.exclude(patreon_campaign_id=''):
        cents = get_active_pledge_amount(user_data, str(net.patreon_campaign_id))
        active_pledges[str(net.patreon_campaign_id)] = cents
    
    profile.active_pledges = active_pledges
    profile.save()

    login(request, user)
    return redirect('home')

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
                network.theme_config = json.loads(theme_config_str)
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

        elif action == 'run_manual_sync':
            network_id = request.POST.get('network_id')
            logger.info(f"Manual Sync requested for Network ID: {network_id} by User: {request.user.email}")
            
            network = get_object_or_404(allowed_networks, id=network_id)
            
            count, error = sync_network_patrons(network)
            if error:
                logger.error(f"Manual sync failed: {error}")
                messages.error(request, f"Sync Failed: {error}")
            else:
                logger.info(f"Manual sync success. {count} records updated.")
                messages.success(request, f"Successfully synced {count} patrons for {network.name}.")
            
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
    show_slug = request.GET.get('show')
    search_query = request.GET.get('q', '').strip()
    selected_networks = request.GET.getlist('network')
    
    # 1. Determine user's active pledged networks
    user_networks = Network.objects.none()
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        pledges = request.user.patron_profile.active_pledges or {}
        # Get slugs of networks where the user has an active pledge > $0
        active_campaign_ids = [cid for cid, amt in pledges.items() if amt > 0]
        user_networks = Network.objects.filter(patreon_campaign_id__in=active_campaign_ids).distinct()

    # 2. Base Queries
    podcasts = Podcast.objects.all().order_by('title')
    query = Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier')

    # 3. Determine Scope and Theme
    if selected_networks:
        if 'all' in selected_networks:
            # BLENDED MODE
            if user_networks.exists():
                query = query.filter(podcast__network__in=user_networks)
                podcasts = podcasts.filter(network__in=user_networks)
            selected_networks = []
            # Fallback to domain theme
            current_view_network = request.network
        else:
            # FILTERED MODE
            query = query.filter(podcast__network__slug__in=selected_networks)
            podcasts = podcasts.filter(network__slug__in=selected_networks)
            
            # THEME LOGIC:
            if len(selected_networks) == 1:
                # If exactly one is selected, use its specific theme
                current_view_network = get_object_or_404(Network, slug=selected_networks[0])
            else:
                # If multiple are selected, use the domain's default theme
                current_view_network = request.network
    else:
        # DEFAULT: Current domain's network
        query = query.filter(podcast__network=request.network)
        podcasts = podcasts.filter(network=request.network)
        selected_networks = [request.network.slug]
        current_view_network = request.network

    # 4. Apply Show Filter
    if show_slug:
        query = query.filter(podcast__slug=show_slug)

    # 4.5 Apply Search Filter
    if search_query:
        query = query.filter(
            Q(title__icontains=search_query) | 
            Q(raw_description__icontains=search_query) |
            Q(tags__icontains=search_query)
        )
        
    all_episodes = query.order_by('-pub_date')
    paginator = Paginator(all_episodes, 20)
    
    try:
        page_number = int(request.GET.get('page', 1))
    except ValueError:
        page_number = 1
        
    page_obj = paginator.get_page(page_number)
    
    # 5. Check Patreon Access per episode
    user_active_pledges = {}
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        user_active_pledges = request.user.patron_profile.active_pledges or {}

    for ep in page_obj:
        req_cents = ep.podcast.required_tier.minimum_cents if ep.podcast.required_tier else 0
        camp_id = str(ep.podcast.network.patreon_campaign_id)
        user_cents = user_active_pledges.get(camp_id, 0)
        ep.user_has_access = (user_cents >= req_cents)

    context = {
        'episodes': page_obj,          
        'page_obj': page_obj,          
        'custom_page_range': range(max(1, page_obj.number - 5), min(paginator.num_pages, page_obj.number + 5) + 1), 
        'podcasts': podcasts,          
        'current_filter': show_slug,   
        'current_network': current_view_network, 
        'user_networks': user_networks,
        'selected_networks': selected_networks,
        'search_query': search_query,
    }
    return render(request, 'pod_manager/home.html', context)

def episode_detail(request, episode_id):
    # 1. Fetch the episode without strictly filtering by request.network
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier'), id=episode_id)

    # 2. Determine user's pledge status
    user_active_pledges = {}
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        user_active_pledges = request.user.patron_profile.active_pledges or {}
        
    req_cents = ep.podcast.required_tier.minimum_cents if ep.podcast.required_tier else 0
    camp_id = str(ep.podcast.network.patreon_campaign_id)
    user_cents = user_active_pledges.get(camp_id, 0)
    ep.user_has_access = (user_cents >= req_cents)
    
    # 3. Cross-Network Security Check:
    # If the episode is from a different network than the current domain, 
    # ensure the user actually has access to it before rendering the page.
    if ep.podcast.network != request.network and not ep.user_has_access:
        raise Http404("No Episode matches the given query.")
    
    # 4. Build the footer and description
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
    show_slug = request.GET.get('show')
    selected_networks = request.GET.getlist('network') #
    
    profile = None
    active_pledges = {}
    total_dollars = 0
    user_networks = Network.objects.none()

    # 1. Gather authenticated data
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        profile = request.user.patron_profile
        active_pledges = profile.active_pledges or {}
        total_dollars = sum(active_pledges.values()) / 100 if active_pledges else 0
        
        # Identify all networks this user supports
        active_campaign_ids = [cid for cid, amt in active_pledges.items() if amt > 0]
        user_networks = Network.objects.filter(patreon_campaign_id__in=active_campaign_ids).distinct()

        # === HANDLE MIX POST ACTIONS ===
        if request.method == 'POST' and 'create_mix' in request.POST:
            mix_name = request.POST.get('mix_name', '').strip() or f"UserMix {UserMix.objects.filter(user=request.user).count() + 1}"
            selected_podcast_ids = request.POST.getlist('podcasts')
            image_url_input = request.POST.get('mix_image', '').strip()
            
            if selected_podcast_ids:
                # Create the instance in memory (don't save to DB yet)
                user_mix = UserMix(user=request.user, network=request.network, name=mix_name)
                
                # Prioritize file upload, fallback to URL
                if 'mix_image_upload' in request.FILES:
                    user_mix.image_upload = request.FILES['mix_image_upload']
                elif image_url_input:
                    error_msg = process_mix_image_url(image_url_input, user_mix)
                    if error_msg:
                        messages.warning(request, f"Mix created, but artwork failed: {error_msg}")
                
                # Save to DB and attach ManyToMany relationships
                user_mix.save()
                user_mix.selected_podcasts.set(selected_podcast_ids)
                if 'mix_image_upload' in request.FILES or (image_url_input and not user_mix.image_url):
                    messages.success(request, f"Mix '{mix_name}' created with custom artwork!")
                else:
                    messages.success(request, f"Mix '{mix_name}' created!")
            return redirect('user_feeds')

        if request.method == 'POST' and 'edit_mix' in request.POST:
            user_mix = UserMix.objects.filter(id=request.POST.get('mix_id'), user=request.user).first()
            if user_mix:
                user_mix.name = request.POST.get('mix_name', '').strip() or user_mix.name
                image_url_input = request.POST.get('mix_image', '').strip()
                
                # Prioritize file upload, fallback to URL
                if 'mix_image_upload' in request.FILES:
                    user_mix.image_upload = request.FILES['mix_image_upload']
                elif image_url_input and image_url_input != user_mix.image_url:
                    error_msg = process_mix_image_url(image_url_input, user_mix)
                    if error_msg:
                        messages.warning(request, f"Mix updated, but new artwork failed: {error_msg}")

                selected_ids = request.POST.getlist('podcasts')
                if selected_ids: 
                    user_mix.selected_podcasts.set(selected_ids)
                    
                user_mix.save()
                cache.delete(f"mix_feed_{user_mix.unique_id}")
                messages.success(request, f"Mix '{user_mix.name}' updated successfully!")
            return redirect('user_feeds')

        if request.method == 'POST' and 'delete_mix' in request.POST:
            user_mix = UserMix.objects.filter(id=request.POST.get('mix_id'), user=request.user).first()
            if user_mix:
                cache.delete(f"mix_feed_{user_mix.unique_id}")
                user_mix.delete()
            return redirect('user_feeds')

    # 2. Determine target networks for display
    if selected_networks:
        target_networks = user_networks if 'all' in selected_networks else user_networks.filter(slug__in=selected_networks)
    else:
        target_networks = [request.network]

    # 3. Build Feed Data (Shows public feeds for guests)
    feed_data = []
    for podcast in Podcast.objects.filter(network__in=target_networks).select_related('network', 'required_tier'):
        req_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
        camp_id = str(podcast.network.patreon_campaign_id)
        user_cents = active_pledges.get(camp_id, 0)
        has_access = (user_cents >= req_cents) and (req_cents > 0) and (profile is not None)
        
        # visibility logic: show if has access or if a public fallback exists
        if has_access:
            raw_url = reverse('custom_feed') + f"?auth={profile.feed_token}&show={podcast.slug}&network={podcast.network.slug}"
            feed_data.append({'podcast': podcast, 'has_access': True, 'feed_url': request.build_absolute_uri(raw_url)})
        elif req_cents == 0 or podcast.public_feed_url:
            raw_url = reverse('public_feed', args=[podcast.slug]) + f"?network={podcast.network.slug}"
            feed_data.append({'podcast': podcast, 'has_access': False, 'req_dollars': req_cents/100, 'feed_url': request.build_absolute_uri(raw_url)})

    # Fix modal podcast list (include all supported networks)
    available_podcasts = []
    if request.user.is_authenticated:
        for p in Podcast.objects.filter(network__in=user_networks).select_related('network', 'required_tier'):
            available_podcasts.append({'podcast': p, 'has_access': (active_pledges.get(str(p.network.patreon_campaign_id), 0) >= (p.required_tier.minimum_cents if p.required_tier else 0))})

    user_mixes = UserMix.objects.filter(user=request.user, network=request.network) if request.user.is_authenticated else []
    mix_data = [{'mix': m, 'feed_url': request.build_absolute_uri(reverse('mix_feed', args=[m.unique_id]))} for m in user_mixes]

    context = {
        'profile': profile, 'dollars': total_dollars, 'feed_data': feed_data,
        'available_podcasts': available_podcasts, 'mix_data': mix_data,
        'user_networks': user_networks, 'selected_networks': selected_networks or [request.network.slug],
        'current_network': request.network,
    }
    return render(request, 'pod_manager/user_feeds.html', context)

def generate_custom_feed(request):
    feed_token = request.GET.get('auth')
    podcast_slug = request.GET.get('show')
    network_slug = request.GET.get('network')

    if not feed_token or not podcast_slug:
        return HttpResponseForbidden("Missing authentication or show parameters.")

    # Use network__slug (double underscore) to lookup by the string parameter
    if network_slug:
        podcast = get_object_or_404(Podcast, slug=podcast_slug, network__slug=network_slug)
    else:
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
            if not ep.audio_url_subscriber:
                continue

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
            
        raw_xml = p.rss_str()
        xml_output = inject_rss_categories(raw_xml, episodes)
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)

    return HttpResponse(xml_output, content_type='application/rss+xml')

def generate_public_feed(request, podcast_slug):
    network_slug = request.GET.get('network')
    
    if network_slug:
        podcast = get_object_or_404(Podcast, slug=podcast_slug, network__slug=network_slug)
    else:
        podcast = get_object_or_404(Podcast, slug=podcast_slug, network=request.network)

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
            if not ep.audio_url_public:
                continue
                
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
            
        raw_xml = p.rss_str()
        xml_output = inject_rss_categories(raw_xml, episodes)
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)

    return HttpResponse(xml_output, content_type='application/rss+xml')

def generate_mix_feed(request, unique_id):
    cache_key = f"mix_feed_{unique_id}"
    feed_xml = cache.get(cache_key)

    if not feed_xml:
        user_mix = get_object_or_404(UserMix, unique_id=unique_id, is_active=True)
        
        profile = user_mix.user.patron_profile
        active_pledges = profile.active_pledges or {}
        
        selected_podcasts = user_mix.selected_podcasts.select_related('required_tier', 'network').all()
        
        access_map = {}
        for podcast in selected_podcasts:
            req_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
            camp_id = str(podcast.network.patreon_campaign_id)
            access_map[podcast.id] = (active_pledges.get(camp_id, 0) >= req_cents)
                
        episodes = Episode.objects.filter(podcast__in=selected_podcasts).select_related('podcast', 'podcast__network').order_by('-pub_date')[:5000]
        
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
            
        raw_xml = feed.rss_str()
        xml_output = inject_rss_categories(raw_xml, episodes)
        cache.set(cache_key, xml_output, 300)
        
    return HttpResponse(xml_output, content_type='application/rss+xml')

@csrf_exempt
def patreon_webhook(request):
    if request.method != 'POST':
        return HttpResponse(status=405)

    signature = request.headers.get('X-Patreon-Signature')
    body = request.body

    logger.info("--- Incoming Patreon Webhook ---")
    logger.info(f"Headers: {request.headers}")
    logger.info(f"Signature Provided: {signature}")

    webhook_secret = settings.PATREON_WEBHOOK_SECRET.encode('utf-8')
    if not webhook_secret:
        logger.error("PATREON_WEBHOOK_SECRET is not set in environment variables!")
        return HttpResponse("Internal Setup Error", status=500)
    
    expected_signature = hmac.new(webhook_secret.encode('utf-8'), body, digestmod=hashlib.md5).hexdigest()

    logger.info(f"Expected Signature: {expected_signature}")

    if not signature or not hmac.compare_digest(signature, expected_signature):
        logger.warning("Invalid Webhook Signature detected.")
        return HttpResponseForbidden("Invalid Signature")

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
            logger.info(f"Webhook Success: Updated {profile.user.email} on Campaign {campaign_id} to {final_amount} cents.")
        
        logger.info("Webhook processed successfully.")
        return HttpResponse("Success", status=200)
        
    except PatronProfile.DoesNotExist as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return HttpResponse("User has not logged in via OAuth yet.", status=200)

    except (ValueError, KeyError, AttributeError) as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return HttpResponse("Error processing webhook payload.", status=400)
    
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return HttpResponse("Error", status=500)

def sync_network_patrons(network):
    """
    Fetches all members for a campaign and updates local PatronProfiles.
    Also revokes access for local patrons no longer present in the Patreon member list.
    """
    logger.debug(f"--- Starting COMPLETE Sync for Network: {network.name} ---")

    if not network.patreon_creator_access_token or not network.patreon_campaign_id:
        return 0, "Network is not properly linked to Patreon."

    campaign_id_str = str(network.patreon_campaign_id)
    base_url = f"https://www.patreon.com/api/oauth2/v2/campaigns/{campaign_id_str}/members"
    params = {
        "include": "user",
        "fields[member]": "patron_status,currently_entitled_amount_cents",
        "fields[user]": "email",
        "page[count]": 100
    }
    headers = {'Authorization': f'Bearer {network.patreon_creator_access_token}'}
    
    updated_count = 0
    seen_patreon_ids = set() # Track everyone currently on Patreon for this campaign
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    # 1. Iterate through all active/historical members on Patreon
    while url:
        logger.debug(f"[SYNC] Fetching Patreon URL: {url}")
        res = requests.get(url, headers=headers)
        
        # --- THE DIAGNOSTIC LOG ---
        logger.debug(f"[SYNC] Raw HTTP Status Code received: {res.status_code}")
        
        # --- Handle 401 Unauthorized (Expired Token) ---
        # We also check if the text '401' is in the response just in case Patreon sends a 403
        if res.status_code == 401 or "Unauthorized" in res.text:
            logger.warning(f"[SYNC] Access Token rejected! Attempting auto-refresh...")
            
            refresh_success = refresh_patreon_token(network)
            logger.debug(f"[SYNC] Auto-refresh returned: {refresh_success}")
            
            if refresh_success:
                logger.debug("[SYNC] Applying new access token to headers and retrying the same URL...")
                headers['Authorization'] = f'Bearer {network.patreon_creator_access_token}'
                continue 
            else:
                logger.error("[SYNC] Refresh failed. Aborting sync.")
                network.patreon_sync_enabled = False
                network.save()
                return updated_count, "Patreon authorization permanently expired. Please re-link your Patreon campaign in settings."
        
        # --- Handle 429 Rate Limits Gracefully ---
        if res.status_code == 429:
            retry_after = int(res.headers.get('Retry-After', 5))
            logger.warning(f"[SYNC] Rate Limit hit. Pausing for {retry_after}s.")
            time.sleep(retry_after)
            continue 
            
        if res.status_code != 200:
            logger.error(f"[SYNC] Unhandled Patreon API Error ({res.status_code}): {res.text}")
            return updated_count, f"Patreon API Error: {res.text}"
        
        data = res.json()
        members = data.get('data', [])
        included = {i['id']: i for i in data.get('included', []) if i['type'] == 'user'}

        for member in members:
            attrs = member.get('attributes', {})
            rel_user = member.get('relationships', {}).get('user', {}).get('data', {})
            
            if not rel_user:
                continue
                
            patreon_id = rel_user['id']
            seen_patreon_ids.add(patreon_id) # Mark as present
            
            user_data = included.get(patreon_id, {})
            email = user_data.get('attributes', {}).get('email')

            # Match local profile
            profile = PatronProfile.objects.filter(patreon_id=patreon_id).first()
            if not profile and email:
                profile = PatronProfile.objects.filter(user__email=email).first()

            if profile:
                status = attrs.get('patron_status')
                cents = attrs.get('currently_entitled_amount_cents', 0)
                final_amount = cents if status == 'active_patron' else 0
                
                active_pledges = profile.active_pledges or {}
                active_pledges[campaign_id_str] = final_amount
                profile.active_pledges = active_pledges
                profile.save()
                updated_count += 1

        url = data.get('links', {}).get('next')
        
        if url:
            time.sleep(0.5)

    # 2. Cleanup: Revoke access for anyone NOT seen in the API response
    # Find local profiles that currently have this campaign ID in their JSON data
    stale_profiles = PatronProfile.objects.filter(
        active_pledges__has_key=campaign_id_str
    ).exclude(
        patreon_id__in=seen_patreon_ids
    )

    revoked_count = 0
    for profile in stale_profiles:
        active_pledges = profile.active_pledges or {}
        if active_pledges.get(campaign_id_str, 0) > 0:
            logger.info(f"Revoking access for {profile.user.email}: Not found in Patreon member list.")
            active_pledges[campaign_id_str] = 0
            profile.active_pledges = active_pledges
            profile.save()
            revoked_count += 1

    logger.info(f"--- Sync Complete. Updated: {updated_count} | Revoked: {revoked_count} ---")
    return updated_count, None

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