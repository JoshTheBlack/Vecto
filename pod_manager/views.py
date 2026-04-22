"""
This module contains all the view logic for the pod_manager application.
It handles user authentication via Patreon, serves user-facing pages,
generates dynamic RSS feeds, processes Patreon webhooks, and manages
a background task queue for importing podcast feeds.
"""
import logging
import hashlib
import hmac
import json
import asyncio
import threading
import time
import urllib.parse
import warnings
from datetime import timedelta

import requests
from django.conf import settings
from django.contrib import messages
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.files.base import ContentFile
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Q, F, Case, When, Value, CharField, Max, Count
from django.db.models.functions import Substr, Lower
from django.http import JsonResponse, HttpResponse, HttpResponseForbidden, StreamingHttpResponse, Http404, HttpResponseRedirect
from django.shortcuts import redirect, get_object_or_404, render
from django.urls import reverse
from django.utils import timezone
from datetime import timedelta
from django.views.decorators.csrf import csrf_exempt
from podgen import Podcast as PodgenPodcast, Episode as PodgenEpisode, Media, Person, Category
from lxml import etree

from .models import PatronProfile, Podcast, Episode, Network, PatreonTier, UserMix, NetworkMix
from .tasks import task_ingest_feed

warnings.filterwarnings("ignore", message=".*Image URL must end with.*")
warnings.filterwarnings("ignore", message=".*Size is set to 0.*")

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
            sec = int(float(parts[-1])) # Safely handle float seconds
            if len(parts) == 3:
                return timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=sec)
            elif len(parts) == 2:
                return timedelta(minutes=int(parts[0]), seconds=sec)
        else:
            return timedelta(seconds=int(float(duration_str)))
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

def inject_rss_extensions(request, rss_str, episodes, access_map=None, default_feed_type='private'):
    """
    Injects standard <category> tags and <podcast:chapters> tags.
    access_map is used for mix feeds to know which version of chapters to serve.
    """
    # Matches the exact ID assigned in the PodgenEpisode generators below
    tag_map = {str(ep.guid_public or ep.guid_private or ep.id): ep for ep in episodes}
    if not tag_map:
        return rss_str

    root = etree.fromstring(rss_str.encode('utf-8'))
    podcast_ns = "https://podcastindex.org/namespace/1.0"
    etree.register_namespace('podcast', podcast_ns)

    for item in root.findall('.//item'):
        guid_elem = item.find('guid')
        if guid_elem is not None and guid_elem.text in tag_map:
            ep = tag_map[guid_elem.text]
            
            if ep.tags:
                for tag in ep.tags:
                    cat_elem = etree.SubElement(item, 'category')
                    cat_elem.text = etree.CDATA(str(tag))
            
            if access_map is not None:
                has_access = access_map.get(ep.podcast_id, False)
                feed_type = 'private' if has_access else 'public'
            else:
                feed_type = default_feed_type
                
            has_chapters = bool(ep.chapters_private or ep.chapters_public)
            
            # Inject Chapters ONLY if data exists
            if has_chapters:
                chapter_url = request.build_absolute_uri(reverse('episode_chapters', args=[ep.id, feed_type]))
                chap_elem = etree.SubElement(item, f'{{{podcast_ns}}}chapters')
                chap_elem.set('url', chapter_url)
                chap_elem.set('type', 'application/json')
                
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
    logger.info(f"Initiating Patreon OAuth login. Target network ID: {network_id}")
    
    # Dynamically build the redirect URI based on the current tenant's domain
    dynamic_redirect_uri = request.build_absolute_uri('/oauth/patreon/callback')
    
    # Use creator scopes if linking a network, otherwise use basic listener scopes
    if network_id:
        scope = "identity identity[email] identity.memberships campaigns campaigns.members campaigns.members[email]"
    else:
        scope = "identity identity[email] identity.memberships"
    
    base_url = "https://www.patreon.com/oauth2/authorize"
    params = {
        "response_type": "code",
        "client_id": settings.PATREON_CLIENT_ID,
        "redirect_uri": dynamic_redirect_uri,
        "scope": scope,
    }
    
    if network_id:
        params["state"] = network_id
        
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    logger.debug(f"Redirecting user to Patreon OAuth URL: {url}")
    return redirect(url)

def patreon_callback(request):
    code = request.GET.get('code')
    state_network_id = request.GET.get('state')
    logger.info(f"Received Patreon OAuth callback. Code present: {bool(code)}, State: {state_network_id}")

    if not code:
        logger.warning("Patreon callback failed: No authorization code provided.")
        return HttpResponse("No code provided by Patreon", status=400)

    dynamic_redirect_uri = request.build_absolute_uri('/oauth/patreon/callback')

    token_url = "https://www.patreon.com/api/oauth2/token"
    data = {
        "code": code,
        "grant_type": "authorization_code",
        "client_id": settings.PATREON_CLIENT_ID,
        "client_secret": settings.PATREON_CLIENT_SECRET,
        "redirect_uri": dynamic_redirect_uri,
    }

    try:
        logger.debug("Attempting to exchange authorization code for access token...")
        res = requests.post(token_url, data=data, timeout=10)
        if res.status_code != 200:
            logger.error(f"Patreon token exchange failed: {res.text}")
            return HttpResponse(f"Failed to get token: {res.text}", status=400)
            
        token_data = res.json()
        access_token = token_data['access_token']
        refresh_token = token_data['refresh_token']
        logger.debug("Token exchange successful.")
        
        # If this was initiated from the Creator Settings page to link a campaign
        if state_network_id and request.user.is_authenticated:
            logger.info(f"Linking Patreon Campaign to Network ID {state_network_id} for user {request.user.username}")
            network = get_object_or_404(Network, id=state_network_id)
            
            headers = {"Authorization": f"Bearer {access_token}"}
            camp_res = requests.get("https://www.patreon.com/api/oauth2/v2/campaigns", headers=headers, timeout=10)
            
            if camp_res.status_code == 200:
                camp_data = camp_res.json().get('data', [])
                if camp_data:
                    campaign_id = camp_data[0]['id']
                    network.patreon_campaign_id = campaign_id
                    network.patreon_sync_enabled = True
                    network.patreon_creator_access_token = access_token
                    network.patreon_creator_refresh_token = refresh_token
                    network.save()
                    
                    messages.success(request, f"Successfully linked Patreon Campaign {campaign_id}!")
                    logger.info(f"Successfully linked Campaign ID {campaign_id} to Network {network.name}")
                    
                    # Fire off an initial sync
                    threading.Thread(target=sync_network_patrons, args=(network,), daemon=True).start()
                else:
                    logger.warning("Patreon linked, but no campaigns were found for this creator.")
                    messages.warning(request, "Linked, but no campaigns found on your Patreon account.")
            else:
                logger.error(f"Failed to fetch campaigns during linking: {camp_res.text}")
                messages.error(request, "Failed to fetch your campaigns from Patreon.")
                
            return redirect('creator_settings')

        # Normal User Login Flow
        logger.debug("Fetching user identity and memberships from Patreon...")
        headers = {"Authorization": f"Bearer {access_token}"}
        
        # Include memberships and campaign relationships to get pledge amounts
        identity_url = (
            "https://www.patreon.com/api/oauth2/v2/identity"
            "?include=memberships.campaign"
            "&fields[user]=email,first_name,last_name"
            "&fields[member]=patron_status,currently_entitled_amount_cents"
        )
        user_res = requests.get(identity_url, headers=headers, timeout=10)
        
        if user_res.status_code != 200:
            logger.error(f"Failed to fetch user identity: {user_res.text}")
            return HttpResponse("Failed to fetch user info", status=400)
            
        payload = user_res.json()
        user_data = payload.get('data', {})
        included_data = payload.get('included', [])
        
        patreon_id = user_data.get('id')
        email = user_data.get('attributes', {}).get('email')
        first_name = user_data.get('attributes', {}).get('first_name', '')
        last_name = user_data.get('attributes', {}).get('last_name', '')

        if not email:
            logger.error("Patreon identity response did not include an email address.")
            return HttpResponse("Patreon did not provide an email address.", status=400)

        # Get or Create User
        user, created = User.objects.get_or_create(username=email, defaults={
            'email': email,
            'first_name': first_name,
            'last_name': last_name
        })
        
        if created:
            logger.info(f"Created new local User account for {email}")

        # Get or Create Patron Profile
        profile, p_created = PatronProfile.objects.get_or_create(user=user, defaults={
            'patreon_id': patreon_id
        })
        
        if not p_created and profile.patreon_id != patreon_id:
            logger.warning(f"Updating existing Patreon ID for {email} from {profile.patreon_id} to {patreon_id}")
            profile.patreon_id = patreon_id

        # --- INSTANT PLEDGE SYNC LOGIC ---
        active_pledges = profile.active_pledges or {}
        
        for item in included_data:
            if item.get('type') == 'member':
                attrs = item.get('attributes', {})
                rels = item.get('relationships', {})
                
                status = attrs.get('patron_status')
                cents = attrs.get('currently_entitled_amount_cents', 0)
                
                campaign_data = rels.get('campaign', {}).get('data', {})
                if campaign_data:
                    campaign_id = str(campaign_data.get('id'))
                    # If active, set the cents. Otherwise, set to 0 to revoke.
                    active_pledges[campaign_id] = cents if status == 'active_patron' else 0

        profile.active_pledges = active_pledges
        profile.last_active = timezone.now()
        profile.save()
        # ---------------------------------

        login(request, user)
        logger.info(f"User {email} successfully logged in via Patreon and pledges synced.")
        
        return redirect('home')

    except Exception as e:
        logger.error(f"Critical error during Patreon callback: {str(e)}", exc_info=True)
        return HttpResponse(f"Error: {str(e)}", status=500)

def logout_view(request):
    logger.info(f"User {request.user.username if request.user.is_authenticated else 'Anonymous'} logged out.")
    from django.contrib.auth import logout
    logout(request)
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
            if tier_id:
                # Force a check to ensure this tier belongs to THIS network
                valid_tier = get_object_or_404(PatreonTier, id=tier_id, network=network)
                show.required_tier = valid_tier
            else:
                show.required_tier = None
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
                
                if tier_id:
                    valid_tier = get_object_or_404(PatreonTier, id=tier_id, network=network)
                    new_show.required_tier = valid_tier
                else:
                    new_show.required_tier = None
                    
                new_show.save()
                
                # Redirect with an 'auto_import' flag in the URL!
                messages.success(request, f"Show '{title}' added! Starting live ingestion...")
                return redirect(f"{reverse('creator_settings')}?network={network.slug}&auto_import={new_show.id}")
                
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

        # --- NETWORK MIX: CREATE ---
        elif action == 'add_network_mix':
            # BUG FIX: Explicitly fetch the target network from the form, NOT request.network
            target_network_id = request.POST.get('network_id')
            target_network = get_object_or_404(Network, id=target_network_id)

            name = request.POST.get('name')
            slug = request.POST.get('slug')
            tier_id = request.POST.get('tier_id')
            image_url_input = request.POST.get('mix_image', '').strip()
            podcast_ids = request.POST.getlist('podcasts')

            try:
                new_mix = NetworkMix(network=target_network, name=name, slug=slug)
                if tier_id:
                    new_mix.required_tier = get_object_or_404(PatreonTier, id=tier_id, network=target_network)

                if 'mix_image_upload' in request.FILES:
                    new_mix.image_upload = request.FILES['mix_image_upload']
                elif image_url_input:
                    error_msg = process_mix_image_url(image_url_input, new_mix)
                    if error_msg:
                        messages.warning(request, f"Mix created, but artwork failed: {error_msg}")

                new_mix.save()
                if podcast_ids:
                    new_mix.selected_podcasts.set(podcast_ids)
                messages.success(request, f"Network Mix '{name}' created successfully!")
            except Exception as e:
                messages.error(request, f"Error adding mix: {str(e)}")
            return redirect(f"{reverse('creator_settings')}?network={target_network.slug}")

        # --- NETWORK MIX: EDIT ---
        elif action == 'edit_network_mix':
            mix_id = request.POST.get('mix_id')
            target_mix = get_object_or_404(NetworkMix, id=mix_id)
            
            target_mix.name = request.POST.get('name')
            target_mix.slug = request.POST.get('slug')
            
            tier_id = request.POST.get('tier_id')
            if tier_id:
                target_mix.required_tier = get_object_or_404(PatreonTier, id=tier_id, network=target_mix.network)
            else:
                target_mix.required_tier = None
                
            image_url_input = request.POST.get('mix_image', '').strip()
            
            if 'mix_image_upload' in request.FILES:
                target_mix.image_upload = request.FILES['mix_image_upload']
            elif image_url_input and image_url_input != target_mix.image_url:
                error_msg = process_mix_image_url(image_url_input, target_mix)
                if error_msg:
                    messages.warning(request, f"Mix updated, but new artwork failed: {error_msg}")

            podcast_ids = request.POST.getlist('podcasts')
            target_mix.selected_podcasts.set(podcast_ids)
            
            target_mix.save()
            messages.success(request, f"Network Mix '{target_mix.name}' updated successfully!")
            return redirect(f"{reverse('creator_settings')}?network={target_mix.network.slug}")

        # --- NETWORK MIX: DELETE ---
        elif action == 'delete_network_mix':
            mix_id = request.POST.get('mix_id')
            target_mix = get_object_or_404(NetworkMix, id=mix_id)
            net_slug = target_mix.network.slug
            
            mix_name = target_mix.name
            target_mix.delete()
            messages.success(request, f"Network Mix '{mix_name}' deleted.")
            return redirect(f"{reverse('creator_settings')}?network={net_slug}")

        # --- MERGE DESK: MERGE EPISODES ---
        elif action == 'merge_episodes':
            pub_id = request.POST.get('public_episode_id')
            priv_id = request.POST.get('private_episode_id')

            if pub_id and priv_id:
                try:
                    pub_ep = Episode.objects.get(id=pub_id, podcast__network=current_network)
                    priv_ep = Episode.objects.get(id=priv_id, podcast__network=current_network)
                    podcast_id = request.POST.get('merge_podcast_id', '')

                    # Stitch the Private data into the Public episode
                    pub_ep.guid_private = priv_ep.guid_private or priv_ep.guid_public
                    pub_ep.audio_url_subscriber = priv_ep.audio_url_subscriber
                    if priv_ep.chapters_private:
                        pub_ep.chapters_private = priv_ep.chapters_private
                        
                    # Inherit tags if the private feed scraped them but public missed them
                    if priv_ep.tags and not pub_ep.tags:
                        pub_ep.tags = priv_ep.tags

                    pub_ep.match_reason = "Manual Merge (Merge Desk)"
                    
                    pub_ep.save()
                    priv_ep.delete()

                    messages.success(request, f"Successfully merged '{priv_ep.title}' into '{pub_ep.title}'.")
                except Episode.DoesNotExist:
                    messages.error(request, "One or both episodes could not be found.")
                except Exception as e:
                    messages.error(request, f"Merge failed: {str(e)}")
            else:
                messages.error(request, "You must select one episode from each column to merge.")
            return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&merge_view=orphans&merge_podcast_id={podcast_id}")

        # --- MERGE DESK: SPLIT EPISODES ---
        elif action == 'split_episode':
            ep_id = request.POST.get('episode_id')
            podcast_id = request.POST.get('merge_podcast_id', '')
            try:
                ep = Episode.objects.get(id=ep_id, podcast__network=current_network)

                # 1. Create the Private Orphan clone
                priv_ep = Episode(
                    podcast=ep.podcast,
                    title=ep.title,
                    pub_date=ep.pub_date,
                    raw_description=ep.raw_description,
                    clean_description=ep.clean_description,
                    duration=ep.duration,
                    link=ep.link,
                    tags=ep.tags,
                    guid_private=ep.guid_private,
                    audio_url_subscriber=ep.audio_url_subscriber,
                    chapters_private=ep.chapters_private,
                    match_reason="Manually Unpaired"
                )
                priv_ep.save()

                # 2. Strip private data from original (making it a Public Orphan)
                ep.guid_private = None
                ep.audio_url_subscriber = ""
                ep.chapters_private = None
                ep.match_reason = "Manually Unpaired"
                ep.save()

                messages.success(request, f"Successfully split '{ep.title}' into two orphaned episodes.")
            except Episode.DoesNotExist:
                messages.error(request, "Episode not found.")
            return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&merge_view=matched&merge_podcast_id={podcast_id}")

    allowed_networks = allowed_networks.prefetch_related('podcasts', 'podcasts__required_tier')
    total_patrons = 0
    if current_network and current_network.patreon_campaign_id:
        thirty_days_ago = timezone.now() - timedelta(days=30)
        kwargs = {
            "active_pledges__has_key": str(current_network.patreon_campaign_id),
            "last_active__gte": thirty_days_ago
        }
        total_patrons = PatronProfile.objects.filter(**kwargs).count()
    tiers = PatreonTier.objects.filter(network__in=allowed_networks).order_by('minimum_cents')
    
    allowed_networks = allowed_networks.prefetch_related('podcasts', 'podcasts__required_tier')
    total_patrons = 0
    if current_network and current_network.patreon_campaign_id:
        thirty_days_ago = timezone.now() - timedelta(days=30)
        kwargs = {
            "active_pledges__has_key": str(current_network.patreon_campaign_id),
            "last_active__gte": thirty_days_ago
        }
        total_patrons = PatronProfile.objects.filter(**kwargs).count()
    tiers = PatreonTier.objects.filter(network__in=allowed_networks).order_by('minimum_cents')
    
    # =========================================================
    # MANAGE PODCASTS ENGINE (Search, Filter, Sort)
    # =========================================================
    show_q = request.GET.get('show_q', '').strip()
    show_sort = request.GET.get('show_sort', 'alpha')
    show_mix = request.GET.get('show_mix', '')

    # 1. Annotate the query with clean sorting titles, max dates, and episode counts
    # (Using istartswith instead of regex ensures it runs perfectly on both Postgres AND your local SQLite!)
    manage_podcasts = current_network.podcasts.annotate(
        clean_title=Case(
            When(title__istartswith='The ', then=Substr('title', 5)),
            When(title__istartswith='A ', then=Substr('title', 3)),
            When(title__istartswith='An ', then=Substr('title', 4)),
            default='title',
            output_field=CharField()
        ),
        latest_episode_date=Max('episodes__pub_date'),
        episode_count=Count('episodes', distinct=True)
    )

    # 2. Apply Text Search Filter
    if show_q:
        manage_podcasts = manage_podcasts.filter(
            Q(title__icontains=show_q) | Q(slug__icontains=show_q)
        )

    # 3. Apply Super Mix Filter
    if show_mix:
        try:
            mix = current_network.mixes.get(id=show_mix)
            manage_podcasts = manage_podcasts.filter(id__in=mix.selected_podcasts.all())
        except Exception:
            pass

    # 4. Apply Smart Sorting
    if show_sort == 'recent':
        manage_podcasts = manage_podcasts.order_by(F('latest_episode_date').desc(nulls_last=True))
    elif show_sort == 'oldest':
        manage_podcasts = manage_podcasts.order_by(F('latest_episode_date').asc(nulls_last=True))
    elif show_sort == 'count_desc':
        manage_podcasts = manage_podcasts.order_by('-episode_count')
    else:
        # Default to Alpha (A-Z) utilizing the stripped 'clean_title'
        manage_podcasts = manage_podcasts.order_by(Lower('clean_title'))

    # =========================================================
    # THE MERGE DESK ENGINE (Search, Filter, Paginate)
    # =========================================================
    
    merge_view = request.GET.get('merge_view', 'orphans')
    merge_q = request.GET.get('merge_q', '').strip()
    merge_reason = request.GET.get('merge_reason', '')
    merge_podcast_id = request.GET.get('merge_podcast_id')

    # 1. Determine the Selected Podcast (Default to first in network)
    network_podcasts = current_network.podcasts.all()
    selected_podcast = None
    if merge_podcast_id:
        selected_podcast = network_podcasts.filter(id=merge_podcast_id).first()
    
    if not selected_podcast and network_podcasts.exists():
        selected_podcast = network_podcasts.first()
        
    merge_podcast_id = str(selected_podcast.id) if selected_podcast else ''

    # 2. Base Query (ISOLATED TO SINGLE PODCAST)
    if selected_podcast:
        base_episodes = Episode.objects.filter(podcast=selected_podcast).select_related('podcast').order_by('-pub_date')
    else:
        base_episodes = Episode.objects.none()

    # Apply Deep Search
    if merge_q:
        search_q = (
            Q(title__icontains=merge_q) |
            Q(guid_public__icontains=merge_q) |
            Q(guid_private__icontains=merge_q) |
            Q(audio_url_public__icontains=merge_q) |
            Q(audio_url_subscriber__icontains=merge_q)
        )
        base_episodes = base_episodes.filter(search_q)

    # ---------------------------------------------------------
    # ORPHAN LOGIC (Matches the boolean properties in models.py)
    # ---------------------------------------------------------
    empty_pub = Q(audio_url_public__isnull=True) | Q(audio_url_public__exact='') | Q(audio_url_public__iexact='none')
    empty_sub = Q(audio_url_subscriber__isnull=True) | Q(audio_url_subscriber__exact='') | Q(audio_url_subscriber__iexact='none') | Q(audio_url_subscriber=F('audio_url_public'))

    # 1. Public Orphans: Has distinct Public audio, NO distinct Premium audio
    public_orphans_qs = base_episodes.exclude(empty_pub).filter(empty_sub)

    # 2. Private Orphans: Has distinct Premium audio, NO Public audio
    private_orphans_qs = base_episodes.exclude(empty_sub).filter(empty_pub)

    # 3. Matched: Has BOTH distinct audio files
    matched_qs = base_episodes.exclude(empty_pub).exclude(empty_sub)
    
    if merge_reason:
        matched_qs = matched_qs.filter(match_reason=merge_reason)

    # 3. Get distinct Match Reasons for the dropdown filter (ISOLATED TO PODCAST)
    match_reasons = []
    if selected_podcast:
        match_reasons = Episode.objects.filter(podcast=selected_podcast)\
            .exclude(match_reason__in=['Public Only (No Match)', 'Private Exclusive', '', 'Manually Unpaired'])\
            .exclude(match_reason__isnull=True)\
            .values_list('match_reason', flat=True).distinct()

    # 4. Paginate Results (50 per page to keep the UI snappy)
    pub_page = request.GET.get('pub_page', 1)
    priv_page = request.GET.get('priv_page', 1)
    match_page = request.GET.get('match_page', 1)

    public_orphans = Paginator(public_orphans_qs, 50).get_page(pub_page)
    private_orphans = Paginator(private_orphans_qs, 50).get_page(priv_page)
    matched_episodes = Paginator(matched_qs, 50).get_page(match_page)

    context = {
        'networks': allowed_networks,
        'current_network': current_network,
        'total_patrons': total_patrons,
        'tiers': tiers,
        'theme_config_json': json.dumps(current_network.theme_config, indent=2) if current_network else "{}",
        # Manage Podcasts Context
        'manage_podcasts': manage_podcasts,
        'show_q': show_q,
        'show_sort': show_sort,
        'show_mix': show_mix,
        # Merge Desk Context
        'merge_view': merge_view,
        'merge_q': merge_q,
        'merge_reason': merge_reason,
        'match_reasons': match_reasons,
        'public_orphans': public_orphans,
        'private_orphans': private_orphans,
        'matched_episodes': matched_episodes,
        'network_podcasts': network_podcasts,
        'merge_podcast_id': merge_podcast_id,
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
    logger.debug(f"Episode detail requested: ID {episode_id}")
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier'), id=episode_id)

    user_active_pledges = {}
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        user_active_pledges = request.user.patron_profile.active_pledges or {}
        
    req_cents = ep.podcast.required_tier.minimum_cents if ep.podcast.required_tier else 0
    camp_id = str(ep.podcast.network.patreon_campaign_id)
    user_cents = user_active_pledges.get(camp_id, 0)
    ep.user_has_access = (user_cents >= req_cents)
    
    if ep.podcast.network != request.network and not ep.user_has_access:
        logger.warning(f"Cross-network block: User {request.user.username} attempted to view episode {episode_id} from a different network without access.")
        raise Http404("No Episode matches the given query.")
    
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

def episode_chapters(request, episode_id, feed_type):
    ep = get_object_or_404(Episode, id=episode_id)
    
    if feed_type == 'public':
        data = ep.chapters_public or ep.chapters_private
    else:
        data = ep.chapters_private or ep.chapters_public
        
    if not data:
        raise Http404("Chapters not found.")
        
    response = JsonResponse(data)
    response["Access-Control-Allow-Origin"] = "*"
    return response

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

        if not profile.last_active or profile.last_active < timezone.now() - timedelta(hours=24):
            profile.last_active = timezone.now()
            profile.save(update_fields=['last_active'])

        active_pledges = profile.active_pledges or {}
        total_dollars = sum(active_pledges.values()) / 100 if active_pledges else 0
        
        # Identify all networks this user supports
        active_campaign_ids = [cid for cid, amt in active_pledges.items() if amt > 0]
        user_networks = Network.objects.filter(patreon_campaign_id__in=active_campaign_ids).distinct()

        # === HANDLE MIX POST ACTIONS ===
        if request.method == 'POST' and 'create_mix' in request.POST:
            mix_name = request.POST.get('mix_name', '').strip() or f"UserMix {UserMix.objects.filter(user=request.user).count() + 1}"
            logger.info(f"User {request.user.username} is creating a new mix: '{mix_name}'")
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
            mix_id = request.POST.get('mix_id')
            logger.info(f"User {request.user.username} is editing mix ID: {mix_id}")
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
            mix_id = request.POST.get('mix_id')
            logger.info(f"User {request.user.username} is deleting mix ID: {mix_id}")
            user_mix = UserMix.objects.filter(id=mix_id, user=request.user).first()
            if user_mix:
                cache.delete(f"mix_feed_{user_mix.unique_id}")
                user_mix.delete()
                logger.debug(f"Mix ID {mix_id} successfully deleted.")
                messages.success(request, "Mix deleted successfully.")
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
        
        # visibility logic: If logged in, ALWAYS give the unique feed so it can upgrade/downgrade dynamically.
        if profile is not None:
            raw_url = reverse('custom_feed') + f"?auth={profile.feed_token}&show={podcast.slug}&network={podcast.network.slug}"
            feed_data.append({'is_network_mix': False, 'podcast': podcast, 'has_access': has_access, 'req_dollars': req_cents/100, 'feed_url': request.build_absolute_uri(raw_url)})
        elif req_cents == 0 or podcast.public_feed_url:
            # Guest visitors get the public URL
            raw_url = reverse('public_feed', args=[podcast.slug]) 
            feed_data.append({'is_network_mix': False, 'podcast': podcast, 'has_access': False, 'req_dollars': req_cents/100, 'feed_url': request.build_absolute_uri(raw_url)})

    # === NEW: INJECT NETWORK MIXES INTO "YOUR FEEDS" ===
    from .models import NetworkMix
    network_mixes = NetworkMix.objects.filter(network__in=target_networks).select_related('network', 'required_tier')
    for nmix in network_mixes:
        mix_req_cents = nmix.required_tier.minimum_cents if nmix.required_tier else 0
        camp_id = str(nmix.network.patreon_campaign_id)
        user_cents = active_pledges.get(camp_id, 0) if profile else 0
        has_access = (mix_req_cents == 0) or (user_cents >= mix_req_cents)
        
        if profile:
            raw_url = reverse('network_mix_feed', args=[nmix.network.slug, nmix.slug]) + f"?auth={profile.feed_token}"
        else:
            raw_url = reverse('network_mix_feed', args=[nmix.network.slug, nmix.slug])
            
        feed_data.append({
            'is_network_mix': True,
            'mix': nmix,
            'has_access': has_access,
            'req_dollars': mix_req_cents / 100,
            'feed_url': request.build_absolute_uri(raw_url)
        })

    # Sort feed_data: Network Mixes first (0), then Podcasts (1). Sorted alphabetically within each group.
    feed_data = sorted(feed_data, key=lambda x: (
        0 if x.get('is_network_mix') else 1,
        x['mix'].name.lower() if x.get('is_network_mix') else x['podcast'].title.lower()
    ))

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

    logger.info(f"Custom feed requested: show='{podcast_slug}', network='{network_slug}'")

    if not feed_token or not podcast_slug:
        logger.warning("Custom feed request rejected: Missing auth or show parameters.")
        return HttpResponseForbidden("Missing authentication or show parameters.")

    if network_slug:
        podcast = get_object_or_404(Podcast, slug=podcast_slug, network__slug=network_slug)
    else:
        podcast = get_object_or_404(Podcast, slug=podcast_slug, network=request.network)

    try:
        profile = get_object_or_404(PatronProfile, feed_token=feed_token)
    except ValueError:
        logger.warning(f"Invalid token format received: {feed_token}")
        return HttpResponseForbidden("Invalid authentication token format.")

    if not profile.last_active or profile.last_active < timezone.now() - timedelta(hours=24):
        profile.last_active = timezone.now()
        profile.save(update_fields=['last_active'])

    active_pledges = profile.active_pledges or {}
    required_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
    camp_id = str(podcast.network.patreon_campaign_id)
    user_cents = active_pledges.get(camp_id, 0)
    
    has_access = (user_cents >= required_cents)

    version = cache.get(f"podcast_cache_version_{podcast.id}", 1)
    cache_key = f"xml_feed_{version}_{profile.feed_token}_{podcast.slug}_{has_access}"
    xml_output = cache.get(cache_key)

    if xml_output:
        logger.debug(f"Cache HIT for custom feed: {cache_key}")
    else:
        logger.debug(f"Cache MISS for custom feed: {cache_key}. Generating XML...")
        episodes = podcast.episodes.all().order_by('-pub_date')[:2000]
        
        p = PodgenPodcast(
            name=f"{podcast.title} for {profile.user.first_name}",
            description=f"Personalized feed for {profile.user.first_name}.",
            website=podcast.network.website_url or "https://example.com",
            explicit=True,
            image=podcast.image_url or podcast.network.default_image_url or "https://example.com/logo.png",
            authors=[Person(name=podcast.network.name, email="hosts@baldmove.com")],
            owner=Person(name=podcast.network.name, email="hosts@baldmove.com"),
            withhold_from_itunes=True,
        )

        for ep in episodes:
            if not ep.audio_url_subscriber and not ep.audio_url_public:
                continue

            audio_url = request.build_absolute_uri(reverse('play_episode', args=[ep.id])) + f"?auth={profile.feed_token}"

            assembled_desc = ep.clean_description
            footer_parts = []
            
            if has_access:
                if podcast.show_footer_private: footer_parts.append(podcast.show_footer_private)
                if podcast.network.global_footer_private: footer_parts.append(podcast.network.global_footer_private)
            else:
                if podcast.show_footer_public: footer_parts.append(podcast.show_footer_public)
                if podcast.network.global_footer_public: footer_parts.append(podcast.network.global_footer_public)

            if footer_parts:
                assembled_desc += "<br><br>" + "<br><br>".join(footer_parts)

            p.episodes.append(PodgenEpisode(
                title=ep.title,
                media=Media(url=audio_url, size=0, type="audio/mpeg", duration=parse_duration(ep.duration)),
                id=ep.guid_public or ep.guid_private or str(ep.id),
                publication_date=ep.pub_date,
                summary=assembled_desc, 
            ))
   
        raw_xml = p.rss_str()
        feed_type = 'private' if has_access else 'public'
        xml_output = inject_rss_extensions(request, raw_xml, episodes, default_feed_type=feed_type)
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)
        logger.debug(f"Successfully generated and cached custom feed: {cache_key}")

    return HttpResponse(xml_output, content_type='application/rss+xml')

def generate_public_feed(request, podcast_slug):
    network_slug = request.GET.get('network')
    logger.info(f"Public feed requested: show='{podcast_slug}', network='{network_slug}'")
    
    if network_slug:
        podcast = get_object_or_404(Podcast, slug=podcast_slug, network__slug=network_slug)
    else:
        podcast = get_object_or_404(Podcast, slug=podcast_slug, network=request.network)

    version = cache.get(f"podcast_cache_version_{podcast.id}", 1)
    cache_key = f"xml_feed_public_{version}_{podcast.slug}"
    xml_output = cache.get(cache_key)

    if xml_output:
        logger.debug(f"Cache HIT for public feed: {cache_key}")
    else:
        logger.debug(f"Cache MISS for public feed: {cache_key}. Generating XML...")
        episodes = podcast.episodes.all().order_by('-pub_date')[:500]
        
        p = PodgenPodcast(
            name=podcast.title,
            description=f"Public feed for {podcast.title}.",
            website=podcast.network.website_url or "https://example.com",
            explicit=True,
            image=podcast.image_url or podcast.network.default_image_url or "https://example.com/logo.png",
            authors=[Person(name=podcast.network.name, email="hosts@baldmove.com")],
            owner=Person(name=podcast.network.name, email="hosts@baldmove.com"),
            withhold_from_itunes=True,
        )

        for ep in episodes:
            if not ep.audio_url_public:
                continue
                
            audio_url = request.build_absolute_uri(reverse('play_episode', args=[ep.id]))
                
            assembled_desc = ep.clean_description

            p.episodes.append(PodgenEpisode(
                title=ep.title,
                # Use the new router URL
                media=Media(url=audio_url, size=0, type="audio/mpeg", duration=parse_duration(ep.duration)),
                id=ep.guid_public or ep.guid_private or str(ep.id),
                publication_date=ep.pub_date,
                summary=assembled_desc, 
            ))
            
        raw_xml = p.rss_str()
        xml_output = inject_rss_extensions(request, raw_xml, episodes, default_feed_type='public')
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)
        logger.debug(f"Successfully generated and cached public feed: {cache_key}")

    return HttpResponse(xml_output, content_type='application/rss+xml')

def generate_mix_feed(request, unique_id):
    logger.info(f"Custom Mix feed requested: unique_id='{unique_id}'")
    cache_key = f"mix_feed_{unique_id}"
    feed_xml = cache.get(cache_key)

    if feed_xml:
        logger.debug(f"Cache HIT for mix feed: {cache_key}")
    else:
        logger.debug(f"Cache MISS for mix feed: {cache_key}. Generating XML...")
        user_mix = get_object_or_404(UserMix, unique_id=unique_id, is_active=True)
        
        profile = user_mix.user.patron_profile

        if not profile.last_active or profile.last_active < timezone.now() - timedelta(hours=24):
            profile.last_active = timezone.now()
            profile.save(update_fields=['last_active'])

        active_pledges = profile.active_pledges or {}
        
        selected_podcasts = user_mix.selected_podcasts.select_related('required_tier', 'network').all()
        
        access_map = {}
        for podcast in selected_podcasts:
            req_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
            camp_id = str(podcast.network.patreon_campaign_id)
            has_access = (active_pledges.get(camp_id, 0) >= req_cents)
            access_map[podcast.id] = has_access
            logger.debug(f"Mix Feed Access Check: {profile.user.username} -> {podcast.title}: {has_access}")
                
        episodes = Episode.objects.filter(podcast__in=selected_podcasts).select_related('podcast', 'podcast__network').order_by('-pub_date')[:500]
        
        if user_mix.display_image: image_url = request.build_absolute_uri(user_mix.display_image)
        else: image_url = request.build_absolute_uri(user_mix.network.default_image_url)

        feed = PodgenPodcast(
            name=user_mix.name,
            description=f"A custom blended podcast feed generated by Vecto for {user_mix.user.first_name}.",
            website=request.build_absolute_uri('/'),
            explicit=False,
            image=image_url,
            withhold_from_itunes=True,
            authors=[Person(name=podcast.network.name, email="hosts@baldmove.com")],
            owner=Person(name=podcast.network.name, email="hosts@baldmove.com"),
        )
        
        for ep in episodes:
            has_access = access_map.get(ep.podcast_id, False)
            
            if not ep.audio_url_subscriber and not ep.audio_url_public:
                continue
                
            audio_url = request.build_absolute_uri(reverse('play_episode', args=[ep.id])) + f"?auth={profile.feed_token}"

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
                id=ep.guid_public or ep.guid_private or str(ep.id),
                title=display_title,
                summary=assembled_desc, 
                publication_date=ep.pub_date,
                media=Media(url=audio_url, size=0, type="audio/mpeg", duration=parse_duration(ep.duration))
            ))
            
        raw_xml = feed.rss_str()
        feed_xml = inject_rss_extensions(request, raw_xml, episodes, access_map=access_map) 
        cache.set(cache_key, feed_xml, 300)
        logger.debug(f"Successfully generated and cached mix feed: {cache_key}")
        
    return HttpResponse(feed_xml, content_type='application/rss+xml')

def generate_network_mix_feed(request, network_slug, mix_slug):
    logger.info(f"Network Mix feed requested: network='{network_slug}', mix='{mix_slug}'")
    network_mix = get_object_or_404(NetworkMix, slug=mix_slug, network__slug=network_slug)
    
    feed_token = request.GET.get('auth')
    profile = None
    active_pledges = {}
    
    if feed_token:
        try:
            profile = PatronProfile.objects.get(feed_token=feed_token)
            if not profile.last_active or profile.last_active < timezone.now() - timedelta(hours=24):
                profile.last_active = timezone.now()
                profile.save(update_fields=['last_active'])
            active_pledges = profile.active_pledges or {}
        except PatronProfile.DoesNotExist:
            logger.warning(f"Invalid token format received for network mix: {feed_token}")

    mix_req_cents = network_mix.required_tier.minimum_cents if network_mix.required_tier else 0
    camp_id = str(network_mix.network.patreon_campaign_id)
    user_cents = active_pledges.get(camp_id, 0)
    
    user_meets_mix_tier = (mix_req_cents == 0) or (user_cents >= mix_req_cents)

    cache_key = f"network_mix_{network_mix.unique_id}_{feed_token}"
    feed_xml = cache.get(cache_key)

    if feed_xml:
        logger.debug(f"Cache HIT for network mix: {cache_key}")
    else:
        logger.debug(f"Cache MISS for network mix: {cache_key}. Generating XML...")
        selected_podcasts = network_mix.selected_podcasts.select_related('required_tier', 'network').all()
        
        access_map = {}
        for podcast in selected_podcasts:
            req_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
            access_map[podcast.id] = user_meets_mix_tier and (user_cents >= req_cents)
                
        episodes = Episode.objects.filter(podcast__in=selected_podcasts).select_related('podcast', 'podcast__network').order_by('-pub_date')[:5000]
        
        image_url = request.build_absolute_uri(network_mix.display_image) if network_mix.display_image else request.build_absolute_uri(network_mix.network.default_image_url)

        from podgen import Podcast as PodgenPodcast, Episode as PodgenEpisode, Media, Person
        from django.utils.dateparse import parse_duration

        feed = PodgenPodcast(
            name=network_mix.name,
            description=f"A curated network mix by {network_mix.network.name}.",
            website=network_mix.network.website_url or request.build_absolute_uri('/'),
            explicit=False,
            image=image_url,
            withhold_from_itunes=True,
            authors=[Person(name=network_mix.network.name, email="hosts@example.com")],
            owner=Person(name=network_mix.network.name, email="hosts@example.com"),
        )
        
        for ep in episodes:
            has_access = access_map.get(ep.podcast_id, False)
            
            if not has_access and not ep.audio_url_public:
                continue
                
            audio_url = request.build_absolute_uri(reverse('play_episode', args=[ep.id]))
            if feed_token:
                audio_url += f"?auth={feed_token}"

            display_title = f"[{ep.podcast.title}] {ep.title}"
            
            audio_banner = ""
            if ep.audio_url_subscriber and ep.audio_url_public and ep.audio_url_subscriber != ep.audio_url_public:
                if has_access:
                    audio_banner = "Premium: "
                else:
                    audio_banner = "Standard: "
            elif not ep.audio_url_public:
                audio_banner = "Premium Exclusive: "

            assembled_desc = audio_banner + (ep.clean_description or ep.raw_description)
            
            footer_parts = []
            if has_access:
                if ep.podcast.show_footer_private: footer_parts.append(ep.podcast.show_footer_private)
                if ep.podcast.network.global_footer_private: footer_parts.append(ep.podcast.network.global_footer_private)
            else:
                if ep.podcast.show_footer_public: footer_parts.append(ep.podcast.show_footer_public)
                if ep.podcast.network.global_footer_public: footer_parts.append(ep.podcast.network.global_footer_public)
            
            if footer_parts:
                assembled_desc += "<br><br>" + "<br><br>".join(footer_parts)

            ep_duration = parse_duration(ep.duration) if ep.duration else None

            feed.episodes.append(PodgenEpisode(
                id=ep.guid_public or ep.guid_private or str(ep.id),
                title=display_title,
                summary=assembled_desc, 
                publication_date=ep.pub_date,
                media=Media(url=audio_url, size=0, type="audio/mpeg", duration=ep_duration)
            ))
            
        raw_xml = feed.rss_str()
        from .views import inject_rss_extensions
        feed_xml = inject_rss_extensions(request, raw_xml, episodes, access_map=access_map) 
        cache.set(cache_key, feed_xml, 300)
        
    return HttpResponse(feed_xml, content_type='application/rss+xml')

@csrf_exempt
def patreon_webhook(request):
    logger.debug("Received incoming Patreon webhook request.")
    if request.method != 'POST':
        logger.warning(f"Webhook rejected: Invalid HTTP method {request.method}")
        return HttpResponse("Method not allowed", status=405)

    signature = request.headers.get('X-Patreon-Signature')
    if not signature:
        logger.warning("Webhook rejected: Missing X-Patreon-Signature header.")
        return HttpResponseForbidden("Missing signature")
        
    secret = settings.PATREON_WEBHOOK_SECRET.encode('utf-8')
    expected_signature = hmac.new(secret, request.body, hashlib.md5).hexdigest()
    
    if not hmac.compare_digest(expected_signature, signature):
        logger.warning(f"Webhook rejected: Invalid signature. Expected {expected_signature}, got {signature}")
        return HttpResponseForbidden("Invalid signature")

    logger.debug("Webhook signature verified successfully.")

    try:
        data = json.loads(request.body)
        member_data = data.get('data', {})
        
        user_relationship = member_data.get('relationships', {}).get('user', {}).get('data', {})
        if not user_relationship or user_relationship.get('type') != 'user':
            logger.error("Webhook payload missing user relationship data.")
            return HttpResponse("Could not find user relationship in webhook.", status=400)
            
        patreon_user_id = user_relationship.get('id')
        if not patreon_user_id:
            logger.error("Webhook payload missing Patreon User ID.")
            return HttpResponse("Missing user ID in webhook.", status=400)

        logger.info(f"Processing webhook for Patreon User ID: {patreon_user_id}")

        with transaction.atomic():
            profile = PatronProfile.objects.select_for_update().get(patreon_id=patreon_user_id)

            attributes = member_data.get('attributes', {})
            new_cents = attributes.get('currently_entitled_amount_cents', 0)
            status = attributes.get('patron_status') 

            final_amount = new_cents if status == 'active_patron' else 0
            logger.debug(f"Parsed Webhook Data - Status: {status}, Cents: {new_cents}, Final Amount: {final_amount}")
            
            campaign_relationship = member_data.get('relationships', {}).get('campaign', {}).get('data', {})
            campaign_id = str(campaign_relationship.get('id', ''))
            
            if campaign_id:
                active_pledges = profile.active_pledges or {}
                active_pledges[campaign_id] = final_amount
                profile.active_pledges = active_pledges
                profile.save()
                logger.info(f"Webhook Success: Updated {profile.user.email} on Campaign {campaign_id} to {final_amount} cents.")
            else:
                logger.warning(f"Webhook processed for {patreon_user_id}, but no campaign ID was found in the payload.")
                
        return HttpResponse("Success", status=200)
        
    except PatronProfile.DoesNotExist:
        logger.warning(f"Webhook skipped: Patreon User ID {patreon_user_id} does not exist in local database.")
        return HttpResponse("User has not logged in via OAuth yet.", status=200)

    except (ValueError, KeyError, AttributeError) as e:
        logger.error(f"Error parsing webhook JSON payload: {str(e)}", exc_info=True)
        return HttpResponse("Error processing webhook payload.", status=400)
    
    except Exception as e:
        logger.error(f"Critical error processing webhook: {str(e)}", exc_info=True)
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

def play_episode(request, episode_id):
    logger.info(f"[ROUTER] Request received for episode_id: {episode_id}")
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier'), id=episode_id)
    feed_token = request.GET.get('auth')
    
    logger.debug(f"[ROUTER] Episode: '{ep.title}' | Auth Token Provided: {bool(feed_token)}")
    
    # 1. Evaluate their pledge status on the fly
    has_access = False
    if feed_token:
        profile = PatronProfile.objects.filter(feed_token=feed_token).first()
        if profile:
            if not profile.last_active or profile.last_active < timezone.now() - timedelta(hours=24):
                profile.last_active = timezone.now()
                profile.save(update_fields=['last_active'])

            req_cents = ep.podcast.required_tier.minimum_cents if ep.podcast.required_tier else 0
            camp_id = str(ep.podcast.network.patreon_campaign_id)
            user_cents = profile.active_pledges.get(camp_id, 0) if profile.active_pledges else 0
            has_access = (user_cents >= req_cents)
            logger.info(f"[ROUTER] User: {profile.user.email} | Has: {user_cents}¢ | Needs: {req_cents}¢ | Access Granted: {has_access}")
        else:
            logger.warning(f"[ROUTER] Invalid auth token provided: {feed_token}")
    else:
        logger.info("[ROUTER] No auth token provided. Defaulting to public access.")
            
    # 2. Route to the correct audio file
    target_url = ep.audio_url_subscriber if (has_access and ep.audio_url_subscriber) else ep.audio_url_public
    
    if not target_url:
        logger.error(f"[ROUTER] CRITICAL: No audio file found for episode {episode_id} (Access: {has_access})")
        raise Http404("Audio file not found.")
        
    logger.info(f"[ROUTER] Redirecting to: {target_url}")
        
    # 3. Use 302 Temporary Redirect so podcast apps evaluate it EVERY time they press play
    response = HttpResponseRedirect(target_url)
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response

def traefik_config_api(request):
    # Security: Require a token so only Traefik can read this endpoint
    # You can set this token to any secure string you want
    expected_token = getattr(settings, 'TRAEFIK_API_TOKEN', None)
    if request.GET.get('token') != expected_token:
        return HttpResponseForbidden("Unauthorized access.")

    routers = {}

    # Fetch all networks that have a custom domain explicitly set
    networks = Network.objects.exclude(custom_domain__isnull=True).exclude(custom_domain__exact='')

    for network in networks:
        # Router names must be unique. We use the network ID to guarantee this.
        router_name = f"custom-domain-{network.id}"

        routers[router_name] = {
            "rule": f"Host(`{network.custom_domain}`)",
            # This MUST exactly match the service name defined in your fileConfig.yml
            "service": "vecto-service",
            "tls": {
                # Replace 'myresolver' with your actual Let's Encrypt certResolver name
                "certResolver": "letsencrypt" 
            }
        }

    # Traefik expects this exact nested JSON structure
    traefik_json = {
        "http": {
            "routers": routers
        }
    }

    return JsonResponse(traefik_json)

# ==========================================
# BACKGROUND QUEUE & IMPORT STREAMING
# ==========================================

@login_required(login_url='/login/')
def stream_feed_import(request, show_id):
    task_id = f"import_logs_{show_id}"
    
    if not cache.get(task_id):
        cache.set(task_id, "data: [QUEUED] Waiting for Celery worker...\n\n", timeout=3600)
        task_ingest_feed.delay(show_id)

    # By making this async, Hypercorn can stream it instantly without buffering!
    async def event_stream():
        last_length = 0
        while True:
            # We use Django's native async .aget() instead of .get()
            logs = await cache.aget(task_id, "")
            
            if len(logs) > last_length:
                new_logs = logs[last_length:]
                yield new_logs
                last_length = len(logs)
                
                if "[DONE]" in new_logs:
                    logger.debug(f"Stream complete for task {task_id}. Closing connection.")
                    await cache.adelete(task_id) # async delete
                    break
                    
            # Awaitable sleep gives control back to the server so it doesn't freeze
            await asyncio.sleep(0.5)

    return StreamingHttpResponse(event_stream(), content_type='text/event-stream')