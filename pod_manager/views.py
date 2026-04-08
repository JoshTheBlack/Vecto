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
from django.http import JsonResponse, HttpResponse, HttpResponseForbidden, StreamingHttpResponse
from django.shortcuts import redirect, get_object_or_404, render
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
from podgen import Podcast as PodgenPodcast, Episode as PodgenEpisode, Media

from .models import PatronProfile, Podcast, Episode, Network, PatreonTier, UserMix

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def parse_duration(duration_str: str) -> timedelta | None:
    """Converts a duration string (seconds or HH:MM:SS) into a timedelta object for Podgen."""
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
    """
    Parses the Patreon API response and returns the user's active 
    pledge amount (in cents) for the specified Campaign ID. Returns 0 if not a patron.
    """
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
    """
    Increments the cache version for a specific show. 
    This instantly invalidates all public and private cached feeds for this show
    without wiping the rest of the Redis database.
    """
    version_key = f"podcast_cache_version_{show_id}"
    try:
        cache.incr(version_key)
    except ValueError:
        cache.set(version_key, 1, timeout=None)  # Set it to 1 if it doesn't exist

# ==========================================
# OAUTH AUTHENTICATION
# ==========================================

def patreon_login(request):
    """
    Step 1: Redirect the user to Patreon's authorization page.
    The 'scope' parameter requests access to the user's identity and email.
    """
    params = {
        'response_type': 'code',
        'client_id': settings.PATREON_CLIENT_ID,
        'redirect_uri': settings.PATREON_REDIRECT_URI,
        'scope': 'identity identity[email]', 
    }
    
    # We use quote_via to ensure spaces are safely encoded as %20
    # The scope parameter contains spaces, so we must safely encode it.
    url = f"https://www.patreon.com/oauth2/authorize?{urllib.parse.urlencode(params, quote_via=urllib.parse.quote)}"
    
    return redirect(url)


def patreon_callback(request):
    """
    Step 2: Patreon sends the user back here with a 'code'.
    We exchange that code for an access token, fetch their profile, and log them in.
    """
    code = request.GET.get('code')
    if not code:
        return JsonResponse({"error": "No code provided by Patreon."}, status=400)

    # A. Trade the temporary code for a real access_token
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

    # B. Ask Patreon for the User's Profile and Membership Status
    user_url = (
        "https://www.patreon.com/api/oauth2/v2/identity"
        "?include=memberships,memberships.campaign"
        "&fields[user]=full_name,email"
        "&fields[member]=patron_status,currently_entitled_amount_cents"
    )
    headers = {'Authorization': f'Bearer {access_token}'}
    
    user_res = requests.get(user_url, headers=headers)
    user_data = user_res.json()

    # C. Extract core info, using .get() to prevent KeyErrors if the API response changes.
    user_info = user_data.get('data', {})
    patreon_id = user_info.get('id')
    if not patreon_id:
        return JsonResponse({"error": "Could not retrieve Patreon user ID from API response."}, status=400)

    attributes = user_info.get('attributes', {})
    
    # A user can hide their email on Patreon. Use their Patreon ID as a fallback username.
    raw_email = attributes.get('email')
    safe_username = raw_email if raw_email else patreon_id
    safe_email = raw_email if raw_email else ''
    
    full_name = attributes.get('full_name', '')

    # Fetch the primary network to check its Patreon Campaign ID
    primary_network = Network.objects.first()
    campaign_id = primary_network.patreon_campaign_id if primary_network else None
    if not campaign_id:
        messages.error(request, "Site configuration error: Patreon Campaign ID not set.")
    
    pledge_amount = get_active_pledge_amount(user_data, campaign_id)

    # D. Get or Create the Django User (Using safe_username)
    user, created = User.objects.get_or_create(
        username=safe_username, 
        defaults={'email': safe_email, 'first_name': full_name}
    )

    # E. Get or Create their Patron Profile and update their pledge
    profile, p_created = PatronProfile.objects.get_or_create(
        user=user, 
        defaults={'patreon_id': patreon_id}
    )
    profile.pledge_amount_cents = pledge_amount
    profile.save()

    # F. Log them into the Django session!
    login(request, user)

    # G. Redirect them to their feeds page.
    return redirect('user_feeds')


# ==========================================
# USER-FACING VIEWS
# ==========================================

@staff_member_required(login_url='/login/')
def creator_settings(request):
    """
    The Custom Admin Dashboard for Network Owners.
    Handles Network updates, Show updates, and Adding new Shows.
    """
    if request.method == 'POST':
        action = request.POST.get('action')

        # --- ACTION: Update Network Settings ---
        if action == 'update_network':
            network_id = request.POST.get('network_id')
            theme_config_str = request.POST.get('theme_config', '{}')
            footer_public = request.POST.get('footer_public', '')
            footer_private = request.POST.get('footer_private', '')

            try:
                network = Network.objects.get(id=network_id)
                try:
                    clean_json_str = theme_config_str.replace("'", '"')
                    network.theme_config = json.loads(clean_json_str)
                except json.JSONDecodeError:
                    messages.error(request, f"Invalid JSON format for {network.name}. Settings not saved.")
                    return redirect('creator_settings')

                # Save the network-agnostic fields
                network.patreon_campaign_id = request.POST.get('patreon_campaign_id', '')
                network.website_url = request.POST.get('website_url', '')
                network.default_image_url = request.POST.get('default_image_url', '')
                network.ignored_title_tags = request.POST.get('ignored_title_tags', '')
                network.description_cut_triggers = request.POST.get('description_cut_triggers', '')

                network.global_footer_public = footer_public
                network.global_footer_private = footer_private
                network.save()

                # Invalidate caches for all shows in this network since global settings changed.
                for show in network.podcasts.all():
                    invalidate_show_cache(show.id)

                messages.success(request, f"{network.name} settings saved successfully! All related feed caches invalidated.")
                
            except Network.DoesNotExist:
                messages.error(request, "Error finding that Network.")

        # --- ACTION: Update a specific Show's Settings ---
        elif action == 'update_show':
            show_id = request.POST.get('show_id')
            tier_id = request.POST.get('tier_id')
            show_footer_public = request.POST.get('show_footer_public', '')
            show_footer_private = request.POST.get('show_footer_private', '')

            try:
                show = Podcast.objects.get(id=show_id)
                
                if tier_id:
                    show.required_tier_id = tier_id
                else:
                    show.required_tier = None
                    
                show.show_footer_public = show_footer_public
                show.show_footer_private = show_footer_private
                show.save()
                
                # Invalidate this specific show's cache instead of clearing everything.
                invalidate_show_cache(show.id)
                messages.success(request, f"{show.title} updated successfully! Feed cache invalidated.")
            except Podcast.DoesNotExist:
                messages.error(request, "Error finding that Show.")

        # --- ACTION: Add a new Show ---
        elif action == 'add_show':
            network_id = request.POST.get('network_id')
            title = request.POST.get('title')
            slug = request.POST.get('slug')
            public_feed_url = request.POST.get('public_feed_url')
            subscriber_feed_url = request.POST.get('subscriber_feed_url')
            tier_id = request.POST.get('tier_id')

            try:
                network = Network.objects.get(id=network_id)
                new_show = Podcast(
                    network=network,
                    title=title,
                    slug=slug,
                    public_feed_url=public_feed_url,
                    subscriber_feed_url=subscriber_feed_url,
                )
                if tier_id:
                    new_show.required_tier_id = tier_id
                
                new_show.save()
                
                # Automatically trigger a feed import and display the log output.
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

        return redirect('creator_settings')

    # --- DEFAULT GET REQUEST LOGIC ---
    networks = Network.objects.prefetch_related('podcasts', 'podcasts__required_tier').all()
    total_patrons = PatronProfile.objects.filter(pledge_amount_cents__gt=0).count()
    tiers = PatreonTier.objects.all().order_by('minimum_cents')
    
    context = {
        'networks': networks,
        'total_patrons': total_patrons,
        'tiers': tiers,
    }
    return render(request, 'pod_manager/creator_settings.html', context)

def home(request):
    """
    The main homepage, displaying a paginated list of all episodes from all podcasts.
    Can be filtered by a specific show using a URL parameter.
    """
    podcasts = Podcast.objects.all().order_by('title')
    show_slug = request.GET.get('show')
    
    query = Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier')
    
    if show_slug:
        query = query.filter(podcast__slug=show_slug)
        
    all_episodes = query.order_by('-pub_date')
    
    paginator = Paginator(all_episodes, 20)
    
    # Safely get the page number (defaults to 1 if missing or invalid)
    try:
        page_number = int(request.GET.get('page', 1))
    except ValueError:
        page_number = 1
        
    page_obj = paginator.get_page(page_number)
    
    # Calculate a 10-page sliding window for cleaner pagination controls (5 before, 5 after current page)
    start_index = max(1, page_obj.number - 5)
    end_index = min(paginator.num_pages, page_obj.number + 5)
    custom_page_range = range(start_index, end_index + 1)
    
    # Determine the current user's pledge amount to check access on the fly.
    user_cents = 0
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        user_cents = request.user.patron_profile.pledge_amount_cents
        
    
    # Annotate each episode on the current page with access information.
    for ep in page_obj:
        req_cents = ep.podcast.required_tier.minimum_cents if ep.podcast.required_tier else 0
        ep.user_has_access = (user_cents >= req_cents)

    # Grab the primary network to load its theme config
    current_network = Network.objects.first()

    context = {
        'episodes': page_obj,          
        'page_obj': page_obj,          
        'custom_page_range': custom_page_range,  # Pass the new range to the template
        'podcasts': podcasts,          
        'current_filter': show_slug,   
        'current_network': current_network,
    }
    return render(request, 'pod_manager/home.html', context)

def episode_detail(request, episode_id):
    """
    Displays a single episode. Dynamically loads the premium ad-free audio
    and assembles the correct footers based on Patreon access.
    """
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier'), id=episode_id)

    # 1. Determine user's active pledge
    user_cents = 0
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        user_cents = request.user.patron_profile.pledge_amount_cents
        
    # 2. Determine if the user has access to this specific episode's tier.
    req_cents = ep.podcast.required_tier.minimum_cents if ep.podcast.required_tier else 0
    ep.user_has_access = (user_cents >= req_cents)
    
    # 3. Assemble the dynamic description, appending the correct footers based on access.
    footer_parts = []
    if ep.user_has_access:
        show_footer = ep.podcast.show_footer_private
        global_footer = ep.podcast.network.global_footer_private
    else:
        show_footer = ep.podcast.show_footer_public
        global_footer = ep.podcast.network.global_footer_public

    if show_footer:
        footer_parts.append(show_footer)
    if global_footer:
        footer_parts.append(global_footer)

    ep.display_description = ep.clean_description
    if footer_parts:
        ep.display_description += "<br><br>" + "<br><br>".join(footer_parts)

    context = {
        'ep': ep,
    }
    return render(request, 'pod_manager/episode_detail.html', context)

@login_required(login_url='/login/')
def user_feeds(request):
    """
    The Listener Dashboard. Displays the user's pledge status and provides the
    private RSS feed URLs for each podcast they have access to.
    """
    if not hasattr(request.user, 'patron_profile'):
        return render(request, 'pod_manager/no_patreon.html')

    profile = request.user.patron_profile
    
    # Pre-build the feed data so the template is clean and logic-free.
    feed_data = []
    for podcast in Podcast.objects.select_related('network', 'required_tier').all():
        required_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
        has_access = profile.pledge_amount_cents >= required_cents
        
        base_feed_url = reverse('custom_feed', args=[podcast.network.slug])
        raw_url = f"{base_feed_url}?auth={profile.feed_token}&show={podcast.slug}"
        full_feed_url = request.build_absolute_uri(raw_url)

        feed_data.append({
            'podcast': podcast,
            'has_access': has_access,
            'req_dollars': required_cents / 100,
            'feed_url': full_feed_url if has_access else None
        })

    context = {
        'profile': profile,
        'dollars': profile.pledge_amount_cents / 100,
        'feed_data': feed_data,
    }
    return render(request, 'pod_manager/user_feeds.html', context)

@login_required(login_url='/login/')
def user_feeds(request):
    """
    Displays the RSS links to copy to a podcast app, and handles the Custom Mix Builder.
    """
    if not hasattr(request.user, 'patron_profile'):
        return render(request, 'pod_manager/no_patreon.html')

    profile = request.user.patron_profile
    network = Network.objects.first()

    # === HANDLE MIX CREATION ===
    if request.method == 'POST' and 'create_mix' in request.POST:
        mix_name = request.POST.get('mix_name', '').strip()
        if not mix_name:
            mix_count = UserMix.objects.filter(user=request.user).count()
            mix_name = f"UserMix {mix_count + 1}"
            
        mix_image = request.POST.get('mix_image', '').strip()
        # NEW: Grab the uploaded file
        mix_image_upload = request.FILES.get('mix_image_upload')
        selected_podcast_ids = request.POST.getlist('podcasts')
        
        if selected_podcast_ids:
            user_mix = UserMix.objects.create(
                user=request.user,
                network=network,
                name=mix_name,
                image_url=mix_image,
                image_upload=mix_image_upload # Save it!
            )
            user_mix.selected_podcasts.set(selected_podcast_ids)
            messages.success(request, f"Mix '{mix_name}' created successfully!")
        else:
            messages.warning(request, "You must select at least one show to create a mix.")
        return redirect('user_feeds')

    # === HANDLE MIX EDITING ===
    if request.method == 'POST' and 'edit_mix' in request.POST:
        mix_id = request.POST.get('mix_id')
        user_mix = UserMix.objects.filter(id=mix_id, user=request.user).first()
        
        if user_mix:
            mix_name = request.POST.get('mix_name', '').strip()
            if not mix_name:
                mix_count = UserMix.objects.filter(user=request.user).count()
                mix_name = f"UserMix {mix_count + 1}"
                
            user_mix.name = mix_name
            user_mix.image_url = request.POST.get('mix_image', '').strip()
            
            # NEW: Check if a new file was uploaded during the edit
            if 'mix_image_upload' in request.FILES:
                user_mix.image_upload = request.FILES['mix_image_upload']
            
            selected_podcast_ids = request.POST.getlist('podcasts')
            if selected_podcast_ids:
                user_mix.selected_podcasts.set(selected_podcast_ids)
                user_mix.save()
                messages.success(request, "Mix updated successfully!")
            else:
                messages.warning(request, "You must select at least one show. Changes were not saved.")
                
        return redirect('user_feeds')

    # === HANDLE MIX DELETION ===
    if request.method == 'POST' and 'delete_mix' in request.POST:
        mix_id = request.POST.get('mix_id')
        UserMix.objects.filter(id=mix_id, user=request.user).delete()
        messages.success(request, "Custom mix deleted.")
        return redirect('user_feeds')

    # === BUILD PREMIUM FEED DATA ===
    feed_data = []
    available_podcasts = []
    
    for podcast in Podcast.objects.select_related('network', 'required_tier').all():
        required_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
        has_access = profile.pledge_amount_cents >= required_cents
        
        base_feed_url = reverse('custom_feed', args=[podcast.network.slug])
        raw_url = f"{base_feed_url}?auth={profile.feed_token}&show={podcast.slug}"
        
        feed_data.append({
            'podcast': podcast,
            'has_access': has_access,
            'req_dollars': required_cents / 100,
            'feed_url': request.build_absolute_uri(raw_url) if has_access else None
        })
        
        if has_access:
            available_podcasts.append(podcast)

    # === GET ALL OF THIS USER'S MIXES ===
    user_mixes = UserMix.objects.filter(user=request.user, network=network).prefetch_related('selected_podcasts')
    mix_data = []
    for mix in user_mixes:
        raw_mix_url = f"/feed/{network.slug}/mix/{mix.unique_id}"
        mix_data.append({
            'mix': mix,
            'feed_url': request.build_absolute_uri(raw_mix_url)
        })

    context = {
        'profile': profile,
        'dollars': profile.pledge_amount_cents / 100,
        'feed_data': feed_data,
        'available_podcasts': available_podcasts,
        'mix_data': mix_data,
    }
    return render(request, 'pod_manager/user_feeds.html', context)

# ==========================================
# FEED GENERATOR
# ==========================================

def generate_custom_feed(request, network_slug):
    """
    Generates a personalized, private RSS feed for a user and a specific podcast.
    Access is verified using the user's unique feed token and their pledge amount.
    The generated XML is cached for performance.
    """
    # 1. Validate Input
    feed_token = request.GET.get('auth')
    podcast_slug = request.GET.get('show')

    if not feed_token or not podcast_slug:
        return HttpResponseForbidden("Missing authentication or show parameters.")

    podcast = get_object_or_404(Podcast, slug=podcast_slug)
    
    if podcast.network.slug != network_slug:
        return HttpResponseForbidden("This podcast does not belong to the requested network.")

    try:
        profile = get_object_or_404(PatronProfile, feed_token=feed_token)
    except ValueError:
        return HttpResponseForbidden("Invalid authentication token format.")

    required_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
    if profile.pledge_amount_cents < required_cents:
        return HttpResponseForbidden("Your current Patreon pledge does not grant access to this feed.")

    # ==========================================
    # CACHE CHECK
    # ==========================================
    # 2. Check Cache
    # The cache key includes a version number that can be incremented to force-invalidate.
    version = cache.get(f"podcast_cache_version_{podcast.id}", 1)
    cache_key = f"xml_feed_{version}_{profile.feed_token}_{podcast.slug}"
    xml_output = cache.get(cache_key)

    if not xml_output:
        # 3. Build Feed on Cache Miss
        # print("CACHE MISS - Building private XML with Podgen")
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
                summary=assembled_desc, # Podgen handles the CDATA escaping for us
            ))
            
        # 3. Generate the final XML string
        xml_output = p.rss_str()
        
        # Save to cache
        # 4. Save to Cache
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)

    # Return the perfectly compliant RSS feed
    # 5. Return Response
    return HttpResponse(xml_output, content_type='application/rss+xml')

def generate_public_feed(request, podcast_slug):
    """
    Generates a public, unauthenticated feed for a specific podcast.
    Uses public audio URLs and public footers.
    """
    # 1. Look up the podcast and check cache
    podcast = get_object_or_404(Podcast, slug=podcast_slug)

    # ==========================================
    # CACHE CHECK
    # ==========================================
    version = cache.get(f"podcast_cache_version_{podcast.id}", 1)
    cache_key = f"xml_feed_public_{version}_{podcast.slug}"
    xml_output = cache.get(cache_key)

    if not xml_output:
        # 2. Build Feed on Cache Miss
        # print(f"CACHE MISS - Building Public XML for {podcast.title}")
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
            
            # Apply public footers
            if podcast.show_footer_public:
                assembled_desc += f"<br><br>{podcast.show_footer_public}"
            if podcast.network.global_footer_public:
                assembled_desc += f"<br><br>{podcast.network.global_footer_public}"

            p.episodes.append(PodgenEpisode(
                title=ep.title,
                # Use the public audio URL for the public feed
                media=Media(ep.audio_url_public,duration=parse_duration(ep.duration)),
                id=ep.guid,
                publication_date=ep.pub_date,
                summary=assembled_desc, 
            ))
            
        xml_output = p.rss_str()
        
        # 3. Save to Cache
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)

    return HttpResponse(xml_output, content_type='application/rss+xml')

# ==========================================
# WEBHOOK HANDLERS
# ==========================================

@csrf_exempt
def patreon_webhook(request):
    """
    Handles incoming webhooks from Patreon to update a patron's pledge status in real-time.
    Listens for `members:create`, `members:update`, and `members:delete` events.
    """
    signature = request.headers.get('X-Patreon-Signature')
    if not signature:
        return HttpResponseForbidden("Missing signature")

    secret = settings.PATREON_WEBHOOK_SECRET.encode('utf-8')
    expected_sig = hmac.new(secret, request.body, hashlib.md5).hexdigest()

    if not hmac.compare_digest(expected_sig, signature):
        return HttpResponseForbidden("Invalid signature")

    # 2. Parse the JSON and update the database.
    try:
        data = json.loads(request.body)
        # The primary data object in a 'members' webhook is the 'member' resource.
        member_data = data.get('data', {})
        
        # We must find the user's ID via the relationships block. The top-level ID
        # on the member object is a *membership* ID, not the user's ID.
        user_relationship = member_data.get('relationships', {}).get('user', {}).get('data', {})
        if not user_relationship or user_relationship.get('type') != 'user':
            return HttpResponse("Could not find user relationship in webhook.", status=400)
            
        patreon_user_id = user_relationship.get('id')
        if not patreon_user_id:
            return HttpResponse("Missing user ID in webhook.", status=400)

        # Now find our local profile using the correct Patreon User ID.
        profile = PatronProfile.objects.get(patreon_id=patreon_user_id)

        # Get pledge attributes from the member resource.
        attributes = member_data.get('attributes', {})
        new_cents = attributes.get('currently_entitled_amount_cents', 0)
        status = attributes.get('patron_status')  # e.g., 'active_patron', 'declined_patron'

        # If status isn't active, their effective pledge is $0.
        final_amount = new_cents if status == 'active_patron' else 0
        
        if profile.pledge_amount_cents != final_amount:
            profile.pledge_amount_cents = final_amount
            profile.save()
            # In a production app, this should use the logging library.
            print(f"Webhook Success: Updated {profile.user.email} to {final_amount} cents.")
        
        return HttpResponse("Success", status=200)
        
    except PatronProfile.DoesNotExist:
        # This user has a Patreon membership but hasn't logged into our app yet.
        # This is a valid state, not an error. We can't do anything until they log in.
        return HttpResponse("User has not logged in via OAuth yet.", status=200)

    except (ValueError, KeyError, AttributeError) as e:
        # In production, log the error `e` and the request body for debugging.
        return HttpResponse("Error processing webhook payload.", status=400)

# ==========================================
# BACKGROUND PROCESSING (FEED INGESTION)
# ==========================================

class CacheLogStream:
    """A virtual terminal that writes output directly to the Django Cache."""
    def __init__(self, task_id):
        self.task_id = task_id
        cache.set(self.task_id, "data: Initiating Background Import...\n\n", timeout=3600)
        
    def write(self, text):
        # Ignore empty newlines from standard Python print() statements.
        if not text.strip(): return 
        
        current_log = cache.get(self.task_id, "")
        clean_line = text.replace('\n', '')
        cache.set(self.task_id, current_log + f"data: {clean_line}\n\n", timeout=3600)
        
    def flush(self):
        pass

# A single, global queue to serialize all feed import tasks.
feed_import_queue = queue.Queue()

def feed_import_worker():
    """
    A long-running background worker thread that processes imports one by one from the queue.
    This serialized approach is crucial for preventing database lock errors, especially with SQLite,
    by ensuring only one `ingest_feed` command runs at a time.
    """
    while True:
        # This is a blocking call; the thread will sleep here until an item is available.
        # Grab the next show in line (this pauses automatically if the queue is empty)
        show_id, task_id = feed_import_queue.get()
        stream = CacheLogStream(task_id)
        
        try:
            stream.write("\n[SYSTEM] Worker acquired task. Starting ingestion...\n")
            call_command('ingest_feed', show_id, stdout=stream, stderr=stream, no_color=True)
            # Invalidate the cache for this show now that it has new data.
            invalidate_show_cache(show_id) 
        except Exception as e:
            stream.write(f"\n[ERROR] {str(e)}\n")
        finally:
            # Signal the frontend that the process is complete.
            stream.write("[DONE]")
            feed_import_queue.task_done() # Signals the queue that the task is finished.

# Start the worker thread exactly once when Django boots up
threading.Thread(target=feed_import_worker, daemon=True).start()

@staff_member_required(login_url='/login/')
def stream_feed_import(request, show_id):
    """
    An endpoint that provides a Server-Sent Events (SSE) stream to the client.
    It adds a feed import task to the background queue and then streams the
    log output from the cache as it's being written by the worker.
    """
    task_id = f"import_logs_{show_id}"
    
    # 1. Add the task to the queue, but only if it's not already running or queued.
    # We check for the existence of the cache key to determine this.
    if not cache.get(task_id):
        # Set an initial cache message so the UI knows the task is queued.
        cache.set(task_id, "data: [QUEUED] Waiting for database availability...\n\n", timeout=3600)
        feed_import_queue.put((show_id, task_id))

    # 2. Define the generator that will stream logs from the cache to the browser.
    def event_stream():
        last_length = 0
        while True:
            logs = cache.get(task_id, "")
            
            # If new logs have been added since we last checked, send them.
            if len(logs) > last_length:
                new_logs = logs[last_length:]
                yield new_logs
                last_length = len(logs)
                
                # If the background thread wrote [DONE], exit the loop and close connection
                if "[DONE]" in new_logs:
                    cache.delete(task_id) # Clean up the cache key.
                    break
                    
            # Non-blocking pause before checking the cache again
            time.sleep(0.5)

    return StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    
