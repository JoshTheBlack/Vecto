import urllib.parse
import requests
import sys
import subprocess
from django.conf import settings
from django.shortcuts import redirect, get_object_or_404, render
from django.http import JsonResponse, HttpResponse, HttpResponseForbidden, StreamingHttpResponse
from django.contrib.auth.models import User
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.urls import reverse
from django.utils.html import escape
from django.contrib.admin.views.decorators import staff_member_required
from django.conf import settings
import hmac
import hashlib
from django.views.decorators.csrf import csrf_exempt
import json
from django.core.cache import cache
from podgen import Podcast as PodgenPodcast, Episode as PodgenEpisode, Media
from datetime import timedelta
from .models import PatronProfile, Podcast, Episode, Network, PatreonTier
from django.contrib import messages
from django.core.management import call_command
import io
from django.core.paginator import Paginator

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def parse_duration(duration_str):
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
    
def get_bald_move_pledge_amount(patreon_json):
    """
    Parses the Patreon API response and returns the user's active 
    pledge amount (in cents) for Bald Move. Returns 0 if not a patron.
    """
    BALD_MOVE_CAMPAIGN_ID = "113757"
    
    if 'included' not in patreon_json:
        return 0
        
    for item in patreon_json['included']:
        if item.get('type') == 'member':
            relationships = item.get('relationships', {})
            campaign_data = relationships.get('campaign', {}).get('data', {})
            
            if campaign_data and campaign_data.get('id') == BALD_MOVE_CAMPAIGN_ID:
                attributes = item.get('attributes', {})
                if attributes.get('patron_status') == 'active_patron':
                    return attributes.get('currently_entitled_amount_cents', 0)
                    
    return 0


# ==========================================
# OAUTH AUTHENTICATION
# ==========================================

def patreon_login(request):
    """
    Step 1: Redirect the user to Patreon's authorization page.
    """
    params = {
        'response_type': 'code',
        'client_id': settings.PATREON_CLIENT_ID,
        'redirect_uri': settings.PATREON_REDIRECT_URI,
        'scope': 'identity identity[email]', 
    }
    
    # We use quote_via to ensure spaces are safely encoded as %20
    url = f"https://www.patreon.com/oauth2/authorize?{urllib.parse.urlencode(params, quote_via=urllib.parse.quote)}"
    
    return redirect(url)


def patreon_callback(request):
    """
    Step 2: Patreon sends the user back here with a 'code'.
    We trade that code for an access token, fetch their profile, and log them in.
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

    # C. Extract core info
    patreon_id = user_data['data']['id']
    attributes = user_data['data']['attributes']
    
    # Safely handle the case where a user has hidden their email
    raw_email = attributes.get('email')
    safe_username = raw_email if raw_email else patreon_id
    safe_email = raw_email if raw_email else ''
    
    full_name = attributes.get('full_name', '')

    pledge_amount = get_bald_move_pledge_amount(user_data)

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

    # G. Redirect them to the dashboard
    return redirect('dashboard')


# ==========================================
# USER DASHBOARD
# ==========================================

@login_required(login_url='/login/')
def dashboard(request):
    """
    The Listener Dashboard: Shows a user their active pledge and generates
    their custom private RSS URLs for the apps.
    """
    if not hasattr(request.user, 'patron_profile'):
        return render(request, 'pod_manager/no_patreon.html')

    profile = request.user.patron_profile
    
    # Pre-build the feed data so the template is clean and logic-free
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
    return render(request, 'pod_manager/dashboard.html', context)


@staff_member_required(login_url='/login/')
def creator_settings(request):
    """
    The Custom Admin Dashboard for Network Owners.
    Handles Network updates, Show updates, and Adding new Shows.
    """
    if request.method == 'POST':
        action = request.POST.get('action')

        # === UPDATE NETWORK ACTION ===
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

                network.global_footer_public = footer_public
                network.global_footer_private = footer_private
                network.save()

                cache.clear()
                messages.success(request, f"{network.name} settings saved successfully! Cache cleared.")
                
            except Network.DoesNotExist:
                messages.error(request, "Error finding that Network.")

        # === UPDATE SHOW ACTION ===
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
                
                cache.clear()
                messages.success(request, f"{show.title} updated successfully! Cache cleared.")
            except Podcast.DoesNotExist:
                messages.error(request, "Error finding that Show.")

        # === ADD SHOW ACTION ===
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
                
                # AUTOMATICALLY INGEST FEED AND CAPTURE OUTPUT
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

    # DEFAULT GET LOGIC
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
    
    # NEW: Calculate a 10-page sliding window (5 before, 5 after)
    start_index = max(1, page_obj.number - 5)
    end_index = min(paginator.num_pages, page_obj.number + 5)
    custom_page_range = range(start_index, end_index + 1)
    
    user_cents = 0
    if request.user.is_authenticated and hasattr(request.user, 'patron_profile'):
        user_cents = request.user.patron_profile.pledge_amount_cents
        
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
        
    # 2. Annotate access flags
    req_cents = ep.podcast.required_tier.minimum_cents if ep.podcast.required_tier else 0
    ep.user_has_access = (user_cents >= req_cents)
    
    # 3. Assemble the dynamic description with Footers
    assembled_desc = ep.clean_description
    if ep.user_has_access:
        if hasattr(ep.podcast, 'show_footer_private') and ep.podcast.show_footer_private:
            assembled_desc += f"<br><br>{ep.podcast.show_footer_private}"
        if ep.podcast.network.global_footer_private:
            assembled_desc += f"<br><br>{ep.podcast.network.global_footer_private}"
    else:
        if hasattr(ep.podcast, 'show_footer_public') and ep.podcast.show_footer_public:
            assembled_desc += f"<br><br>{ep.podcast.show_footer_public}"
        if ep.podcast.network.global_footer_public:
            assembled_desc += f"<br><br>{ep.podcast.network.global_footer_public}"
            
    ep.display_description = assembled_desc

    context = {
        'ep': ep,
    }
    return render(request, 'pod_manager/episode_detail.html', context)

@login_required(login_url='/login/')
def user_feeds(request):
    """
    Formerly the 'dashboard'. Displays the RSS links to copy to a podcast app.
    """
    if not hasattr(request.user, 'patron_profile'):
        return render(request, 'pod_manager/no_patreon.html')

    profile = request.user.patron_profile
    
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
    # Notice the template name change here!
    return render(request, 'pod_manager/user_feeds.html', context)

# ==========================================
# FEED GENERATOR
# ==========================================

def generate_custom_feed(request, network_slug):
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
    cache_key = f"xml_feed_{profile.feed_token}_{podcast.slug}"
    xml_output = cache.get(cache_key)

    if not xml_output:
        print("CACHE MISS - Building XML with Podgen")
        episodes = podcast.episodes.all().order_by('-pub_date')
        
        # 1. Initialize the Podgen object
        p = PodgenPodcast(
            name=f"{podcast.title} (Custom Feed)",
            description=f"Premium ad-free feed for {profile.user.first_name}.",
            website="https://baldmove.com",
            explicit=True,
            image=podcast.image_url or "https://baldmove.com/wp-content/uploads/2014/06/bald-move-logo.png",
        )

        # 2. Add Episodes
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
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)

    # Return the perfectly compliant RSS feed
    return HttpResponse(xml_output, content_type='application/rss+xml')

def generate_public_feed(request, podcast_slug):
    """
    Generates a public, unauthenticated feed for a specific podcast.
    Uses public audio URLs and public footers.
    """
    # 1. Look up the podcast directly (No network slug needed for public links)
    podcast = get_object_or_404(Podcast, slug=podcast_slug)

    # ==========================================
    # CACHE CHECK
    # ==========================================
    # Unique key for the public version of this show
    cache_key = f"xml_feed_public_{podcast.slug}"
    xml_output = cache.get(cache_key)

    if not xml_output:
        print(f"CACHE MISS - Building Public XML for {podcast.title}")
        episodes = podcast.episodes.all().order_by('-pub_date')
        
        # Initialize the Podgen object for the public feed
        p = PodgenPodcast(
            name=podcast.title,
            description=f"Public feed for {podcast.title}.",
            website="https://baldmove.com",
            explicit=True,
            image=podcast.image_url or "https://baldmove.com/wp-content/uploads/2014/06/bald-move-logo.png",
        )

        for ep in episodes:
            assembled_desc = ep.clean_description
            
            # Apply PUBLIC footers instead of private
            if podcast.show_footer_public:
                assembled_desc += f"<br><br>{podcast.show_footer_public}"
            if podcast.network.global_footer_public:
                assembled_desc += f"<br><br>{podcast.network.global_footer_public}"

            p.episodes.append(PodgenEpisode(
                title=ep.title,
                # CRITICAL: Serve the public audio file!
                media=Media(ep.audio_url_public,duration=parse_duration(ep.duration)),
                id=ep.guid,
                publication_date=ep.pub_date,
                summary=assembled_desc, 
            ))
            
        xml_output = p.rss_str()
        
        timeout_seconds = podcast.network.feed_cache_minutes * 60
        cache.set(cache_key, xml_output, timeout=timeout_seconds)

    return HttpResponse(xml_output, content_type='application/rss+xml')

@csrf_exempt
def patreon_webhook(request):
    """
    Handles Patreon Webhooks: members:create, members:update, members:delete
    """
    signature = request.headers.get('X-Patreon-Signature')
    if not signature:
        return HttpResponseForbidden("Missing signature")

    # 1. Verify the signature
    # In .env, set PATREON_WEBHOOK_SECRET to a random string for now
    secret = settings.PATREON_WEBHOOK_SECRET.encode('utf-8')
    # Patreon uses MD5 for their webhook signatures
    expected_sig = hmac.new(secret, request.body, hashlib.md5).hexdigest()

    if not hmac.compare_digest(expected_sig, signature):
        return HttpResponseForbidden("Invalid signature")

    # 2. Parse the JSON
    try:
        data = json.loads(request.body)
        attributes = data['data']['attributes']
       # relationships = data['data']['relationships']
        
        # Patreon ID for the member (the link between user and campaign)
        patreon_id = data['data']['id']
        
        # The amount they are currently paying
        new_cents = attributes.get('currently_entitled_amount_cents', 0)
        status = attributes.get('patron_status') # 'active_patron', 'declined_patron', etc.

        # 3. Update the database
        try:
            profile = PatronProfile.objects.get(patreon_id=patreon_id)
            
            # If status isn't active, they effectively pay $0
            final_amount = new_cents if status == 'active_patron' else 0
            
            if profile.pledge_amount_cents != final_amount:
                profile.pledge_amount_cents = final_amount
                profile.save()
                print(f"Webhook Success: Updated {profile.user.email} to {final_amount} cents.")
            
            return HttpResponse("Success", status=200)
            
        except PatronProfile.DoesNotExist:
            # We don't have this user in our DB yet, which is fine
            return HttpResponse("User not in Vecto", status=200)

    except (ValueError, KeyError) as e:
        return HttpResponse("Malformed JSON", status=400)

@staff_member_required(login_url='/login/')
def stream_feed_import(request, show_id):
    """
    Spawns the ingest_feed management command in a separate process
    and streams its stdout line-by-line to the frontend via Server-Sent Events.
    """
    def event_stream():
        yield f"data: Initiating Vecto Feed Importer for Show ID {show_id}...\n\n"
        
        # Spawn the terminal command as a background process
        process = subprocess.Popen(
            [sys.executable, 'manage.py', 'ingest_feed', str(show_id)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1, # Line buffered so it streams instantly
            cwd=settings.BASE_DIR
        )
        
        # Read lines as they are printed in real-time
        for line in iter(process.stdout.readline, ''):
            # Clean up the line break since SSE uses \n\n to separate events
            clean_line = line.replace('\n', '')
            yield f"data: {clean_line}\n\n"
            
        process.stdout.close()
        process.wait()
        
        # Send a special completion flag
        yield "data: [DONE]\n\n"

    return StreamingHttpResponse(event_stream(), content_type='text/event-stream')