import urllib.parse
import requests
from django.conf import settings
from django.shortcuts import redirect, get_object_or_404
from django.http import JsonResponse, HttpResponse, HttpResponseForbidden
from django.contrib.auth.models import User
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.urls import reverse
from django.utils.html import escape

# Ensure you import your models correctly based on your app name
from .models import PatronProfile, Podcast

# ==========================================
# HELPER FUNCTIONS
# ==========================================

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
    email = attributes.get('email')
    full_name = attributes.get('full_name', '')

    pledge_amount = get_bald_move_pledge_amount(user_data)

    # D. Get or Create the Django User (Using email as the username)
    user, created = User.objects.get_or_create(
        username=email, 
        defaults={'email': email, 'first_name': full_name}
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
    Displays the user's customized RSS links using the query parameter format.
    """
    # Safely check if this user has a Patreon profile (in case an Admin logs in directly)
    if hasattr(request.user, 'patron_profile'):
        profile = request.user.patron_profile
    else:
        return HttpResponse(
            "<h1>No Patreon Account Linked</h1>"
            f"<p>You are currently logged in as: <strong>{request.user.username}</strong>, but this account has no Patreon data.</p>"
            "<p><a href='/login/'>Click here to authenticate with Patreon</a></p>"
        )

    dollars = profile.pledge_amount_cents / 100
    
    html = f"""
        <h1>Welcome, {request.user.first_name}!</h1>
        <p>Your current Bald Move pledge is: <strong>${dollars:.2f}</strong></p>
        <hr>
        <h2>Your Private Podcast Feeds</h2>
        <ul>
    """
    
    # Use select_related to optimize the database query for network and tier data
    for podcast in Podcast.objects.select_related('network', 'required_tier').all():
        required_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
        
        if profile.pledge_amount_cents >= required_cents:
            # Build the base URL using the podcast's network slug
            base_feed_url = reverse('custom_feed', args=[podcast.network.slug])
            raw_url = f"{base_feed_url}?auth={profile.feed_token}&show={podcast.slug}"
            
            # Convert to a full absolute URI (e.g., http://localhost:8000/...)
            full_feed_url = request.build_absolute_uri(raw_url)
            
            html += f'<li><strong>{podcast.title}</strong>: <a href="{full_feed_url}">{full_feed_url}</a></li>'
        else:
            req_dollars = required_cents / 100
            html += f'<li style="color: gray;"><em>{podcast.title}</em> (Requires ${req_dollars:.2f} tier)</li>'
            
    html += "</ul>"
    return HttpResponse(html)


# ==========================================
# FEED GENERATOR
# ==========================================

def generate_custom_feed(request, network_slug):
    """
    Validates the user via query parameters and generates a safe XML feed.
    Format: /feed/<network_slug>/?auth=<uuid>&show=<slug>
    """
    feed_token = request.GET.get('auth')
    podcast_slug = request.GET.get('show')

    if not feed_token or not podcast_slug:
        return HttpResponseForbidden("Missing authentication or show parameters.")

    # 1. Look up the podcast
    podcast = get_object_or_404(Podcast, slug=podcast_slug)
    
    # 2. Ensure the requested podcast actually belongs to the network in the URL
    if podcast.network.slug != network_slug:
        return HttpResponseForbidden("This podcast does not belong to the requested network.")

    # 3. Look up the user securely via their UUID token
    try:
        profile = get_object_or_404(PatronProfile, feed_token=feed_token)
    except ValueError:
        # Protects against malformed UUID strings crashing the database query
        return HttpResponseForbidden("Invalid authentication token format.")

    # 4. Check if they are paying enough for this specific show
    required_cents = podcast.required_tier.minimum_cents if podcast.required_tier else 0
    if profile.pledge_amount_cents < required_cents:
        return HttpResponseForbidden("Your current Patreon pledge does not grant access to this feed.")

    # 5. Assemble the Feed Data
    episodes = podcast.episodes.all().order_by('-pub_date')
    
    # CRITICAL FIX: Escape ampersands and special characters for strict XML compliance
    safe_podcast_title = escape(f"{podcast.title} (Custom Feed)")
    safe_user_name = escape(profile.user.first_name)
    
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>{safe_podcast_title}</title>
    <link>https://baldmove.com</link>
    <description>Premium ad-free feed for {safe_user_name}.</description>
    <language>en-us</language>
"""
    for ep in episodes:
        # Stitch together the main description and the footers
        assembled_desc = ep.clean_description
        if podcast.show_footer_private:
            assembled_desc += f"<br><br>{podcast.show_footer_private}"
        if podcast.network.global_footer_private:
            assembled_desc += f"<br><br>{podcast.network.global_footer_private}"

        # CRITICAL FIX: Escape episode titles and URLs so characters like "&" don't crash the parser
        safe_ep_title = escape(ep.title)
        safe_audio_url = escape(ep.audio_url_subscriber)

        xml += f"""
    <item>
      <title>{safe_ep_title}</title>
      <guid isPermaLink="false">{ep.guid}</guid>
      <pubDate>{ep.pub_date.strftime("%a, %d %b %Y %H:%M:%S %z")}</pubDate>
      <description><![CDATA[{assembled_desc}]]></description>
      <enclosure url="{safe_audio_url}" type="audio/mpeg" />
    </item>"""

    xml += "\n  </channel>\n</rss>"
    
    # Return as XML so podcast apps treat it as a valid RSS feed
    return HttpResponse(xml, content_type='application/xml')