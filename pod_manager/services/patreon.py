"""
Patreon integration: profile sync, full network sync, refresh tokens, and
the Patreon webhook receiver (HTTP entrypoint kept here for cohesion).
"""
import hashlib
import hmac
import json
import logging
import time
import urllib.parse

import requests

from django.conf import settings
from django.db import transaction
from django.http import HttpResponse, HttpResponseForbidden
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt

from ..models import PatronProfile, NetworkMembership, Network

logger = logging.getLogger(__name__)


def _sync_patron_profile(user, user_data, included_data, current_network=None):
    logger.info(f"[_sync_patron_profile] Starting sync for user: {user.email} (ID: {user.id})")

    patreon_id = user_data.get('id')
    attributes = user_data.get('attributes', {})
    logger.debug(f"[_sync_patron_profile] Extracted Patreon ID: {patreon_id}")

    # 1. Update Global Profile
    profile, created = PatronProfile.objects.get_or_create(user=user, defaults={'patreon_id': patreon_id})
    logger.debug(f"[_sync_patron_profile] PatronProfile created: {created}")

    profile.profile_image_url = attributes.get('image_url')
    socials = attributes.get('social_connections', {}) or {}
    discord_info = socials.get('discord') or {}
    profile.discord_id = discord_info.get('user_id') if discord_info else None
    profile.last_active = timezone.now()
    if profile.patreon_id != patreon_id:
        profile.patreon_id = patreon_id
    profile.save()
    logger.info("[_sync_patron_profile] Global profile saved successfully.")

    # Even if they pay $0, they are now a registered free listener on this network.
    if current_network:
        _, mem_created = NetworkMembership.objects.get_or_create(user=user, network=current_network)
        logger.info(f"[_sync_patron_profile] Default NetworkMembership for '{current_network.name}' ensured (Created: {mem_created}).")

    # Exclude empty strings to prevent dictionary overwrite bugs!
    known_campaigns = {str(n.patreon_campaign_id): n for n in Network.objects.exclude(patreon_campaign_id__isnull=True).exclude(patreon_campaign_id__exact='')}
    logger.info(f"[_sync_patron_profile] Known campaigns loaded: {list(known_campaigns.keys())}")

    seen_campaigns = set()
    logger.debug(f"[_sync_patron_profile] Scanning {len(included_data)} items in included_data...")

    for item in included_data:
        if item.get('type') == 'member':
            attrs = item.get('attributes', {})
            campaign_data = item.get('relationships', {}).get('campaign', {}).get('data', {})

            logger.debug(f"[_sync_patron_profile] Found 'member' item. Status: '{attrs.get('patron_status')}', Cents: {attrs.get('currently_entitled_amount_cents')}")

            if campaign_data:
                campaign_id = str(campaign_data.get('id'))
                logger.debug(f"[_sync_patron_profile] Member item is attached to Campaign ID: {campaign_id}")

                if campaign_id in known_campaigns:
                    seen_campaigns.add(campaign_id)
                    network = known_campaigns[campaign_id]
                    logger.info(f"[_sync_patron_profile] Campaign MATCH! Mapping to Network: '{network.name}'")

                    membership, mem_created = NetworkMembership.objects.get_or_create(user=user, network=network)

                    if attrs.get('patron_status') == 'active_patron':
                        cents = attrs.get('currently_entitled_amount_cents', 0)
                        logger.info(f"[_sync_patron_profile] Applying ACTIVE pledge to '{network.name}': {cents} cents.")
                        membership.patreon_pledge_cents = cents
                        membership.is_active_patron = True

                        start_date_str = attrs.get('pledge_relationship_start')
                        if start_date_str:
                            try:
                                membership.patreon_join_date = timezone.datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                            except Exception as e:
                                logger.error(f"[_sync_patron_profile] Date parsing failed: {e}")
                    else:
                        logger.info(f"[_sync_patron_profile] Patron status is '{attrs.get('patron_status')}'. Setting {network.name} pledge to 0.")
                        membership.patreon_pledge_cents = 0
                        membership.is_active_patron = False

                    membership.save()
                else:
                    logger.warning(f"[_sync_patron_profile] Ignoring member item: Campaign ID '{campaign_id}' is NOT in our database.")
            else:
                logger.warning("[_sync_patron_profile] Ignoring member item: No campaign relationship data found.")

    logger.info(f"[_sync_patron_profile] Checking for stale memberships. Seen campaigns: {list(seen_campaigns)}")

    # Check if the user has any active memberships that were NOT confirmed in this API response
    for mem in NetworkMembership.objects.filter(user=user, is_active_patron=True):
        camp_id = str(mem.network.patreon_campaign_id)
        if camp_id not in seen_campaigns:
            logger.info(f"[_sync_patron_profile] REVOKING stale membership for '{mem.network.name}' (Campaign ID '{camp_id}' was not in API response).")
            mem.is_active_patron = False
            mem.patreon_pledge_cents = 0
            mem.save()

    logger.info("[_sync_patron_profile] Sync complete.")
    return profile


def sync_network_patrons(network):
    logger.debug(f"--- Starting COMPLETE Sync for Network: {network.name} ---")
    if not network.patreon_creator_access_token or not network.patreon_campaign_id:
        return 0, "Network is not properly linked to Patreon."

    campaign_id_str = str(network.patreon_campaign_id)
    base_url = f"https://www.patreon.com/api/oauth2/v2/campaigns/{campaign_id_str}/members"
    params = {"include": "user", "fields[member]": "patron_status,currently_entitled_amount_cents", "fields[user]": "email", "page[count]": 100}
    headers = {'Authorization': f'Bearer {network.patreon_creator_access_token}'}

    updated_count, seen_patreon_ids, url = 0, set(), f"{base_url}?{urllib.parse.urlencode(params)}"

    while url:
        res = requests.get(url, headers=headers)
        if res.status_code == 401 or "Unauthorized" in res.text:
            network.patreon_sync_enabled = False
            network.save()
            return updated_count, "Patreon authorization permanently expired."

        if res.status_code == 429:
            time.sleep(int(res.headers.get('Retry-After', 5)))
            continue

        if res.status_code != 200: return updated_count, f"API Error: {res.text}"

        data = res.json()
        included = {i['id']: i for i in data.get('included', []) if i['type'] == 'user'}

        for member in data.get('data', []):
            rel_user = member.get('relationships', {}).get('user', {}).get('data', {})
            if not rel_user: continue

            patreon_id = rel_user['id']
            seen_patreon_ids.add(patreon_id)
            email = included.get(patreon_id, {}).get('attributes', {}).get('email')

            profile = PatronProfile.objects.filter(patreon_id=patreon_id).first()
            if not profile and email: profile = PatronProfile.objects.filter(user__email=email).first()

            if profile:
                attrs = member.get('attributes', {})
                status = attrs.get('patron_status')
                cents = attrs.get('currently_entitled_amount_cents', 0)

                membership, _ = NetworkMembership.objects.get_or_create(user=profile.user, network=network)
                membership.patreon_pledge_cents = cents if status == 'active_patron' else 0
                membership.is_active_patron = (membership.patreon_pledge_cents > 0)
                membership.save()
                updated_count += 1

        url = data.get('links', {}).get('next')
        if url: time.sleep(0.5)

    stale = NetworkMembership.objects.filter(network=network, is_active_patron=True).exclude(user__patron_profile__patreon_id__in=seen_patreon_ids)
    revoked_count = stale.update(is_active_patron=False, patreon_pledge_cents=0)

    logger.info(f"Sync Complete. Updated: {updated_count} | Revoked: {revoked_count}")
    return updated_count, None


def refresh_patreon_token(network):
    if not network.patreon_creator_refresh_token: return False

    token_url = "https://www.patreon.com/api/oauth2/token"
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': network.patreon_creator_refresh_token,
        'client_id': settings.PATREON_CLIENT_ID,
        'client_secret': settings.PATREON_CLIENT_SECRET,
    }

    try:
        res = requests.post(token_url, data=data, timeout=10)
        if res.status_code == 200:
            tokens = res.json()
            network.patreon_creator_access_token = tokens['access_token']
            if 'refresh_token' in tokens: network.patreon_creator_refresh_token = tokens['refresh_token']
            network.save()
            return True
        return False
    except Exception:
        return False


@csrf_exempt
def patreon_webhook(request):
    if request.method != 'POST': return HttpResponse("Method not allowed", status=405)
    signature = request.headers.get('X-Patreon-Signature') or ''
    secret = settings.PATREON_WEBHOOK_SECRET
    if not secret:
        return HttpResponseForbidden("Webhook secret not configured")

    expected = hmac.new(secret.encode('utf-8'), request.body, hashlib.md5).hexdigest()
    if not hmac.compare_digest(expected, signature): return HttpResponseForbidden("Invalid signature")

    try:
        data = json.loads(request.body)
        member_data = data.get('data', {})
        patreon_user_id = member_data.get('relationships', {}).get('user', {}).get('data', {}).get('id')

        with transaction.atomic():
            profile = PatronProfile.objects.select_for_update().get(patreon_id=patreon_user_id)
            attrs = member_data.get('attributes', {})
            cents = attrs.get('currently_entitled_amount_cents', 0)
            status = attrs.get('patron_status')
            final_amount = cents if status == 'active_patron' else 0

            campaign_id = str(member_data.get('relationships', {}).get('campaign', {}).get('data', {}).get('id', ''))
            network = Network.objects.filter(patreon_campaign_id=campaign_id).first()

            if network:
                membership, _ = NetworkMembership.objects.get_or_create(user=profile.user, network=network)
                membership.patreon_pledge_cents = final_amount
                membership.is_active_patron = (final_amount > 0)
                membership.save()

        return HttpResponse("Success", status=200)
    except PatronProfile.DoesNotExist:
        return HttpResponse("User not found.", status=200)
    except Exception as e:
        logger.error(f"Webhook Error: {str(e)}", exc_info=True)
        return HttpResponse("Error", status=500)
