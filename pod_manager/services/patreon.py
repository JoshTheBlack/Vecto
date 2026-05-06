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


def update_global_profile_from_patreon(user, user_data) -> 'PatronProfile':
    """Creates or updates the global PatronProfile from a Patreon identity payload."""
    patreon_id = user_data.get('id')
    attributes = user_data.get('attributes', {})

    profile, created = PatronProfile.objects.get_or_create(user=user, defaults={'patreon_id': patreon_id})
    logger.debug(f"[Patreon Sync] PatronProfile {'created' if created else 'found'} for {user.email}")

    profile.profile_image_url = attributes.get('image_url')
    socials = attributes.get('social_connections', {}) or {}
    discord_info = socials.get('discord') or {}
    profile.discord_id = discord_info.get('user_id') if discord_info else None
    profile.last_active = timezone.now()
    if profile.patreon_id != patreon_id:
        profile.patreon_id = patreon_id
    profile.save()
    logger.debug(f"[Patreon Sync] Global profile saved for {user.email}")
    return profile


def apply_membership_updates(profile, included_data, current_network=None) -> set:
    """Walks the Patreon included payload, writes NetworkMembership rows, and
    returns the set of campaign IDs seen in this response."""
    user = profile.user

    # Even $0 patrons become registered free listeners on the current network.
    if current_network:
        _, created = NetworkMembership.objects.get_or_create(user=user, network=current_network)
        logger.debug(f"[Patreon Sync] Default membership for '{current_network.name}' ensured (created={created})")

    # Exclude empty strings to prevent dictionary overwrite bugs.
    known_campaigns = {
        str(n.patreon_campaign_id): n
        for n in Network.objects.exclude(patreon_campaign_id__isnull=True).exclude(patreon_campaign_id__exact='')
    }
    logger.debug(f"[Patreon Sync] Known campaigns: {list(known_campaigns.keys())}")

    seen_campaigns: set = set()

    for item in included_data:
        if item.get('type') != 'member':
            continue
        attrs = item.get('attributes', {})
        campaign_data = item.get('relationships', {}).get('campaign', {}).get('data', {})

        if not campaign_data:
            logger.warning("[Patreon Sync] Skipping member item: no campaign relationship data.")
            continue

        campaign_id = str(campaign_data.get('id'))
        if campaign_id not in known_campaigns:
            logger.warning(f"[Patreon Sync] Ignoring campaign ID '{campaign_id}' — not in our database.")
            continue

        seen_campaigns.add(campaign_id)
        network = known_campaigns[campaign_id]
        membership, _ = NetworkMembership.objects.get_or_create(user=user, network=network)

        if attrs.get('patron_status') == 'active_patron':
            cents = attrs.get('currently_entitled_amount_cents', 0)
            membership.patreon_pledge_cents = cents
            membership.is_active_patron = True
            start_date_str = attrs.get('pledge_relationship_start')
            if start_date_str:
                try:
                    membership.patreon_join_date = timezone.datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                except Exception as e:
                    logger.error(f"[Patreon Sync] Date parsing failed for {network.name}: {e}")
            logger.debug(f"[Patreon Sync] Active pledge on '{network.name}': {cents} cents")
        else:
            membership.patreon_pledge_cents = 0
            membership.is_active_patron = False
            logger.debug(f"[Patreon Sync] Inactive status '{attrs.get('patron_status')}' on '{network.name}' — zeroing pledge")

        membership.save()

    return seen_campaigns


def revoke_stale_memberships(user, seen_campaign_ids: set):
    """Revokes any active memberships for campaigns not present in this response."""
    for mem in NetworkMembership.objects.filter(user=user, is_active_patron=True):
        camp_id = str(mem.network.patreon_campaign_id)
        if camp_id not in seen_campaign_ids:
            logger.info(f"[Patreon Sync] Revoking stale membership: '{mem.network.name}' (campaign {camp_id} absent from response)")
            mem.is_active_patron = False
            mem.patreon_pledge_cents = 0
            mem.save()


def _sync_patron_profile(user, user_data, included_data, current_network=None):
    logger.info(f"[Patreon Sync] Starting sync for {user.email}")
    profile = update_global_profile_from_patreon(user, user_data)
    seen_campaigns = apply_membership_updates(profile, included_data, current_network)
    revoke_stale_memberships(user, seen_campaigns)
    logger.info(f"[Patreon Sync] Sync complete for {user.email}")
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
            logger.warning(f"[Patreon Sync] Token expired for network '{network.name}' — disabling sync")
            network.patreon_sync_enabled = False
            network.save()
            return updated_count, "Patreon authorization permanently expired."

        if res.status_code == 429:
            retry_after = int(res.headers.get('Retry-After', 5))
            logger.warning(f"[Patreon Sync] Rate-limited for network '{network.name}'; retrying after {retry_after}s")
            time.sleep(retry_after)
            continue

        if res.status_code != 200:
            logger.error(f"[Patreon Sync] API error for network '{network.name}': {res.status_code} {res.text[:200]}")
            return updated_count, f"API Error: {res.text}"

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
            logger.info(f"[Patreon Sync] Access token refreshed for network '{network.name}'")
            return True
        logger.warning(f"[Patreon Sync] Token refresh failed for network '{network.name}': {res.status_code} {res.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"[Patreon Sync] Exception during token refresh for network '{network.name}': {e}", exc_info=True)
        return False


@csrf_exempt
def patreon_webhook(request):
    if request.method != 'POST': return HttpResponse("Method not allowed", status=405)
    signature = request.headers.get('X-Patreon-Signature') or ''
    secret = settings.PATREON_WEBHOOK_SECRET
    if not secret:
        return HttpResponseForbidden("Webhook secret not configured")

    # MD5 is Patreon's documented webhook spec — not our choice. The HMAC
    # construction still provides integrity; only collision-resistance is weak,
    # which is not the threat model here (attacker cannot choose the body).
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
                logger.info(f"[Webhook] Membership updated for {profile.user.email} on '{network.name}': status={status}, cents={final_amount}")

        return HttpResponse("Success", status=200)
    except PatronProfile.DoesNotExist:
        # User hasn't registered with Vecto yet. Their membership will be
        # bootstrapped on first login — no retry needed.
        logger.debug(f"Webhook for unregistered Patreon user {patreon_user_id!r}; discarding.")
        return HttpResponse("User not found.", status=200)
    except Exception as e:
        logger.error(f"Webhook Error: {str(e)}", exc_info=True)
        return HttpResponse("Error", status=500)
