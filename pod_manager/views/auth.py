"""
Authentication & identity views: Patreon OAuth (listener login + creator
campaign linking), Recurly email/TOTP login, logout, impersonation, and
authenticator-app setup.
"""
import base64
import enum
import hmac as _hmac
import logging
import secrets
import threading
import urllib.parse
from io import BytesIO

import qrcode
import recurly
import requests

from django.conf import settings
from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.core.cache import cache
from django.http import HttpResponse, HttpResponseForbidden
from django.shortcuts import redirect, get_object_or_404, render
from django.utils import timezone
from django.views import View

from django_otp.plugins.otp_totp.models import TOTPDevice

from ..models import PatronProfile, NetworkMembership, Network, PatreonTier
from ..services.recurly import sync_recurly_plans_for_profile
from ..security import (
    _sign_oauth_state, _unsign_oauth_state,
    _is_rate_limited, _client_ip,
    _record_otp_failure, _clear_otp_state, MAX_OTP_ATTEMPTS,
)
from ..services.patreon import _sync_patron_profile, sync_network_patrons

logger = logging.getLogger(__name__)


def _secure_login(request, user):
    """
    Wrapper around django.contrib.auth.login that scrubs any leftover
    impersonation session key. Without this scrub, a staff user who was
    impersonating someone and then re-logs in via OTP/Patreon would carry
    the stale `impersonated_user_id` into the new session.
    """
    request.session.pop('impersonated_user_id', None)
    login(request, user)


def _exchange_patreon_token(code, redirect_uri):
    token_url = "https://www.patreon.com/api/oauth2/token"
    data = {
        "code": code,
        "grant_type": "authorization_code",
        "client_id": settings.PATREON_CLIENT_ID,
        "client_secret": settings.PATREON_CLIENT_SECRET,
        "redirect_uri": redirect_uri,
    }
    res = requests.post(token_url, data=data, timeout=10)
    if res.status_code != 200:
        logger.error(f"Patreon token exchange failed: {res.text}")
        return None, HttpResponse(f"Failed to get token: {res.text}", status=400)
    return res.json(), None


def _link_creator_campaign(request, network_id, access_token, refresh_token):
    """Handles the flow when a creator links their Patreon to a Vecto Network."""
    logger.info(f"Linking Patreon Campaign to Network ID {network_id} for user {request.user.username}")
    # Defense-in-depth: even though `patreon_login` already verified ownership
    # and `patreon_callback` verified the signed state, reject anything that
    # isn't an owner here too.
    network = get_object_or_404(Network, id=network_id, owners=request.user)

    headers = {"Authorization": f"Bearer {access_token}"}

    url = (
        "https://www.patreon.com/api/oauth2/v2/campaigns"
        "?include=tiers"
        "&fields[campaign]=created_at,image_url,image_small_url,url,vanity,summary,one_liner,discord_server_id"
        "&fields[tier]=title,amount_cents,url"
    )

    camp_res = requests.get(url, headers=headers, timeout=10)

    if camp_res.status_code == 200:
        payload = camp_res.json()
        camp_data = payload.get('data', [])
        included_data = payload.get('included', [])

        if camp_data:
            campaign_id = camp_data[0]['id']
            attrs = camp_data[0].get('attributes', {})

            # Only overwrite these if they are empty, so we don't destroy manual edits
            if not network.logo_url: network.logo_url = attrs.get('image_small_url', '')
            if not network.banner_image_url: network.banner_image_url = attrs.get('image_url', '')
            if not network.patreon_url: network.patreon_url = attrs.get('url', '')
            if not network.summary: network.summary = attrs.get('summary', '')
            if not network.one_liner: network.one_liner = attrs.get('one_liner', '')
            if not network.discord_server_id: network.discord_server_id = attrs.get('discord_server_id', '')

            network.patreon_campaign_id = campaign_id
            created_at_str = attrs.get('created_at')
            if created_at_str:
                network.patreon_campaign_created_at = timezone.datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))

            network.patreon_sync_enabled = True
            network.patreon_creator_access_token = access_token
            network.patreon_creator_refresh_token = refresh_token
            network.save()

            tiers_created = 0
            for item in included_data:
                if item.get('type') == 'tier':
                    tier_attrs = item.get('attributes', {})
                    title = tier_attrs.get('title', 'Unnamed Tier')
                    amount = tier_attrs.get('amount_cents', 0)
                    checkout_url = tier_attrs.get('url', '')

                    if amount > 0:
                        formatted_name = f"{network.name} - {title}"

                        tier, created = PatreonTier.objects.get_or_create(
                            network=network,
                            minimum_cents=amount,
                            defaults={
                                'name': formatted_name,
                                'checkout_url': checkout_url
                            }
                        )
                        if created: tiers_created += 1

            messages.success(request, f"Successfully linked Campaign! Auto-imported {tiers_created} reward tiers.")

            # Kick off the async patron sync
            threading.Thread(target=sync_network_patrons, args=(network,), daemon=True).start()
        else:
            messages.warning(request, "Linked, but no campaigns found on your Patreon account.")
    else:
        logger.error(f"Failed to fetch campaigns during linking: {camp_res.text}")
        messages.error(request, "Failed to fetch your campaigns from Patreon.")

    return redirect('creator_settings')


def _fetch_patreon_identity(access_token):
    headers = {"Authorization": f"Bearer {access_token}"}
    identity_url = (
        "https://www.patreon.com/api/oauth2/v2/identity"
        "?include=memberships.campaign"
        "&fields[user]=email,first_name,last_name,image_url,social_connections"
        "&fields[member]=patron_status,currently_entitled_amount_cents,pledge_relationship_start"
    )
    res = requests.get(identity_url, headers=headers, timeout=10)
    if res.status_code != 200:
        return None, HttpResponse("Failed to fetch user info", status=400)
    return res.json(), None


def patreon_login(request):
    network_id = request.GET.get('network_id')
    dynamic_redirect_uri = request.build_absolute_uri('/oauth/patreon/callback')

    # Creator-link flow: only logged-in owners of the target network may
    # initiate, and the network_id is signed.
    if network_id:
        if not request.user.is_authenticated:
            return HttpResponseForbidden("Login required to link a Patreon campaign.")
        if not Network.objects.filter(id=network_id, owners=request.user).exists():
            return HttpResponseForbidden("You are not an owner of that network.")

    scope = "identity identity[email] identity.memberships campaigns campaigns.members campaigns.members[email]" if network_id else "identity identity[email] identity.memberships"

    params = {
        "response_type": "code",
        "client_id": settings.PATREON_CLIENT_ID,
        "redirect_uri": dynamic_redirect_uri,
        "scope": scope,
    }
    if network_id:
        params["state"] = _sign_oauth_state(f"link:{request.user.id}:{network_id}")
    else:
        # Sign a nonce even on the listener flow so we can later detect/replay-protect.
        params["state"] = _sign_oauth_state(f"login:{secrets.token_urlsafe(16)}")
    return redirect(f"https://www.patreon.com/oauth2/authorize?{urllib.parse.urlencode(params)}")


def patreon_callback(request):
    code = request.GET.get('code')
    raw_state = request.GET.get('state')

    if not code: return HttpResponse("No code provided by Patreon", status=400)

    state_payload = _unsign_oauth_state(raw_state) if raw_state else None
    link_user_id = None
    state_network_id = None
    if state_payload and state_payload.startswith('link:'):
        try:
            _, link_user_id, state_network_id = state_payload.split(':', 2)
            link_user_id = int(link_user_id)
        except (ValueError, IndexError):
            link_user_id = None
            state_network_id = None

    try:
        dynamic_redirect_uri = request.build_absolute_uri('/oauth/patreon/callback')
        token_data, error_response = _exchange_patreon_token(code, dynamic_redirect_uri)
        if error_response: return error_response

        access_token = token_data['access_token']
        refresh_token = token_data['refresh_token']

        if state_network_id and request.user.is_authenticated and request.user.id == link_user_id:
            return _link_creator_campaign(request, state_network_id, access_token, refresh_token)

        payload, error_response = _fetch_patreon_identity(access_token)
        if error_response: return error_response

        user_data = payload.get('data', {})
        included_data = payload.get('included', [])
        raw_email = user_data.get('attributes', {}).get('email')
        if not raw_email:
            return HttpResponse("Patreon did not provide an email address.", status=400)

        email = raw_email.strip().lower()

        user, created = User.objects.get_or_create(username=email, defaults={
            'email': email,
            'first_name': user_data.get('attributes', {}).get('first_name', ''),
            'last_name': user_data.get('attributes', {}).get('last_name', '')
        })
        if created:
            logger.info(f"New user account created via Patreon OAuth: {email}")

        profile = _sync_patron_profile(user, user_data, included_data, current_network=request.network)

        # Trigger Discord Avatar Sync if a Discord ID exists
        if profile and profile.discord_id and hasattr(request, 'network'):
            membership = NetworkMembership.objects.filter(user=user, network=request.network).first()
            if membership:
                from ..tasks import task_sync_discord_avatar
                task_sync_discord_avatar.delay(profile.discord_id, membership.id)

        _secure_login(request, user)
        return redirect('home')

    except Exception as e:
        logger.error(f"Critical error during Patreon callback: {str(e)}", exc_info=True)
        return HttpResponse(f"Error: {str(e)}", status=500)


def logout_view(request):
    from django.contrib.auth import logout
    logout(request)
    return redirect('home')


class LoginState(enum.Enum):
    LOOKUP        = 'lookup'
    AWAITING_EMAIL = 'awaiting_email'
    AWAITING_TOTP  = 'awaiting_totp'


_LOGIN_SESSION_KEY = 'recurly_login'


class RecurlyLoginView(View):
    """
    State machine for Recurly-based login.

    Session payload (stored under _LOGIN_SESSION_KEY):
        state          — LoginState value
        email          — the email being authenticated
        account_id     — Recurly account ID resolved at lookup
        is_second_factor — True when TOTP is the MFA second gate after email OTP
    """

    def dispatch(self, request, *args, **kwargs):
        if not getattr(request, 'network', None):
            logger.warning(f"[Recurly Auth] Hit without tenant network (host={request.get_host()})")
            return redirect('patreon_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        if request.GET.get('reset'):
            return self._reset(request)
        payload = request.session.get(_LOGIN_SESSION_KEY, {})
        state = payload.get('state')
        return render(request, 'pod_manager/login_request.html', {
            'pending_email':    payload.get('email') if state == LoginState.AWAITING_EMAIL.value else None,
            'pending_totp':     payload.get('email') if state == LoginState.AWAITING_TOTP.value else None,
            'is_second_factor': payload.get('is_second_factor', False),
        })

    def post(self, request):
        payload = request.session.get(_LOGIN_SESSION_KEY, {})
        state = payload.get('state', LoginState.LOOKUP.value)
        if state == LoginState.AWAITING_TOTP.value:
            return self._handle_totp(request, payload)
        if state == LoginState.AWAITING_EMAIL.value:
            return self._handle_email_otp(request, payload)
        return self._handle_lookup(request)

    # ------------------------------------------------------------------
    # Step handlers
    # ------------------------------------------------------------------

    def _reset(self, request):
        payload = request.session.pop(_LOGIN_SESSION_KEY, {})
        if payload.get('email'):
            _clear_otp_state(payload['email'])
        return redirect('recurly_login')

    def _handle_lookup(self, request):
        email = request.POST.get('email', '').strip().lower()
        ip = _client_ip(request)

        if _is_rate_limited(f"login:ip:{ip}", limit=10, window_seconds=600):
            logger.warning(f"[Recurly Auth] IP rate-limit hit: {ip}")
            messages.error(request, "Too many login attempts from this network. Please wait and try again.")
            return redirect('recurly_login')
        if email and _is_rate_limited(f"login:email:{email}", limit=5, window_seconds=600):
            logger.warning(f"[Recurly Auth] Email rate-limit hit: {email}")
            messages.error(request, "Too many login attempts for this address. Please wait and try again.")
            return redirect('recurly_login')

        try:
            logger.debug(f"[Recurly Auth] Lookup initiated for email: {email}")
            client = recurly.Client(settings.RECURLY_API_KEY)
            accounts = client.list_accounts(params={'email': email})
            account_id = next((acc.id for acc in accounts.items()), None)

            if not account_id:
                logger.warning(f"[Recurly Auth] Login failed: No Recurly account found for {email}")
                messages.error(request, "No active subscription found for that email address.")
                return redirect('recurly_login')

            user = User.objects.filter(username__iexact=email).first()
            has_totp = user and user.totpdevice_set.filter(confirmed=True).exists()
            logger.debug(f"[Recurly Auth] User found: {bool(user)} | has_totp: {has_totp}")
            cache.delete(f"recurly_otp_attempts:{email}")

            if has_totp:
                profile = getattr(user, 'patron_profile', None)
                is_mfa = profile and profile.totp_mode == PatronProfile.TOTP_MFA
                if is_mfa:
                    self._send_otp(request, email, account_id)
                    request.session[_LOGIN_SESSION_KEY] = {
                        'state': LoginState.AWAITING_EMAIL.value,
                        'email': email, 'account_id': account_id, 'is_second_factor': True,
                    }
                    messages.success(request, "Step 1 of 2: A 6-digit code has been sent to your email.")
                else:
                    cache.set(f"recurly_account_{email}", account_id, timeout=600)
                    request.session[_LOGIN_SESSION_KEY] = {
                        'state': LoginState.AWAITING_TOTP.value,
                        'email': email, 'account_id': account_id, 'is_second_factor': False,
                    }
                    messages.info(request, "Please enter your 6-digit Authenticator App code.")
            else:
                self._send_otp(request, email, account_id)
                request.session[_LOGIN_SESSION_KEY] = {
                    'state': LoginState.AWAITING_EMAIL.value,
                    'email': email, 'account_id': account_id, 'is_second_factor': False,
                }
                messages.success(request, "A 6-digit code has been sent to your email.")

        except Exception as e:
            logger.error(f"[Recurly Auth] Error during lookup for {email}: {e}")
            messages.error(request, "System error verifying account.")

        return redirect('recurly_login')

    def _handle_email_otp(self, request, payload):
        email = payload['email']
        is_second_factor = payload.get('is_second_factor', False)

        user_otp = request.POST.get('otp', '').strip()
        cached_data = cache.get(f"recurly_otp_{email}")

        if not cached_data:
            messages.error(request, "Code has expired. Please request a new one.")
            request.session.pop(_LOGIN_SESSION_KEY, None)
            return redirect('recurly_login')

        correct_otp, account_id = cached_data.split('|')

        if _hmac.compare_digest(user_otp, correct_otp):
            _clear_otp_state(email)
            if is_second_factor:
                # Email verified — advance to TOTP as the second gate
                request.session[_LOGIN_SESSION_KEY] = {
                    'state': LoginState.AWAITING_TOTP.value,
                    'email': email, 'account_id': account_id, 'is_second_factor': True,
                }
                messages.info(request, "Step 2 of 2: Please enter your Authenticator code.")
                return redirect('recurly_login')
            return self._complete_login(request, email, account_id, "Successfully logged in!")
        else:
            attempts = _record_otp_failure(email)
            if attempts >= MAX_OTP_ATTEMPTS:
                _clear_otp_state(email)
                request.session.pop(_LOGIN_SESSION_KEY, None)
                logger.warning(f"[Recurly Auth] Email OTP attempts exhausted for {email}")
                messages.error(request, "Too many failed attempts. Please request a new code.")
            else:
                messages.error(request, f"Invalid code. {MAX_OTP_ATTEMPTS - attempts} attempts left.")
            return redirect('recurly_login')

    def _handle_totp(self, request, payload):
        email = payload['email']
        account_id = payload.get('account_id') or cache.get(f"recurly_account_{email}")
        is_second_factor = payload.get('is_second_factor', False)

        # Fallback to email is only offered in replace mode (not during MFA second factor)
        if request.POST.get('fallback_to_email') and not is_second_factor:
            return self._handle_totp_fallback(request, email)

        user = User.objects.filter(email=email).first()
        if not account_id or not user:
            messages.error(request, "Session expired. Please start over.")
            request.session.pop(_LOGIN_SESSION_KEY, None)
            return redirect('recurly_login')

        device = user.totpdevice_set.filter(confirmed=True).first()
        user_otp = request.POST.get('otp', '').strip()

        if device and device.verify_token(user_otp):
            _clear_otp_state(email)
            request.session.pop(_LOGIN_SESSION_KEY, None)
            suffix = " (MFA)" if is_second_factor else " via Authenticator"
            return self._complete_login(request, email, account_id, f"Successfully logged in{suffix}!", user=user)
        else:
            attempts = _record_otp_failure(email)
            if attempts >= MAX_OTP_ATTEMPTS:
                _clear_otp_state(email)
                request.session.pop(_LOGIN_SESSION_KEY, None)
                logger.warning(f"[Recurly Auth] TOTP attempts exhausted for {email}")
                messages.error(request, "Too many failed attempts. Please start over.")
            else:
                messages.error(request, f"Invalid Authenticator code. {MAX_OTP_ATTEMPTS - attempts} attempts left.")
            return redirect('recurly_login')

    def _handle_totp_fallback(self, request, email):
        if _is_rate_limited(f"login:fallback:{email}", limit=3, window_seconds=600):
            logger.warning(f"[Recurly Auth] Fallback rate-limit hit for {email}")
            messages.error(request, "Too many fallback requests. Please wait and try again.")
            return redirect('recurly_login')
        try:
            client = recurly.Client(settings.RECURLY_API_KEY)
            accounts = client.list_accounts(params={'email': email})
            account_id = next((acc.id for acc in accounts.items()), None)
            if account_id:
                self._send_otp(request, email, account_id)
                request.session[_LOGIN_SESSION_KEY] = {
                    'state': LoginState.AWAITING_EMAIL.value,
                    'email': email, 'account_id': account_id, 'is_second_factor': False,
                }
                messages.success(request, "Authenticator bypassed. A 6-digit code has been sent to your email.")
            else:
                request.session.pop(_LOGIN_SESSION_KEY, None)
                messages.error(request, "Session expired. Please start over.")
        except Exception as e:
            logger.error(f"[Recurly Auth] Exception in TOTP fallback for {email}: {e}", exc_info=True)
            request.session.pop(_LOGIN_SESSION_KEY, None)
            messages.error(request, "System error during fallback. Please start over.")
        return redirect('recurly_login')

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _send_otp(self, request, email, account_id):
        otp = f"{secrets.randbelow(900_000) + 100_000}"
        cache.set(f"recurly_otp_{email}", f"{otp}|{account_id}", timeout=600)
        from ..tasks import task_send_otp_email
        task_send_otp_email.delay(email, otp, request.network.name, request.network.theme_config)
        logger.debug(f"[Recurly Auth] OTP sent for {email}")

    def _complete_login(self, request, email, account_id, success_msg, user=None):
        if user is None:
            user, _ = User.objects.get_or_create(username=email, defaults={'email': email})
        profile, _ = PatronProfile.objects.get_or_create(user=user)
        if profile.recurly_account_code != account_id:
            profile.recurly_account_code = account_id
            profile.save()
        if not sync_recurly_plans_for_profile(profile, account_id):
            messages.warning(request, "Logged in, but could not sync latest subscription data.")
        _secure_login(request, user)
        messages.success(request, success_msg)
        return redirect('user_feeds')


# Expose as a function so existing imports, URL configs, and tests keep working.
recurly_login = RecurlyLoginView.as_view()


@staff_member_required
def start_impersonation(request, user_id):
    target_user = get_object_or_404(User, id=user_id)
    if target_user.is_superuser:
        messages.error(request, "Security restriction: You cannot impersonate a superuser.")
        return redirect('admin:auth_user_changelist')

    if target_user == request.user:
        messages.warning(request, "You are already logged in as yourself.")
        return redirect('admin:auth_user_changelist')

    request.session['impersonated_user_id'] = target_user.id
    messages.success(request, f"Now viewing site as {target_user.email}.")
    return redirect('home')


def stop_impersonation(request):
    # @staff_member_required can't be used here: ImpersonationMiddleware has already swapped
    # request.user to the impersonated (non-staff) user by the time the decorator runs.
    # Authorize against request.impersonator (the real staff user) instead.
    real_user = getattr(request, 'impersonator', None) or request.user
    if not getattr(real_user, 'is_authenticated', False) or not real_user.is_staff:
        return redirect('/admin/login/?next=/impersonate/stop/')

    if 'impersonated_user_id' in request.session:
        del request.session['impersonated_user_id']
        messages.success(request, "Impersonation ended. Welcome back.")
    return redirect('admin:auth_user_changelist')


@login_required(login_url='/login/')
def generate_qr_code(request):
    """Returns (or creates) an unverified device and renders its QR code."""
    device, _ = TOTPDevice.objects.get_or_create(user=request.user, confirmed=False, name="Vecto")

    url = device.config_url

    raw_key = base64.b32encode(device.bin_key).decode('utf-8')
    formatted_key = " ".join([raw_key[i:i+4] for i in range(0, len(raw_key), 4)])

    img = qrcode.make(url)
    stream = BytesIO()
    img.save(stream, format="PNG")
    qr_b64 = base64.b64encode(stream.getvalue()).decode('utf-8')
    qr_data_uri = f"data:image/png;base64,{qr_b64}"

    return render(request, 'pod_manager/verify_authenticator.html', {
        'qr_data_uri': qr_data_uri,
        'setup_key': formatted_key,
        'device_id': device.id
    })


@login_required(login_url='/login/')
def verify_authenticator(request):
    """Verifies the first 6-digit code to finalize setup."""
    if request.method == 'POST':
        code = request.POST.get('code')
        device_id = request.POST.get('device_id')

        device = TOTPDevice.objects.filter(id=device_id, user=request.user, confirmed=False).first()
        if device and device.verify_token(code):
            device.confirmed = True
            device.save()
            logger.info(f"Authenticator app confirmed for user {request.user.username}")
            messages.success(request, "Authenticator App successfully linked!")
            return redirect('user_profile')

        logger.warning(f"Failed authenticator verification attempt for user {request.user.username}")
        messages.error(request, "Invalid code. Please try again.")
    return redirect('generate_qr_code')


@login_required(login_url='/login/')
def remove_authenticator(request):
    """Destroys all devices, falling the user back to email login."""
    if request.method == 'POST':
        TOTPDevice.objects.filter(user=request.user).delete()
        logger.info(f"Authenticator app removed for user {request.user.username}")
        messages.success(request, "Authenticator App removed. You will now log in via Email Links.")
    return redirect('user_profile')
