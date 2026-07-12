"""
Pure domain logic for access control and episode description rendering.
Used by views and feed builders; never touches request/response objects.
"""
from ..models import NetworkMembership


def _evaluate_access(user, podcast, network=None, *, membership=None, is_owner=None):
    """Check episode-level access for a user against a podcast's tier.

    Pass pre-fetched `membership` and `is_owner` to avoid per-call queries when
    iterating over many podcasts/episodes in a view loop.
    """
    if not user.is_authenticated:
        return False, False

    net = network or podcast.network

    if is_owner is None:
        is_owner = net.owners.filter(id=user.id).exists()
    if is_owner:
        return True, True

    if membership is None:
        membership = NetworkMembership.objects.filter(user=user, network=net).first()
    if not membership:
        return False, False

    # 1. Base check: Is this a free podcast?
    if not podcast.required_tier:
        return True, False

    req_cents = podcast.required_tier.minimum_cents
    if req_cents == 0:
        return True, False

    # --- 2. THE PATREON CHECK ---
    patreon_access = membership.is_active_patron and (membership.patreon_pledge_cents >= req_cents)

    # --- 3. THE RECURLY CHECK ---
    # Plans are global to the Recurly account, so they live on PatronProfile.
    recurly_access = False

    if podcast.required_tier:
        allowed_plans = podcast.required_tier.recurly_plan_codes
        profile = getattr(user, 'patron_profile', None)
        user_plans = profile.active_recurly_plans if profile else []

        if allowed_plans and user_plans:
            if any(plan in allowed_plans for plan in user_plans):
                recurly_access = True

    # 4. The Final Verdict
    has_access = patreon_access or recurly_access

    return has_access, False


def _evaluate_mix_access(user, network_mix, *, membership=None, is_owner=None):
    """Returns True if user can access this NetworkMix's gated feed.

    Pass pre-fetched `membership` and `is_owner` to avoid per-call queries
    when iterating over multiple mixes in a view loop.
    """
    if not user.is_authenticated:
        return not network_mix.required_tier or network_mix.required_tier.minimum_cents == 0

    if is_owner is None:
        is_owner = network_mix.network.owners.filter(id=user.id).exists()
    if is_owner:
        return True

    if not network_mix.required_tier or network_mix.required_tier.minimum_cents == 0:
        return True

    req_cents = network_mix.required_tier.minimum_cents

    # Patreon: pledge must be against the mix's own network membership
    if membership is None:
        membership = NetworkMembership.objects.filter(user=user, network=network_mix.network).first()
    if membership and membership.is_active_patron and membership.patreon_pledge_cents >= req_cents:
        return True

    # Recurly: plans are global on PatronProfile; codes are on the tier
    allowed_plans = network_mix.required_tier.recurly_plan_codes
    if allowed_plans:
        profile = getattr(user, 'patron_profile', None)
        user_plans = profile.active_recurly_plans if profile else []
        if user_plans and any(p in allowed_plans for p in user_plans):
            return True

    return False


def patron_profile_for_token(raw_token):
    """PatronProfile for a ``?auth=<feed_token>`` query value, or None.

    feed_token is a UUIDField, so filtering it on a malformed string raises
    ValidationError — a 500 for what is just a bad credential. These tokens
    ride on public-facing URLs (feed audio and transcript links), so junk
    values arrive from the open internet; treat anything unparseable exactly
    like an unknown token.
    """
    if not raw_token:
        return None
    from django.core.exceptions import ValidationError
    from ..models import PatronProfile
    try:
        return PatronProfile.objects.filter(feed_token=raw_token).first()
    except (ValidationError, ValueError, TypeError):
        return None


def can_view_transcript(episode, has_premium_access):
    """The single authority for transcript visibility, shared by the serve view,
    the episode page, and the feed builder so the rule can't drift between them.

    Premium access always serves. Otherwise the episode's ORIGIN podcast's
    `allow_public_transcripts` flag governs — never is_premium (ad-only public/
    private differences make it true almost everywhere), and never the feed being
    rendered (the fragment cache keys only on the origin podcast). The deferred
    episode-level public-audio term lands here, in this one place.
    """
    return bool(has_premium_access or episode.podcast.allow_public_transcripts)


def _build_episode_description(episode, has_access):
    desc = episode.clean_description or episode.raw_description
    footer_parts = []

    if has_access:
        if episode.podcast.show_footer_private: footer_parts.append(episode.podcast.show_footer_private)
        if episode.podcast.network.global_footer_private: footer_parts.append(episode.podcast.network.global_footer_private)
    else:
        if episode.podcast.show_footer_public: footer_parts.append(episode.podcast.show_footer_public)
        if episode.podcast.network.global_footer_public: footer_parts.append(episode.podcast.network.global_footer_public)

    if footer_parts:
        desc += "<br><br>" + "<br><br>".join(footer_parts)
    return desc
