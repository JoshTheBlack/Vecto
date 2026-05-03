"""
Pure domain logic for access control and episode description rendering.
Used by views and feed builders; never touches request/response objects.
"""
from ..models import NetworkMembership


def _evaluate_access(user, podcast, network=None):
    if not user.is_authenticated:
        return False, False

    net = network or podcast.network
    is_owner = net.owners.filter(id=user.id).exists()
    if is_owner:
        return True, True

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
