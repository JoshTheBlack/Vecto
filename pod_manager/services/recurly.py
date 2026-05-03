import logging

from django.conf import settings
from recurly import Client

logger = logging.getLogger(__name__)


def sync_recurly_plans_for_profile(profile, account_id) -> bool:
    """Fetches active Recurly subscriptions and stores them on profile.
    Returns True on success, False if the API call failed."""
    client = Client(settings.RECURLY_API_KEY)
    active_plans = []
    try:
        subs = client.list_account_subscriptions(account_id=account_id)
        for sub in subs.items():
            if sub.state in ['active', 'in_trial', 'past_due']:
                active_plans.append(sub.plan.code)
        profile.active_recurly_plans = active_plans
        profile.save(update_fields=['active_recurly_plans'])
        return True
    except Exception as e:
        logger.error(f"[Recurly] Failed to fetch subscriptions for {account_id}: {e}")
        return False
