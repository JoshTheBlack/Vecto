"""Emails about commissions: one template (email/commission.html), one kind
per event. Queued on commit so a rolled-back status change never emails.

Sent to the requester: received, approved, released, withdrawn, expired.
Sent to every network owner: new_request.
Never sent: anything about removal (the owner writes that one), watch dates,
or the Scheduled/Watched statuses.
"""
from urllib.parse import urlencode

from django.conf import settings
from django.core.mail import send_mail
from django.db import transaction
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils.html import strip_tags

from ..ingesters.default import _network_base_url

SUBJECTS = {
    'received':    "We got your commission request ({ref})",
    'approved':    "Your commission is in the queue ({ref})",
    'released':    "Your commission is out! ({ref})",
    'withdrawn':   "Your commission request was withdrawn ({ref})",
    'expired':     "Your commission request expired ({ref})",
    'new_request': "New commission request: {pick} ({ref})",
}
REQUESTER_KINDS = set(SUBJECTS) - {'new_request'}


def queue_commission_email(commission, kind):
    """Send `kind` about `commission` after the current transaction commits.
    A requester email with no address to send to is skipped."""
    if kind not in SUBJECTS:
        raise ValueError(f"Unknown commission email: {kind}")
    if kind in REQUESTER_KINDS and not commission.requester_email:
        return
    from ..tasks import task_send_commission_email
    transaction.on_commit(lambda: task_send_commission_email.delay(commission.id, kind))


def request_page_path(commission, **params):
    """The requester's page for this commission. A guest request's path
    carries its private token; signed-in requesters get in by logging in."""
    path = reverse('commission_detail', args=[commission.reference])
    if commission.access_token:
        params['t'] = commission.access_token
    return f"{path}?{urlencode(params)}" if params else path


def request_url(commission):
    return _network_base_url(commission.network) + request_page_path(commission)


def send_commission_email(commission, kind):
    """Render and send now (the Celery task calls this)."""
    network = commission.network
    base = _network_base_url(network)
    settings_row = getattr(network, 'commission_settings', None)
    if kind in REQUESTER_KINDS:
        recipients = [commission.requester_email]
    else:
        recipients = list(network.owners.exclude(email='').values_list('email', flat=True))
    if not recipients:
        return

    episode_url = None
    if commission.episode_id and commission.episode.is_published:
        episode_url = base + reverse('episode_detail', args=[commission.episode_id])

    html = render_to_string('pod_manager/email/commission.html', {
        'kind': kind,
        'c': commission,
        'network_name': network.name,
        'theme': network.theme_config or {},
        'currency': settings_row.currency if settings_row else 'USD',
        'expiry_days': settings_row.pending_expiry_days if settings_row else None,
        'request_url': request_url(commission),
        'queue_url': base + reverse('commission_queue'),
        'manage_url': f"{base}{reverse('creator_settings')}?network={network.slug}&tab=commissions",
        'episode_url': episode_url,
    })
    subject = f"{network.name}: " + SUBJECTS[kind].format(ref=commission.reference, pick=commission.pick[:60])
    send_mail(
        subject=subject,
        message=strip_tags(html),
        from_email=settings.DEFAULT_FROM_EMAIL,
        recipient_list=recipients,
        html_message=html,
        fail_silently=False,
    )
