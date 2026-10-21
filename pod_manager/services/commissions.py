"""Commission queue lifecycle: the status transitions and queue ordering that
every caller (the request form, Creator Settings, PayPal's IPN, the expiry
task, login) goes through, so the rules and the emails live in one place."""
import datetime

from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from ..models import Commission, CommissionSettings, new_commission_access_token
from . import commission_pricing as pricing
from .commission_emails import queue_commission_email

Status = Commission.Status

# Pending requests one requester (account or guest email) may have open on a
# network at once.
MAX_PENDING_PER_REQUESTER = 10


class RequestError(ValueError):
    """A request the requester can fix; the message is shown to them."""


def next_position(network):
    """A position after every existing commission, so a new entry joins the end."""
    last = network.commissions.aggregate(m=Max('position'))['m']
    return 0 if last is None else last + 1


def expires_at(commission, settings):
    """When an unpaid pending request expires, or None if it can't."""
    if commission.status != Status.PENDING or commission.is_paid or not commission.pending_since:
        return None
    return commission.pending_since + datetime.timedelta(days=settings.pending_expiry_days)


def pending_count_for(network, *, user=None, email=''):
    """Open pending requests on `network` by this account, or this guest email."""
    pending = network.commissions.filter(status=Status.PENDING)
    if user is not None and user.is_authenticated:
        return pending.filter(requester=user).count()
    return pending.filter(requester__isnull=True, requester_email__iexact=email).count()


def create_request(network, settings, *, user, name, email, pick, version, credit_name,
                   credit_public, notes):
    """A requester's commission request: pending, its price locked at today's
    queue length. `user` is None for a guest, who gets a private-link token.
    Raises RequestError for a limit the requester can act on."""
    signed_in = user is not None and user.is_authenticated
    if signed_in:
        name = name or user.get_full_name() or user.email
        email = user.email
    if pending_count_for(network, user=user if signed_in else None, email=email) >= MAX_PENDING_PER_REQUESTER:
        raise RequestError(f"You already have {MAX_PENDING_PER_REQUESTER} requests waiting. "
                           "Pay for or withdraw one before making another.")

    now = timezone.now()
    commission = Commission.objects.create(
        network=network,
        requester=user if signed_in else None,
        requester_name=name[:100],
        requester_email=email,
        access_token=None if signed_in else new_commission_access_token(),
        pick=pick[:200],
        requester_notes=notes,
        version=version,
        version_name=version.name if version else '',
        quoted_price=pricing.price_for(settings, version, pricing.queue_length(network)),
        credit_name=(credit_name or name)[:100],
        credit_public=credit_public,
        status=Status.PENDING,
        requested_at=now,
        pending_since=now,
    )
    queue_commission_email(commission, 'received')
    queue_commission_email(commission, 'new_request')
    return commission


def approve(commission):
    """The owner accepts a pending request: it joins the end of the queue."""
    if commission.status != Status.PENDING:
        raise ValueError("Only a request awaiting approval can be approved.")
    commission.status = Status.QUEUED
    commission.approved_at = timezone.now()
    commission.position = next_position(commission.network)
    commission.save(update_fields=['status', 'approved_at', 'position', 'updated_at'])
    queue_commission_email(commission, 'approved')


def mark_paid(commission):
    """The owner confirms a payment Vecto didn't see (stops expiry)."""
    if commission.status != Status.PENDING:
        raise ValueError("Only a request awaiting approval can be marked paid.")
    if not commission.is_paid:
        commission.paid_marked_at = timezone.now()
        commission.save(update_fields=['paid_marked_at', 'updated_at'])


def withdraw(commission):
    """The requester cancels a request that hasn't been approved yet."""
    if commission.status != Status.PENDING:
        raise ValueError("Only a request that hasn't been approved can be withdrawn.")
    commission.status = Status.WITHDRAWN
    commission.save(update_fields=['status', 'updated_at'])
    queue_commission_email(commission, 'withdrawn')


def set_status(commission, status):
    """Move an approved commission to another approved status (the owner's
    manual status select). Pending and closed entries have their own actions.
    Raises ValueError for anything else. Does not save. Returns True when this
    newly released it: the caller saves, then calls announce_release."""
    if status not in Commission.PUBLIC_STATUSES or commission.status not in Commission.PUBLIC_STATUSES:
        raise ValueError(f"Can't change a {commission.status} commission to {status}.")
    if status == commission.status:
        return False
    released = status == Status.RELEASED
    if released:
        commission.released_at = commission.released_at or timezone.now()
    else:
        if commission.status == Status.RELEASED:
            # Back into the queue: it rejoins at the end.
            commission.position = next_position(commission.network)
        commission.released_at = None
    commission.status = status
    return released


def announce_release(commission):
    """Email the requester that their commission is out (after it's saved)."""
    queue_commission_email(commission, 'released')


def remove(commission, reason=''):
    """Take a commission off every public surface and out of the price (declined
    or refunded). Sends nothing; the owner emails the requester themselves."""
    if commission.status in Commission.CLOSED_STATUSES:
        raise ValueError("That commission is already closed.")
    commission.status = Status.REMOVED
    commission.removal_reason = reason
    commission.save(update_fields=['status', 'removal_reason', 'updated_at'])


def restore(commission):
    """Undo a closing: a removed commission rejoins the end of the queue; an
    expired request goes back to awaiting approval with its clock restarted."""
    if commission.status == Status.REMOVED:
        commission.status = Status.QUEUED
        commission.removal_reason = ''
        commission.position = next_position(commission.network)
        commission.save(update_fields=['status', 'removal_reason', 'position', 'updated_at'])
    elif commission.status == Status.EXPIRED:
        commission.status = Status.PENDING
        commission.pending_since = timezone.now()
        commission.save(update_fields=['status', 'pending_since', 'updated_at'])
    else:
        raise ValueError("Only a removed or expired commission can be put back.")


def expire_stale_requests(now=None):
    """Expire every unpaid pending request past its network's window. Returns
    the expired commissions (each requester is emailed)."""
    now = now or timezone.now()
    expired = []
    for settings in CommissionSettings.objects.select_related('network'):
        cutoff = now - datetime.timedelta(days=settings.pending_expiry_days)
        stale = settings.network.commissions.filter(
            status=Status.PENDING, paid_marked_at__isnull=True, pending_since__lt=cutoff)
        for commission in stale:
            commission.status = Status.EXPIRED
            commission.save(update_fields=['status', 'updated_at'])
            queue_commission_email(commission, 'expired')
            expired.append(commission)
    return expired


def sort_by_request_date(network):
    """Reset the active queue to request-date order (the default order)."""
    with transaction.atomic():
        rows = list(network.commissions.select_for_update()
                    .filter(status__in=Commission.ACTIVE_STATUSES)
                    .order_by('requested_at', 'id'))
        for position, row in enumerate(rows):
            row.position = position
        Commission.objects.bulk_update(rows, ['position'])


def attach_guest_requests(user):
    """Link every guest request made with `user`'s email (any network, any
    case) to their account. Call ONLY after a login that proves they own the
    email: a guest's address is typed in and unverified. Returns the count."""
    if not user.email:
        return 0
    return (Commission.objects
            .filter(requester__isnull=True, access_token__isnull=False, requester_email__iexact=user.email)
            .update(requester=user))
