"""Commission queue lifecycle: the status transitions and queue ordering that
every caller (Creator Settings today; requests and release automation later)
goes through, so the rules live in one place."""
from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from ..models import Commission

Status = Commission.Status


def next_position(network):
    """A position after every existing commission, so a new entry joins the end."""
    last = network.commissions.aggregate(m=Max('position'))['m']
    return 0 if last is None else last + 1


def set_status(commission, status):
    """Move an approved commission to another approved status (the owner's
    manual status select). Pending and closed entries have their own actions.
    Raises ValueError for anything else. Does not save."""
    if status not in Commission.PUBLIC_STATUSES or commission.status not in Commission.PUBLIC_STATUSES:
        raise ValueError(f"Can't change a {commission.status} commission to {status}.")
    if status == commission.status:
        return
    if status == Status.RELEASED:
        commission.released_at = commission.released_at or timezone.now()
    else:
        if commission.status == Status.RELEASED:
            # Back into the queue: it rejoins at the end.
            commission.position = next_position(commission.network)
        commission.released_at = None
    commission.status = status


def remove(commission, reason=''):
    """Take a commission off every public surface and out of the price (declined
    or refunded). Sends nothing; the owner emails the requester themselves."""
    if commission.status in Commission.CLOSED_STATUSES:
        raise ValueError("That commission is already closed.")
    commission.status = Status.REMOVED
    commission.removal_reason = reason
    commission.save(update_fields=['status', 'removal_reason', 'updated_at'])


def restore(commission):
    """Undo a removal: the commission rejoins the end of the queue."""
    if commission.status != Status.REMOVED:
        raise ValueError("Only a removed commission can be put back.")
    commission.status = Status.QUEUED
    commission.removal_reason = ''
    commission.position = next_position(commission.network)
    commission.save(update_fields=['status', 'removal_reason', 'position', 'updated_at'])


def sort_by_request_date(network):
    """Reset the active queue to request-date order (the default order)."""
    with transaction.atomic():
        rows = list(network.commissions.select_for_update()
                    .filter(status__in=Commission.ACTIVE_STATUSES)
                    .order_by('requested_at', 'id'))
        for position, row in enumerate(rows):
            row.position = position
        Commission.objects.bulk_update(rows, ['position'])
