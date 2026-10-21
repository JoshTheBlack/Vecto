"""Creator-settings handlers for the network's commission queue: settings and
pricing, versions, and the owner's queue management.

Same contract as actions.py: each POST handler takes (request, current_network)
and returns None (the dispatcher redirects back to the tab) or a response.
"""
import datetime
import re
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from django.http import HttpResponse, HttpResponseBadRequest
from django.shortcuts import get_object_or_404
from django.utils import timezone

from ...models import Commission, CommissionSettings, CommissionVersion
from ...services import commission_pricing as pricing
from ...services import commissions as lifecycle
from ...services.ordering import apply_order, parse_id_list
from ...utils import sanitize_user_html

_CURRENCY = re.compile(r'^[A-Z]{3}$')
PREVIEW_STEPS = 5


class FormError(ValueError):
    """A user-facing validation message."""


def settings_for(network):
    """The network's CommissionSettings, unsaved defaults if it has none yet."""
    return (CommissionSettings.objects.filter(network=network).first()
            or CommissionSettings(network=network))


def _money(data, name, label, *, required=False, signed=False):
    raw = (data.get(name) or '').strip().replace(',', '').lstrip('$')
    if not raw:
        if required:
            raise FormError(f"{label} is required.")
        return None
    try:
        value = Decimal(raw).quantize(Decimal('0.01'))
    except InvalidOperation:
        raise FormError(f"{label} must be an amount, e.g. 500 or 12.50.")
    if not value.is_finite() or abs(value) >= Decimal('100000000'):
        raise FormError(f"{label} is out of range.")
    if value < 0 and not signed:
        raise FormError(f"{label} can't be negative.")
    return value


def _whole(data, name, label, *, minimum=0, required=False):
    raw = (data.get(name) or '').strip()
    if not raw:
        if required:
            raise FormError(f"{label} is required.")
        return None
    if not raw.isdigit() or int(raw) < minimum:
        raise FormError(f"{label} must be a whole number of at least {minimum}.")
    return int(raw)


def _date(data, name, label):
    raw = (data.get(name) or '').strip()
    if not raw:
        return None
    try:
        day = datetime.date.fromisoformat(raw)
    except ValueError:
        raise FormError(f"{label} must be a date.")
    # Noon, so the date reads the same in every US timezone.
    return timezone.make_aware(datetime.datetime.combine(day, datetime.time(12)))


def apply_pricing(data, settings):
    """Copy the pricing fields from `data` onto `settings` (not saved). Shared
    by the save handler and the live preview, so both read the form the same way."""
    settings.base_price = _money(data, 'base_price', 'Base price', required=True)
    settings.step_amount = _money(data, 'step_amount', 'Step amount') or Decimal('0.00')
    settings.step_every = _whole(data, 'step_every', '"Every"', minimum=1) or 1
    settings.min_price = _money(data, 'min_price', 'Minimum price')
    settings.max_price = _money(data, 'max_price', 'Maximum price')
    settings.override_price = _money(data, 'override_price', 'Override price')
    if (settings.min_price is not None and settings.max_price is not None
            and settings.max_price < settings.min_price):
        raise FormError("The maximum price can't be below the minimum.")


def handle_update_commission_settings(request, current_network):
    settings = settings_for(current_network)
    data = request.POST
    try:
        apply_pricing(data, settings)
        currency = (data.get('currency') or 'USD').strip().upper()
        if not _CURRENCY.match(currency):
            raise FormError("Currency must be a 3-letter code, e.g. USD.")
        paypal_email = (data.get('paypal_email') or '').strip()
        if paypal_email:
            try:
                validate_email(paypal_email)
            except ValidationError:
                raise FormError("The PayPal email isn't a valid address.")
        settings.cadence_days = _whole(data, 'cadence_days', 'Cadence', minimum=1)
        settings.pending_expiry_days = _whole(data, 'pending_expiry_days', 'Expiry', minimum=1) or 7
    except FormError as e:
        messages.error(request, str(e))
        return

    podcast_id = data.get('default_podcast')
    settings.default_podcast = (current_network.podcasts.filter(id=podcast_id).first()
                                if podcast_id and podcast_id.isdigit() else None)
    settings.enabled = data.get('enabled') == 'on'
    settings.currency = currency
    settings.paypal_email = paypal_email
    settings.page_title = data.get('page_title', '').strip()[:100] or 'Commission a Podcast'
    settings.intro = sanitize_user_html(data.get('intro', ''))
    settings.details = sanitize_user_html(data.get('details', ''))
    settings.request_terms = sanitize_user_html(data.get('request_terms', ''))
    settings.show_price_hint = data.get('show_price_hint') == 'on'
    settings.save()
    messages.success(request, "Commission settings saved.")


def price_preview_context(network, settings):
    n = pricing.queue_length(network)
    versions = network.commission_versions.filter(is_active=True)
    return {
        'preview_settings': settings,
        'preview_queue_length': n,
        'preview_prices': pricing.price_table(settings, versions, n),
        'preview_steps': [
            (until, n + until, base) for until, base in pricing.price_steps(settings, n, PREVIEW_STEPS)
        ],
    }


# --- Versions ---------------------------------------------------------------

def _apply_version_fields(data, version):
    name = (data.get('name') or '').strip()
    if not name:
        raise FormError("A version needs a name.")
    version.name = name[:100]
    version.description = (data.get('description') or '').strip()[:255]
    version.price_adjustment = _money(data, 'price_adjustment', 'Price adjustment', signed=True) or Decimal('0.00')
    version.is_active = data.get('is_active') == 'on'


def handle_add_commission_version(request, current_network):
    version = CommissionVersion(network=current_network)
    try:
        _apply_version_fields(request.POST, version)
    except FormError as e:
        messages.error(request, str(e))
        return
    version.position = current_network.commission_versions.count()
    version.save()
    messages.success(request, f"Version '{version.name}' added.")


def handle_edit_commission_version(request, current_network):
    version = get_object_or_404(CommissionVersion, id=request.POST.get('version_id'), network=current_network)
    try:
        _apply_version_fields(request.POST, version)
    except FormError as e:
        messages.error(request, str(e))
        return
    version.save()
    messages.success(request, f"Version '{version.name}' saved.")


def handle_delete_commission_version(request, current_network):
    version = get_object_or_404(CommissionVersion, id=request.POST.get('version_id'), network=current_network)
    # Commissions keep their version_name snapshot; the FK just goes null.
    name = version.name
    version.delete()
    messages.warning(request, f"Version '{name}' deleted.")


def handle_reorder_commission_versions(request, current_network):
    return _reorder(request, current_network.commission_versions.all())


# --- Queue ------------------------------------------------------------------

def _apply_commission_fields(data, commission, network):
    """The owner's edit fields, shared by "Add entry" and each row's edit form."""
    pick = (data.get('pick') or '').strip()
    if not pick:
        raise FormError("A commission needs a pick.")
    commission.pick = pick[:200]
    commission.credit_name = (data.get('credit_name') or '').strip()[:100]
    commission.credit_public = data.get('credit_public') == 'on'
    commission.quoted_price = _money(data, 'quoted_price', 'Price')
    commission.owner_notes = (data.get('owner_notes') or '').strip()
    commission.requested_at = _date(data, 'requested_at', 'Request date') or commission.requested_at

    version_id = data.get('version') or ''
    version = network.commission_versions.filter(id=version_id).first() if version_id.isdigit() else None
    if version != commission.version or (version and not commission.version_name):
        commission.version = version
        commission.version_name = version.name if version else ''

    email = (data.get('requester_email') or '').strip()
    if email:
        requester = User.objects.filter(email__iexact=email).first()
        if requester is None:
            raise FormError(f"No account uses {email}. Leave it blank for an entry with no account.")
        commission.requester = requester
    elif 'requester_email' in data:
        commission.requester = None


def _approved_status(data, field='status'):
    status = data.get(field) or Commission.Status.QUEUED
    if status not in Commission.PUBLIC_STATUSES:
        raise FormError("Pick Queued, Scheduled, Watched or Released.")
    return status


def handle_add_commission(request, current_network):
    """A manual or backfilled entry: created already approved, no PayPal step."""
    now = timezone.now()
    commission = Commission(network=current_network, approved_at=now, requested_at=now,
                            status=Commission.Status.QUEUED,
                            position=lifecycle.next_position(current_network))
    try:
        _apply_commission_fields(request.POST, commission, current_network)
        lifecycle.set_status(commission, _approved_status(request.POST))
    except (FormError, ValueError) as e:
        messages.error(request, str(e))
        return
    commission.save()
    messages.success(request, f"'{commission.pick}' added to the queue.")


def handle_edit_commission(request, current_network):
    commission = get_object_or_404(Commission, id=request.POST.get('commission_id'), network=current_network)
    try:
        _apply_commission_fields(request.POST, commission, current_network)
        if commission.status in Commission.PUBLIC_STATUSES:
            lifecycle.set_status(commission, _approved_status(request.POST))
    except (FormError, ValueError) as e:
        messages.error(request, str(e))
        return
    commission.save()
    messages.success(request, f"'{commission.pick}' saved.")


def handle_set_commission_status(request, current_network):
    """The status select on each queue row (no other fields)."""
    commission = get_object_or_404(Commission, id=request.POST.get('commission_id'), network=current_network)
    try:
        lifecycle.set_status(commission, _approved_status(request.POST))
    except (FormError, ValueError) as e:
        messages.error(request, str(e))
        return
    commission.save()
    messages.success(request, f"'{commission.pick}' is now {commission.get_status_display()}.")


def handle_remove_commission(request, current_network):
    commission = get_object_or_404(Commission, id=request.POST.get('commission_id'), network=current_network)
    try:
        lifecycle.remove(commission, (request.POST.get('removal_reason') or '').strip())
    except ValueError as e:
        messages.error(request, str(e))
        return
    messages.warning(request, f"'{commission.pick}' removed from the queue. Vecto sent nothing; "
                              "refund it in PayPal and email them yourself if needed.")


def handle_restore_commission(request, current_network):
    commission = get_object_or_404(Commission, id=request.POST.get('commission_id'), network=current_network)
    try:
        lifecycle.restore(commission)
    except ValueError as e:
        messages.error(request, str(e))
        return
    messages.success(request, f"'{commission.pick}' is back at the end of the queue.")


def handle_delete_commission(request, current_network):
    commission = get_object_or_404(Commission, id=request.POST.get('commission_id'), network=current_network,
                                   status__in=Commission.CLOSED_STATUSES)
    pick = commission.pick
    commission.delete()
    messages.warning(request, f"'{pick}' deleted permanently.")


def handle_reorder_commissions(request, current_network):
    return _reorder(request, current_network.commissions.filter(status__in=Commission.ACTIVE_STATUSES))


def handle_sort_commissions_by_date(request, current_network):
    lifecycle.sort_by_request_date(current_network)
    messages.success(request, "Queue sorted by request date.")


def _reorder(request, queryset):
    """Drag-to-reorder endpoint (sortable_list.js): 204, or 400 when the posted
    order doesn't name exactly the rows in `queryset`."""
    ids = parse_id_list(request.POST.get('order'))
    if ids is None or not apply_order(queryset, ids):
        return HttpResponseBadRequest("Order must list every row exactly once.")
    return HttpResponse(status=204)


COMMISSION_ACTION_HANDLERS = {
    'update_commission_settings':   handle_update_commission_settings,
    'add_commission_version':       handle_add_commission_version,
    'edit_commission_version':      handle_edit_commission_version,
    'delete_commission_version':    handle_delete_commission_version,
    'reorder_commission_versions':  handle_reorder_commission_versions,
    'add_commission':               handle_add_commission,
    'edit_commission':              handle_edit_commission,
    'set_commission_status':        handle_set_commission_status,
    'remove_commission':            handle_remove_commission,
    'restore_commission':           handle_restore_commission,
    'delete_commission':            handle_delete_commission,
    'reorder_commissions':          handle_reorder_commissions,
    'sort_commissions_by_date':     handle_sort_commissions_by_date,
}
