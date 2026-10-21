"""Public commission pages: the queue, the request form, each requester's
request page (and its PayPal button), and PayPal's IPN endpoint.

Requesting needs no account. Guests reach their request through a private
link (?t=<access_token>); signing in is encouraged, never required. Owners
manage everything in Creator Settings > Commissions.
"""
import logging

from django.contrib import messages
from django.core.exceptions import ValidationError
from django.core.paginator import Paginator
from django.core.validators import validate_email
from django.http import Http404, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from ..models import Commission, CommissionSettings
from ..security import _client_ip, _is_rate_limited
from ..services import commission_pricing as pricing
from ..services import commissions as lifecycle
from ..services.commission_emails import request_page_path
from ..services.paypal_ipn import CHECKOUT_URL, handle_ipn
from .creator.publish import _require_owner

logger = logging.getLogger(__name__)

RELEASED_PAGE_SIZE = 10
# Guest submissions allowed from one IP per hour.
GUEST_REQUESTS_PER_IP_PER_HOUR = 10
# Requester-visible states shown under "Your requests".
YOUR_REQUEST_STATUSES = (Commission.Status.PENDING,) + Commission.CLOSED_STATUSES


def network_or_404(request):
    network = getattr(request, 'network', None)
    if network is None:
        raise Http404("Nothing is configured for this domain.")
    return network


def is_owner(request, network):
    return request.user.is_authenticated and _require_owner(request.user, network)


def commission_settings_or_404(request):
    """(network, settings, is_owner) for the commission pages. 404 unless the
    queue is switched on; owners can preview it while it's off."""
    network = network_or_404(request)
    settings = CommissionSettings.objects.filter(network=network).first()
    owner = is_owner(request, network)
    if settings is None or (not settings.enabled and not owner):
        raise Http404("This network doesn't take commissions.")
    return network, settings, owner


def starting_price(network, settings):
    """The cheapest active version's current price: the "from $X" figure."""
    n = pricing.queue_length(network)
    versions = network.commission_versions.filter(is_active=True)
    return min(price for _, price in pricing.price_table(settings, versions, n))


def commission_queue(request):
    network, settings, owner = commission_settings_or_404(request)
    n = pricing.queue_length(network)
    versions = network.commission_versions.filter(is_active=True)
    your_requests = []
    if request.user.is_authenticated:
        your_requests = network.commissions.filter(requester=request.user, status__in=YOUR_REQUEST_STATUSES) \
                                           .order_by('-requested_at')
        for c in your_requests:
            c.expires = lifecycle.expires_at(c, settings)
    return render(request, 'pod_manager/commission_queue.html', {
        'current_network': network,
        'settings': settings,
        'is_owner': owner,
        'prices': pricing.price_table(settings, versions, n),
        'next_increase': pricing.next_increase(settings, n) if settings.show_price_hint else None,
        'queue': network.commissions.filter(status__in=Commission.ACTIVE_STATUSES),
        'released_count': network.commissions.filter(status=Commission.Status.RELEASED).count(),
        'your_requests': your_requests,
        'awaiting_approval': network.commissions.filter(status=Commission.Status.PENDING).count() if owner else 0,
    })


def commission_released(request):
    """One page of released commissions for the queue page's "Released" section,
    newest first. htmx appends each page; the "Load more" button replaces itself."""
    network, settings, _ = commission_settings_or_404(request)
    released = (network.commissions.filter(status=Commission.Status.RELEASED)
                .select_related('episode').order_by('-released_at', '-requested_at', '-id'))
    page = Paginator(released, RELEASED_PAGE_SIZE).get_page(request.GET.get('page'))
    return render(request, 'pod_manager/_commission_released_page.html', {
        'current_network': network,
        'page': page,
    })


def _read_request_form(post, versions, settings, signed_in):
    """The request form's values and a list of problems with them."""
    errors = []
    data = {
        'name': post.get('name', '').strip()[:100],
        'email': post.get('email', '').strip(),
        'pick': post.get('pick', '').strip()[:200],
        'credit_name': post.get('credit_name', '').strip()[:100],
        'credit_public': post.get('credit_public') == 'on',
        'notes': post.get('notes', '').strip()[:2000],
        'version': None,
    }
    if not data['pick']:
        errors.append("Tell us what you'd like us to watch.")
    if versions:
        version_id = post.get('version', '')
        data['version'] = next((v for v in versions if str(v.id) == version_id), None)
        if data['version'] is None:
            errors.append("Pick a version.")
    if not signed_in:
        if not data['name']:
            errors.append("Your name is required (or sign in).")
        try:
            validate_email(data['email'])
        except ValidationError:
            errors.append("A valid email is required, so we can reach you (or sign in).")
    if settings.request_terms and post.get('accept_terms') != 'on':
        errors.append("Please accept the terms.")
    return data, errors


def commission_request(request):
    """The request form. Open to guests; signed-in requesters skip the name and
    email fields and get their requests listed on the queue page."""
    network, settings, _ = commission_settings_or_404(request)
    versions = list(network.commission_versions.filter(is_active=True))
    signed_in = request.user.is_authenticated
    n = pricing.queue_length(network)
    data, errors = {'credit_public': True}, []

    if request.method == 'POST':
        data, errors = _read_request_form(request.POST, versions, settings, signed_in)
        if not errors and not signed_in and _is_rate_limited(
                f"commission_request:{_client_ip(request)}", GUEST_REQUESTS_PER_IP_PER_HOUR, 3600):
            errors.append("Too many requests from here. Please try again in an hour, or sign in.")
        if not errors:
            try:
                commission = lifecycle.create_request(
                    network, settings, user=request.user if signed_in else None,
                    name=data['name'], email=data['email'], pick=data['pick'], version=data['version'],
                    credit_name=data['credit_name'], credit_public=data['credit_public'], notes=data['notes'])
            except lifecycle.RequestError as e:
                errors.append(str(e))
            else:
                return redirect(request_page_path(commission))

    return render(request, 'pod_manager/commission_request.html', {
        'current_network': network,
        'settings': settings,
        'prices': pricing.price_table(settings, versions, n),
        'form': data,
        'errors': errors,
        'login_next': reverse('commission_request'),
    })


def _commission_for_viewer(request, reference):
    """(network, settings, commission, is_owner) when the viewer may see this
    request: an owner, the signed-in requester, or a guest with its token.
    Everyone else gets the same 404 as a reference that doesn't exist."""
    network, settings, owner = commission_settings_or_404(request)
    commission = network.commissions.filter(reference=reference).first()
    token = request.GET.get('t') or request.POST.get('t')
    if commission is None or not (owner or commission.can_be_viewed_by(request.user, token)):
        raise Http404("No such commission request.")
    return network, settings, commission, owner


def paypal_form_fields(request, commission, settings):
    """The hidden fields of the "Pay with PayPal" form (Website Payments
    Standard, cmd=_xclick, POSTed to PayPal as PayPal documents it): the locked
    price, the reference as `invoice` for matching, and notify_url so PayPal
    confirms the payment by IPN."""
    page = request.build_absolute_uri(request_page_path(commission))
    sep = '&' if '?' in page else '?'
    item = f"{commission.network.name} commission: {commission.pick}"
    if commission.version_name:
        item += f" ({commission.version_name})"
    fields = {
        'cmd': '_xclick',
        'business': settings.active_paypal_email,
        'item_name': item[:127],
        'amount': f"{commission.quoted_price:.2f}",
        'currency_code': settings.currency,
        'invoice': commission.reference,
        'custom': str(commission.id),
        'no_shipping': '1',
        'charset': 'utf-8',
        'rm': '1',
        'notify_url': request.build_absolute_uri(reverse('commission_paypal_ipn')),
        'return': f"{page}{sep}paid=1",
        'cancel_return': f"{page}{sep}cancelled=1",
    }
    return fields


def commission_detail(request, reference):
    """A requester's page for one request: its status, and the PayPal button
    while it's unpaid. Guests reach it by their private link."""
    network, settings, commission, owner = _commission_for_viewer(request, reference)
    can_pay = (commission.status == Commission.Status.PENDING and not commission.is_paid
               and commission.quoted_price and settings.active_paypal_email)
    return render(request, 'pod_manager/commission_detail.html', {
        'current_network': network,
        'settings': settings,
        'c': commission,
        'is_owner': owner,
        'token': commission.access_token if request.GET.get('t') or request.POST.get('t') else '',
        'expires': lifecycle.expires_at(commission, settings),
        'can_pay': can_pay,
        'paypal_url': CHECKOUT_URL[settings.paypal_sandbox],
        'paypal_fields': paypal_form_fields(request, commission, settings) if can_pay else {},
        'just_paid': request.GET.get('paid') == '1',
        'just_cancelled': request.GET.get('cancelled') == '1',
        'is_guest_link': not commission.requester_id,
    })


@require_POST
def commission_withdraw(request, reference):
    network, settings, commission, _ = _commission_for_viewer(request, reference)
    try:
        lifecycle.withdraw(commission)
    except ValueError as e:
        messages.error(request, str(e))
    else:
        messages.success(request, "Your request has been withdrawn.")
    return redirect(request_page_path(commission))


@csrf_exempt
@require_POST
def commission_paypal_ipn(request):
    """PayPal's Instant Payment Notification. Unauthenticated by nature, so
    nothing is trusted until PayPal verifies it (services/paypal_ipn). Always
    200, so PayPal doesn't keep retrying a message we've already judged."""
    network = getattr(request, 'network', None)
    if network is None:
        return HttpResponse(status=200)
    if _is_rate_limited(f"paypal_ipn:{_client_ip(request)}", 120, 60):
        return HttpResponse(status=429)
    outcome = handle_ipn(network, request.body, request.POST)
    logger.info(f"[PayPal IPN] {network.slug} invoice={request.POST.get('invoice', '')} "
                f"status={request.POST.get('payment_status', '')}: {outcome}")
    return HttpResponse(status=200)
