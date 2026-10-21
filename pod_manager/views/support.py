"""Public per-network Support Us page and commission queue.

Support Us: the owner's cards (Patreon, merch, donate, the commission queue,
...) in their chosen order. The commission queue: what's coming up, what it
costs, and how it works. Both are edited in Creator Settings, and owners can
preview either before switching it on.
"""
from django.core.paginator import Paginator
from django.http import Http404
from django.shortcuts import render
from django.urls import reverse

from ..models import Commission, CommissionSettings, SupportPage
from ..services import commission_pricing as pricing
from .creator.publish import _require_owner

RELEASED_PAGE_SIZE = 10


def _network_or_404(request):
    network = getattr(request, 'network', None)
    if network is None:
        raise Http404("Nothing is configured for this domain.")
    return network


def _is_owner(request, network):
    return request.user.is_authenticated and _require_owner(request.user, network)


def _commission_settings_or_404(request):
    """(network, settings, is_owner) for the commission pages. 404 unless the
    queue is switched on; owners can preview it while it's off."""
    network = _network_or_404(request)
    settings = CommissionSettings.objects.filter(network=network).first()
    is_owner = _is_owner(request, network)
    if settings is None or (not settings.enabled and not is_owner):
        raise Http404("This network doesn't take commissions.")
    return network, settings, is_owner


def _starting_price(network, settings):
    """The cheapest active version's current price: the "from $X" figure."""
    n = pricing.queue_length(network)
    versions = network.commission_versions.filter(is_active=True)
    return min(price for _, price in pricing.price_table(settings, versions, n))


def support_page(request):
    network = _network_or_404(request)
    page = SupportPage.objects.filter(network=network).first() or SupportPage(network=network)
    is_owner = _is_owner(request, network)
    # Owners can preview the page before publishing it; everyone else 404s.
    if not page.enabled and not is_owner:
        raise Http404("This network has no support page.")

    commission_settings = CommissionSettings.objects.filter(network=network).first()
    commissions_open = bool(commission_settings and commission_settings.enabled)
    links = []
    for link in network.support_links.filter(is_visible=True):
        link.from_price = None
        if link.is_commission:
            # Listeners never see a card for a closed queue; owners do, flagged.
            if not commissions_open and not is_owner:
                continue
            link.target_url = reverse('commission_queue')
            link.commissions_open = commissions_open
            if commission_settings:
                link.from_price = _starting_price(network, commission_settings)
                link.currency = commission_settings.currency
        else:
            link.target_url = link.url
        links.append(link)

    return render(request, 'pod_manager/support.html', {
        'current_network': network,
        'page': page,
        'links': links,
        'is_owner': is_owner,
    })


def commission_queue(request):
    network, settings, is_owner = _commission_settings_or_404(request)
    n = pricing.queue_length(network)
    versions = network.commission_versions.filter(is_active=True)
    return render(request, 'pod_manager/commission_queue.html', {
        'current_network': network,
        'settings': settings,
        'is_owner': is_owner,
        'prices': pricing.price_table(settings, versions, n),
        'next_increase': pricing.next_increase(settings, n) if settings.show_price_hint else None,
        'queue': network.commissions.filter(status__in=Commission.ACTIVE_STATUSES),
        'released_count': network.commissions.filter(status=Commission.Status.RELEASED).count(),
    })


def commission_released(request):
    """One page of released commissions for the queue page's "Released" section,
    newest first. htmx appends each page; the "Load more" button replaces itself."""
    network, settings, _ = _commission_settings_or_404(request)
    released = (network.commissions.filter(status=Commission.Status.RELEASED)
                .select_related('episode').order_by('-released_at', '-requested_at', '-id'))
    page = Paginator(released, RELEASED_PAGE_SIZE).get_page(request.GET.get('page'))
    return render(request, 'pod_manager/_commission_released_page.html', {
        'current_network': network,
        'page': page,
    })
