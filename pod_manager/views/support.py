"""Public per-network Support Us page: the owner's cards (Patreon, merch,
donate, the commission queue, ...) in their chosen order. Edited in Creator
Settings > Support Page; owners can preview it before publishing."""
from django.http import Http404
from django.shortcuts import render
from django.urls import reverse

from ..models import CommissionSettings, SupportPage
from .commissions import network_or_404, is_owner as _is_owner, starting_price


def support_page(request):
    network = network_or_404(request)
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
                link.from_price = starting_price(network, commission_settings)
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
