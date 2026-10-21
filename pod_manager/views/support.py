"""Public per-network Support Us page: the owner's cards (Patreon, merch,
donate, ...) in their chosen order. Edited in Creator Settings > Support Page."""
from django.http import Http404
from django.shortcuts import render

from ..models import SupportPage
from .creator.publish import _require_owner


def support_page(request):
    network = getattr(request, 'network', None)
    if network is None:
        raise Http404("No support page is configured for this domain.")

    page = SupportPage.objects.filter(network=network).first() or SupportPage(network=network)
    is_owner = request.user.is_authenticated and _require_owner(request.user, network)
    # Owners can preview the page before publishing it; everyone else 404s.
    if not page.enabled and not is_owner:
        raise Http404("This network has no support page.")

    return render(request, 'pod_manager/support.html', {
        'current_network': network,
        'page': page,
        'links': network.support_links.filter(is_visible=True),
        'is_owner': is_owner,
    })
