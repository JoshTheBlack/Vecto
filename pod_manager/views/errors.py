import logging

from django.shortcuts import render

from ..models import NetworkMembership
from .listener.main import search_bar_podcasts

logger = logging.getLogger(__name__)


def custom_404(request, exception):
    network = getattr(request, 'network', None)
    entry = None
    if network:
        entry = network.notfound_entries.order_by('?').first()
        if entry and request.user.is_authenticated:
            # Only members get credit for the hunt — a random 404 hit from
            # someone who has never joined this network shouldn't spin up a
            # membership row just to log a sighting.
            membership = NetworkMembership.objects.filter(user=request.user, network=network).first()
            if membership:
                membership.seen_notfound_entries.add(entry)
    podcasts = []
    if network:
        try:
            podcasts = list(search_bar_podcasts(request, network))
        except Exception:
            # The error page must render even if the show list can't be built.
            logger.exception("404: could not build the search bar show list")
    return render(request, 'pod_manager/404.html', {
        'current_network': network,
        'entry': entry,
        # Feeds the shared dashboard search bar's show filter.
        'podcasts': podcasts,
    }, status=404)
