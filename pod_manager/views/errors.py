from django.shortcuts import render

from ..models import NetworkMembership


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
    return render(request, 'pod_manager/404.html', {
        'current_network': network,
        'entry': entry,
    }, status=404)
