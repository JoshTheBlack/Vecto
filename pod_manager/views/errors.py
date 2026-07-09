from django.shortcuts import render


def custom_404(request, exception):
    network = getattr(request, 'network', None)
    entry = None
    if network:
        entry = network.notfound_entries.order_by('?').first()
    return render(request, 'pod_manager/404.html', {
        'current_network': network,
        'entry': entry,
    }, status=404)
