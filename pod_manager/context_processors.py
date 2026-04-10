from .models import Network

def current_network(request):
    """
    Grabs the primary network and makes it available to ALL HTML templates
    automatically so the theme loads on every page.
    """
    if hasattr(request, 'network'):
        return {'current_network': request.network}
    return {}