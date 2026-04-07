from .models import Network

def current_network(request):
    """
    Grabs the primary network and makes it available to ALL HTML templates
    automatically so the theme loads on every page.
    """
    network = Network.objects.first()
    return {'current_network': network}