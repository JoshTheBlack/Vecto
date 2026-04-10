from django.http import Http404
from .models import Network

class NetworkMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        host = request.get_host().split(':')[0] # Remove port if present
        
        # 1. Attempt to find a network by custom domain
        network = Network.objects.filter(custom_domain=host).first()
        
        # 2. Fallback: For local dev or if no domain matches, 
        # you might want to default to the first network or a 'system' network.
        if not network:
            network = Network.objects.first()

        # Attach the network to the request object
        request.network = network
        
        response = self.get_response(request)
        return response