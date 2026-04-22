import logging
from django.http import Http404
from django.shortcuts import render
from .models import Network

logger = logging.getLogger(__name__)

class NetworkMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path.startswith('/api/') or request.path.startswith('/admin/') or request.path.startswith('/static/'):
            # Just pass the request through safely without attaching a network
            return self.get_response(request)
        host = request.get_host().split(':')[0] # Remove port if present
        logger.debug(f"Incoming request for host: '{host}' | Path: {request.path}")
        
        # 1. Attempt to find a network by custom domain
        network = Network.objects.filter(custom_domain=host).first()
        request.network = network
        
        if network:
            logger.debug(f"Matched host '{host}' to Network: {network.name} ({network.slug})")
        
        # 2. Strict Fallback Handling (Prevent Domain Bleed)
        if not network:
            path = request.path
            logger.debug(f"No matching network found for host '{host}'. Evaluating fallback rules.")
            
            # Whitelist global access routes
            if (path.startswith('/admin') or 
                path.startswith('/creator') or 
                path.startswith('/login') or 
                path.startswith('/oauth') or 
                path.startswith('/patreon') or 
                path.startswith('/static') or 
                path.startswith('/media')):
                logger.debug(f"Allowing whitelisted path without network: {path}")
                pass
                
            # Serve the fancy Vecto landing page on the root URL
            elif path == '/':
                logger.info(f"Serving Vecto landing page for unknown host: {host}")
                return render(request, 'pod_manager/vecto_landing.html')
                
            # If an unknown domain tries to access anything else, hard 404.
            else:
                logger.warning(f"Blocked access to {path} on unknown host: {host}. Raising 404.")
                raise Http404("Tenant not found. No network is configured for this domain.")

        return self.get_response(request)