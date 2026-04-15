from django.http import Http404
from django.shortcuts import render
from .models import Network

class NetworkMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        host = request.get_host().split(':')[0] # Remove port if present
        
        # 1. Attempt to find a network by custom domain
        network = Network.objects.filter(custom_domain=host).first()
        request.network = network
        
        # 2. Strict Fallback Handling (Prevent Domain Bleed)
        if not network:
            path = request.path
            
            # Whitelist global access routes (Admin, Creator Settings, Auth, and Static assets)
            if (path.startswith('/admin') or 
                path.startswith('/creator') or 
                path.startswith('/login') or 
                path.startswith('/oauth') or 
                path.startswith('/patreon') or 
                path.startswith('/static') or 
                path.startswith('/media')):
                pass
                
            # Serve the fancy Vecto landing page on the root URL
            elif path == '/':
                return render(request, 'pod_manager/vecto_landing.html')
                
            # If an unknown domain tries to access anything else (like /feed/), hard 404.
            else:
                raise Http404("Tenant not found. No network is configured for this domain.")

        return self.get_response(request)