import logging
from django.core.cache import cache
from django.http import Http404
from django.shortcuts import render
from django.contrib.auth.models import User
from django.utils import timezone
from django.utils.deprecation import MiddlewareMixin
from .models import Network, NetworkMembership

logger = logging.getLogger(__name__)

class NetworkMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path.startswith('/api/') or request.path.startswith('/admin/') or request.path.startswith('/static/'):
            return self.get_response(request)
        host = request.get_host().split(':')[0] # Remove port if present
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            client_ip = x_forwarded_for.split(',')[0].strip()
        else:
            client_ip = request.META.get('REMOTE_ADDR', 'unknown')
        logger.debug(f"Incoming request for host: '{host}' | Path: {request.path}")
        
        # 1. Attempt to find a network by custom domain
        network = Network.objects.filter(custom_domain=host).first()
        request.network = network
        request.tenant_profile = None
        
        if network:
            logger.debug(f"Matched host '{host}' to Network: {network.name} ({network.slug})")
            
            if hasattr(request, 'user') and request.user.is_authenticated:
                request.tenant_profile = NetworkMembership.objects.filter(
                    user=request.user, 
                    network=network
                ).first()
        
        # 2. Strict Fallback Handling
        if not network:
            path = request.path
            logger.debug(f"No matching network found for host '{host}'. Evaluating fallback rules.")
            
            # Whitelist global access routes
            if (path.startswith('/admin') or 
                path.startswith('/creator') or 
                path.startswith('/login') or 
                path.startswith('/logout') or
                path.startswith('/oauth') or 
                path.startswith('/patreon') or 
                path.startswith('/static') or 
                path.startswith('/media')):
                logger.debug(f"Allowing whitelisted path without network: {path}")
                pass
            elif path == '/':
                logger.warning(f"Serving Vecto landing page for unknown host: {host} [IP: {client_ip}]")
                return render(request, 'pod_manager/vecto_landing.html')
                
            # If an unknown domain tries to access anything else, hard 404.
            else:
                raise Http404("Tenant not found. No network is configured for this domain.")

        return self.get_response(request)

class ImpersonationMiddleware(MiddlewareMixin):
    """
    Allows staff members to view the site as a standard user.
    Maintains a strict security boundary preventing superuser hijacking.
    """
    def process_request(self, request):
        request.is_impersonating = False
        request.impersonator = None

        # 1. If not logged in, or not a staff member, ignore completely.
        if not hasattr(request, 'user') or not request.user.is_authenticated:
            return
            
        # Security: If a non-staff user somehow gets this session key, purge it immediately.
        if not request.user.is_staff:
            if 'impersonated_user_id' in request.session:
                del request.session['impersonated_user_id']
            return

        impersonated_user_id = request.session.get('impersonated_user_id')
        if impersonated_user_id:
            try:
                impersonated_user = User.objects.get(id=impersonated_user_id)
                
                # Security: NEVER allow impersonation of a superuser
                if impersonated_user.is_superuser:
                    logger.warning(f"SECURITY: Staff user {request.user.email} attempted to impersonate SUPERUSER {impersonated_user.email}. Session purged.")
                    del request.session['impersonated_user_id']
                    return

                # 2. Swap the user object and set the flags for the templates
                request.impersonator = request.user
                request.user = impersonated_user
                request.is_impersonating = True
                
            except User.DoesNotExist:
                del request.session['impersonated_user_id']

class BillingPresenceMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # 1. WAY IN: Drop the billing flag immediately
        if hasattr(request, 'user') and request.user.is_authenticated and hasattr(request, 'network') and request.network:
            if not request.path.startswith('/static/') and not request.path.startswith('/admin/'):
                billing_key = f"billing:active:{request.network.id}:{request.user.id}:{timezone.now().strftime('%Y-%m-%d')}"
                cache.set(billing_key, 1, timeout=172800)
                logger.debug(f"BILLING MIDDLEWARE FIRED FOR USER {request.user.id}")

        # 2. Process the view
        response = self.get_response(request)
        return response