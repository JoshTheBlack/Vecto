from django.contrib import admin
from django.urls import path
from pod_manager import views

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # Patreon OAuth routes
    path('login/', views.patreon_login, name='patreon_login'),
    path('oauth/patreon/callback', views.patreon_callback, name='patreon_callback'),
    
    # NEW: The Webhook Receiver
    path('patreon/callback/webhook/', views.patreon_webhook, name='patreon_webhook'),
    
    path('dashboard/', views.dashboard, name='dashboard'),
    path('feed/<slug:network_slug>', views.generate_custom_feed, name='custom_feed'),
]