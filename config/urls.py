from django.contrib import admin
from django.urls import path
from pod_manager import views # Adjust this import based on your app name

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # Patreon OAuth routes
    path('login/', views.patreon_login, name='patreon_login'),
    
    # CRITICAL: This path must exactly match the path in your PATREON_REDIRECT_URI setting
    path('oauth/patreon/callback', views.patreon_callback, name='patreon_callback'),
    path('dashboard/', views.dashboard, name='dashboard'),
    path('feed/<slug:network_slug>', views.generate_custom_feed, name='custom_feed'),
]