from django.contrib import admin
from django.urls import path
from pod_manager import views

urlpatterns = [
    path('', views.home, name='home'),
    path('episode/<int:episode_id>/', views.episode_detail, name='episode_detail'),
    
    # NEW & RENAMED: Listener Routes
    path('subscriber/', views.subscriber_dashboard, name='subscriber_dashboard'),
    path('feeds/', views.user_feeds, name='user_feeds'),
    
    # Creator Route
    path('creator/', views.creator_settings, name='creator_settings'),
    
    # Auth & API Routes
    path('admin/', admin.site.urls),
    path('login/', views.patreon_login, name='patreon_login'),
    path('oauth/patreon/callback', views.patreon_callback, name='patreon_callback'),
    path('patreon/callback/webhook/', views.patreon_webhook, name='patreon_webhook'),
    
    # Feed Endpoints
    path('feed/<slug:network_slug>', views.generate_custom_feed, name='custom_feed'),
    path('public/feed/<slug:podcast_slug>/', views.generate_public_feed, name='public_feed'),
]