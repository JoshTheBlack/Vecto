from django.contrib import admin
from django.urls import path
from django.contrib.auth.views import LogoutView
from django.conf import settings
from django.conf.urls.static import static
from pod_manager import views

urlpatterns = [
    path('', views.home, name='home'),
    path('episode/<int:episode_id>/', views.episode_detail, name='episode_detail'),
    
    # NEW & RENAMED: Listener Routes
    path('feeds/', views.user_feeds, name='user_feeds'),
    
    # Creator Route
    path('creator/', views.creator_settings, name='creator_settings'),
    
    # Auth & API Routes
    path('admin/', admin.site.urls),
    path('login/', views.patreon_login, name='patreon_login'),
    path('logout/', LogoutView.as_view(), name='logout'),
    path('oauth/patreon/callback', views.patreon_callback, name='patreon_callback'),
    path('patreon/callback/webhook/', views.patreon_webhook, name='patreon_webhook'),

    # Feed Endpoints
    path('feed/<slug:network_slug>', views.generate_custom_feed, name='custom_feed'),
    path('public/feed/<slug:podcast_slug>/', views.generate_public_feed, name='public_feed'),
    path('feed/<slug:network_slug>/mix/<uuid:unique_id>', views.generate_mix_feed, name='mix_feed'),
    path('import/stream/<int:show_id>/', views.stream_feed_import, name='stream_feed_import'),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)