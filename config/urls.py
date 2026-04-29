from django.contrib import admin
from django.urls import path, re_path
from django.contrib.auth.views import LogoutView
from django.conf import settings
from django.conf.urls.static import static
from django.views.static import serve
from pod_manager import views

urlpatterns = [
    path('', views.home, name='home'),
    path('episode/<int:episode_id>/', views.episode_detail, name='episode_detail'),
    path('episode/<int:episode_id>/chapters/<str:feed_type>.json', views.episode_chapters, name='episode_chapters'),
    
    # Listener Routes
    path('feeds/', views.user_feeds, name='user_feeds'),
    path('profile/', views.user_profile, name='user_profile'),
    
    # Creator Route
    path('creator/', views.creator_settings, name='creator_settings'),
    
    # Auth & API Routes
    path('admin/', admin.site.urls),
    path('login/', views.patreon_login, name='patreon_login'),
    path('logout/', views.logout_view, name='logout'),
    path('oauth/patreon/callback', views.patreon_callback, name='patreon_callback'),
    path('patreon/callback/webhook/', views.patreon_webhook, name='patreon_webhook'),
    path('api/traefik-config/', views.traefik_config_api, name='traefik_config_api'),
    path('impersonate/start/<int:user_id>/', views.start_impersonation, name='start_impersonation'),
    path('impersonate/stop/', views.stop_impersonation, name='stop_impersonation'),
    path('episode/<int:episode_id>/edit/', views.submit_episode_edit, name='submit_episode_edit'),

    # Feed Endpoints
    path('feed/', views.generate_custom_feed, name='custom_feed'),
    path('feed/<slug:network_slug>/mix/<slug:mix_slug>/', views.generate_network_mix_feed, name='network_mix_feed'),
    path('public/feed/<slug:podcast_slug>/', views.generate_public_feed, name='public_feed'),
    path('feed/mix/<uuid:unique_id>', views.generate_mix_feed, name='mix_feed'),
    path('import/stream/<int:show_id>/', views.stream_feed_import, name='stream_feed_import'),
    path('play/<int:episode_id>.mp3', views.play_episode, name='play_episode'),
    path('login/magic/', views.request_magic_link, name='request_magic_link'),
    path('login/verify/<str:token>/', views.verify_magic_link, name='verify_magic_link'),
    path('api/check-audio/', views.check_audio_status, name='check_audio_status'),
]

urlpatterns += [
    re_path(r'^media/(?P<path>.*)$', serve, {
        'document_root': settings.MEDIA_ROOT,
    }),
]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)