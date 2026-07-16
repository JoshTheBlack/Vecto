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

    # Transcript serving
    re_path(r'^transcripts/(?P<episode_id>\d+)\.(?P<ext>vtt|json|srt|html|words)$', views.serve_transcript, name='serve_transcript'),
    
    # Listener Routes
    path('feeds/', views.user_feeds, name='user_feeds'),
    path('profile/', views.user_profile, name='user_profile'),
    
    # Creator Route
    path('creator/', views.creator_settings, name='creator_settings'),
    path('creator/show/<int:show_id>/form/', views.creator_show_form, name='creator_show_form'),
    path('creator/mix/<int:mix_id>/form/', views.creator_mix_form, name='creator_mix_form'),
    path('creator/audit/<int:edit_id>/diff/', views.creator_audit_edit, name='creator_audit_edit'),
    path('creator/tab/<str:tab>/', views.creator_tab_partial, name='creator_tab_partial'),

    # Public per-network release calendar (Feature 4, A14)
    path('calendar/', views.calendar_page, name='calendar'),
    path('calendar/events/', views.calendar_events, name='calendar_events'),
    path('calendar/manage/', views.calendar_manage, name='calendar_manage'),
    
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
    path('episode/<int:episode_id>/speaker-labels/', views.submit_speaker_labels, name='submit_speaker_labels'),
    path('ajax/update_avatar_preference/', views.update_avatar_preference, name='update_avatar_preference'),
    path('ajax/upload_custom_avatar/', views.upload_custom_avatar, name='upload_custom_avatar'),
    path('ajax/toggle_totp_mode/', views.toggle_totp_mode, name='toggle_totp_mode'),
    path('auth/totp/setup/', views.generate_qr_code, name='generate_qr_code'),
    path('auth/totp/verify/', views.verify_authenticator, name='verify_authenticator'),
    path('auth/totp/remove/', views.remove_authenticator, name='remove_authenticator'),
    
    # Feed Endpoints
    path('feed/', views.generate_custom_feed, name='custom_feed'),
    path('feed/<slug:network_slug>/mix/<slug:mix_slug>/', views.generate_network_mix_feed, name='network_mix_feed'),
    path('public/feed/<slug:podcast_slug>/', views.generate_public_feed, name='public_feed'),
    path('feed/mix/<uuid:unique_id>', views.generate_mix_feed, name='mix_feed'),
    path('feed/<slug:network_slug>/calendar.ics', views.generate_calendar_feed, name='calendar_feed'),
    path('import/start/<int:show_id>/', views.import_feed_start, name='import_feed_start'),
    path('import/poll/<int:show_id>/', views.import_feed_poll, name='import_feed_poll'),
    path('play/<int:episode_id>.mp3', views.play_episode, name='play_episode'),
    path('login/legacy/', views.recurly_login, name='recurly_login'),
    path('api/check-audio/', views.check_audio_status, name='check_audio_status'),
    path('api/transcripts/backfill/', views.backfill_transcripts_api, name='backfill_transcripts_api'),
    path('api/transcripts/retranscribe/<int:episode_id>/', views.retranscribe_episode_api, name='retranscribe_episode_api'),

    # Creator publishing
    path('creator/publish/', views.publish_episode, name='publish_episode'),
    path('creator/episode/<int:episode_id>/manage/', views.manage_episode, name='manage_episode'),

    # GDrive Recovery
    path('creator/gdrive-recovery/files/', views.gdrive_recovery_files, name='gdrive_recovery_files'),
    path('creator/gdrive-recovery/run/', views.gdrive_recovery_run, name='gdrive_recovery_run'),
    path('creator/gdrive-recovery/poll/<str:run_id>/', views.gdrive_recovery_poll, name='gdrive_recovery_poll'),
    path('creator/gdrive-recovery/rewind/', views.gdrive_recovery_rewind, name='gdrive_recovery_rewind'),

    # Staff tools
    path('staff/logs/', views.log_viewer, name='log_viewer'),
    path('staff/logs/stream/', views.log_stream, name='log_stream'),
    path('staff/logs/poll/', views.log_poll, name='log_poll'),
    path('staff/logs/level/', views.log_level_toggle, name='log_level_toggle'),
    path('staff/logs/resources/', views.log_resources, name='log_resources'),

    # Admin Command Console (superuser-only; under /admin-console/ to avoid Django /admin/)
    path('admin-console/', views.admin_console, name='admin_console'),
    path('admin-console/command/<str:name>/', views.admin_console_command_detail, name='admin_console_command_detail'),
    path('admin-console/command/<str:name>/build/', views.admin_console_build, name='admin_console_build'),
    path('admin-console/command/<str:name>/run/', views.admin_console_run, name='admin_console_run'),
    path('admin-console/run/<uuid:run_id>/poll/', views.admin_console_run_poll, name='admin_console_run_poll'),
    path('admin-console/run/<uuid:run_id>/cancel/', views.admin_console_run_cancel, name='admin_console_run_cancel'),
    path('admin-console/runs/', views.admin_console_history, name='admin_console_history'),
    path('admin-console/run/<uuid:run_id>/', views.admin_console_run_detail, name='admin_console_run_detail'),
    path('admin-console/lookup/episodes/', views.admin_console_episode_search, name='admin_console_episode_search'),
]

urlpatterns += [
    re_path(r'^media/(?P<path>.*)$', serve, {
        'document_root': settings.MEDIA_ROOT,
    }),
]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

handler404 = 'pod_manager.views.errors.custom_404'