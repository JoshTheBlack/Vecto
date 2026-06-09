import json
from django import forms
from django.contrib import admin
from django.contrib.admin import SimpleListFilter
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import User
from django.db.models import Q, F, BooleanField, ExpressionWrapper
from django.utils.html import format_html, mark_safe
from django.urls import reverse
from .models import Network, PatreonTier, Podcast, Episode, UserMix, PatronProfile, EpisodeEditSuggestion, NetworkMembership, LogEntry, Transcript

class S3SubscriberAudioFilter(SimpleListFilter):
    title = 'S3 Hosted Audio (Affected)'
    parameter_name = 's3_audio'

    def lookups(self, request, model_admin):
        return (
            ('yes', 'Hosted on Amazon S3'),
            ('no', 'Hosted Elsewhere'),
        )

    def queryset(self, request, queryset):
        # Using icontains catches both s3.amazonaws.com/bucket and bucket.s3.amazonaws.com
        if self.value() == 'yes':
            return queryset.filter(audio_url_subscriber__icontains='s3.amazonaws.com')
        if self.value() == 'no':
            return queryset.exclude(audio_url_subscriber__icontains='s3.amazonaws.com')
        return queryset
    
@admin.register(Network)
class NetworkAdmin(admin.ModelAdmin):
    list_display = ('name', 'slug', 'custom_domain', 'patreon_sync_enabled')
    search_fields = ('name', 'slug', 'custom_domain')
    list_filter = ('patreon_sync_enabled',)
    filter_horizontal = ('owners',)

@admin.register(NetworkMembership)
class NetworkMembershipAdmin(admin.ModelAdmin):
    list_display = ('user', 'network', 'is_active_patron', 'pledge_dollars', 'trust_score')
    search_fields = ('user__username', 'user__email', 'network__name')
    # Filter by Network and Patron Status!
    list_filter = ('network', 'is_active_patron')
    
    def pledge_dollars(self, obj):
        return f"${obj.patreon_pledge_cents / 100:.2f}"
    pledge_dollars.short_description = "Pledge Amount"

@admin.register(Episode)
class EpisodeAdmin(admin.ModelAdmin):
    list_display = ('title', 'podcast', 'pub_date', 'is_published', 'scheduled_at', 'episode_type', 'match_reason', 'is_metadata_locked', 'audio_locked', 'has_public_audio', 'has_premium_audio', 'transcript_status')
    list_filter = ('podcast__network', 'podcast', 'is_published', 'episode_type', 'audio_locked', S3SubscriberAudioFilter, 'pub_date', 'match_reason')
    search_fields = ('title', 'raw_description', 'guid_public', 'guid_private')
    list_editable = ('is_metadata_locked', 'audio_locked', 'is_published')
    
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        # Calculate the boolean logic inside the database so the admin table can sort it
        qs = qs.annotate(
            _has_public=ExpressionWrapper(
                Q(audio_url_public__isnull=False) & ~Q(audio_url_public=''),
                output_field=BooleanField()
            ),
            _has_premium=ExpressionWrapper(
                Q(audio_url_subscriber__isnull=False) & ~Q(audio_url_subscriber='') & ~Q(audio_url_subscriber=F('audio_url_public')),
                output_field=BooleanField()
            )
        )
        return qs

    def has_public_audio(self, obj):
        return bool(obj.audio_url_public)
    has_public_audio.boolean = True
    has_public_audio.short_description = "Public"
    has_public_audio.admin_order_field = '_has_public'  # Links the column to the SQL annotation

    def has_premium_audio(self, obj):
        return bool(obj.audio_url_subscriber) and obj.audio_url_subscriber != obj.audio_url_public
    has_premium_audio.boolean = True
    has_premium_audio.short_description = "Premium"
    has_premium_audio.admin_order_field = '_has_premium'  # Links the column to the SQL annotation

    def transcript_status(self, obj):
        from pod_manager.models import Transcript
        try:
            t = obj.transcript
        except Transcript.DoesNotExist:
            return '—'
        colours = {
            Transcript.Status.PENDING:    '#888',
            Transcript.Status.PROCESSING: '#d97706',
            Transcript.Status.COMPLETED:  '#16a34a',
            Transcript.Status.FAILED:     '#dc2626',
        }
        colour = colours.get(t.status, '#888')
        return format_html('<span style="color:{}">{}</span>', colour, t.get_status_display())
    transcript_status.short_description = 'Transcript'

    ordering = ('-pub_date',)
    actions = ['trigger_transcription']

    def trigger_transcription(self, request, queryset):
        from django.conf import settings
        from pod_manager.services.transcription import run_transcription
        from pod_manager.tasks import transcribe_episode

        _SKIP_STATUSES = {
            Transcript.Status.PENDING,
            Transcript.Status.PROCESSING,
            Transcript.Status.COMPLETED,
        }
        queued = skipped_no_audio = skipped_in_progress = skipped_done = 0
        for i, episode in enumerate(queryset):
            if not episode.audio_url_subscriber:
                skipped_no_audio += 1
                continue
            t = getattr(episode, 'transcript', None)
            if t is not None:
                if t.status == Transcript.Status.COMPLETED:
                    skipped_done += 1
                    continue
                if t.status in (Transcript.Status.PENDING, Transcript.Status.PROCESSING):
                    skipped_in_progress += 1
                    continue
            if settings.IS_IDE:
                run_transcription(episode.pk)
            else:
                transcribe_episode.apply_async(args=[episode.pk], countdown=i * 30)
            queued += 1

        if queued:
            verb = "transcribed" if settings.IS_IDE else "queued for transcription"
            self.message_user(request, f"{queued} episode(s) {verb}.")
        if skipped_no_audio:
            self.message_user(request, f"{skipped_no_audio} skipped (no subscriber audio).", level='warning')
        if skipped_in_progress:
            self.message_user(request, f"{skipped_in_progress} skipped (already pending/processing).", level='warning')
        if skipped_done:
            self.message_user(request, f"{skipped_done} skipped (already completed). Delete the Transcript record to force re-transcription.", level='warning')
    trigger_transcription.short_description = "Queue transcription"

@admin.register(PatronProfile)
class PatronProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'patreon_id', 'last_sync')
    search_fields = ('user__username', 'user__email', 'patreon_id')

class PatronProfileInline(admin.StackedInline):
    model = PatronProfile
    can_delete = False
    verbose_name_plural = 'Patron Profile'
    
    # We only include actual database fields here.
    fields = ('patreon_id', 'discord_id', 'feed_token')
    readonly_fields = ('feed_token',)

class NetworkMembershipInline(admin.TabularInline):
    model = NetworkMembership
    extra = 1  # Adds a blank row so you can quickly grant a user access to a new network
    verbose_name_plural = 'Network Memberships (Pledges & Billing)'
    
    fields = ('network', 'is_active_patron', 'patreon_pledge_cents', 'last_active_date', 'trust_score')
    readonly_fields = ('last_active_date',)
    
    # Optional: Adds a tiny bit of helper text to remind admins that it's in cents
    help_texts = {
        'patreon_pledge_cents': 'Enter value in cents (e.g., 500 for $5.00)'
    }

class UserAdmin(BaseUserAdmin):
    # Keep your existing inlines
    inlines = (PatronProfileInline, NetworkMembershipInline)
    
    # Append the custom impersonate column to the standard Django columns
    list_display = BaseUserAdmin.list_display + ('impersonate_action',)

    def impersonate_action(self, obj):
        if not obj.is_superuser:
            url = reverse('start_impersonation', args=[obj.id])
            return format_html(
                '<a class="button" style="background-color: #ffc107; color: black; font-weight: bold; padding: 5px 10px; border-radius: 4px;" href="{}">Impersonate</a>', 
                url
            )
        return mark_safe('<span style="color: gray;">Superuser (Locked)</span>')
        
    impersonate_action.short_description = 'Impersonate User'

admin.site.unregister(User)
admin.site.register(User, UserAdmin)


@admin.register(PatreonTier)
class PatreonTierAdmin(admin.ModelAdmin):
    list_display = ('name', 'network', 'tier_dollars')
    list_filter = ('network',)
    search_fields = ('name', 'network__name')

    def tier_dollars(self, obj):
        return f"${obj.minimum_cents / 100:.2f}"
    tier_dollars.short_description = "Minimum Pledge"

@admin.register(Podcast)
class PodcastAdmin(admin.ModelAdmin):
    list_display = ('title', 'network', 'required_tier')
    # Filter podcasts by Network and what Tier they require!
    list_filter = ('network', 'required_tier')
    search_fields = ('title', 'slug')

admin.site.register(UserMix)



@admin.register(EpisodeEditSuggestion)
class EpisodeEditSuggestionAdmin(admin.ModelAdmin):
    list_display = ('episode', 'user', 'status', 'is_first_responder', 'created_at', 'resolved_at')
    list_filter = ('status', 'is_first_responder', 'created_at', 'episode__podcast__network')
    search_fields = ('episode__title', 'user__username', 'user__email')
    readonly_fields = ('created_at', 'resolved_at')
    fieldsets = (
        ('Reference', {
            'fields': ('episode', 'user', 'status', 'is_first_responder')
        }),
        ('Payload Data (JSON)', {
            'fields': ('suggested_data', 'original_data')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'resolved_at')
        }),
    )


_LEVEL_COLORS = {
    'DEBUG': '#6c757d',
    'INFO': '#0dcaf0',
    'WARNING': '#ffc107',
    'ERROR': '#dc3545',
    'CRITICAL': '#6f42c1',
}

@admin.register(LogEntry)
class LogEntryAdmin(admin.ModelAdmin):
    list_display = ('created_at', 'level_badge', 'user', 'logger_name', 'module', 'short_message')
    list_filter = ('level', 'user')
    search_fields = ('message', 'logger_name', 'module', 'func_name', 'user__username', 'user__email')
    date_hierarchy = 'created_at'
    readonly_fields = ('level', 'level_no', 'logger_name', 'module', 'func_name', 'lineno', 'message', 'user', 'created_at')
    ordering = ('-created_at',)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def level_badge(self, obj):
        color = _LEVEL_COLORS.get(obj.level, '#6c757d')
        return format_html(
            '<span style="background:{};color:#fff;padding:2px 8px;border-radius:3px;font-size:0.75rem;font-weight:bold;">{}</span>',
            color, obj.level,
        )
    level_badge.short_description = 'Level'

    def short_message(self, obj):
        return obj.message[:120]
    short_message.short_description = 'Message'


_STATUS_COLORS = {
    'pending':    '#6c757d',
    'processing': '#0d6efd',
    'completed':  '#198754',
    'failed':     '#dc3545',
}

@admin.register(Transcript)
class TranscriptAdmin(admin.ModelAdmin):
    list_display  = ('episode', 'status_badge', 'language', 'whisper_model_used', 'retry_count', 'requested_at', 'completed_at')
    list_filter   = ('status', 'language', 'whisper_model_used')
    search_fields = ('episode__title', 'episode__podcast__title', 'error_message')
    readonly_fields = (
        'episode', 'status', 'language',
        'vtt_file', 'json_file', 'srt_file', 'html_file', 'words_json_file',
        'source_audio_url', 'whisper_model_used', 'retry_count',
        'requested_at', 'started_at', 'completed_at', 'error_message',
    )
    ordering = ('-requested_at',)

    def has_add_permission(self, request):
        return False

    def status_badge(self, obj):
        color = _STATUS_COLORS.get(obj.status, '#6c757d')
        return format_html(
            '<span style="background:{};color:#fff;padding:2px 8px;border-radius:3px;font-size:0.75rem;font-weight:bold;">{}</span>',
            color, obj.get_status_display(),
        )
    status_badge.short_description = 'Status'
    status_badge.admin_order_field = 'status'