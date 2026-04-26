import json
from django import forms
from django.contrib import admin
from django.contrib.admin import SimpleListFilter
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import User
from django.db.models import Q, F, BooleanField, ExpressionWrapper, Sum
from django.utils.html import format_html, mark_safe
from django.urls import reverse
from .models import Network, PatreonTier, Podcast, Episode, UserMix, PatronProfile, EpisodeEditSuggestion, NetworkMembership

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
    list_display = ('title', 'podcast', 'pub_date', 'match_reason', 'is_metadata_locked', 'has_public_audio', 'has_premium_audio')
    list_filter = ('podcast__network', 'podcast', S3SubscriberAudioFilter, 'pub_date', 'match_reason')
    search_fields = ('title', 'raw_description', 'guid_public', 'guid_private')
    list_editable = ('is_metadata_locked',) # Allows you to check the box directly from the list view!
    
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
    
    ordering = ('-pub_date',)

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
    inlines = (PatronProfileInline, NetworkMembershipInline)

admin.site.unregister(User)
admin.site.register(User, UserAdmin)

"""
@admin.register(User)
class CustomUserAdmin(BaseUserAdmin):
    inlines = (PatronProfileInline,)
    
    # Append BOTH of our custom columns to the end of the standard Django list
    list_display = BaseUserAdmin.list_display + ('get_total_pledge_display', 'impersonate_action')

    # --- Column 1: Patreon Pledge ---
    def get_total_pledge_display(self, obj):
        total_cents = NetworkMembership.objects.filter(
            user=obj, 
            is_active_patron=True
        ).aggregate(
            total=Sum('patreon_pledge_cents')
        )['total'] or 0
        
        return f"${total_cents / 100:.2f}"
    
    get_total_pledge_display.short_description = 'Total Network Pledge'

    # --- Column 2: Impersonation ---
    def impersonate_action(self, obj):
        if not obj.is_superuser:
            url = reverse('start_impersonation', args=[obj.id])
            # format_html is correct here because we are injecting the 'url' variable
            return format_html('<a class="button" style="background-color: #ffc107; color: black; font-weight: bold; padding: 5px 10px; border-radius: 4px;" href="{}">Impersonate</a>', url)
        
        # mark_safe is required here because there are no variables to inject
        return mark_safe('<span style="color: gray;">Superuser (Locked)</span>')
        
    impersonate_action.short_description = 'Impersonate User'
"""

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