from django.contrib import admin
from django.contrib.admin import SimpleListFilter
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import User
from django.db.models import Q, F, BooleanField, ExpressionWrapper
from .models import Network, PatreonTier, Podcast, Episode, UserMix, PatronProfile

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
    list_display = ('name', 'slug')
    search_fields = ('name',)
    filter_horizontal = ('owners',)

@admin.register(Episode)
class EpisodeAdmin(admin.ModelAdmin):
    list_display = ('title', 'podcast', 'pub_date', 'match_reason', 'has_public_audio', 'has_premium_audio')
    list_filter = ('podcast__network', 'podcast', S3SubscriberAudioFilter, 'pub_date', 'match_reason')
    search_fields = ('title', 'raw_description', 'guid')
    
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
    list_display = ('user', 'patreon_id', 'get_combined_pledge_cents', 'last_sync')
    search_fields = ('user__username', 'user__email', 'patreon_id')

    def get_combined_pledge_cents(self, obj):
        """Sums up all campaign pledges from the active_pledges JSON field."""
        if not obj.active_pledges:
            return 0
        return sum(obj.active_pledges.values())
    
    get_combined_pledge_cents.short_description = 'Pledge Amount Cents'

class PatronProfileInline(admin.StackedInline):
    model = PatronProfile
    can_delete = False
    verbose_name_plural = 'Patron Profile'
    fields = ('patreon_id', 'active_pledges', 'feed_token')
    readonly_fields = ('feed_token',)

admin.site.unregister(User)

@admin.register(User)
class UserAdmin(BaseUserAdmin):
    inlines = (PatronProfileInline,)
    list_display = BaseUserAdmin.list_display + ('get_total_pledge_display',)

    def get_total_pledge_display(self, instance):
        if hasattr(instance, 'patron_profile') and instance.patron_profile.active_pledges:
            total_cents = sum(instance.patron_profile.active_pledges.values())
            return f"${total_cents / 100:.2f}"
        return "$0.00"
    
    get_total_pledge_display.short_description = 'Total Pledge'

admin.site.register(PatreonTier)
admin.site.register(Podcast)
admin.site.register(UserMix)