from django.contrib import admin
from .models import Network, PatreonTier, Podcast, Episode, UserMix, PatronProfile

@admin.register(Network)
class NetworkAdmin(admin.ModelAdmin):
    list_display = ('name', 'slug')
    search_fields = ('name',)
    filter_horizontal = ('owners',)

@admin.register(Episode)
class EpisodeAdmin(admin.ModelAdmin):
    list_display = ('title', 'podcast', 'pub_date', 'match_reason', 'has_premium_audio')
    list_filter = ('podcast__network', 'podcast', 'pub_date', 'match_reason')
    search_fields = ('title', 'raw_description', 'guid')
    
    def has_premium_audio(self, obj):
        return obj.audio_url_public != obj.audio_url_subscriber
    has_premium_audio.boolean = True
    ordering = ('-pub_date',)

# NEW: Register the PatronProfile with a custom layout
@admin.register(PatronProfile)
class PatronProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'patreon_id', 'pledge_amount_cents', 'last_sync')
    search_fields = ('user__username', 'user__email', 'patreon_id')

admin.site.register(PatreonTier)
admin.site.register(Podcast)
admin.site.register(UserMix)