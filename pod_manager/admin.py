from django.contrib import admin
from .models import Network, PatreonTier, Podcast, Episode, UserMix

@admin.register(Episode)
class EpisodeAdmin(admin.ModelAdmin):
    list_display = ('title', 'podcast', 'pub_date', 'has_premium_audio')
    list_filter = ('podcast', 'pub_date')
    search_fields = ('title', 'raw_description')
    
    def has_premium_audio(self, obj):
        return obj.audio_url_public != obj.audio_url_subscriber
    has_premium_audio.boolean = True # Shows a nice Green Check / Red X
    # Orders by newest first
    ordering = ('-pub_date',)

# Keep these simple for now or customize them similarly
admin.site.register(Network)
admin.site.register(PatreonTier)
admin.site.register(Podcast)
admin.site.register(UserMix)