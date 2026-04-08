import uuid
from django.db import models
from django.contrib.auth.models import User

class Network(models.Model):
    name = models.TextField()
    slug = models.SlugField(unique=True)
    theme_config = models.JSONField(default=dict, blank=True)
    
    # NEW: Network Agnostic Info
    patreon_campaign_id = models.CharField(max_length=100, blank=True, help_text="The numeric ID of the Patreon Campaign")
    website_url = models.URLField(blank=True, help_text="e.g., https://yournetwork.com")
    default_image_url = models.URLField(blank=True, help_text="Fallback logo for RSS feeds")
    ignored_title_tags = models.TextField(blank=True, help_text="Comma-separated list of tags to strip during import (e.g., '(ad-free), premium')")
    description_cut_triggers = models.TextField(blank=True, help_text="Comma-separated phrases to trigger paragraph deletion (e.g., 'ad choices, leave a review')")
    
    feed_cache_minutes = models.IntegerField(default=15, help_text="How long to cache feeds in minutes.")
    
    global_footer_public = models.TextField(blank=True, help_text="Appended to all public feeds in this network.")
    global_footer_private = models.TextField(blank=True, help_text="Appended to all private feeds in this network.")

    def __str__(self):
        return self.name

class PatreonTier(models.Model):
    """
    Dynamically configurable tiers managed in the Django Admin.
    """
    network = models.ForeignKey(Network, on_delete=models.CASCADE, related_name='tiers')
    name = models.CharField(max_length=100, help_text="e.g., 'In Association With'")
    minimum_cents = models.IntegerField(help_text="e.g., 600 for $6.00")

    is_default = models.BooleanField(default=False, help_text="Automatically assign to new podcasts.")

    class Meta:
        ordering = ['minimum_cents']

    @property
    def minimum_dollars(self):
        return self.minimum_cents / 100
    
    def save(self, *args, **kwargs):
        # Safety check: If this tier is marked as default, uncheck 'default' on all other tiers for this network
        if self.is_default:
            PatreonTier.objects.filter(network=self.network).update(is_default=False)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name} (${self.minimum_cents / 100:.2f})"
    
class Podcast(models.Model):
    network = models.ForeignKey(Network, on_delete=models.CASCADE, related_name='podcasts')
    title = models.TextField()
    slug = models.SlugField()
    
    public_feed_url = models.URLField()
    subscriber_feed_url = models.URLField()
    required_tier = models.ForeignKey(PatreonTier, on_delete=models.SET_NULL, null=True, blank=True)

    image_url = models.URLField(blank=True, help_text="Automatically populated from the RSS feed.")

    # Show-Specific Footers (e.g., Apple Podcasts Review Link for this specific show)
    show_footer_public = models.TextField(blank=True, help_text="Appended above the global public footer.")
    show_footer_private = models.TextField(blank=True, help_text="Appended above the global private footer.")

    class Meta:
        unique_together = ('network', 'slug')

    def save(self, *args, **kwargs):
        if not self.required_tier and self.network_id:
            default_tier = PatreonTier.objects.filter(network=self.network, is_default=True).first()
            if default_tier:
                self.required_tier = default_tier
        super().save(*args, **kwargs)

    def __str__(self):
        return self.title

class Episode(models.Model):
    """An individual episode harvested from a feed."""
    podcast = models.ForeignKey(Podcast, on_delete=models.CASCADE, related_name='episodes')
    guid = models.TextField() # Unique ID from the RSS feed
    title = models.TextField()
    pub_date = models.DateTimeField()
    
    # We store both versions of the audio
    audio_url_public = models.URLField()
    audio_url_subscriber = models.URLField()
    
    # Store descriptions separately for cleaning/normalization
    raw_description = models.TextField()
    clean_description = models.TextField(blank=True)

    duration = models.CharField(max_length=20, blank=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['podcast', '-pub_date']),
        ]
        unique_together = ('podcast', 'guid')
    
    def __str__(self):
        # This will show "Podcast Title | Episode Title"
        # We truncate the title to 50 chars so the admin list stays clean
        # short_title = (self.title[:47] + '..') if len(self.title) > 50 else self.title
        return f"{self.podcast.title} | {self.title}"
    
    @property
    def is_premium(self):
        """Returns True if this episode has a unique subscriber URL."""
        return self.audio_url_public != self.audio_url_subscriber

class UserMix(models.Model):
    """A user's custom-built feed."""
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    network = models.ForeignKey(Network, on_delete=models.CASCADE)
    unique_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    selected_podcasts = models.ManyToManyField(Podcast)
    
    # Track when they last checked in
    last_accessed = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.user.username}'s Mix - {self.network.name}"
    
class PatronProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='patron_profile')
    patreon_id = models.CharField(max_length=50, unique=True)
    pledge_amount_cents = models.IntegerField(default=0)
    last_sync = models.DateTimeField(auto_now=True)
    
    # NEW: The secret token for their podcast app
    feed_token = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)

    def __str__(self):
        return f"{self.user.email} - ${self.pledge_amount_cents / 100:.2f}"