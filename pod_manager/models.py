import uuid, os, requests, base64
from django.db import models
from django.contrib.auth.models import User
from django.core.files.base import ContentFile
from django.core.files.storage import FileSystemStorage
from django.db.models.signals import post_delete
from django.dispatch import receiver
from cryptography.fernet import Fernet
from PIL import Image
from io import BytesIO

from django.conf import settings

def default_theme_config():
    return {
        "bg_color": "#121212",
        "bg_text_color": "#f8f9fa",
        "bg_muted_text_color": "#6c757d",
        "surface_bg_color": "#1e1e1e",
        "surface_text_color": "#f8f9fa",
        "surface_muted_text_color": "#adb5bd",
        "nav_bg_color": "#000000",
        "nav_text_color": "#ffffff",
        "nav_muted_text_color": "#6c757d",
        "nav_socials_bg_color": "#222222",
        "primary_color": "#ffc107",
        "primary_text_color": "#000000",
        "success_color": "#198754",
        "success_text_color": "#ffffff",
        "border_color": "#343a40",
        "font_family": "system-ui, sans-serif",
        "google_fonts_url": "",
        "border_radius": "0.375rem",
        "logo_url": ""
    }

class EncryptedCharField(models.CharField):
    """A custom field that encrypts data at rest using settings.SECRET_KEY."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Use your Django Secret Key to derive a 32-byte Fernet key
        key = base64.urlsafe_b64encode(settings.SECRET_KEY[:32].encode().ljust(32, b'0'))
        self.fernet = Fernet(key)

    def get_prep_value(self, value):
        if value is None: return None
        return self.fernet.encrypt(value.encode()).decode()

    def from_db_value(self, value, expression, connection):
        if value is None: return None
        return self.fernet.decrypt(value.encode()).decode()
    
class Network(models.Model):
    name = models.TextField()
    slug = models.SlugField(unique=True)
    owners = models.ManyToManyField(User, related_name="owned_networks", blank=True, help_text="Users who have admin access to this network's settings.")
    theme_config = models.JSONField(default=default_theme_config, blank=True)
    custom_domain = models.CharField(max_length=255, unique=True, blank=True, null=True, db_index=True)
    
    patreon_campaign_id = models.CharField(max_length=100, blank=True, help_text="The numeric ID of the Patreon Campaign")
    patreon_sync_enabled = models.BooleanField(default=False)
    patreon_creator_access_token = EncryptedCharField(max_length=500, blank=True, null=True)
    patreon_creator_refresh_token = EncryptedCharField(max_length=500, blank=True, null=True)
    website_url = models.URLField(blank=True, help_text="e.g., https://yournetwork.com")
    default_image_url = models.URLField(blank=True, help_text="Fallback logo for RSS feeds")
    ignored_title_tags = models.TextField(blank=True, help_text="Comma-separated list of tags to strip during import (e.g., '(ad-free), premium')")
    description_cut_triggers = models.TextField(blank=True, help_text="Comma-separated phrases to trigger paragraph deletion (e.g., 'ad choices, leave a review')")

    # NEW: Social Media Links
    url_patreon = models.URLField(blank=True)
    url_youtube = models.URLField(blank=True)
    url_twitch = models.URLField(blank=True)
    url_bluesky = models.URLField(blank=True)
    url_twitter = models.URLField(blank=True)

    feed_cache_minutes = models.IntegerField(default=15, help_text="How long to cache feeds in minutes.")
    
    global_footer_public = models.TextField(blank=True, help_text="Appended to all public feeds in this network.")
    global_footer_private = models.TextField(blank=True, help_text="Appended to all private feeds in this network.")
    
    ingester_module = models.CharField(
        max_length=50, 
        default='default', 
        help_text="The script in pod_manager/ingesters/ to use. Default is 'default'."
    )

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
    match_reason = models.CharField(max_length=100, blank=True, help_text="Audit trail for how the private audio was matched during ingestion.")
    
    # Store descriptions separately for cleaning/normalization
    raw_description = models.TextField()
    clean_description = models.TextField(blank=True)

    duration = models.CharField(max_length=20, blank=True)

    link = models.URLField(max_length=1000, blank=True, null=True)

    tags = models.JSONField(default=list, blank=True)
    
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

def mix_cover_path(instance, filename):
    """Always name the file exactly <UUID>.<ext>"""
    ext = filename.split('.')[-1]
    return os.path.join('mix_covers', f"{instance.unique_id}.{ext}")

class OverwriteStorage(FileSystemStorage):
    def get_available_name(self, name, max_length=None):
        """Returns the same name even if it already exists on the system."""
        if self.exists(name):
            os.remove(os.path.join(settings.MEDIA_ROOT, name))
        return name
    
mix_storage = OverwriteStorage()

class UserMix(models.Model):
    """A user's custom-built feed."""
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    network = models.ForeignKey(Network, on_delete=models.CASCADE)
    
    name = models.CharField(max_length=200, default="My Custom Mix")
    image_url = models.URLField(blank=True, help_text="Optional custom artwork URL")
    image_upload = models.ImageField(upload_to=mix_cover_path, storage=mix_storage, blank=True, null=True, help_text="Uploaded artwork")
    
    unique_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    selected_podcasts = models.ManyToManyField(Podcast)
    last_accessed = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.user.username}'s Mix - {self.name}"
        
    @property
    def display_image(self):
        if self.image_upload:
            return self.image_upload.url
        return self.image_url

    def save(self, *args, **kwargs):
        # 1. Save the record (triggers OverwriteStorage via mix_cover_path)
        super().save(*args, **kwargs)

        # 2. Post-process (Crop & Resize)
        if self.image_upload:
            img_path = self.image_upload.path
            if os.path.exists(img_path):
                try:
                    img = Image.open(img_path)
                    
                    # Square Crop
                    width, height = img.size
                    if width != height:
                        new_size = min(width, height)
                        left = (width - new_size) / 2
                        top = (height - new_size) / 2
                        right = (width + new_size) / 2
                        bottom = (height + new_size) / 2
                        img = img.crop((left, top, right, bottom))
                    
                    # Resize to 500x500
                    if img.height > 500 or img.width > 500:
                        img.thumbnail((500, 500), Image.Resampling.LANCZOS)
                    
                    img.save(img_path)
                except Exception as e:
                    print(f"Image processing failed: {e}")
    
class PatronProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='patron_profile')
    patreon_id = models.CharField(max_length=50, unique=True)
    pledge_amount_cents = models.IntegerField(default=0)
    active_pledges = models.JSONField(default=dict, blank=True)
    
    last_sync = models.DateTimeField(auto_now=True)
    feed_token = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)

    def __str__(self):
        return f"{self.user.email} - Profile"
    
@receiver(post_delete, sender=UserMix)
def auto_delete_file_on_delete(sender, instance, **kwargs):
    """
    Deletes the physical file from the filesystem 
    whenever a UserMix object is deleted from the database.
    """
    if instance.image_upload:
        if os.path.isfile(instance.image_upload.path):
            os.remove(instance.image_upload.path)