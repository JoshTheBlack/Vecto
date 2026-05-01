import uuid, os, base64, logging
from decimal import Decimal
from django.db import models
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.files.base import ContentFile
from django.core.files.storage import FileSystemStorage
from django.db.models.signals import post_delete
from django.dispatch import receiver
from cryptography.fernet import Fernet, InvalidToken
import hashlib
from PIL import Image
from io import BytesIO

from django.conf import settings

logger = logging.getLogger(__name__)

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

import hashlib

class EncryptedCharField(models.CharField):
    """A custom field that encrypts data at rest using settings.CRYPTOGRAPHY_KEY."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Hash the environment variable to guarantee exactly 32 bytes, 
        # then encode it to the URL-safe base64 format Fernet requires.
        raw_key = settings.CRYPTOGRAPHY_KEY.encode()
        hashed_key = hashlib.sha256(raw_key).digest()
        fernet_key = base64.urlsafe_b64encode(hashed_key)
        
        self.fernet = Fernet(fernet_key)

    def get_prep_value(self, value):
        if value is None or value == "": 
            return None
        return self.fernet.encrypt(value.encode()).decode()

    def from_db_value(self, value, expression, connection):
        if value is None or value == "": 
            return None
        try:
            return self.fernet.decrypt(value.encode()).decode()
        except InvalidToken:
            # If the decryption key changes, old data will fail signature verification.
            # Catching this prevents the entire app from crashing, effectively "clearing"
            # the corrupted token so the user can re-authenticate.
            return None
    
class Network(models.Model):
    name = models.TextField()
    slug = models.SlugField(unique=True)
    owners = models.ManyToManyField(User, related_name="owned_networks", blank=True, help_text="Users who have admin access to this network's settings.")
    theme_config = models.JSONField(default=default_theme_config, blank=True)
    custom_domain = models.CharField(max_length=255, unique=True, blank=True, null=True, db_index=True)
    contact_email = models.EmailField(default="hosts@example.com", help_text="The official contact email displayed in RSS feeds for podcatcher verification.")
    
    patreon_campaign_id = models.CharField(max_length=100, blank=True, help_text="The numeric ID of the Patreon Campaign")
    patreon_sync_enabled = models.BooleanField(default=False)
    patreon_creator_access_token = EncryptedCharField(max_length=500, blank=True, null=True)
    patreon_creator_refresh_token = EncryptedCharField(max_length=500, blank=True, null=True)
    patreon_campaign_created_at = models.DateTimeField(blank=True, null=True, help_text="The date the creator launched their Patreon campaign.")
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
    
    # --- BRANDING & METADATA ---
    summary = models.TextField(blank=True, null=True)
    one_liner = models.CharField(max_length=255, blank=True, null=True)
    logo_url = models.URLField(max_length=500, blank=True, null=True) # Maps to image_small_url
    banner_image_url = models.URLField(max_length=500, blank=True, null=True) # Maps to image_url
    patreon_url = models.URLField(max_length=500, blank=True, null=True)
    discord_server_id = models.CharField(max_length=100, blank=True, null=True)

    auto_approve_trust_threshold = models.IntegerField(
        default=80, 
        help_text="Users with a trust score equal to or above this number bypass the review inbox. (Set to 101 to disable auto-approve)."
    )

    ingester_module = models.CharField(
        max_length=50, 
        default='default', 
        help_text="The script in pod_manager/ingesters/ to use. Default is 'default'."
    )

    # Billing Configuration
    base_cost = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal('0.00'), help_text="Flat monthly platform fee")
    per_user_cost = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal('0.00'), help_text="Cost per active 30-day patron")

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        
        # Automatically invalidate podcast shells when the network is updated 
        # (Catches changes made via the Django /admin panel)
        try:
            from django.core.cache import cache
            for pod in self.podcasts.all():
                cache.delete(f"feed_shell_public_{pod.id}")
                cache.delete(f"feed_shell_private_{pod.id}")
        except Exception as e:
            pass # Failsafe during initial migrations or empty DBs

    def __str__(self):
        return self.name

class PatreonTier(models.Model):
    """
    Dynamically configurable tiers managed in the Django Admin.
    """
    network = models.ForeignKey(Network, on_delete=models.CASCADE, related_name='tiers')
    name = models.CharField(max_length=100, help_text="e.g., 'In Association With'")
    minimum_cents = models.IntegerField(help_text="e.g., 600 for $6.00")
    checkout_url = models.URLField(max_length=500, blank=True, null=True)
    recurly_plan_codes = models.JSONField(
        default=list, 
        blank=True, 
        help_text='List of Recurly plan codes: ["cbm-monthly", "cbm-yearly"]'
    )

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
    description = models.TextField(blank=True, null=True, help_text="The main description for the podcast feed.")
    
    public_feed_url = models.URLField(max_length=2000, blank=True, null=True)
    subscriber_feed_url = models.URLField(max_length=2000, blank=True, null=True)
    required_tier = models.ForeignKey(PatreonTier, on_delete=models.SET_NULL, null=True, blank=True)

    image_url = models.URLField(max_length=2000, blank=True, help_text="Automatically populated from the RSS feed.")

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
    guid_public = models.TextField(blank=True, null=True, db_index=True)
    guid_private = models.TextField(blank=True, null=True, db_index=True)
    is_metadata_locked = models.BooleanField(default=False, help_text="If checked, future feed ingests will ONLY update the audio URLs. Title, Description, and Dates will not be overwritten.")
    
    title = models.TextField()
    pub_date = models.DateTimeField()
    
    # We store both versions of the audio
    audio_url_public = models.URLField(max_length=2000, blank=True, null=True)
    audio_url_subscriber = models.URLField(max_length=2000, blank=True, null=True)
    match_reason = models.CharField(max_length=100, blank=True, help_text="Audit trail for how the private audio was matched during ingestion.")
    
    # Store descriptions separately for cleaning/normalization
    raw_description = models.TextField()
    clean_description = models.TextField(blank=True)

    duration = models.CharField(max_length=20, blank=True)

    link = models.URLField(max_length=2000, blank=True, null=True)

    tags = models.JSONField(default=list, blank=True)

    # Podcast Chapters (JSON)
    chapters_public = models.JSONField(blank=True, null=True, help_text="Podcast Index Namespace Chapters for public feed")
    chapters_private = models.JSONField(blank=True, null=True, help_text="Podcast Index Namespace Chapters for private feed")
    
    class Meta:
        indexes = [
            models.Index(fields=['podcast', '-pub_date']),
            models.Index(fields=['podcast', 'guid_public']),
            models.Index(fields=['podcast', 'guid_private']),
        ]
        ordering = ['-pub_date']
    
    def __str__(self):
        # This will show "Podcast Title | Episode Title"
        # We truncate the title to 50 chars so the admin list stays clean
        # short_title = (self.title[:47] + '..') if len(self.title) > 50 else self.title
        return f"{self.podcast.title} | {self.title}"
    
    @property
    def has_public_audio(self):
        """Returns True if a public audio URL exists."""
        return bool(self.audio_url_public)

    @property
    def is_premium(self):
        """Returns True if a subscriber audio URL exists AND it is different from the public one."""
        return bool(self.audio_url_subscriber) and self.audio_url_subscriber != self.audio_url_public

def mix_cover_path(instance, filename):
    """Always name the file exactly <UUID>.<ext>"""
    ext = filename.split('.')[-1]
    return os.path.join('mix_covers', f"{instance.unique_id}.{ext}")

class OverwriteStorage(FileSystemStorage):
    def get_available_name(self, name, max_length=None):
        """Returns the same name even if it already exists on the system."""
        if self.exists(name):
            self.delete(name) # Use Django's backend-agnostic delete
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
        logger.debug(f"Saving UserMix: '{self.name}' for user {self.user.username}")
        super().save(*args, **kwargs)

        # Post-process (Crop & Resize)
        if self.image_upload:
            try:
                logger.debug(f"Processing image upload for mix: {self.unique_id}")
                
                with Image.open(self.image_upload) as img:
                    original_format = img.format or 'JPEG'
                    
                    width, height = img.size
                    if width != height:
                        logger.debug("Image is not square. Cropping...")
                        new_size = min(width, height)
                        left = (width - new_size) / 2
                        top = (height - new_size) / 2
                        right = (width + new_size) / 2
                        bottom = (height + new_size) / 2
                        img = img.crop((left, top, right, bottom))
                    
                    if img.height > 500 or img.width > 500:
                        logger.debug("Image exceeds 500x500. Resizing...")
                        img.thumbnail((500, 500), Image.Resampling.LANCZOS)
                    
                    if original_format == 'JPEG' and img.mode in ('RGBA', 'LA', 'P'):
                        logger.debug("Converting image to RGB to safely save as JPEG.")
                        img = img.convert('RGB')
                        
                    from io import BytesIO
                    temp_handle = BytesIO()
                    img.save(temp_handle, format=original_format)
                    temp_handle.seek(0)
                    
                self.image_upload.close()
                
                self.image_upload.save(self.image_upload.name, ContentFile(temp_handle.read()), save=False)
                logger.debug(f"Image processing complete for mix: {self.unique_id}")
                
            except Exception as e:
                logger.error(f"Image processing failed for mix {self.unique_id}: {e}", exc_info=True)
    
class PatronProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='patron_profile')
    patreon_id = models.CharField(max_length=50, unique=True)
    recurly_account_code = models.CharField(max_length=255, unique=True, null=True, blank=True)
    profile_image_url = models.URLField(max_length=500, null=True, blank=True)
    discord_id = models.CharField(max_length=100, null=True, blank=True)

    last_sync = models.DateTimeField(auto_now=True)
    feed_token = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    
    def __str__(self):
        return f"{self.user.email} - Profile"

class NetworkMembership(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='network_memberships')
    network = models.ForeignKey(Network, on_delete=models.CASCADE, related_name='memberships')
    
    # Patreon Fields
    patreon_join_date = models.DateTimeField(null=True, blank=True)
    patreon_pledge_cents = models.IntegerField(default=0)
    is_active_patron = models.BooleanField(default=False)
    
    #Recurly Fields
    active_recurly_plans = models.JSONField(default=list, blank=True)

    # Billing Presence Tracker ---
    last_active_date = models.DateField(null=True, blank=True, db_index=True, help_text="The last date this user interacted with this network's web or RSS properties.")

    discord_image_url = models.URLField(max_length=500, null=True, blank=True)

    # Gamification & Stats
    trust_score = models.IntegerField(default=0)
    total_playback_hits = models.IntegerField(default=0)
    total_hours_accessed = models.FloatField(default=0.0)
    streak_days = models.IntegerField(default=0)
    streak_weeks = models.IntegerField(default=0)
    last_playback_date = models.DateField(null=True, blank=True)
    last_play_week = models.IntegerField(null=True, blank=True)
    current_obsession = models.ForeignKey('Podcast', null=True, blank=True, on_delete=models.SET_NULL)
    
    # Contribution Tracking
    edits_title = models.IntegerField(default=0)
    edits_chapters = models.IntegerField(default=0)
    edits_tags = models.IntegerField(default=0)
    edits_descriptions = models.IntegerField(default=0)
    first_responder_count = models.IntegerField(default=0)

    # Avatar Preferences
    AVATAR_CHOICES = [
        ('patreon', 'Patreon'),
        ('discord', 'Discord'),
        ('custom', 'Custom'),
    ]
    preferred_avatar_source = models.CharField(max_length=20, choices=AVATAR_CHOICES, default='discord')
    custom_image_url = models.URLField(max_length=500, blank=True, null=True)
    custom_image_upload = models.ImageField(upload_to='custom_avatars/', blank=True, null=True)

    class Meta:
        unique_together = ('user', 'network')

    def __str__(self):
        return f"{self.user.username} - {self.network.name}"

    @property
    def display_avatar(self):
        if self.preferred_avatar_source == 'discord' and self.discord_image_url:
            return self.discord_image_url
        elif self.preferred_avatar_source == 'custom':
            if self.custom_image_upload: return self.custom_image_upload.url
            if self.custom_image_url: return self.custom_image_url
            
        # Default fallback to Patreon
        if hasattr(self.user, 'patron_profile') and self.user.patron_profile.profile_image_url:
            return self.user.patron_profile.profile_image_url
        return "https://ui-avatars.com/api/?name=V+P&background=random" # Failsafe

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)

        # Post-process Custom Avatar (Crop & Resize to 256x256)
        if self.custom_image_upload:
            try:
                from PIL import Image
                from io import BytesIO
                from django.core.files.base import ContentFile

                with Image.open(self.custom_image_upload) as img:
                    original_format = img.format or 'JPEG'
                    width, height = img.size
                    
                    if width != height:
                        new_size = min(width, height)
                        left = (width - new_size) / 2
                        top = (height - new_size) / 2
                        right = (width + new_size) / 2
                        bottom = (height + new_size) / 2
                        img = img.crop((left, top, right, bottom))
                    
                    if img.height > 256 or img.width > 256:
                        img.thumbnail((256, 256), Image.Resampling.LANCZOS)
                    
                    if original_format == 'JPEG' and img.mode in ('RGBA', 'LA', 'P'):
                        img = img.convert('RGB')
                        
                    temp_handle = BytesIO()
                    img.save(temp_handle, format=original_format)
                    temp_handle.seek(0)
                    
                # Save without triggering another save() recursion
                self.custom_image_upload.close()
                self.custom_image_upload.file = ContentFile(temp_handle.read())
                NetworkMembership.objects.filter(id=self.id).update(custom_image_upload=self.custom_image_upload.name)
            except Exception as e:
                logger.error(f"Avatar processing failed for membership {self.id}: {e}", exc_info=True)

@receiver(post_delete, sender=UserMix)
def auto_delete_file_on_delete(sender, instance, **kwargs):
    """
    Deletes the physical file from the filesystem 
    whenever a UserMix object is deleted from the database.
    """
    if instance.image_upload:
        try:
            instance.image_upload.storage.delete(instance.image_upload.name)
            logger.debug(f"Deleted physical file for UserMix: {instance.image_upload.name}")
        except Exception as e:
            logger.error(f"Failed to delete physical file {instance.image_upload.name}: {e}", exc_info=True)

class Invoice(models.Model):
    network = models.ForeignKey(Network, on_delete=models.CASCADE, related_name='invoices')
    created_at = models.DateTimeField(auto_now_add=True)
    active_user_count = models.IntegerField(default=0)
    amount_due = models.DecimalField(max_digits=10, decimal_places=2)
    pdf_file = models.FileField(upload_to='invoices/')
    
    def __str__(self):
        return f"{self.network.name} - {self.created_at.strftime('%Y-%m')}"
    
class NetworkMix(models.Model):
    """A curated super-feed managed by the network creators."""
    network = models.ForeignKey(Network, on_delete=models.CASCADE, related_name='mixes')
    name = models.CharField(max_length=200)
    slug = models.SlugField()
    
    image_url = models.URLField(max_length=2000, blank=True, help_text="Optional custom artwork URL")
    image_upload = models.ImageField(upload_to=mix_cover_path, storage=mix_storage, blank=True, null=True, help_text="Uploaded artwork")
    
    unique_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    selected_podcasts = models.ManyToManyField('Podcast')
    required_tier = models.ForeignKey('PatreonTier', on_delete=models.SET_NULL, null=True, blank=True, help_text="If set, the entire feed requires this tier to access.")

    class Meta:
        unique_together = ('network', 'slug')

    def __str__(self):
        return f"{self.network.name} Mix - {self.name}"
        
    @property
    def display_image(self):
        if self.image_upload:
            return self.image_upload.url
        return self.image_url

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        # Post-process (Crop & Resize) using the exact same logic as UserMix
        if self.image_upload:
            try:
                with Image.open(self.image_upload) as img:
                    original_format = img.format or 'JPEG'
                    width, height = img.size
                    if width != height:
                        new_size = min(width, height)
                        left = (width - new_size) / 2
                        top = (height - new_size) / 2
                        right = (width + new_size) / 2
                        bottom = (height + new_size) / 2
                        img = img.crop((left, top, right, bottom))
                    if img.height > 500 or img.width > 500:
                        img.thumbnail((500, 500), Image.Resampling.LANCZOS)
                    if original_format == 'JPEG' and img.mode in ('RGBA', 'LA', 'P'):
                        img = img.convert('RGB')
                        
                    from io import BytesIO
                    temp_handle = BytesIO()
                    img.save(temp_handle, format=original_format)
                    temp_handle.seek(0)
                    
                self.image_upload.close()
                self.image_upload.save(self.image_upload.name, ContentFile(temp_handle.read()), save=False)
            except Exception as e:
                logger.error(f"Image processing failed for network mix {self.unique_id}: {e}", exc_info=True)

class EpisodeEditSuggestion(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('approved', 'Approved'),
        ('rejected', 'Rejected'),
        ('rolled_back', 'Rolled Back'),
    ]

    episode = models.ForeignKey(Episode, on_delete=models.CASCADE, related_name='edit_suggestions')
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='submitted_edits')
    
    # Store the proposed changes (e.g., {"description": "New text", "tags": "new, tags"})
    suggested_data = models.JSONField(default=dict)
    # Store the exact state of the fields BEFORE the edit was applied, used for rollbacks
    original_data = models.JSONField(default=dict, null=True, blank=True)
    
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    is_first_responder = models.BooleanField(default=False)
    
    created_at = models.DateTimeField(auto_now_add=True)
    resolved_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"Edit by {self.user.username} for {self.episode.title} ({self.status})"

@receiver(post_delete, sender=NetworkMix)
def auto_delete_file_on_delete_network_mix(sender, instance, **kwargs):
    if instance.image_upload:
        try:
            instance.image_upload.storage.delete(instance.image_upload.name)
        except Exception:
            pass