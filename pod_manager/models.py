import uuid, os, base64, logging
from decimal import Decimal
from urllib.parse import urlparse
from django.db import models
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.files.base import ContentFile
from django.core.files.storage import FileSystemStorage
from django.db.models.signals import post_delete, post_save, pre_save
from django.dispatch import receiver
from cryptography.fernet import Fernet, InvalidToken
import hashlib
from PIL import Image
from io import BytesIO

from django.conf import settings
from django.utils import timezone

from .services.images import process_image_field
from .services.r2_storage import select_media_storage

logger = logging.getLogger(__name__)

# Legacy local-storage key prefixes from before the R2 (vecto-cdn) cutover. Rows
# still holding one of these names are served straight from /media until the
# backfill re-keys them to the stable webp key — this keeps the cutover window at
# zero. DELETE this branch (and the constant) once every row is migrated.
_LEGACY_IMAGE_PREFIXES = ('custom_avatars/', 'mix_covers/')


def _versioned_image_url(fieldfile, version):
    """Public URL for an R2-backed image field, cache-busted with ?v=version.

    New stable webp keys resolve through the field's storage (the cdn, or local
    /media when R2 is disabled) plus the version query string. Legacy pre-cutover
    rows are served from /media unchanged so they keep working until the backfill
    re-keys them.
    """
    name = fieldfile.name or ''
    if name.startswith(_LEGACY_IMAGE_PREFIXES):
        return f"{settings.MEDIA_URL}{name}"
    url = fieldfile.url
    return f"{url}?v={version}" if version else url

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

    # Transcription defaults — inherited by podcasts unless overridden at podcast level
    whisper_initial_prompt = models.TextField(
        blank=True,
        help_text="Vocabulary hint passed to Whisper. E.g. host names, show titles, proper nouns. Leave blank to omit.",
    )
    whisper_model = models.CharField(
        max_length=50, default='medium.en', blank=True,
        help_text="Whisper model size: tiny/base/small/medium/large or language-specific (e.g. medium.en). "
                  "Leave blank to use the system default (WHISPER_DEFAULT_MODEL / WHISPER_MODEL).",
    )
    whisper_language = models.CharField(
        max_length=10, default='en',
        help_text="BCP-47 language code passed to Whisper. E.g. 'en', 'es', 'fr'.",
    )
    whisper_min_speakers = models.IntegerField(default=1, help_text="Minimum expected speakers for diarization.")
    whisper_num_speakers = models.IntegerField(null=True, blank=True, help_text="Expected speaker count hint for diarization. Null = auto-detect.")
    whisper_max_speakers = models.IntegerField(default=4, help_text="Maximum expected speakers for diarization.")

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

    # Transcription overrides — null means inherit from network defaults
    whisper_initial_prompt = models.TextField(
        blank=True, null=True,
        help_text="Override network initial_prompt for this podcast. Null = inherit.",
    )
    whisper_model = models.CharField(
        max_length=50, blank=True, null=True,
        help_text="Override network whisper model for this podcast. Null = inherit.",
    )
    whisper_language = models.CharField(
        max_length=10, blank=True, null=True,
        help_text="Override network language for this podcast. Null = inherit.",
    )
    whisper_min_speakers = models.IntegerField(null=True, blank=True, help_text="Override min speakers. Null = inherit.")
    whisper_num_speakers = models.IntegerField(null=True, blank=True, help_text="Override default speakers. Null = inherit.")
    whisper_max_speakers = models.IntegerField(null=True, blank=True, help_text="Override max speakers. Null = inherit.")

    # Per-feed override for the R2 serving precedence: when True, every episode
    # in this podcast that has an r2_url is served from R2 instead of its origin
    # (e.g. force a whole Patreon/Libsyn feed onto the mirror). See Episode.playback_url.
    force_r2_serve = models.BooleanField(
        default=False,
        help_text="Serve this podcast's audio from the R2 mirror whenever a mirror exists, "
                  "instead of the original host.",
    )

    class Meta:
        unique_together = ('network', 'slug')


    def __str__(self):
        return self.title

class Episode(models.Model):
    """An individual episode harvested from a feed."""
    podcast = models.ForeignKey(Podcast, on_delete=models.CASCADE, related_name='episodes')
    guid_public = models.TextField(blank=True, null=True, db_index=True)
    guid_private = models.TextField(blank=True, null=True, db_index=True)
    is_metadata_locked = models.BooleanField(default=False, help_text="If checked, future feed ingests will ONLY update the audio URLs. Title, Description, and Dates will not be overwritten.")
    audio_locked = models.BooleanField(default=False, help_text="If checked, future feed ingests will NOT overwrite audio URLs. Set automatically by the GDrive recovery script.")
    
    title = models.TextField()
    pub_date = models.DateTimeField()
    
    # We store both versions of the audio
    audio_url_public = models.URLField(max_length=2000, blank=True, null=True)
    audio_url_subscriber = models.URLField(max_length=2000, blank=True, null=True)
    match_reason = models.CharField(max_length=100, blank=True, help_text="Audit trail for how the private audio was matched during ingestion.")

    # R2 audio mirror (subscriber audio only — see planned_features.txt).
    # r2_url presence == "we have a backup"; it is the field the serving layer
    # reads and the source of truth for orphan reference checks (hence indexed).
    r2_url = models.URLField(
        max_length=2000, blank=True, null=True, db_index=True,
        help_text="Public URL of the mirrored subscriber audio on R2. Presence means a backup exists.",
    )
    r2_uploaded_at = models.DateTimeField(null=True, blank=True, help_text="When the R2 mirror was last written.")
    # Cheap change-detection fingerprint of the source the mirror was made from
    # (e.g. "{etag}:{content_length}"). Lets re-ingest detect a genuinely changed
    # source and re-mirror without re-downloading every file every cycle.
    r2_source_signature = models.CharField(max_length=255, blank=True, default='')
    
    # Store descriptions separately for cleaning/normalization
    raw_description = models.TextField()
    clean_description = models.TextField(blank=True)

    duration = models.CharField(max_length=20, blank=True)

    link = models.URLField(max_length=2000, blank=True, null=True)

    tags = models.JSONField(default=list, blank=True)

    # Podcast Chapters (JSON)
    chapters_public = models.JSONField(blank=True, null=True, help_text="Podcast Index Namespace Chapters for public feed")
    chapters_private = models.JSONField(blank=True, null=True, help_text="Podcast Index Namespace Chapters for private feed")

    # iTunes / Podcast 2.0 sequence metadata
    season_number  = models.PositiveSmallIntegerField(null=True, blank=True)
    episode_number = models.PositiveSmallIntegerField(null=True, blank=True)
    episode_type   = models.CharField(max_length=50, default='', blank=True)
    # Per-episode iTunes explicit flag. Null = inherit the channel-level rating
    # (feeds emit no item-level tag); True/False emit <itunes:explicit>true|false.
    # Ingested from the feed even when metadata is locked (see commit_episode).
    explicit       = models.BooleanField(null=True, blank=True)

    # Publication status
    is_published = models.BooleanField(default=True, db_index=True,
        help_text="Uncheck to hide this episode from all RSS feeds.")
    scheduled_at = models.DateTimeField(null=True, blank=True, db_index=True,
        help_text="If set and is_published=False, Celery will publish at this time.")

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
    
    # Host substring for the dead S3 bucket whose audio is mid-recovery. An
    # episode whose served playback URL still points here has no real audio yet.
    AUDIO_HOST_S3 = 's3.amazonaws.com'

    def audio_origin(self):
        """Classify the subscriber audio host: one of
        {'megaphone','libsyn','gdrive','s3_dead','other','none'}.

        Drives the R2 serving precedence and the "Libsyn/other serve from origin,
        R2 is backup only" policy. Pure host detection — no DB or network access."""
        url = self.audio_url_subscriber or ''
        if not url:
            return 'none'
        host = urlparse(url).netloc.lower()
        if 'docs.google.com' in url or 'drive.google.com' in host:
            return 'gdrive'
        if self.AUDIO_HOST_S3 in url:
            return 's3_dead'
        if 'megaphone.fm' in host:
            return 'megaphone'
        if 'libsyn' in host:
            return 'libsyn'
        return 'other'

    def playback_url(self, has_access):
        """The URL play_episode would redirect to for a listener with this
        access level. Single source of truth for both playback and feeds.

        Serving precedence (subscriber audio only; public/Megaphone is never
        mirrored — see planned_features.txt section C):
          - No access            -> audio_url_public (R2 never applies).
          - Access + r2_url and global R2_FORCE_SERVE        -> r2_url
          - Access + r2_url and podcast.force_r2_serve       -> r2_url
          - Access + r2_url and origin is GDrive             -> r2_url
                                   (GDrive can't serve a browser)
          - Access + origin is Libsyn (durable origin)       -> audio_url_subscriber
                                   (R2 is backup-only unless forced above)
          - Access + r2_url (recovered dead-S3, Patreon, etc) -> r2_url
                                   (Patreon enclosure URLs are ephemeral/signed;
                                    once mirrored we must serve from R2, not origin)
          - Access otherwise                                 -> audio_url_subscriber
        """
        if not (has_access and self.audio_url_subscriber):
            return self.audio_url_public

        has_r2 = bool(self.r2_url)
        if has_r2:
            if getattr(settings, 'R2_FORCE_SERVE', False):
                return self.r2_url
            if self.podcast.force_r2_serve:
                return self.r2_url

        origin = self.audio_origin()
        if has_r2 and origin == 'gdrive':
            return self.r2_url
        if origin == 'libsyn':
            # Libsyn is a durable origin with its own CDN; R2 is backup-only
            # unless forced above. Other hosts (Patreon's ephemeral signed URLs,
            # recovered dead-S3) prefer the mirror whenever one exists.
            return self.audio_url_subscriber
        if has_r2:
            return self.r2_url
        return self.audio_url_subscriber

    def serves_s3_audio(self, has_access):
        """True when the playback redirect would land on the dead S3 bucket, so
        the episode must be withheld from feeds until its audio is recovered.
        An episode with an r2_url no longer lands on S3, so it is auto-un-withheld."""
        url = self.playback_url(has_access)
        return bool(url) and self.AUDIO_HOST_S3 in url

    @property
    def is_gdrive_recovery(self):
        """Identifies if the premium audio is a temporary Google Drive link."""
        return bool(self.audio_url_subscriber and 'docs.google.com' in self.audio_url_subscriber)

    @property
    def gdrive_preview_url(self):
        """Generates the native Google Drive web player link for validation."""
        if self.is_gdrive_recovery:
            try:
                # Extracts FILE_ID from https://docs.google.com/uc?export=download&id=FILE_ID
                file_id = self.audio_url_subscriber.split('id=')[1].split('&')[0]
                return f"https://drive.google.com/file/d/{file_id}/view"
            except IndexError:
                pass
        return None

class EpisodeCrossPublication(models.Model):
    """Places an episode into another podcast's feeds in addition to its
    parent. The episode stays a single entity (one parent podcast, one set of
    fragments/GUIDs); this link only affects feed membership and, optionally,
    which podcast's tier gates the subscriber audio."""

    class AccessMode(models.TextChoices):
        INHERIT = 'inherit', "Keep episode's own gating (parent podcast tier)"
        TARGET = 'target', "Use the target podcast's tier"

    episode = models.ForeignKey(Episode, on_delete=models.CASCADE, related_name='cross_publications')
    podcast = models.ForeignKey(
        Podcast, on_delete=models.CASCADE, related_name='cross_publications_in',
        help_text="Target podcast whose feeds also carry this episode.")
    access_mode = models.CharField(max_length=10, choices=AccessMode.choices, default=AccessMode.INHERIT)
    added_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='+')
    added_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['episode', 'podcast'], name='uniq_episode_crosspub_target'),
        ]
        indexes = [
            models.Index(fields=['podcast', 'episode']),
        ]

    def clean(self):
        from django.core.exceptions import ValidationError
        if self.episode_id and self.podcast_id and self.episode.podcast_id == self.podcast_id:
            raise ValidationError("An episode cannot be cross-published into its own parent podcast.")

    def __str__(self):
        return f"{self.episode_id} also in {self.podcast.title} ({self.access_mode})"


class R2OrphanedObject(models.Model):
    """A deletion-CANDIDATE list for R2 objects no Episode.r2_url points at.

    Orphans arise from (1) re-versioning (new content hash -> new key, old key
    abandoned) and (2) partial failures (upload succeeded, DB commit interrupted).
    The object is LEFT AT ITS ORIGINAL KEY for a retention window so in-flight
    streaming sessions don't break, then a cleanup task hard-deletes it AFTER
    re-validating against live Episode.r2_url. See planned_features.txt section I.

    This table is never an authority — Episode.r2_url is the source of truth.
    Content-hash keying intentionally dedupes, so a key may be referenced by more
    than one episode; recording and cleanup both re-check live references first.
    """

    class Reason(models.TextChoices):
        REVERSION = 'reversion', 'Re-versioned (new content hash)'
        MOVE_REKEY = 'move_rekey', 'Re-keyed on move (byte-identical copy at new key)'
        RECONCILIATION = 'reconciliation', 'Found unreferenced by reconciliation sweep'
        MANUAL = 'manual', 'Manually recorded'

    # Host-stripped R2 object key. Unique so concurrent re-versions / sweeps
    # can't double-insert the same orphan.
    key = models.CharField(max_length=1024, unique=True)
    orphaned_at = models.DateTimeField(default=timezone.now, db_index=True,
        help_text="Retention clock start; cleanup expiry is computed per reason.")
    reason = models.CharField(max_length=20, choices=Reason.choices, default=Reason.MANUAL)
    # Audit/restore convenience; null for reconciliation-discovered orphans.
    episode = models.ForeignKey(Episode, on_delete=models.SET_NULL, null=True, blank=True, related_name='+')

    def __str__(self):
        return f"orphan {self.key} ({self.reason} @ {self.orphaned_at:%Y-%m-%d})"


def mix_cover_path(instance, filename):
    """Stable R2 key for a mix cover: covers/<UUID>.webp.

    The extension is always .webp (process_image_field normalizes output), so the
    key never changes on re-upload — the object is OVERWRITTEN in place, never
    orphaned. The passed filename is ignored.
    """
    return f"covers/{instance.unique_id}.webp"


def avatar_upload_path(instance, filename):
    """Stable R2 key for a custom avatar: avatars/<user>-<network>.webp.

    custom_image_upload is per membership (unique on user+network), so the
    id-derived key is unique and stable; .webp keeps the extension constant for a
    true overwrite. The passed filename is ignored.
    """
    return f"avatars/{instance.user_id}-{instance.network_id}.webp"


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
    image_upload = models.ImageField(upload_to=mix_cover_path, storage=select_media_storage, blank=True, null=True, help_text="Uploaded artwork")
    # Cache-bust counter for the stable R2 key — bumped on each new upload and
    # appended to the cover URL as ?v=N (the object key never changes).
    image_version = models.IntegerField(default=0)

    unique_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    selected_podcasts = models.ManyToManyField(Podcast)
    last_accessed = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.user.username}'s Mix - {self.name}"

    @property
    def display_image(self):
        if self.image_upload:
            return _versioned_image_url(self.image_upload, self.image_version)
        return self.image_url

    def save(self, *args, **kwargs):
        logger.debug(f"Saving UserMix: '{self.name}' for user {self.user.username}")
        # Single-write: process the fresh upload to WebP and write it ONCE at the
        # stable key, then persist. _committed is False only for a just-uploaded
        # file, so plain row re-saves never reprocess/re-PUT.
        if self.image_upload and not self.image_upload._committed:
            try:
                data = process_image_field(self.image_upload, 500)
                self.image_upload.save('cover.webp', ContentFile(data), save=False)
                self.image_version = (self.image_version or 0) + 1
            except Exception as e:
                logger.error(f"Image processing failed for mix {self.unique_id}: {e}", exc_info=True)
        super().save(*args, **kwargs)
    
class PatronProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='patron_profile')
    # Nullable so Recurly-only users (no Patreon link) don't all collide on
    # the empty string. PostgreSQL allows multiple NULLs under a unique
    # constraint; the constraint only applies to non-NULL values.
    patreon_id = models.CharField(max_length=50, unique=True, null=True, blank=True)
    recurly_account_code = models.CharField(max_length=255, unique=True, null=True, blank=True)
    profile_image_url = models.URLField(max_length=500, null=True, blank=True)
    discord_id = models.CharField(max_length=100, null=True, blank=True)

    # Recurly plan codes are an intrinsic property of the Recurly account
    # (one account → one plan set), so they live on the global profile rather
    # than per-network. _evaluate_access reads from here.
    active_recurly_plans = models.JSONField(default=list, blank=True)

    TOTP_REPLACE = 'replace'
    TOTP_MFA = 'mfa'
    TOTP_MODE_CHOICES = [('replace', 'Replace email OTP'), ('mfa', 'Require both (MFA)')]
    totp_mode = models.CharField(max_length=10, choices=TOTP_MODE_CHOICES, default='replace')

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
    # Speaker-label identifications (user_edit_rollback.md §3.4) — moved by the
    # per-speaker points award at approval and the exact wash on rollback.
    edits_speakers = models.IntegerField(default=0)
    # iTunes / Sequence Metadata (season / episode # / episode type) edits (§8a).
    edits_sequence = models.IntegerField(default=0)
    first_responder_count = models.IntegerField(default=0)

    # Avatar Preferences
    AVATAR_CHOICES = [
        ('patreon', 'Patreon'),
        ('discord', 'Discord'),
        ('gravatar', 'Gravatar'),
        ('custom', 'Custom'),
    ]
    preferred_avatar_source = models.CharField(max_length=20, choices=AVATAR_CHOICES, default='discord')
    custom_image_url = models.URLField(max_length=500, blank=True, null=True)
    custom_image_upload = models.ImageField(upload_to=avatar_upload_path, storage=select_media_storage, blank=True, null=True)
    # Cache-bust counter for the stable R2 avatar key — bumped on each new upload
    # and appended to the avatar URL as ?v=N (the object key never changes).
    image_version = models.IntegerField(default=0)

    class Meta:
        unique_together = ('user', 'network')

    def __str__(self):
        return f"{self.user.username} - {self.network.name}"

    @property
    def email_hash(self):
        import hashlib
        if self.user and self.user.email:
            # V3 API REQUIRES SHA256 (MD5 is strictly prohibited)
            return hashlib.sha256(self.user.email.strip().lower().encode('utf-8')).hexdigest()
        # SHA256 hashes are 64 characters long
        return "0" * 64
    
    @property
    def custom_avatar_url(self):
        """Versioned, legacy-safe URL for the uploaded custom avatar (or None).

        Use this anywhere the custom upload is rendered DIRECTLY (e.g. the
        profile avatar picker), rather than .url — it appends the ?v=N cache-bust
        and routes pre-cutover legacy rows to local /media. display_avatar uses
        the same helper for the 'custom' branch.
        """
        if self.custom_image_upload:
            return _versioned_image_url(self.custom_image_upload, self.image_version)
        return None

    @property
    def display_avatar(self):
        discord_url = self.discord_image_url
        custom_url = (_versioned_image_url(self.custom_image_upload, self.image_version)
                      if self.custom_image_upload else self.custom_image_url)
        
        patreon_url = None
        if hasattr(self.user, 'patron_profile') and self.user.patron_profile.profile_image_url:
            patreon_url = self.user.patron_profile.profile_image_url

        from urllib.parse import urlencode
        params = urlencode({'d': 'mp', 's': '256'})
        
        # Update base URL to 0.gravatar.com per v3 documentation
        gravatar_url = f"https://0.gravatar.com/avatar/{self.email_hash}?{params}"

        # 1. Try their explicitly preferred source first
        if self.preferred_avatar_source == 'discord' and discord_url: return discord_url
        if self.preferred_avatar_source == 'patreon' and patreon_url: return patreon_url
        if self.preferred_avatar_source == 'custom' and custom_url: return custom_url
        if self.preferred_avatar_source == 'gravatar': return gravatar_url

        # 2. Fallbacks: Cascade down what we actually have
        
        if custom_url: return custom_url
        if discord_url: return discord_url
        if patreon_url: return patreon_url
        
        return gravatar_url

    def save(self, *args, **kwargs):
        # Single-write: process the fresh upload to WebP and write it ONCE at the
        # stable key, then persist. _committed is False only for a just-uploaded
        # file, so plain row re-saves never reprocess/re-PUT.
        if self.custom_image_upload and not self.custom_image_upload._committed:
            try:
                data = process_image_field(self.custom_image_upload, 256)
                self.custom_image_upload.save('avatar.webp', ContentFile(data), save=False)
                self.image_version = (self.image_version or 0) + 1
            except Exception as e:
                logger.error(f"Avatar processing failed for membership {self.id}: {e}", exc_info=True)
        super().save(*args, **kwargs)

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


@receiver(post_delete, sender=NetworkMembership)
def auto_delete_membership_avatar(sender, instance, **kwargs):
    """Deletes the custom avatar file when a NetworkMembership row is deleted."""
    if instance.custom_image_upload:
        try:
            instance.custom_image_upload.storage.delete(instance.custom_image_upload.name)
            logger.debug(f"Deleted custom avatar for membership {instance.id}: {instance.custom_image_upload.name}")
        except Exception as e:
            logger.error(f"Failed to delete avatar file {instance.custom_image_upload.name}: {e}", exc_info=True)

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
    image_upload = models.ImageField(upload_to=mix_cover_path, storage=select_media_storage, blank=True, null=True, help_text="Uploaded artwork")
    # Cache-bust counter for the stable R2 key — bumped on each new upload and
    # appended to the cover URL as ?v=N (the object key never changes).
    image_version = models.IntegerField(default=0)

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
            return _versioned_image_url(self.image_upload, self.image_version)
        return self.image_url

    def save(self, *args, **kwargs):
        # Single-write: process the fresh upload to WebP and write it ONCE at the
        # stable key, then persist. _committed is False only for a just-uploaded
        # file, so plain row re-saves never reprocess/re-PUT.
        if self.image_upload and not self.image_upload._committed:
            try:
                data = process_image_field(self.image_upload, 500)
                self.image_upload.save('cover.webp', ContentFile(data), save=False)
                self.image_version = (self.image_version or 0) + 1
            except Exception as e:
                logger.error(f"Image processing failed for network mix {self.unique_id}: {e}", exc_info=True)
        super().save(*args, **kwargs)

class EpisodeEditSuggestion(models.Model):
    class Status(models.TextChoices):
        PENDING = 'pending', 'Pending'
        APPROVED = 'approved', 'Approved'
        REJECTED = 'rejected', 'Rejected'
        ROLLED_BACK = 'rolled_back', 'Rolled Back'
        # Re-transcription renumbers diarization, so prior speaker edits no longer
        # align with the fresh base — they are SUPERSEDED: retained for audit /
        # trust history but excluded from the replay fold (see user_edit_rollback.md §7).
        SUPERSEDED = 'superseded', 'Superseded'

    episode = models.ForeignKey(Episode, on_delete=models.CASCADE, related_name='edit_suggestions')
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='submitted_edits')

    suggested_data = models.JSONField(default=dict)
    original_data = models.JSONField(default=dict, null=True, blank=True)

    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    is_first_responder = models.BooleanField(default=False)
    # Trust delta banked at approval so rollback reverses the exact amount
    # without recomputation (user_edit_rollback.md §3.4 — speaker edits).
    points = models.IntegerField(default=0)
    # Per-counter NetworkMembership deltas this edit credited at approval, e.g.
    # {"edits_tags": 3, "edits_chapters": 2, "edits_sequence": 1}. Rollback
    # decrements exactly these, so a multi-tag/chapter edit is an exact wash
    # (points stays the TRUST delta). Empty for pre-feature rows → rollback skips
    # the counter decrement; a future counter just appears here and is reversed
    # generically, with edits missing the key left untouched.
    counter_deltas = models.JSONField(default=dict, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    resolved_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"Edit by {self.user.username} for {self.episode.title} ({self.status})"

class LogEntry(models.Model):
    class Level(models.TextChoices):
        DEBUG = 'DEBUG', 'Debug'
        INFO = 'INFO', 'Info'
        WARNING = 'WARNING', 'Warning'
        ERROR = 'ERROR', 'Error'
        CRITICAL = 'CRITICAL', 'Critical'

    level = models.CharField(max_length=10, choices=Level.choices, db_index=True)
    level_no = models.IntegerField(db_index=True)
    logger_name = models.CharField(max_length=200)
    module = models.CharField(max_length=200)
    func_name = models.CharField(max_length=200)
    lineno = models.IntegerField()
    message = models.TextField()
    user = models.ForeignKey(
        'auth.User',
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='log_entries',
        db_index=True,
    )
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name_plural = 'log entries'

    def __str__(self):
        ts = self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else '?'
        return f"[{self.level}] {ts} - {self.message[:80]}"


class Transcript(models.Model):
    class Status(models.TextChoices):
        PENDING    = 'pending',    'Pending'
        PROCESSING = 'processing', 'Processing'
        COMPLETED  = 'completed',  'Completed'
        FAILED     = 'failed',     'Failed'
        # Source audio is permanently unavailable (dead bucket / 404). Parks
        # here without retrying until the episode's audio URL changes.
        AWAITING_RECOVERY = 'awaiting_recovery', 'Awaiting Recovery'

    episode = models.OneToOneField(
        Episode,
        on_delete=models.CASCADE,
        related_name='transcript',
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    language = models.CharField(max_length=10, default='en')

    # File markers — populated once transcription completes. Hold the R2 key
    # (transcripts/{id}.{ext}) when R2-backed, or the legacy MEDIA_ROOT-relative
    # path when local; either way they're existence markers per format.
    vtt_file       = models.CharField(max_length=500, blank=True, null=True)
    json_file      = models.CharField(max_length=500, blank=True, null=True)
    srt_file       = models.CharField(max_length=500, blank=True, null=True)
    html_file      = models.CharField(max_length=500, blank=True, null=True)
    words_json_file = models.CharField(max_length=500, blank=True, null=True)

    # Cache-bust counter for the R2 transcript objects (appended as ?v=N). Bumped
    # on every (re)transcribe and speaker-label rewrite. 0 = legacy local-only
    # (written before the R2 cutover / when R2_MEDIA_ENABLED is off).
    version = models.IntegerField(default=0)

    # Plain text extracted from JSON for full-text search (Phase 8)
    transcript_text = models.TextField(blank=True, null=True)

    # Timestamps
    requested_at  = models.DateTimeField(null=True, blank=True)
    started_at    = models.DateTimeField(null=True, blank=True)
    completed_at  = models.DateTimeField(null=True, blank=True)

    # Audit / debugging
    error_message     = models.TextField(blank=True, null=True)
    source_audio_url  = models.URLField(max_length=2000, blank=True, null=True)
    whisper_model_used = models.CharField(max_length=50, blank=True)
    worker            = models.CharField(max_length=255, blank=True, help_text="Host/worker that ran the transcription.")
    retry_count       = models.IntegerField(default=0)

    class Meta:
        verbose_name_plural = 'transcripts'

    def __str__(self):
        return f"Transcript [{self.status}] – {self.episode}"

    def get_url(self, ext: str) -> str | None:
        """Public URL for a transcript file, or None if not yet written."""
        from django.urls import reverse
        field = 'words_json_file' if ext == 'words' else f'{ext}_file'
        if not getattr(self, field, None):
            return None
        return reverse('serve_transcript', kwargs={'episode_id': self.episode_id, 'ext': ext})


@receiver(post_delete, sender=NetworkMix)
def auto_delete_file_on_delete_network_mix(sender, instance, **kwargs):
    if instance.image_upload:
        try:
            instance.image_upload.storage.delete(instance.image_upload.name)
        except Exception:
            pass


@receiver(pre_save, sender=Podcast)
def assign_default_tier(sender, instance, **kwargs):
    if not instance.required_tier and instance.network_id:
        default_tier = PatreonTier.objects.filter(network=instance.network, is_default=True).first()
        if default_tier:
            instance.required_tier = default_tier


def normalize_chapters(value):
    """Coerce a legacy bare chapter list into the canonical Podcast Index dict
    ``{"version": "1.2.0", "chapters": [...]}``; an empty list becomes None.
    Existing dicts and None pass through untouched.

    Lossless and never raises — it only re-shapes, so it's safe on every save.
    Full sanitization of edit submissions still happens via
    services.edits.parse_chapter_payload at the edit-submission paths.
    """
    if isinstance(value, list):
        return {"version": "1.2.0", "chapters": value} if value else None
    return value


@receiver(pre_save, sender=Episode)
def normalize_episode_chapters(sender, instance, **kwargs):
    """Guarantee an episode never persists chapters as a legacy bare list —
    every write lands in the canonical dict shape (or None)."""
    instance.chapters_public = normalize_chapters(instance.chapters_public)
    instance.chapters_private = normalize_chapters(instance.chapters_private)


@receiver(post_save, sender=Episode)
def queue_transcription_on_episode_save(sender, instance, created=False, **kwargs):
    from django.conf import settings as django_settings
    from django.utils import timezone
    if not getattr(django_settings, 'WHISPER_ENABLED', False):
        return
    if not instance.audio_url_subscriber:
        return

    transcript = Transcript.objects.filter(episode=instance).first()

    # Decide whether to (re)queue. New episodes queue on creation. An existing
    # transcript only requeues when it's parked AWAITING_RECOVERY and the audio
    # URL has actually changed since the failed attempt (e.g. GDrive recovery
    # swapped in a live link) — every other state is left untouched so plain
    # edits don't thrash the queue.
    if transcript is None:
        should_queue = created
    elif (transcript.status == Transcript.Status.AWAITING_RECOVERY
          and instance.audio_url_subscriber != transcript.source_audio_url):
        should_queue = True
    else:
        should_queue = False

    if not should_queue:
        return

    # Create/update the record immediately so the admin and episode page show
    # "Queued" status during the window between dispatch and Celery pickup.
    Transcript.objects.update_or_create(
        episode=instance,
        defaults={
            'status': Transcript.Status.PENDING,
            'requested_at': timezone.now(),
            'error_message': None,
        },
    )
    from pod_manager.services.transcription import dispatch_transcription
    dispatch_transcription(instance.id)


@receiver(post_save, sender=Episode)
def queue_r2_mirror_on_episode_save(sender, instance, created=False, **kwargs):
    """Standalone R2 mirror for episodes that transcription WON'T carry.

    When WHISPER is enabled, run_transcription mirrors inline off its single
    download (Phase 4) — even if whisper itself is unreachable, the mirror runs
    BEFORE the ASR call — so dispatching here too would cause a second download.
    We therefore only fire when transcription is disabled/not configured: a
    newly-created premium episode still needs its off-platform backup (and
    becomes browser-playable from R2 where the serving policy applies).

    Scope: new episodes only (parallel to the transcription signal). Legacy and
    audio-changed episodes are covered by the Phase 5 backfill / --force."""
    from django.conf import settings as django_settings
    if not getattr(django_settings, 'R2_MIRROR_ENABLED', True):
        return
    if not created or not instance.is_premium:
        return
    # Transcription will run on this new episode and mirror inline — defer to it.
    if getattr(django_settings, 'WHISPER_ENABLED', False):
        return
    from pod_manager.tasks import task_mirror_episode_audio
    task_mirror_episode_audio.delay(instance.id)


@receiver(post_delete, sender=Transcript)
def auto_delete_transcript_files(sender, instance, **kwargs):
    from pathlib import Path
    from django.conf import settings as django_settings
    from pod_manager.services.transcription import ALLOWED_EXTENSIONS, transcript_r2_key

    # R2-backed transcript (version >= 1, R2 enabled): delete the cdn objects.
    # Stable keys are overwritten in place, so there's nothing else to track.
    if django_settings.R2_MEDIA_ENABLED and (instance.version or 0) >= 1:
        from pod_manager.services.r2_storage import delete_media_object
        for ext in ALLOWED_EXTENSIONS:
            try:
                delete_media_object(transcript_r2_key(instance.episode_id, ext))
            except Exception as e:
                logger.error("Failed to delete R2 transcript %s.%s: %s", instance.episode_id, ext, e)

    # Legacy local files (always attempted — markers may point at MEDIA_ROOT).
    fields = ['vtt_file', 'json_file', 'srt_file', 'html_file', 'words_json_file']
    deleted_dir = None
    for field in fields:
        rel = getattr(instance, field, None)
        if not rel:
            continue
        path = Path(django_settings.MEDIA_ROOT) / rel
        try:
            if path.exists():
                path.unlink()
                deleted_dir = path.parent
        except Exception as e:
            logger.error("Failed to delete transcript file %s: %s", path, e)
    if deleted_dir and deleted_dir.exists() and not any(deleted_dir.iterdir()):
        try:
            deleted_dir.rmdir()
        except Exception:
            pass


class CommandRun(models.Model):
    """Durable audit record of one Admin Command Console run (design §8a).

    Every console-initiated management command is recorded here — who launched it,
    with what (redacted) arguments, how it ended, and the full captured log — so
    history survives even if the operator closes the browser mid-run. The worker
    (``task_run_management_command``) writes the lifecycle transitions; the run view
    pre-creates the row in ``queued`` state. ``run_id`` matches the live cache stream
    key ``admin_cmd_{run_id}`` the poller tails (§8).
    """

    class Status(models.TextChoices):
        QUEUED    = 'queued',    'Queued'
        RUNNING   = 'running',   'Running'
        COMPLETED = 'completed', 'Completed'
        FAILED    = 'failed',    'Failed'

    run_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False, db_index=True)
    command = models.CharField(max_length=100, db_index=True)
    # args/options/command_line persist the *redacted* invocation — sensitive args
    # (e.g. cookies/tokens) are stored as '***' so history can't leak them (§15.6).
    args = models.JSONField(default=list, blank=True)
    options = models.JSONField(default=dict, blank=True)
    command_line = models.TextField(blank=True, help_text="Redacted 'python manage.py …' string for display/replay.")
    user = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL,
        related_name='command_runs',
    )
    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.QUEUED, db_index=True,
    )
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    error = models.TextField(blank=True)
    log = models.TextField(blank=True, help_text="Full captured stdout/stderr.")
    result_summary = models.JSONField(null=True, blank=True)
    # Celery task id of the dispatched worker job. Lets the console (a) confirm via
    # inspect whether a queued/running row is actually still alive — self-healing a
    # row left stuck after a worker died — and (b) revoke a live task on Cancel.
    celery_task_id = models.CharField(max_length=255, blank=True, db_index=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.command} [{self.status}] {self.run_id}"

    def mark_running(self):
        self.status = self.Status.RUNNING
        self.started_at = timezone.now()
        self.save(update_fields=['status', 'started_at'])

    def mark_finished(self, status, error=None):
        self.status = status
        self.finished_at = timezone.now()
        if error:
            self.error = error
        self.save(update_fields=['status', 'finished_at', 'error'])

    @property
    def duration_seconds(self):
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None