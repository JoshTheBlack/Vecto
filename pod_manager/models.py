import uuid, os, base64, logging, secrets
from decimal import Decimal
from urllib.parse import urlparse
from django.db import models
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.files.base import ContentFile
from django.core.files.storage import FileSystemStorage
from django.db.models.signals import post_delete, post_save, pre_delete, pre_save
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


class CrossPublishAccessMode(models.TextChoices):
    """Tier-gating choice shared by EpisodeCrossPublication rows and the
    feed-level auto cross-publish default on Podcast (which is declared
    first, so the choices live at module level)."""
    INHERIT = 'inherit', "Keep episode's own gating (parent podcast tier)"
    TARGET = 'target', "Use the target podcast's tier"

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

class ProcessedImage:
    """Declarative spec for ONE processed image field on a ProcessedImageMixin
    model. See the mixin for what it does with these.

    field        the ImageField's attribute name
    version_field the IntegerField holding its cache-bust counter
    max_px       longest side of the output, in pixels
    crop_square  centre-crop to a square first (the cover-art treatment). Pass
                 False for imagery that must keep the whole frame — the 404 pool
                 bakes captions into the art, and cropping ate them.
    filename     the name handed to FileField.save(). Only the extension really
                 matters (upload_to ignores the rest and returns the stable key),
                 and it is ALWAYS .webp — see process_image_field.
    """
    __slots__ = ('field', 'version_field', 'max_px', 'crop_square', 'filename')

    def __init__(self, field, version_field, max_px, crop_square=True, filename='image.webp'):
        self.field = field
        self.version_field = version_field
        self.max_px = max_px
        self.crop_square = crop_square
        self.filename = filename


class ProcessedImageMixin:
    """The ImageField + <name>_version + process-on-save triad, once.

    This was copy-pasted across NetworkMix, UserMix, NotFoundEntry, Network and
    NetworkMembership: every save() re-implemented the same single-write, and
    they drifted only in max_px, crop and the log message. Declare PROCESSED_IMAGES
    instead and the mixin does the rest.

    A plain Python mixin, NOT an abstract model: it declares no fields, so adding
    it to a model is a no-op for the schema (`makemigrations --check` is what
    proves that, and it is run in the test suite).

    What each save does, and why:
      - SINGLE WRITE. `_committed` is False only for a just-uploaded file, so a
        plain row re-save never reprocesses or re-PUTs. Process to WebP first,
        then hand the bytes to FileField.save(..., save=False) so the object is
        written to storage exactly ONCE, at the stable key.
      - VERSION BUMP. The stable key is CDN-cached immutable, so without the bump
        a re-upload never reaches a browser — the URL is identical. This is not
        optional decoration; it is the only cache-bust these images have.
      - OVERWRITE, NEVER ORPHAN. upload_to returns a key whose extension is always
        .webp, so a re-upload lands on the same key and replaces the object.
        There is no GC pass to clean up strays.

    Processing failures are logged and recorded in `image_processing_errors`
    rather than raised, so one bad file cannot 500 a settings save. The field is
    CLEARED on failure: persisting it would write the raw, unprocessed bytes
    under a .webp key — the CDN would then serve a JPEG as image/webp, and the
    caller would report success for an image the user never actually gets.
    Callers that surface a message should check image_processing_errors — see
    services.images.handle_image_upload.
    """

    PROCESSED_IMAGES = ()

    @property
    def image_processing_errors(self):
        """Field names whose upload failed to process on the last save()."""
        return getattr(self, '_image_processing_errors', [])

    def _image_spec(self, field):
        return next((s for s in self.PROCESSED_IMAGES if s.field == field), None)

    def apply_image_upload(self, field, upload):
        """Process `upload` and assign it ONLY if that succeeds — the
        non-destructive path (services.images.handle_image_upload uses it).

        The image is processed to WebP and committed to storage FIRST; only then
        does the field point at the new object and the version bump. If
        processing RAISES (a corrupt file, a .txt renamed to .png), the exception
        propagates with the instance UNTOUCHED — the existing image stays exactly
        where it is, both in the DB and at its stable R2 key (which was never
        rewritten, because process_image_field raised before the .save). A bad
        new upload can no longer destroy the good image it was meant to replace.

        Commits to storage (save=False) but does not save the model — the caller
        saves. Raises on a processing failure.
        """
        spec = self._image_spec(field)
        # May raise — deliberately BEFORE any mutation of the instance.
        data = process_image_field(upload, spec.max_px, crop_square=spec.crop_square)
        # getattr(self, field) is still the OLD (or empty) file here; .save writes
        # the new webp to the stable key and repoints the field. upload_to ignores
        # the passed name and returns the stable key, so this overwrites in place.
        getattr(self, field).save(spec.filename, ContentFile(data), save=False)
        setattr(self, spec.version_field,
                (getattr(self, spec.version_field) or 0) + 1)

    def process_pending_images(self):
        """Save-time processing for callers that assign-then-save (the 404 pool,
        the mix handlers, the avatar upload). handle_image_upload does NOT come
        through here — it calls apply_image_upload directly so a failure leaves
        the previous image intact. This path can only blank a bad upload after
        the fact, so it does exactly that."""
        errors = []
        for spec in self.PROCESSED_IMAGES:
            fieldfile = getattr(self, spec.field, None)
            # _committed is False only for a fresh upload — plain re-saves skip.
            if not fieldfile or fieldfile._committed:
                continue
            try:
                self.apply_image_upload(spec.field, fieldfile)
            except Exception as e:
                logger.error(
                    f"Image processing failed for {type(self).__name__}"
                    f"(pk={self.pk}).{spec.field}: {e}", exc_info=True)
                # Drop it rather than store raw bytes at a .webp key. Django's
                # FileField.pre_save commits any still-uncommitted file on save,
                # so leaving it set would PUT the unprocessed original at a key
                # ending .webp — the CDN would serve a JPEG as image/webp, and
                # the caller would report success for an image nobody can see.
                # '' rather than None: not every one of these fields is nullable
                # (NotFoundEntry.image_upload is required), and '' is the
                # FileField-native empty that reads falsy either way.
                setattr(self, spec.field, '')
                errors.append(spec.field)
        self._image_processing_errors = errors

    def versioned_image_url(self, field):
        """Cache-busted public URL for one of this model's processed images, or
        None if it is empty. display_* properties layer their own fallbacks
        (a legacy pasted URL, gravatar, ...) on top of this."""
        fieldfile = getattr(self, field, None)
        if not fieldfile:
            return None
        spec = self._image_spec(field)
        version = getattr(self, spec.version_field, 0) if spec else 0
        return _versioned_image_url(fieldfile, version)

    def save(self, *args, **kwargs):
        self.process_pending_images()
        super().save(*args, **kwargs)


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
        "border_radius": "0.375rem",
        "logo_url": ""
    }

def network_font_path(instance, filename):
    """Stable R2 key for a network's custom font: fonts/<slug>.woff2.

    Matches mix_cover_path — the key never changes on re-upload, so the object
    is OVERWRITTEN in place. The passed filename is ignored.
    """
    return f"fonts/{instance.slug}.woff2"


class OverwriteStorage(FileSystemStorage):
    def get_available_name(self, name, max_length=None):
        """Returns the same name even if it already exists on the system."""
        if self.exists(name):
            self.delete(name) # Use Django's backend-agnostic delete
        return name

# Defined ahead of Network (which is defined ahead of UserMix) since
# select_media_storage() local-imports this name at field-construction time —
# Network.custom_font_upload needs it just as early as UserMix.image_upload does.
mix_storage = OverwriteStorage()

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


def network_logo_path(instance, filename):
    """Stable R2 key for a network's logo: network-logos/<slug>.webp.

    Mirrors network_default_image_path — always .webp, so a re-upload OVERWRITES
    rather than orphaning. The passed filename is ignored.
    """
    return f"network-logos/{instance.slug}.webp"


def network_default_image_path(instance, filename):
    """Stable R2 key for a network's fallback logo: network-defaults/<slug>.webp.

    Mirrors mix_cover_path: the extension is always .webp (process_image_field
    normalizes output), so a re-upload OVERWRITES the same key instead of
    orphaning the old object. Keyed on slug rather than a UUID because Network
    has no unique_id; slug is unique and stable. The passed filename is ignored.
    """
    return f"network-defaults/{instance.slug}.webp"


class Network(ProcessedImageMixin, models.Model):
    PROCESSED_IMAGES = (
        ProcessedImage('default_image_upload', 'default_image_version', 500,
                       filename='default.webp'),
        # NOT cropped square: a logo is usually a wide wordmark, and centre-
        # cropping one to a square eats the word. The navbar renders it at 32px
        # tall and the 404 page at 120px, both width:auto — aspect ratio is the
        # whole point. 512 bounds the longest side for a 2x-DPI 120px render.
        ProcessedImage('logo_upload', 'logo_version', 512, crop_square=False,
                       filename='logo.webp'),
    )

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
    # Fallback artwork for RSS feeds and custom mixes. default_image_url is the
    # legacy paste-a-link field, kept so existing networks keep working and as
    # the fallback; the creator UI now uploads instead. display_default_image
    # resolves the two — read THAT, never the raw field.
    default_image_url = models.URLField(blank=True, help_text="Legacy fallback logo URL (superseded by default_image_upload)")
    default_image_upload = models.ImageField(upload_to=network_default_image_path, storage=select_media_storage, blank=True, null=True, help_text="Uploaded fallback logo for RSS feeds and mixes")
    default_image_version = models.IntegerField(default=0)
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
    # The navbar logo. logo_upload supersedes both URL fields; they survive as
    # the legacy fallbacks (theme_config['logo_url'] has always won over
    # logo_url, and still does). Read display_logo, never these — reading a raw
    # field silently ignores an upload.
    logo_url = models.URLField(max_length=500, blank=True, null=True) # Maps to image_small_url
    logo_upload = models.ImageField(upload_to=network_logo_path, storage=select_media_storage, blank=True, null=True, help_text="Uploaded network logo, rendered in the navbar")
    logo_version = models.IntegerField(default=0)
    banner_image_url = models.URLField(max_length=500, blank=True, null=True) # Maps to image_url
    custom_font_upload = models.FileField(
        upload_to=network_font_path, storage=select_media_storage, blank=True, null=True,
        help_text="Self-hosted .woff2 file for this network's theme font.")
    custom_font_family = models.CharField(
        max_length=100, blank=True,
        help_text="Font name declared in the generated @font-face rule (e.g. 'Brand Sans').")
    custom_font_version = models.IntegerField(
        default=0,
        help_text="Bumped on every font upload — cache-busts the stable fonts/<slug>.woff2 "
                  "key, which the CDN serves with a 1-year immutable cache.")
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

    @property
    def display_font_url(self):
        """Version-busted URL for the custom font. The stable fonts/<slug>.woff2
        key is CDN-cached for a year (immutable), so a re-upload never reaches
        browsers without a fresh query string — same problem/solution as
        display_image on the mix models."""
        if not self.custom_font_upload:
            return ''
        return f"{self.custom_font_upload.url}?v={self.custom_font_version}"

    @property
    def display_logo(self):
        """The network's navbar logo: the uploaded one if there is one, else the
        legacy pasted URLs in their established precedence (theme_config's has
        always beaten the plain field, and an upload now beats both — it is the
        most explicit thing the creator did).

        Read this, never logo_url/theme_config['logo_url'] — reading those
        directly silently ignores an upload. base.html, 404.html and
        snippets/_navbar_logo.html all go through here.
        """
        uploaded = self.versioned_image_url('logo_upload')
        if uploaded:
            return uploaded
        return (self.theme_config or {}).get('logo_url') or self.logo_url or ''

    @property
    def display_default_image(self):
        """The network's fallback artwork for RSS feeds and mixes: the uploaded
        image if there is one, else the legacy pasted URL. Read this, never
        default_image_url — that field is now only the fallback, and reading it
        directly silently ignores an upload."""
        return self.versioned_image_url('default_image_upload') or self.default_image_url

    def save(self, *args, **kwargs):
        # ProcessedImageMixin.save() processes any pending upload before writing.
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

    allow_public_transcripts = models.BooleanField(
        default=True,
        help_text="Serve transcripts (and the podcast:transcript feed tag) "
                  "to listeners without premium access to this podcast. "
                  "Turn off for feeds where the private audio contains "
                  "content never released publicly (e.g. extended spoiler "
                  "sections) — the single transcript is always generated "
                  "from the private audio.",
    )

    is_low_priority = models.BooleanField(
        default=False,
        help_text="Episodes that arrive here first via GUID match will be "
                  "auto-migrated to a normal-priority feed that later ingests "
                  "the same episode. Also polled slightly after normal feeds.",
    )

    is_hidden = models.BooleanField(
        default=False,
        help_text="Hide this feed from the Your Feeds subscription directory. "
                  "The feed's own RSS URL keeps working; existing subscribers "
                  "are unaffected.",
    )

    auto_crosspublish_targets = models.ManyToManyField(
        'self', symmetrical=False, blank=True,
        related_name='auto_crosspublish_sources',
        help_text="Every episode in THIS feed is automatically cross-published "
                  "into each selected destination feed.",
    )

    auto_crosspublish_access_mode = models.CharField(
        max_length=10, choices=CrossPublishAccessMode.choices,
        default=CrossPublishAccessMode.INHERIT,
        help_text="Tier gating for this feed's auto cross-published episodes "
                  "inside their destinations. INHERIT = keep this feed's tier; "
                  "TARGET = adopt the destination feed's tier.",
    )

    class Meta:
        unique_together = ('network', 'slug')


    def __str__(self):
        return self.title

class Episode(models.Model):
    """An individual episode harvested from a feed."""
    podcast = models.ForeignKey(Podcast, on_delete=models.CASCADE, related_name='episodes')
    podcast_pinned_at = models.DateTimeField(null=True, blank=True,
        help_text="Set whenever an episode's podcast is manually changed (single or bulk "
                  "move). GUID-based ingest auto-migration skips pinned episodes so they "
                  "aren't moved back to a prior feed. Never blocks manual moves.")
    podcast_pinned_by = models.ForeignKey(User, null=True, blank=True,
        on_delete=models.SET_NULL, related_name='+',
        help_text="Who performed the most recent manual move (audit trail).")
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

class CalendarEntry(models.Model):
    """A planned release on a network's public calendar. Optionally tied to a
    podcast (season/ep/type mirror Episode's own fields) for planned episodes,
    or fully freeform (title + external_link) for non-episode items. Linked
    OneToOne to a real Episode once one is scheduled/published — an episode
    maps to at most one entry, so "is there already an entry for this episode"
    is a cheap reverse lookup (episode.calendar_entry)."""
    network = models.ForeignKey(Network, on_delete=models.CASCADE, related_name='calendar_entries')
    podcast = models.ForeignKey(Podcast, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='calendar_entries', help_text="Leave blank for a non-episode entry (e.g. a Live Watch).")
    title = models.CharField(max_length=255)
    season_number = models.IntegerField(null=True, blank=True)
    episode_number = models.IntegerField(null=True, blank=True)
    episode_type = models.CharField(max_length=50, blank=True)
    scheduled_at = models.DateTimeField(db_index=True)
    external_link = models.URLField(blank=True, help_text="Where to go for a non-episode entry (e.g. a Live Watch link).")
    episode = models.OneToOneField(Episode, on_delete=models.SET_NULL, null=True, blank=True, related_name='calendar_entry')
    # True when this row was born to mirror an episode (auto-created on
    # schedule/publish with no pre-plan, backfilled, or made via the episode
    # page's "Create entry" button) rather than planned independently. Deleting
    # the episode deletes an episode-born entry but returns a pre-planned one to
    # the unlinked pool (Episode pre_delete → delete_auto_created_calendar_entry).
    created_from_episode = models.BooleanField(default=False)
    notes = models.TextField(blank=True)

    class PrepublishVisibility(models.TextChoices):
        ACTUAL = 'actual', 'Show actual info'
        TEASER = 'teaser', 'Show teaser'
        HIDDEN = 'hidden', 'Hidden until publish'

    # How this entry appears on PUBLIC surfaces (page + ICS) BEFORE its linked
    # episode publishes. 'actual' (default) shows the real title/notes/numbering
    # as today; 'teaser' swaps in placeholder_title/notes and hides SxE; 'hidden'
    # omits it entirely. Ignored once revealed — a published episode's entry
    # always shows the actual info regardless of this setting.
    prepublish_visibility = models.CharField(
        max_length=10, choices=PrepublishVisibility.choices,
        default=PrepublishVisibility.ACTUAL)
    placeholder_title = models.CharField(max_length=255, blank=True,
        help_text="Public-facing teaser title shown until the episode publishes (teaser mode).")
    placeholder_notes = models.TextField(blank=True,
        help_text="Public-facing teaser notes shown until the episode publishes (teaser mode).")
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    TEASER_DEFAULT_TITLE = "To be announced"

    def __str__(self):
        return f"{self.network.slug} | {self.title} @ {self.scheduled_at:%Y-%m-%d %H:%M}"

    # --- Public-surface reveal logic (used by calendar_events + the ICS feed) ---
    # "Revealed" = the linked episode is live; from then on the actual info shows
    # regardless of prepublish_visibility. Everything below is a no-op once
    # revealed, so a published entry always renders its real title/notes/SxE.

    @property
    def is_revealed(self):
        return bool(self.episode_id) and self.episode.is_published

    def public_hidden(self):
        """True when a listener should not see this entry at all right now."""
        return self.prepublish_visibility == self.PrepublishVisibility.HIDDEN and not self.is_revealed

    def _teased(self):
        return self.prepublish_visibility == self.PrepublishVisibility.TEASER and not self.is_revealed

    def public_title(self):
        if self._teased():
            return self.placeholder_title or self.TEASER_DEFAULT_TITLE
        return self.title

    def public_notes(self):
        return self.placeholder_notes if self._teased() else self.notes

    def public_show_sxe(self):
        if self._teased():
            return False
        return self.season_number is not None and self.episode_number is not None


class LiveSchedulePost(models.Model):
    """A Discord `/schedule` message that re-renders itself as episodes publish.
    The bot posts it and records the channel/message id; a Celery task PATCHes
    the embed (via Discord REST) whenever an episode in this network goes live,
    until `window_end` passes. Window bounds are frozen at post time so the
    displayed range stays stable — `window_end` is both the display edge and the
    expiry (see services/discord_schedule.py)."""
    network = models.ForeignKey(Network, on_delete=models.CASCADE, related_name='live_schedule_posts')
    channel_id = models.CharField(max_length=32, db_index=True)
    message_id = models.CharField(max_length=32, unique=True)
    guild_id = models.CharField(max_length=32, blank=True)
    window_kind = models.CharField(max_length=8, default='week')  # 'week' | 'range'
    window_start = models.DateTimeField()
    window_end = models.DateTimeField(db_index=True, help_text="Exclusive window edge; also the live-update expiry.")
    subtitle = models.CharField(max_length=200, blank=True, help_text="Frozen human window label shown on the embed.")
    created_by_discord_id = models.CharField(max_length=32, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.network.slug} live schedule (msg {self.message_id}) until {self.window_end:%Y-%m-%d}"


class EpisodeCrossPublication(models.Model):
    """Places an episode into another podcast's feeds in addition to its
    parent. The episode stays a single entity (one parent podcast, one set of
    fragments/GUIDs); this link only affects feed membership and, optionally,
    which podcast's tier gates the subscriber audio."""

    AccessMode = CrossPublishAccessMode

    episode = models.ForeignKey(Episode, on_delete=models.CASCADE, related_name='cross_publications')
    podcast = models.ForeignKey(
        Podcast, on_delete=models.CASCADE, related_name='cross_publications_in',
        help_text="Target podcast whose feeds also carry this episode.")
    access_mode = models.CharField(max_length=10, choices=AccessMode.choices, default=AccessMode.INHERIT)
    auto_created = models.BooleanField(
        default=False,
        help_text="True if this link was generated by feed-level auto "
                  "cross-publish (teardown removes only these).")
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


class UserMix(ProcessedImageMixin, models.Model):
    """A user's custom-built feed."""
    PROCESSED_IMAGES = (
        ProcessedImage('image_upload', 'image_version', 500, filename='cover.webp'),
    )

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
        return self.versioned_image_url('image_upload') or self.image_url

    def save(self, *args, **kwargs):
        logger.debug(f"Saving UserMix: '{self.name}' for user {self.user.username}")
        # ProcessedImageMixin.save() processes any pending upload before writing.
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

class NetworkMembership(ProcessedImageMixin, models.Model):
    PROCESSED_IMAGES = (
        ProcessedImage('custom_image_upload', 'image_version', 256,
                       filename='avatar.webp'),
    )

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

    # "Random Encounters" achievement — the 404 pool entries this member has
    # stumbled onto. Deliberately not weighted (see notfound view selection):
    # it's a hunt, and the denominator (network.notfound_entries.count()) moves
    # as the pool grows, so the achievement re-opens whenever new art is added.
    seen_notfound_entries = models.ManyToManyField('NotFoundEntry', blank=True, related_name='seen_by_members')

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
        return self.versioned_image_url('custom_image_upload')

    @property
    def display_avatar(self):
        discord_url = self.discord_image_url
        custom_url = self.versioned_image_url('custom_image_upload') or self.custom_image_url

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
    
class NetworkMix(ProcessedImageMixin, models.Model):
    """A curated super-feed managed by the network creators."""
    PROCESSED_IMAGES = (
        ProcessedImage('image_upload', 'image_version', 500, filename='cover.webp'),
    )

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
        return self.versioned_image_url('image_upload') or self.image_url

def notfound_image_path(instance, filename):
    """Stable R2 key for a 404-pool image: notfound/<UUID>.webp.

    Matches mix_cover_path — the extension is always .webp (process_image_field
    normalizes output), so a re-upload overwrites the same key rather than
    orphaning the old object. The passed filename is ignored.
    """
    return f"notfound/{instance.unique_id}.webp"

class NotFoundEntry(ProcessedImageMixin, models.Model):
    """One image+caption pair in a network's curated 404-page pool."""
    PROCESSED_IMAGES = (
        # Full frame, never cropped — 404 art often has text/captions baked in;
        # only the longest side is bounded.
        ProcessedImage('image_upload', 'image_version', 800, crop_square=False,
                       filename='notfound.webp'),
    )

    network = models.ForeignKey(Network, on_delete=models.CASCADE, related_name='notfound_entries')
    unique_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    image_upload = models.ImageField(upload_to=notfound_image_path, storage=select_media_storage)
    image_version = models.IntegerField(default=0)
    caption = models.CharField(max_length=200)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.network.name} 404 - {self.caption}"

    @property
    def display_image(self):
        return self.versioned_image_url('image_upload')

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


def new_transcript_token() -> str:
    """Random suffix mixed into a transcript's R2 object keys so they can't be
    derived from the episode id. 22 URL- and S3-key-safe chars (~128 bits).
    Applied as the field default on INSERT only, so every new Transcript is born
    keyed while existing rows migrate as null (legacy deterministic key)."""
    return secrets.token_urlsafe(16)


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

    r2_key_token = models.CharField(
        max_length=32, null=True, blank=True, default=new_transcript_token,
        help_text="Random suffix mixed into the R2 object keys so they "
                  "can't be derived from the episode id. Null = legacy "
                  "deterministic key (pre-rekey). Set at creation for new "
                  "transcripts; backfilled by rekey_transcripts.",
    )

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


@receiver(pre_delete, sender=Episode)
def delete_auto_created_calendar_entry(sender, instance, **kwargs):
    """An entry that only exists to mirror this episode (created_from_episode)
    dies with it; a pre-planned entry the episode adopted survives — SET_NULL
    returns it to the unlinked pool. Runs in pre_delete so the reverse
    OneToOne still resolves (SET_NULL hasn't nulled it yet). Audio in R2 is NOT
    touched here — it's reclaimed by the orphan GC once the row is gone
    (services/r2_maintenance)."""
    entry = getattr(instance, 'calendar_entry', None)
    if entry is not None and entry.created_from_episode:
        entry.delete()


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
                delete_media_object(transcript_r2_key(instance.episode_id, ext, instance.r2_key_token))
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