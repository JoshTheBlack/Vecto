"""
Image processing utilities shared by model save() methods, plus the one request
handler every image upload in the app goes through (handle_image_upload).
"""
import logging
from io import BytesIO

from django.contrib import messages
from PIL import Image, ImageSequence

logger = logging.getLogger(__name__)

# 8 MB. Generous for cover art but bounded — process_image_field decodes the
# whole thing into memory, and an animated GIF decodes to every frame at once.
MAX_IMAGE_BYTES = 8 * 1024 * 1024


def _normalize_frame(img, max_px: int, crop_square: bool):
    """Optionally centre-crop to square, bound to max_px, normalize mode for WebP."""
    if crop_square:
        w, h = img.size
        if w != h:
            s = min(w, h)
            img = img.crop(((w - s) // 2, (h - s) // 2, (w + s) // 2, (h + s) // 2))
    if img.width > max_px or img.height > max_px:
        img.thumbnail((max_px, max_px), Image.Resampling.LANCZOS)
    # WebP only encodes RGB / RGBA. Preserve alpha where the source has it
    # (RGBA/LA/PA, or a palette image with a transparency entry); otherwise
    # use RGB so opaque images don't carry a pointless alpha channel.
    if img.mode in ('RGBA', 'LA', 'PA') or (img.mode == 'P' and 'transparency' in img.info):
        img = img.convert('RGBA')
    elif img.mode != 'RGB':
        img = img.convert('RGB')
    return img


def process_image_field(field, max_px: int, crop_square: bool = True) -> bytes:
    """Open an ImageField, optionally centre-crop to square, bound the longest
    side to max_px, and encode as WebP.

    crop_square=True (default) is the cover-art treatment. Pass False for
    imagery that must keep the whole frame (e.g. the 404 pool, where captions
    baked into a GIF would be cropped off) — the aspect ratio is preserved and
    the image is only downscaled to fit.

    Output is ALWAYS WebP. The R2 keys these feed are stable and carry an
    extension; normalizing to one format keeps that extension constant, so a
    re-upload OVERWRITES the same key instead of stranding the old object (the
    no-GC design — see planned_features.txt constraint #3). WebP supports alpha,
    so transparency is preserved rather than flattened to RGB.

    Animated sources (GIF, animated WebP/PNG) become ANIMATED WebP: every frame
    gets the same crop/resize, and per-frame durations + loop count carry over.
    Pillow composites optimized-GIF frame disposal on seek, so each yielded
    frame is the full rendered image.

    Returns processed WebP bytes. Raises on any error — callers should catch and
    log with model-specific context.
    """
    with Image.open(field) as img:
        if getattr(img, 'is_animated', False):
            frames, durations = [], []
            for frame in ImageSequence.Iterator(img):
                durations.append(frame.info.get('duration', img.info.get('duration', 100)))
                frames.append(_normalize_frame(frame.convert('RGBA'), max_px, crop_square))
            buf = BytesIO()
            frames[0].save(
                buf, format='WEBP', save_all=True, append_images=frames[1:],
                duration=durations, loop=img.info.get('loop', 0),
                # method 2 / q80 benchmarks ~2x faster than method 4 and ~6x
                # faster than method 6 at essentially identical output size —
                # per-frame encode cost dominates the synchronous upload wait.
                quality=80, method=2,
            )
            return buf.getvalue()

        img = _normalize_frame(img, max_px, crop_square)
        buf = BytesIO()
        img.save(buf, format='WEBP', quality=85, method=6)
        return buf.getvalue()


def handle_image_upload(request, instance, field, *, file_param=None, label='Image',
                        clear_fields=(), max_bytes=MAX_IMAGE_BYTES, save=True):
    """Upload or remove ONE processed image field. The single server-side entry
    point for every image upload in the app.

    There were four hand-rolled copies of this: the network fallback image, the
    404 pool, both mix covers and the custom avatar. They differed in which
    checks they bothered with and in how honestly they reported the result.

    Shape borrowed from handle_update_network_font: ONE action does both jobs,
    keyed on remove=1 in the POST.

    Note what is NOT a parameter: max_px and crop. Those belong to the IMAGE, not
    the request, so they live in the model's PROCESSED_IMAGES spec and the
    mixin's save() applies them. That keeps one source of truth — a caller cannot
    ask for a 500px crop of an image the model resizes to 256.

    field        the ProcessedImage field name on `instance`
    file_param   request.FILES key, if it differs from `field`
    label        human name for the messages ("Fallback image", "Avatar", ...)
    clear_fields attrs to blank when an upload lands — the paste-a-URL field an
                 upload supersedes (e.g. NetworkMix.image_url)
    save         False lets a caller batch this into its own later save()

    Returns True if something changed and was saved, False on a rejected upload.
    Callers reached through creator's ACTION_HANDLERS must not return this value:
    that dispatcher returns any non-None handler result AS the response.
    """
    file_param = file_param or field

    if request.POST.get('remove') == '1':
        current = getattr(instance, field, None)
        if current:
            current.delete(save=False)
        setattr(instance, field, None)
        if save:
            instance.save()
        messages.success(request, f"{label} removed.")
        return True

    upload = request.FILES.get(file_param)
    if not upload:
        messages.error(request, "No image selected.")
        return False
    if upload.size > max_bytes:
        messages.error(request, f"Image too large (max {max_bytes // (1024 * 1024)}MB).")
        return False

    # No pre-delete: the stable key is deterministic and storage overwrites in
    # place, so save() PUTs over any existing object. An explicit delete would
    # only add a round-trip and a momentary 404 gap before the PUT lands.
    setattr(instance, field, upload)
    for name in clear_fields:
        setattr(instance, name, '')
    if save:
        instance.save()

    # A silently dropped file must not report success. The mixin logs and records
    # a processing failure rather than raising (one bad file must not 500 a
    # settings save), so this is the only place the user hears about it — the
    # copy this replaced tested `if instance.field` after save(), which was
    # always truthy and so never once fired.
    if field in getattr(instance, 'image_processing_errors', []):
        messages.error(request, "That image could not be processed — try another file.")
        return False

    messages.success(request, f"{label} saved.")
    return True
