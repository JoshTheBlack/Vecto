"""
Image processing utilities shared by model save() methods.
"""
from io import BytesIO

from PIL import Image, ImageSequence


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
                quality=85, method=4,  # method 6 is disproportionately slow per-frame
            )
            return buf.getvalue()

        img = _normalize_frame(img, max_px, crop_square)
        buf = BytesIO()
        img.save(buf, format='WEBP', quality=85, method=6)
        return buf.getvalue()
