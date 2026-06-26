"""
Image processing utilities shared by model save() methods.
"""
from io import BytesIO

from PIL import Image


def process_image_field(field, max_px: int) -> bytes:
    """Open an ImageField, centre-crop to square, resize to max_px×max_px, and
    encode as WebP.

    Output is ALWAYS WebP. The R2 keys these feed are stable and carry an
    extension; normalizing to one format keeps that extension constant, so a
    re-upload OVERWRITES the same key instead of stranding the old object (the
    no-GC design — see planned_features.txt constraint #3). WebP supports alpha,
    so transparency is preserved rather than flattened to RGB.

    Returns processed WebP bytes. Raises on any error — callers should catch and
    log with model-specific context.
    """
    with Image.open(field) as img:
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
        buf = BytesIO()
        img.save(buf, format='WEBP', quality=85, method=6)
        return buf.getvalue()
