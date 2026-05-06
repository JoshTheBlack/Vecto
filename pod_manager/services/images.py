"""
Image processing utilities shared by model save() methods.
"""
from io import BytesIO

from PIL import Image


def process_image_field(field, max_px: int) -> bytes:
    """Open an ImageField, centre-crop to square, resize to max_px×max_px.

    Returns processed image bytes. Raises on any error — callers should
    catch and log with model-specific context.
    """
    with Image.open(field) as img:
        original_format = img.format or 'JPEG'
        w, h = img.size
        if w != h:
            s = min(w, h)
            img = img.crop(((w - s) // 2, (h - s) // 2, (w + s) // 2, (h + s) // 2))
        if img.width > max_px or img.height > max_px:
            img.thumbnail((max_px, max_px), Image.Resampling.LANCZOS)
        if original_format == 'JPEG' and img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        buf = BytesIO()
        img.save(buf, format=original_format)
        return buf.getvalue()
