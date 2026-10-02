from django import template
from django.utils import timezone

register = template.Library()


@register.filter
def short_timesince(value):
    """Compact relative age for dense lists: 'now', '5m', '3h', '12d', '2mo', '1y'.
    Pair it with a title attribute carrying the full timestamp."""
    if not value:
        return ''
    seconds = int((timezone.now() - value).total_seconds())
    if seconds < 60:
        return 'now'
    minutes = seconds // 60
    if minutes < 60:
        return f'{minutes}m'
    hours = minutes // 60
    if hours < 24:
        return f'{hours}h'
    days = hours // 24
    if days < 30:
        return f'{days}d'
    if days < 365:
        return f'{days // 30}mo'
    return f'{days // 365}y'


@register.filter
def email_local(value):
    """The part of an email-style username before the '@' (usernames here are often
    email addresses, which truncate badly in a narrow column). Anything else is returned
    unchanged; pair it with a title attribute carrying the full value."""
    value = value or ''
    return value.split('@', 1)[0] if '@' in value else value
