"""Creator-settings POST handlers for the network's Support Us page.

Same contract as actions.py: each takes (request, current_network) and returns
None (the dispatcher redirects back to the tab) or a response to return as-is.
"""
import re

from django.contrib import messages
from django.core.exceptions import ValidationError
from django.core.validators import URLValidator
from django.db.models import Max
from django.http import HttpResponse, HttpResponseBadRequest
from django.shortcuts import get_object_or_404

from ...models import SupportLink, SupportPage
from ...services.images import handle_image_upload
from ...services.ordering import apply_order, parse_id_list

_validate_http_url = URLValidator(schemes=['http', 'https'])
# Bootstrap Icons names are lowercase words joined by hyphens ("cup-hot-fill").
_ICON_NAME = re.compile(r'^[a-z0-9]+(-[a-z0-9]+)*$')


def _icon_name(raw):
    """Normalise a pasted icon name: 'bi bi-cup-hot' / 'bi-cup-hot' -> 'cup-hot'.
    Returns None when what's left isn't a plausible icon name."""
    words = (raw or '').lower().split()
    name = words[-1].removeprefix('bi-') if words else ''
    if name and not _ICON_NAME.match(name):
        return None
    return name


def _apply_link_fields(request, link):
    """Copy the card form onto `link`. Returns an error message, or None."""
    title = request.POST.get('title', '').strip()
    url = request.POST.get('url', '').strip()
    icon = _icon_name(request.POST.get('icon'))
    if not title:
        return "A card needs a title."
    try:
        _validate_http_url(url)
    except ValidationError:
        return "The button link must be a full http(s):// URL."
    if icon is None:
        return "Icon must be a Bootstrap Icons name, e.g. cup-hot."

    link.title = title[:100]
    link.description = request.POST.get('description', '').strip()[:500]
    link.button_text = request.POST.get('button_text', '').strip()[:50] or 'Learn more'
    link.url = url
    link.icon = icon
    link.is_visible = request.POST.get('is_visible') == 'on'
    return None


def _apply_link_image(request, link):
    """Remove or replace the card image per the form. Returns False when a new
    upload was rejected (handle_image_upload has already said why)."""
    if request.POST.get('remove_image') == 'on':
        if link.image_upload:
            link.image_upload.delete(save=False)
        link.image_upload = None
        return True
    if request.FILES.get('image_upload'):
        return handle_image_upload(request, link, 'image_upload', label='Card image', save=False)
    return True


def handle_update_support_page(request, current_network):
    page, _ = SupportPage.objects.get_or_create(network=current_network)
    page.enabled = request.POST.get('enabled') == 'on'
    page.title = request.POST.get('title', '').strip()[:100] or 'Support Us'
    page.tagline = request.POST.get('tagline', '').strip()[:255]
    page.save()
    messages.success(request, "Support page saved.")


def handle_add_support_link(request, current_network):
    link = SupportLink(network=current_network)
    error = _apply_link_fields(request, link)
    if error:
        messages.error(request, error)
        return
    if not _apply_link_image(request, link):
        return
    last = current_network.support_links.aggregate(m=Max('position'))['m']
    link.position = 0 if last is None else last + 1
    link.save()
    messages.success(request, f"Card '{link.title}' added.")


def handle_edit_support_link(request, current_network):
    link = get_object_or_404(SupportLink, id=request.POST.get('link_id'), network=current_network)
    error = _apply_link_fields(request, link)
    if error:
        messages.error(request, error)
        return
    if not _apply_link_image(request, link):
        return
    link.save()
    messages.success(request, f"Card '{link.title}' saved.")


def handle_delete_support_link(request, current_network):
    link = get_object_or_404(SupportLink, id=request.POST.get('link_id'), network=current_network)
    if link.image_upload:
        link.image_upload.delete(save=False)
    title = link.title
    link.delete()
    messages.warning(request, f"Card '{title}' deleted.")


def handle_reorder_support_links(request, current_network):
    """Drag-to-reorder endpoint (sortable_list.js): 204, or 400 when the posted
    order doesn't name exactly this network's cards."""
    ids = parse_id_list(request.POST.get('order'))
    if ids is None or not apply_order(current_network.support_links.all(), ids):
        return HttpResponseBadRequest("Order must list every card exactly once.")
    return HttpResponse(status=204)


SUPPORT_ACTION_HANDLERS = {
    'update_support_page':    handle_update_support_page,
    'add_support_link':       handle_add_support_link,
    'edit_support_link':      handle_edit_support_link,
    'delete_support_link':    handle_delete_support_link,
    'reorder_support_links':  handle_reorder_support_links,
}
