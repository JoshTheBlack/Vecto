"""
POST action handlers for the user mix management UI.

Each handler accepts (request,) and mutates state via Django messages.
Imported by main.py which owns the redirect after dispatch.
"""
import logging

from django.contrib import messages
from django.core.cache import cache
from django.shortcuts import get_object_or_404

from ...models import UserMix
from ...utils import validate_public_url

logger = logging.getLogger(__name__)


def _handle_create_mix(request):
    mix_name = request.POST.get('mix_name', '').strip() or f"{request.user.first_name}'s Custom Mix"
    raw_image_url = request.POST.get('mix_image', '').strip()
    if raw_image_url:
        ok, reason = validate_public_url(raw_image_url)
        if not ok:
            messages.warning(request, f"Mix image URL ignored: {reason}")
            raw_image_url = ''
    mix = UserMix.objects.create(
        user=request.user,
        network=request.network,
        name=mix_name,
        image_url=raw_image_url,
    )
    if 'mix_image_upload' in request.FILES:
        mix.image_upload = request.FILES['mix_image_upload']
    mix.selected_podcasts.set(request.POST.getlist('podcasts'))
    mix.save()
    messages.success(request, f"Mix '{mix.name}' created successfully!")


def _handle_edit_mix(request):
    mix = get_object_or_404(UserMix, id=request.POST.get('mix_id'), user=request.user, network=request.network)
    mix.name = request.POST.get('mix_name', '').strip() or mix.name
    if 'mix_image_upload' in request.FILES and request.FILES['mix_image_upload']:
        if mix.image_upload:
            mix.image_upload.delete(save=False)
        mix.image_upload = request.FILES['mix_image_upload']
        mix.image_url = ""
    elif request.POST.get('mix_image'):
        raw_image_url = request.POST.get('mix_image').strip()
        ok, reason = validate_public_url(raw_image_url)
        if not ok:
            messages.warning(request, f"Mix image URL ignored: {reason}")
        else:
            mix.image_url = raw_image_url
            if mix.image_upload:
                mix.image_upload.delete(save=False)
    cache.delete(f"shell_user_mix_{mix.id}")
    mix.selected_podcasts.set(request.POST.getlist('podcasts'))
    mix.save()
    messages.success(request, "Mix updated successfully!")


def _handle_delete_mix(request):
    mix = get_object_or_404(UserMix, id=request.POST.get('mix_id'), user=request.user, network=request.network)
    cache.delete(f"shell_user_mix_{mix.id}")
    if mix.image_upload:
        mix.image_upload.delete(save=False)
    mix.delete()
    messages.warning(request, "Custom mix deleted.")


MIX_ACTION_HANDLERS = {
    'create_mix': _handle_create_mix,
    'edit_mix': _handle_edit_mix,
    'delete_mix': _handle_delete_mix,
}
