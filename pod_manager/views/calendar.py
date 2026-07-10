"""
Public per-network release calendar (Feature 4, amendment A14). ONE page at
/calendar serves listeners (read-only) and owners (add / delete / drag). The
JSON event source is public-read (listeners need it); every mutating endpoint
re-checks ownership server-side via _require_owner — template hiding is
presentation, not enforcement.
"""
import datetime
import logging
from urllib.parse import quote
from zoneinfo import ZoneInfo

from django.contrib import messages
from django.http import Http404, HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.views.decorators.http import require_POST

from ..models import CalendarEntry, Podcast
from .creator.publish import _require_owner

logger = logging.getLogger(__name__)

# The public calendar renders in one fixed timezone (Eastern) so every viewer
# sees the same grid regardless of browser locale. FullCalendar runs in UTC
# mode and we hand it Eastern wall-clock times with the tz stripped, so the
# rendered clock reads as Eastern; storage stays UTC throughout.
EASTERN = ZoneInfo('America/New_York')

# Timezones offered on the owner add-form (Eastern first = default). A posted
# value is validated against this allowlist before any ZoneInfo() lookup.
TZ_CHOICES = [
    ('America/New_York', 'Eastern (ET)'),
    ('America/Chicago', 'Central (CT)'),
    ('America/Denver', 'Mountain (MT)'),
    ('America/Los_Angeles', 'Pacific (PT)'),
    ('America/Anchorage', 'Alaska (AKT)'),
    ('Pacific/Honolulu', 'Hawaii (HT)'),
    ('UTC', 'UTC'),
    ('Europe/London', 'London (GMT/BST)'),
]
_TZ_ALLOWED = {value for value, _ in TZ_CHOICES}


def _eastern_naive_iso(dt):
    """UTC-aware datetime -> Eastern wall-clock ISO, tz stripped, for
    FullCalendar's UTC mode (so the rendered clock reads as Eastern)."""
    return dt.astimezone(EASTERN).replace(tzinfo=None).isoformat()


def _wallclock_to_utc(dt, tz):
    """Interpret `dt`'s clock components as wall-clock in `tz` and convert to
    UTC (DST-correct via zoneinfo). Aware inputs have their tz discarded first —
    FullCalendar hands back UTC-labeled Eastern wall-clock on drag."""
    if timezone.is_aware(dt):
        dt = dt.replace(tzinfo=None)
    return dt.replace(tzinfo=tz).astimezone(datetime.timezone.utc)


def _int(raw):
    try:
        return int((raw or '').strip())
    except (TypeError, ValueError):
        return None


def _scheduled_from_request(request):
    """Combine the add/edit form's date + time (+ chosen tz) into a UTC datetime,
    or None if either is missing/unparseable."""
    date_str = request.POST.get('date', '').strip()
    time_str = request.POST.get('time', '').strip()
    parsed = parse_datetime(f'{date_str}T{time_str}') if date_str and time_str else None
    if parsed is None:
        return None
    tz_name = request.POST.get('tz', '')
    tz = ZoneInfo(tz_name) if tz_name in _TZ_ALLOWED else EASTERN
    return _wallclock_to_utc(parsed, tz)


def _apply_entry_fields(entry, request, network):
    """Write the shared add/edit form fields onto `entry` (podcast vs freeform
    are mutually exclusive)."""
    entry.title = request.POST.get('title', '').strip()
    entry.notes = request.POST.get('notes', '').strip()
    podcast = None
    podcast_id = request.POST.get('podcast_id')
    if podcast_id:
        podcast = get_object_or_404(Podcast, pk=podcast_id, network=network)
    entry.podcast = podcast
    if podcast:
        entry.season_number = _int(request.POST.get('season_number'))
        entry.episode_number = _int(request.POST.get('episode_number'))
        entry.episode_type = request.POST.get('episode_type', '').strip()
        entry.external_link = ''
    else:
        entry.season_number = None
        entry.episode_number = None
        entry.episode_type = ''
        entry.external_link = request.POST.get('external_link', '').strip()


def _network_or_404(request):
    """/calendar lives on a network's configured domain — the middleware sets
    request.network there, and an unknown domain never reaches this view (its
    strict fallback 404s /calendar since it isn't whitelisted). None here means
    the app's own bare domain, which has no calendar."""
    network = getattr(request, 'network', None)
    if network is None:
        raise Http404("No calendar is configured for this domain.")
    return network


def _is_owner(request, network):
    return (
        request.user.is_authenticated
        and _require_owner(request.user, network)
    )


def calendar_page(request):
    network = _network_or_404(request)
    is_owner = _is_owner(request, network)

    ics_url = request.build_absolute_uri(reverse('calendar_feed', args=[network.slug]))
    webcal_url = 'webcal://' + ics_url.split('://', 1)[1]
    subscribe = {
        'ics': ics_url,
        'webcal': webcal_url,
        'google': 'https://calendar.google.com/calendar/render?cid=' + quote(webcal_url, safe=''),
        'outlook': (
            'https://outlook.live.com/calendar/0/addfromweb?url='
            + quote(ics_url, safe='') + '&name=' + quote(network.name, safe='')
        ),
    }

    podcasts = []
    if is_owner:
        podcasts = list(Podcast.objects.filter(network=network).order_by('title'))

    return render(request, 'pod_manager/calendar.html', {
        'current_network': network,
        'is_owner': is_owner,
        'podcasts': podcasts,
        'subscribe': subscribe,
        'tz_choices': TZ_CHOICES,
    })


def calendar_events(request):
    """FullCalendar JSON event source (A7 + A14). Public read, scoped to
    request.network. `editable` is (is_owner AND unlinked); the URL is computed
    live from is_published (A13) — never stored — so a scheduled-unpublished
    entry appears without a link and gains one only after it publishes."""
    network = _network_or_404(request)
    is_owner = _is_owner(request, network)

    # The pool is small (a few dozen rows per network), so we return every
    # entry and let FullCalendar show the visible slice — no server range
    # filtering, which also sidesteps converting FullCalendar's UTC-mode range
    # params back through the Eastern display shift.
    entries = CalendarEntry.objects.filter(network=network).select_related('episode', 'podcast')

    events = []
    for entry in entries:
        numbered = entry.season_number is not None and entry.episode_number is not None
        sxe = f"S{entry.season_number}E{entry.episode_number}" if numbered else ''
        linked = bool(entry.episode_id)
        published = linked and entry.episode.is_published
        # A linked-but-unpublished entry (a scheduled/draft episode) is edited
        # from the publisher, not the freeform modal; published/unlinked have
        # no publisher edit target.
        edit_url = ''
        if entry.episode_id and not published:
            edit_url = f"{reverse('publish_episode')}?edit={entry.episode_id}&network={network.slug}"
        event = {
            'id': entry.id,
            'title': entry.title,
            'start': _eastern_naive_iso(entry.scheduled_at),
            'allDay': False,
            'editable': is_owner and not linked,
            'extendedProps': {
                'podcast': entry.podcast.title if entry.podcast else '',
                'podcastId': entry.podcast_id or '',
                'sxe': sxe,
                'season': entry.season_number if entry.season_number is not None else '',
                'episode': entry.episode_number if entry.episode_number is not None else '',
                'episodeType': entry.episode_type,
                'externalLink': entry.external_link,
                'notes': entry.notes,
                'linked': linked,
                'published': published,
                'editUrl': edit_url,
            },
        }
        if published:
            event['url'] = request.build_absolute_uri(
                reverse('episode_detail', args=[entry.episode_id]))
        elif entry.external_link:
            event['url'] = entry.external_link
        events.append(event)
    return JsonResponse(events, safe=False)


@require_POST
def calendar_manage(request):
    """Owner-only mutations: add / delete / move. Server-side ownership check
    (A14) — the /calendar template hides these controls from listeners, but
    that is not the enforcement boundary; this is."""
    network = _network_or_404(request)
    if not _is_owner(request, network):
        return HttpResponseForbidden("Calendar changes require network ownership.")

    action = request.POST.get('action')

    if action == 'move':
        # Drag-reschedule (FullCalendar eventDrop, AJAX) — UNLINKED entries only
        # (A8). A linked entry's time follows its episode's schedule form, so
        # reject the move server-side even though the client marks it uneditable.
        entry = get_object_or_404(CalendarEntry, pk=request.POST.get('entry_id'), network=network)
        if entry.episode_id:
            return JsonResponse(
                {'ok': False, 'error': 'Linked entries follow their episode schedule.'},
                status=409)
        parsed = parse_datetime(request.POST.get('start', '') or '')
        if parsed is None:
            return JsonResponse({'ok': False, 'error': 'Invalid date.'}, status=400)
        # The calendar renders in Eastern, so a drag hands back Eastern
        # wall-clock (UTC-labeled by FullCalendar's UTC mode) — reinterpret it.
        entry.scheduled_at = _wallclock_to_utc(parsed, EASTERN)
        entry.save(update_fields=['scheduled_at', 'updated_at'])
        return JsonResponse({'ok': True})

    if action == 'delete':
        entry = get_object_or_404(CalendarEntry, pk=request.POST.get('entry_id'), network=network)
        entry.delete()
        messages.success(request, "Calendar entry deleted.")
        return redirect('calendar')

    if action in ('add', 'edit'):
        title = request.POST.get('title', '').strip()
        scheduled_at = _scheduled_from_request(request)
        if not title or scheduled_at is None:
            messages.error(request, "A title, date, and time are required.")
            return redirect('calendar')

        if action == 'edit':
            entry = get_object_or_404(
                CalendarEntry, pk=request.POST.get('entry_id'), network=network)
            if entry.episode_id:
                # Linked entries are edited from the publisher (they track a
                # real episode), never the freeform modal.
                messages.error(
                    request, "This entry is linked to an episode — edit it from the publisher.")
                return redirect('calendar')
        else:
            entry = CalendarEntry(network=network, created_by=request.user)

        _apply_entry_fields(entry, request, network)
        entry.scheduled_at = scheduled_at
        entry.save()
        verb = 'Updated' if action == 'edit' else 'Added'
        messages.success(request, f'{verb} "{title}".')
        return redirect('calendar')

    messages.error(request, "Unknown calendar action.")
    return redirect('calendar')
