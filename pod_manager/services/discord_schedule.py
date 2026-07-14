"""
Release-schedule rendering for the Discord bot, shared by the live-updating
`/schedule` post. Lives in services (not the bot daemon) so the Celery worker
can re-render + PATCH a posted embed WITHOUT importing discord.ext.commands or
starting a gateway connection — it only touches discord.Embed (a data class)
and Discord's REST API, the same way tasks.py already syncs the bot avatar.

Everything renders in Eastern to match the public /calendar List view.
"""
import io
import logging
import os
from collections import OrderedDict
from datetime import datetime, time, timedelta, timezone as datetimezone
from zoneinfo import ZoneInfo

import discord
import requests
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)

SITE_BASE_URL = f"https://{os.getenv('DOMAIN', 'vecto.joshtheblack.com')}"
EASTERN = ZoneInfo('America/New_York')

# Discord embed hard limit; a wide window (e.g. 30 days) can exceed it.
MAX_EMBED_DAYS = 25

_WEEKDAY_NUM = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


def _base_url(network):
    return f"https://{network.custom_domain}" if network.custom_domain else SITE_BASE_URL


def _strf(dt, fmt):
    """strftime with the no-pad flag portable across Linux (%-d) and Windows (%#d)."""
    if os.name == "nt":
        fmt = fmt.replace("%-", "%#")
    return dt.strftime(fmt)


def _et_midnight_utc(d):
    """Eastern wall-clock midnight of date `d`, as a UTC-aware datetime."""
    return datetime.combine(d, time.min, tzinfo=EASTERN).astimezone(datetimezone.utc)


# ---------------------------------------------------------
# WINDOW RESOLUTION
# ---------------------------------------------------------
def resolve_window(kind, *, week_start="sunday", previous_days=0, next_days=7):
    """Turn command params into a concrete [start, end) UTC window plus a human
    subtitle. Bounds are frozen at call time (day granularity, Eastern), so a
    live post keeps the same window it was created with until it expires.

      kind="week"  → the current week, starting on `week_start` weekday.
      kind="range" → `previous_days` back through `next_days` forward from today.

    Returns {"start", "end", "kind", "subtitle"}. `end` is exclusive (midnight
    after the last displayed day) and doubles as the live-post expiry.
    """
    today = timezone.now().astimezone(EASTERN).date()

    if kind == "week":
        sw = _WEEKDAY_NUM.get((week_start or "sunday").lower(), 6)
        start_d = today - timedelta(days=(today.weekday() - sw) % 7)
        end_excl_d = start_d + timedelta(days=7)
        last_d = end_excl_d - timedelta(days=1)
        subtitle = f"This week · {_strf(start_d, '%a %b %-d')} – {_strf(last_d, '%a %b %-d')} · ET"
    else:  # range
        previous_days = max(0, min(int(previous_days), 90))
        next_days = max(1, min(int(next_days), 90))
        start_d = today - timedelta(days=previous_days)
        end_excl_d = today + timedelta(days=next_days + 1)
        last_d = end_excl_d - timedelta(days=1)
        if previous_days:
            subtitle = (f"{_strf(start_d, '%b %-d')} – {_strf(last_d, '%b %-d')} · ET "
                        f"(last {previous_days}d + next {next_days}d)")
        else:
            plural = "s" if next_days != 1 else ""
            subtitle = (f"Next {next_days} day{plural} · "
                        f"{_strf(start_d, '%b %-d')} – {_strf(last_d, '%b %-d')} · ET")

    return {
        "start": _et_midnight_utc(start_d),
        "end": _et_midnight_utc(end_excl_d),
        "kind": kind,
        "subtitle": subtitle,
    }


# ---------------------------------------------------------
# DATA
# ---------------------------------------------------------
def build_schedule(network, start, end):
    """CalendarEntry rows for `network` within [start, end), grouped by Eastern
    calendar day (mirrors the /calendar List view). Past days are included when
    the window reaches back, so recently-published entries render as links."""
    from pod_manager.models import CalendarEntry

    entries = (
        CalendarEntry.objects
        .filter(network=network, scheduled_at__gte=start, scheduled_at__lt=end)
        .select_related('podcast', 'episode')
        .order_by('scheduled_at')
    )
    groups = OrderedDict()
    for entry in entries:
        local = entry.scheduled_at.astimezone(EASTERN)
        groups.setdefault(local.date(), []).append(entry)
    return groups


def _entry_view(entry, network):
    """Flatten a CalendarEntry into the fields both renderers need. `published`
    (and thus the episode link) is read live off the episode, so a scheduled
    entry converts to a linked/published row the moment its episode goes live."""
    local = entry.scheduled_at.astimezone(EASTERN)
    numbered = entry.season_number is not None and entry.episode_number is not None
    sxe = f"S{entry.season_number}E{entry.episode_number}" if numbered else ""
    linked = bool(entry.episode_id)
    published = linked and entry.episode.is_published
    url = ""
    if published:
        url = f"{_base_url(network)}/episode/{entry.episode_id}/"
    elif entry.external_link:
        url = entry.external_link
    return {
        "time": _strf(local, "%-I:%M %p"),
        "podcast": entry.podcast.title if entry.podcast else "",
        "sxe": sxe,
        "title": entry.title,
        "url": url,
        "published": published,
        "linked": linked,
    }


# ---------------------------------------------------------
# EMBED
# ---------------------------------------------------------
def _theme_color(network):
    primary = ((network.theme_config or {}).get("primary_color") or "").lstrip("#")
    if len(primary) == 6:
        try:
            return discord.Color(int(primary, 16))
        except ValueError:
            pass
    return discord.Color.gold()


def render_schedule_embed(network, groups, subtitle, *, live=False):
    """Rich-text embed mimicking the /calendar List (agenda) view: one field per
    day, each entry a line with time · SxE · title (linked when published) ·
    podcast. When `live`, notes that the post updates as episodes publish."""
    embed = discord.Embed(
        title=f"📅 {network.name} — Release Schedule",
        description=subtitle,
        color=_theme_color(network),
    )
    if not groups:
        embed.description += "\n\n*Nothing on the calendar in this window.*"
        return embed

    days = list(groups.items())
    for day, entries in days[:MAX_EMBED_DAYS]:
        lines = []
        for entry in entries:
            v = _entry_view(entry, network)
            label = (f"{v['sxe']} · " if v['sxe'] else "") + v['title']
            if v['url']:
                label = f"[{label}]({v['url']})"
            pod = f" · *{v['podcast']}*" if v['podcast'] else ""
            dot = "🟢" if v['published'] else "🔸"
            lines.append(f"{dot} `{v['time']:>8}`  {label}{pod}")
        embed.add_field(name=_strf(day, "%A · %b %-d"), value="\n".join(lines)[:1024], inline=False)

    if len(days) > MAX_EMBED_DAYS:
        embed.add_field(
            name="…",
            value=f"*+{len(days) - MAX_EMBED_DAYS} more day(s) — see the full calendar.*",
            inline=False)

    footer = "🟢 published · 🔸 planned/scheduled"
    if live:
        footer += "  ·  updates live as episodes publish"
    embed.set_footer(text=footer)
    return embed


# ---------------------------------------------------------
# PNG (Pillow) — image analogue of the List view
# ---------------------------------------------------------
def _load_font(size, bold=False):
    """Best-effort TrueType lookup across Linux (pod) + Windows (local dev);
    falls back to Pillow's bundled bitmap font so rendering never hard-fails."""
    from PIL import ImageFont
    candidates = (
        ["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "C:/Windows/Fonts/arialbd.ttf", "arialbd.ttf"]
        if bold else
        ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "C:/Windows/Fonts/arial.ttf", "arial.ttf"]
    )
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            continue
    try:
        return ImageFont.load_default(size)
    except TypeError:  # Pillow < 10 has no sized default
        return ImageFont.load_default()


def _hex(value, fallback):
    value = (value or "").strip()
    if value.startswith("#") and len(value) in (4, 7):
        try:
            if len(value) == 4:
                value = "#" + "".join(c * 2 for c in value[1:])
            return tuple(int(value[i:i + 2], 16) for i in (1, 3, 5))
        except ValueError:
            pass
    return fallback


def _mix(rgb, other, amt):
    """Blend `rgb` toward `other` by `amt` (0-1) — used to derive a day-band
    shade from the surface color the same way the web calendar overlays
    rgba(0,0,0,0.2) on its card."""
    return tuple(int(a * (1 - amt) + b * amt) for a, b in zip(rgb, other))


def render_schedule_png(network, groups, subtitle):
    """Render the agenda to a PNG (BytesIO) themed with the network's
    theme_config colors — the image analogue of the List view."""
    from PIL import Image, ImageDraw

    # The agenda sits on the network's CARD surface and uses only the surface_*
    # text tokens — never bg_* — so a light-surface theme can't paint dark text
    # onto the dark page bg (the dark-on-dark trap). Everything below pairs
    # surface_text/-muted with the surface_bg they were designed against.
    theme = network.theme_config or {}
    bg = _hex(theme.get("surface_bg_color"), (30, 30, 30))
    text = _hex(theme.get("surface_text_color"), (248, 249, 250))
    muted = _hex(theme.get("surface_muted_text_color"), (173, 181, 189))
    primary = _hex(theme.get("primary_color"), (255, 193, 7))
    success = _hex(theme.get("success_color"), (25, 135, 84))
    border = _hex(theme.get("border_color"), _mix(bg, (0, 0, 0), 0.35))
    band = _mix(bg, (0, 0, 0), 0.22)  # header/day-divider shade

    W = 900
    PAD = 32
    time_col = 150
    f_head = _load_font(30, bold=True)
    f_sub = _load_font(16)
    f_day = _load_font(19, bold=True)
    f_time = _load_font(17)
    f_title = _load_font(19, bold=True)
    f_pod = _load_font(13, bold=True)

    # ---- measure to compute canvas height ----
    header_h = 108
    day_head_h = 46
    row_h = 58
    body_h = 0
    views = OrderedDict()
    for day, entries in groups.items():
        vs = [_entry_view(e, network) for e in entries]
        views[day] = vs
        body_h += day_head_h + row_h * len(vs) + 12
    if not groups:
        body_h = 80
    H = header_h + body_h + PAD

    img = Image.new("RGB", (W, H), bg)
    d = ImageDraw.Draw(img)

    # Header band (shaded surface + primary accent underline)
    d.rectangle([0, 0, W, header_h], fill=band)
    d.rectangle([0, header_h - 3, W, header_h], fill=primary)
    d.text((PAD, 30), network.name, font=f_head, fill=text)
    d.text((PAD, 68), f"Release Schedule · {subtitle}", font=f_sub, fill=muted)

    y = header_h + 8
    if not groups:
        d.text((PAD, y + 20), "Nothing on the calendar in this window.", font=f_day, fill=muted)
    for day, vs in views.items():
        # Day divider (accent underline echoes the web scanner bar)
        d.rectangle([PAD, y, W - PAD, y + day_head_h - 8], fill=band)
        d.text((PAD + 12, y + 8), _strf(day, "%A · %b %-d"), font=f_day, fill=text)
        d.rectangle([PAD, y + day_head_h - 8, W - PAD, y + day_head_h - 6], fill=primary)
        y += day_head_h

        for v in vs:
            # Status pip: published = success, planned/scheduled = accent
            pip = success if v["published"] else primary
            d.ellipse([PAD + 2, y + row_h // 2 - 5, PAD + 12, y + row_h // 2 + 5], fill=pip)
            d.text((PAD + 24, y + 18), v["time"], font=f_time, fill=muted)
            tx = PAD + 24 + time_col
            if v["podcast"]:
                d.text((tx, y + 8), v["podcast"].upper()[:60], font=f_pod, fill=primary)
                title_y = y + 26
            else:
                title_y = y + 18
            title = (f"{v['sxe']} · " if v["sxe"] else "") + v["title"]
            d.text((tx, title_y), title[:70], font=f_title, fill=text)
            d.line([PAD, y + row_h - 1, W - PAD, y + row_h - 1], fill=border, width=1)
            y += row_h
        y += 12

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf


# ---------------------------------------------------------
# REST EDIT (called from Celery — no gateway needed)
# ---------------------------------------------------------
def refresh_live_posts(network_id=None):
    """Re-render + PATCH active live /schedule posts. Scoped to `network_id`, or
    ALL networks when None (the test/management path). Expired posts (window_end
    passed) and vanished messages are dropped. Returns a list of (post, action)
    with action in {'updated', 'expired', 'dropped', 'failed'} for reporting."""
    from pod_manager.models import LiveSchedulePost

    now = timezone.now()
    qs = LiveSchedulePost.objects.select_related('network').order_by('id')
    if network_id is not None:
        qs = qs.filter(network_id=network_id)

    results = []
    for post in qs:
        if post.window_end <= now:
            post.delete()
            results.append((post, 'expired'))
            continue
        groups = build_schedule(post.network, post.window_start, post.window_end)
        embed = render_schedule_embed(post.network, groups, post.subtitle, live=True)
        outcome = edit_live_message(post.channel_id, post.message_id, embed)
        if outcome is None:
            post.delete()
            results.append((post, 'dropped'))
        else:
            results.append((post, 'updated' if outcome else 'failed'))
    return results


def edit_live_message(channel_id, message_id, embed):
    """PATCH an already-posted bot message with a fresh embed via Discord's REST
    API. Returns True on success, None if the message is gone (caller should
    drop its tracking row), False on any other failure."""
    token = settings.DISCORD_BOT_TOKEN
    if not token:
        logger.warning("[LiveSchedule] DISCORD_BOT_TOKEN not set; cannot edit message.")
        return False
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}"
    try:
        resp = requests.patch(
            url,
            headers={"Authorization": f"Bot {token}"},
            json={"embeds": [embed.to_dict()]},
            timeout=10,
        )
    except requests.RequestException as e:
        logger.error(f"[LiveSchedule] PATCH failed for message {message_id}: {e}")
        return False
    if resp.status_code in (404, 403):
        # Message deleted, or the bot lost access to the channel — stop tracking.
        logger.info(f"[LiveSchedule] Message {message_id} unreachable ({resp.status_code}); dropping.")
        return None
    if not resp.ok:
        logger.error(f"[LiveSchedule] PATCH {message_id} -> {resp.status_code}: {resp.text[:200]}")
        return False
    return True
