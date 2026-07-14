import io
import logging
import math
import os
from collections import OrderedDict
from datetime import timedelta
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands
from asgiref.sync import sync_to_async
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

logger = logging.getLogger(__name__)

# Canonical site base URL; used when a network has no custom domain.
SITE_BASE_URL = f"https://{os.getenv('DOMAIN', 'vecto.joshtheblack.com')}"

# The web calendar renders everything in Eastern (see views/calendar.py); the
# bot mirrors that so a posted schedule matches what owners see on /calendar.
EASTERN = ZoneInfo('America/New_York')


def _base_url(network):
    return f"https://{network.custom_domain}" if network.custom_domain else SITE_BASE_URL


# ---------------------------------------------------------
# RELEASE SCHEDULE (shared by /schedule embed + PNG renderers)
# ---------------------------------------------------------
def build_schedule(network, days):
    """Return the network's upcoming calendar entries for the next `days`,
    grouped by Eastern calendar day (mirrors the /calendar List view).

    Returns an OrderedDict {date: [entry, ...]} in chronological order, plus a
    parallel list of per-entry display dicts is built lazily by the renderers.
    """
    from pod_manager.models import CalendarEntry

    now = timezone.now()
    end = now + timedelta(days=days)
    entries = (
        CalendarEntry.objects
        .filter(network=network, scheduled_at__gte=now, scheduled_at__lte=end)
        .select_related('podcast', 'episode')
        .order_by('scheduled_at')
    )
    groups = OrderedDict()
    for entry in entries:
        local = entry.scheduled_at.astimezone(EASTERN)
        groups.setdefault(local.date(), []).append(entry)
    return groups


def _entry_view(entry, network):
    """Flatten a CalendarEntry into the fields both renderers need."""
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
        "time": local.strftime("%-I:%M %p") if os.name != "nt" else local.strftime("%#I:%M %p"),
        "podcast": entry.podcast.title if entry.podcast else "",
        "sxe": sxe,
        "title": entry.title,
        "url": url,
        "published": published,
        "linked": linked,
    }


def render_schedule_embed(network, groups, days):
    """Rich-text embed mimicking the /calendar List (agenda) view: one field
    per day, each entry a line with time · podcast · SxE · title (+ link)."""
    color = discord.Color.gold()
    theme = network.theme_config or {}
    primary = (theme.get("primary_color") or "").lstrip("#")
    if len(primary) == 6:
        try:
            color = discord.Color(int(primary, 16))
        except ValueError:
            pass

    embed = discord.Embed(
        title=f"📅 {network.name} — Release Schedule",
        description=f"Upcoming releases for the next **{days}** day{'s' if days != 1 else ''} · all times ET.",
        color=color,
    )
    if not groups:
        embed.description += "\n\n*Nothing on the calendar in this window.*"
        return embed

    for day, entries in groups.items():
        name = day.strftime("%A · %b %-d") if os.name != "nt" else day.strftime("%A · %b %#d")
        lines = []
        for entry in entries:
            v = _entry_view(entry, network)
            label = (f"{v['sxe']} · " if v['sxe'] else "") + v['title']
            if v['url']:
                label = f"[{label}]({v['url']})"
            pod = f" · *{v['podcast']}*" if v['podcast'] else ""
            dot = "🟢" if v['published'] else "🔸"
            lines.append(f"{dot} `{v['time']:>8}`  {label}{pod}")
        value = "\n".join(lines)
        embed.add_field(name=name, value=value[:1024], inline=False)

    embed.set_footer(text="🟢 published · 🔸 planned/scheduled")
    return embed


# ---------------------------------------------------------
# PNG renderer — draws the same agenda as an image (Pillow)
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


def render_schedule_png(network, groups, days):
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
    d.text((PAD, 68),
           f"Release Schedule · next {days} day{'s' if days != 1 else ''} · times ET",
           font=f_sub, fill=muted)

    y = header_h + 8
    if not groups:
        d.text((PAD, y + 20), "Nothing on the calendar in this window.", font=f_day, fill=muted)
    for day, vs in views.items():
        # Day divider (accent underline echoes the web scanner bar)
        label = day.strftime("%A · %b %-d") if os.name != "nt" else day.strftime("%A · %b %#d")
        d.rectangle([PAD, y, W - PAD, y + day_head_h - 8], fill=band)
        d.text((PAD + 12, y + 8), label, font=f_day, fill=text)
        d.rectangle([PAD, y + day_head_h - 8, W - PAD, y + day_head_h - 6], fill=primary)
        y += day_head_h

        for v in vs:
            # Status pip: published = success, planned/scheduled = accent
            pip = success if v["published"] else primary
            d.ellipse([PAD + 2, y + row_h // 2 - 5, PAD + 12, y + row_h // 2 + 5], fill=pip)
            # Time column
            d.text((PAD + 24, y + 18), v["time"], font=f_time, fill=muted)
            # Podcast (uppercase, tiny) + title
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
# INTERACTIVE PAGINATION UI
# ---------------------------------------------------------
class S3Paginator(discord.ui.View):
    def __init__(self, query: str, data: list):
        super().__init__(timeout=300)
        self.query = query
        self.data = data
        self.current_page = 0
        self.per_page = 10
        self.total_pages = math.ceil(len(data) / self.per_page)
        self.update_buttons()

    def create_embed(self):
        embed = discord.Embed(
            title=f"S3 Recovery Search: '{self.query}'",
            description=f"Found **{len(self.data)}** episodes hosted on S3.",
            color=discord.Color.red(),
        )
        start = self.current_page * self.per_page
        for item in self.data[start:start + self.per_page]:
            embed.add_field(name=item['title'], value=f"[View on {item['network']}]({item['url']})", inline=False)
        embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")
        return embed

    def update_buttons(self):
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    @discord.ui.button(label="✖ Dismiss", style=discord.ButtonStyle.danger, custom_id="close")
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        try:
            await interaction.message.delete()
        except (discord.errors.Forbidden, discord.errors.NotFound):
            await interaction.response.edit_message(content="*(dismissed)*", embed=None, view=None)


# ---------------------------------------------------------
# MAIN BOT COMMAND
# ---------------------------------------------------------
class Command(BaseCommand):
    help = 'Starts the Vecto Discord Bot Daemon with Slash Commands'

    def handle(self, *args, **options):
        from pod_manager.models import (
            Episode, Network, NetworkMembership, NetworkMix,
            PatronProfile, Podcast, UserMix,
        )

        bot_token = settings.DISCORD_BOT_TOKEN
        if not bot_token:
            logger.error("CRITICAL: DISCORD_BOT_TOKEN is not set. Exiting.")
            return

        intents = discord.Intents.default()
        intents.message_content = True
        bot = commands.Bot(command_prefix="!", intents=intents)

        @bot.event
        async def on_ready():
            logger.info("=======================================================")
            logger.info(f"[VECTO BOT] Connected as {bot.user} (ID: {bot.user.id})")
            try:
                synced = await bot.tree.sync()
                logger.info(f"Synced {len(synced)} slash command(s) globally.")
            except Exception as e:
                logger.error(f"Failed to sync slash commands: {e}")
            await _sync_bot_avatar(bot)
            logger.info("=======================================================")

        async def _sync_bot_avatar(bot):
            """Set the bot's avatar to the configured network's guild icon.
            Uses the same Network.discord_server_id source as the Celery task
            so both paths stay consistent."""
            @sync_to_async
            def get_configured_guild_id():
                from pod_manager.models import Network
                network = Network.objects.exclude(
                    discord_server_id__isnull=True
                ).exclude(discord_server_id__exact='').first()
                return int(network.discord_server_id) if network else None

            guild_id = await get_configured_guild_id()
            if not guild_id:
                logger.info("[VECTO BOT] No network with a Discord server ID configured, skipping avatar sync.")
                return

            guild = bot.get_guild(guild_id)
            if not guild or not guild.icon:
                logger.info(f"[VECTO BOT] Guild {guild_id} not found or has no icon, skipping avatar sync.")
                return

            icon_hash = guild.icon.key
            if bot.user.avatar and bot.user.avatar.key == icon_hash:
                logger.info(f"[VECTO BOT] Avatar already matches guild icon ({icon_hash[:8]}…), skipping upload.")
                return

            try:
                avatar_bytes = await guild.icon.read()
                await bot.user.edit(avatar=avatar_bytes)
                logger.info(f"[VECTO BOT] Avatar updated to match '{guild.name}' server icon.")
            except discord.errors.HTTPException as e:
                logger.warning(f"[VECTO BOT] Avatar update failed (rate-limited or forbidden): {e}")

        @bot.tree.error
        async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
            if isinstance(error, app_commands.MissingPermissions):
                await interaction.response.send_message(
                    "You need the **Manage Server** permission to use this command.", ephemeral=True
                )
            else:
                logger.error(f"Unhandled slash command error: {error}", exc_info=True)
                if not interaction.response.is_done():
                    await interaction.response.send_message("An unexpected error occurred.", ephemeral=True)

        # ── /search ───────────────────────────────────────────────────────────
        @bot.tree.command(name="search", description="Search the Vecto database for a podcast episode.")
        @app_commands.describe(
            query="The search term",
            search_by="Field to search in (default: Title)",
            podcast="Optional: filter by a specific podcast title",
            count="Number of results to return (1–10, default 5)",
        )
        @app_commands.choices(search_by=[
            app_commands.Choice(name="Title",       value="title"),
            app_commands.Choice(name="Description", value="description"),
            app_commands.Choice(name="Tags",        value="tags"),
        ])
        async def search_episode(
            interaction: discord.Interaction,
            query: str,
            search_by: app_commands.Choice[str] = None,
            podcast: str = None,
            count: int = 5,
        ):
            await interaction.response.defer()
            count = max(1, min(count, 10))

            @sync_to_async
            def do_search():
                networks = Network.objects.filter(discord_server_id=str(interaction.guild_id))
                if not networks.exists():
                    return None
                qs = Episode.objects.filter(
                    podcast__network__in=networks
                ).select_related('podcast', 'podcast__network')
                if podcast:
                    qs = qs.filter(podcast__title__icontains=podcast)
                field = search_by.value if search_by else "title"
                if field == "title":
                    qs = qs.filter(title__icontains=query)
                elif field == "description":
                    qs = qs.filter(clean_description__icontains=query)
                elif field == "tags":
                    qs = qs.filter(tags__icontains=query)
                return list(qs[:count])

            results = await do_search()
            if results is None:
                await interaction.followup.send("This Discord server is not linked to any Vecto Network.", ephemeral=True)
                return
            if not results:
                await interaction.followup.send(f"No results found for **'{query}'**.")
                return

            embed = discord.Embed(title=f"Search Results for '{query}'", color=discord.Color.gold())
            if results[0].podcast and results[0].podcast.image_url:
                embed.set_thumbnail(url=results[0].podcast.image_url)
            for ep in results:
                network = ep.podcast.network
                embed.add_field(
                    name=ep.title[:250],
                    value=f"[Listen on {network.name}]({_base_url(network)}/episode/{ep.id}/)",
                    inline=False,
                )
            await interaction.followup.send(embed=embed)

        # ── /mystats ──────────────────────────────────────────────────────────
        @bot.tree.command(name="mystats", description="View your private Vecto profile stats.")
        async def my_stats(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=True)

            @sync_to_async
            def get_stats():
                membership = (
                    NetworkMembership.objects
                    .filter(
                        user__patron_profile__discord_id=str(interaction.user.id),
                        network__discord_server_id=str(interaction.guild_id),
                    )
                    .select_related('user__patron_profile')
                    .first()
                )
                if not membership:
                    return None
                total_approved = (
                    (membership.edits_title or 0) +
                    (membership.edits_chapters or 0) +
                    (membership.edits_tags or 0) +
                    (membership.edits_descriptions or 0)
                )
                return membership, total_approved

            result = await get_stats()
            if not result:
                await interaction.followup.send(
                    "I couldn't find a linked Vecto account for this server. Please log in via the website.",
                    ephemeral=True,
                )
                return

            membership, total_approved = result
            level, rank = 1, "Initiate"
            if   total_approved >= 1000: level, rank = 5, "Keeper of the Tome"
            elif total_approved >= 500:  level, rank = 4, "Grand Archivist"
            elif total_approved >= 100:  level, rank = 3, "Archivist"
            elif total_approved >= 25:   level, rank = 2, "Scout"

            embed = discord.Embed(
                title=f"{interaction.user.display_name}'s Vecto Profile",
                color=discord.Color.blue(),
            )
            embed.set_thumbnail(url=membership.discord_image_url or interaction.user.display_avatar.url)
            embed.add_field(name="Guild Rank",  value=f"Level {level} — {rank}", inline=False)
            embed.add_field(name="Trust Score", value=str(membership.trust_score), inline=True)
            embed.add_field(name="Total Edits", value=str(total_approved),         inline=True)
            if membership.last_active_date:
                embed.add_field(name="Last Active", value=str(membership.last_active_date), inline=True)
            profile = getattr(membership.user, 'patron_profile', None)
            if profile and profile.last_play_week:
                embed.add_field(name="Last Active Week", value=str(profile.last_play_week), inline=True)

            await interaction.followup.send(embed=embed, ephemeral=True)

        # ── /s3 ───────────────────────────────────────────────────────────────
        @bot.tree.command(name="s3", description="[Admin] S3 Recovery Tool: search episodes or view stats.")
        @app_commands.describe(
            action="Choose between searching episodes or getting stats",
            query="Optional: episode title to search for (Search mode only)",
            podcast="Optional: filter by a specific podcast title",
        )
        @app_commands.choices(action=[
            app_commands.Choice(name="Search Episodes", value="search"),
            app_commands.Choice(name="View Stats",      value="stats"),
        ])
        @app_commands.checks.has_permissions(manage_guild=True)
        async def s3_command(
            interaction: discord.Interaction,
            action: app_commands.Choice[str],
            query: str = None,
            podcast: str = None,
        ):
            await interaction.response.defer()

            @sync_to_async
            def do_s3_stats():
                from django.db.models import Count
                networks = Network.objects.filter(discord_server_id=str(interaction.guild_id))
                if not networks.exists():
                    return None
                qs = Episode.objects.filter(
                    Q(podcast__network__in=networks),
                    Q(audio_url_public__icontains='s3') | Q(audio_url_subscriber__icontains='s3'),
                )
                if podcast:
                    qs = qs.filter(podcast__title__icontains=podcast)
                    return list(qs.values('podcast__title').annotate(count=Count('id')).order_by('-count'))
                return qs.count()

            @sync_to_async
            def do_s3_search():
                networks = Network.objects.filter(discord_server_id=str(interaction.guild_id))
                if not networks.exists():
                    return None
                qs = Episode.objects.filter(
                    Q(podcast__network__in=networks),
                    Q(audio_url_public__icontains='s3') | Q(audio_url_subscriber__icontains='s3'),
                ).select_related('podcast', 'podcast__network')
                if podcast:
                    qs = qs.filter(podcast__title__icontains=podcast)
                if query:
                    qs = qs.filter(title__icontains=query)
                data = []
                for ep in qs[:500]:
                    network = ep.podcast.network
                    data.append({
                        "title":   ep.title[:250],
                        "url":     f"{_base_url(network)}/episode/{ep.id}/",
                        "network": network.name,
                    })
                return data

            if action.value == "stats":
                result = await do_s3_stats()
                if result is None:
                    await interaction.followup.send("This server is not linked to any Vecto Network.", ephemeral=True)
                    return
                embed = discord.Embed(title="S3 Recovery Stats", color=discord.Color.blue())
                if podcast:
                    embed.description = (
                        "\n".join(f"**{r['count']}** episodes in **{r['podcast__title']}**" for r in result)
                        if result else f"No S3-hosted episodes found for **'{podcast}'**."
                    )[:4096]
                else:
                    embed.description = f"**{result}** episodes found hosted on S3."
                await interaction.followup.send(embed=embed)

            elif action.value == "search":
                results = await do_s3_search()
                if results is None:
                    await interaction.followup.send("This server is not linked to any Vecto Network.", ephemeral=True)
                    return
                if not results:
                    msg = "No S3-hosted episodes found"
                    if podcast: msg += f" for **'{podcast}'**"
                    if query:   msg += f" matching **'{query}'**"
                    await interaction.followup.send(msg + ".")
                    return
                label = f"Search: '{query}'" if query else "All Episodes"
                if podcast: label += f" | Podcast: '{podcast}'"
                view = S3Paginator(label, results)
                await interaction.followup.send(embed=view.create_embed(), view=view)

        # ── /recent ───────────────────────────────────────────────────────────
        @bot.tree.command(name="recent", description="Show the most recently published episodes for this network.")
        @app_commands.describe(count="Number of episodes to show (1–10, default 5)")
        async def recent_episodes(interaction: discord.Interaction, count: int = 5):
            await interaction.response.defer()
            count = max(1, min(count, 10))

            @sync_to_async
            def get_recent():
                networks = Network.objects.filter(discord_server_id=str(interaction.guild_id))
                if not networks.exists():
                    return None
                return list(
                    Episode.objects
                    .filter(podcast__network__in=networks)
                    .select_related('podcast', 'podcast__network')
                    .order_by('-pub_date')[:count]
                )

            results = await get_recent()
            if results is None:
                await interaction.followup.send("This server is not linked to any Vecto Network.", ephemeral=True)
                return
            if not results:
                await interaction.followup.send("No episodes found for this network.")
                return

            embed = discord.Embed(title="Recently Published Episodes", color=discord.Color.green())
            for ep in results:
                network = ep.podcast.network
                date_str = ep.pub_date.strftime("%b %d, %Y") if ep.pub_date else "Unknown date"
                embed.add_field(
                    name=ep.title[:250],
                    value=f"{ep.podcast.title} — {date_str}\n[Listen]({_base_url(network)}/episode/{ep.id}/)",
                    inline=False,
                )
            await interaction.followup.send(embed=embed)

        # ── /myfeed ───────────────────────────────────────────────────────────
        @bot.tree.command(name="myfeed", description="Get your private feed URL for a podcast or mix (only visible to you).")
        @app_commands.describe(
            search="Name of the podcast or mix to search for",
            feed_type="Type of feed (default: Podcast)",
        )
        @app_commands.choices(feed_type=[
            app_commands.Choice(name="Podcast",     value="podcast"),
            app_commands.Choice(name="Super Mix",   value="supermix"),
            app_commands.Choice(name="Private Mix", value="privatemix"),
        ])
        async def my_feed(
            interaction: discord.Interaction,
            search: str,
            feed_type: app_commands.Choice[str] = None,
        ):
            await interaction.response.defer(ephemeral=True)
            ftype = feed_type.value if feed_type else "podcast"

            @sync_to_async
            def get_feeds():
                profile = PatronProfile.objects.filter(
                    discord_id=str(interaction.user.id)
                ).select_related('user').first()
                if not profile:
                    return None, []

                token = str(profile.feed_token)
                feeds = []

                if ftype == "podcast":
                    networks = Network.objects.filter(discord_server_id=str(interaction.guild_id))
                    for p in Podcast.objects.filter(network__in=networks, title__icontains=search).select_related('network'):
                        feeds.append((p.title, f"{_base_url(p.network)}/feed/?auth={token}&show={p.slug}"))

                elif ftype == "supermix":
                    networks = Network.objects.filter(discord_server_id=str(interaction.guild_id))
                    for m in NetworkMix.objects.filter(network__in=networks, name__icontains=search).select_related('network'):
                        feeds.append((m.name, f"{_base_url(m.network)}/feed/{m.network.slug}/mix/{m.slug}/?auth={token}"))

                elif ftype == "privatemix":
                    for m in UserMix.objects.filter(user=profile.user, name__icontains=search).select_related('network'):
                        feeds.append((m.name, f"{_base_url(m.network)}/feed/mix/{m.unique_id}?auth={token}"))

                return profile, feeds

            profile, feeds = await get_feeds()
            if profile is None:
                await interaction.followup.send(
                    "No linked Vecto account found. Please log in via the website first.",
                    ephemeral=True,
                )
                return
            if not feeds:
                type_label = {"podcast": "podcasts", "supermix": "super mixes", "privatemix": "private mixes"}.get(ftype, "feeds")
                await interaction.followup.send(
                    f"No {type_label} found matching **'{search}'**.", ephemeral=True
                )
                return

            lines = [f"**{name}**\n```{url}```" for name, url in feeds]
            header = f"Your private feed{'s' if len(feeds) > 1 else ''} matching **'{search}'**:\n\n"
            content = header + "\n".join(lines)
            if len(content) > 2000:
                content = content[:1990] + "\n*(truncated)*"
            await interaction.followup.send(content, ephemeral=True)

        # ── /getfeed ──────────────────────────────────────────────────────────
        @bot.tree.command(name="getfeed", description="Get the public feed URL for a podcast.")
        @app_commands.describe(search="Name of the podcast to search for")
        async def get_feed(interaction: discord.Interaction, search: str):
            await interaction.response.defer()

            @sync_to_async
            def find_feeds():
                networks = Network.objects.filter(discord_server_id=str(interaction.guild_id))
                if not networks.exists():
                    return None
                feeds = []
                for p in Podcast.objects.filter(network__in=networks, title__icontains=search).select_related('network'):
                    # Prefer public_feed_url (e.g. Megaphone) when set; fall back to Vecto-generated feed.
                    if p.public_feed_url:
                        feeds.append((p.title, p.public_feed_url, True))
                    else:
                        feeds.append((p.title, f"{_base_url(p.network)}/public/feed/{p.slug}/", False))
                return feeds

            feeds = await find_feeds()
            if feeds is None:
                await interaction.followup.send("This server is not linked to any Vecto Network.", ephemeral=True)
                return
            if not feeds:
                await interaction.followup.send(f"No public podcasts found matching **'{search}'**.")
                return

            embed = discord.Embed(
                title=f"Public Feeds matching '{search}'",
                color=discord.Color.blurple(),
            )
            for title, url, is_external in feeds:
                source = "External Feed" if is_external else "Vecto Feed"
                embed.add_field(name=f"{title}  ·  {source}", value=f"`{url}`", inline=False)
            await interaction.followup.send(embed=embed)

        # ── /schedule ─────────────────────────────────────────────────────────
        @bot.tree.command(name="schedule", description="Post this network's upcoming release schedule.")
        @app_commands.describe(
            days="How many days ahead to show (1–30, default 7)",
            style="Rich embed (default) or a rendered PNG of the list view",
        )
        @app_commands.choices(style=[
            app_commands.Choice(name="Embed (rich text)", value="embed"),
            app_commands.Choice(name="Image (PNG)",       value="image"),
        ])
        async def schedule_command(
            interaction: discord.Interaction,
            days: int = 7,
            style: app_commands.Choice[str] = None,
        ):
            await interaction.response.defer()
            days = max(1, min(days, 30))
            mode = style.value if style else "embed"

            @sync_to_async
            def gather():
                network = Network.objects.filter(discord_server_id=str(interaction.guild_id)).first()
                if not network:
                    return None, None
                groups = build_schedule(network, days)
                if mode == "image":
                    # Render inside the sync context so the ORM entries are
                    # fully evaluated before we touch them off-thread.
                    png = render_schedule_png(network, groups, days)
                    return network, png
                return network, render_schedule_embed(network, groups, days)

            network, payload = await gather()
            if network is None:
                await interaction.followup.send(
                    "This Discord server is not linked to any Vecto Network.", ephemeral=True)
                return

            if mode == "image":
                file = discord.File(payload, filename="schedule.png")
                await interaction.followup.send(
                    content=f"**{network.name}** — release schedule, next {days} day{'s' if days != 1 else ''} (ET):",
                    file=file)
            else:
                await interaction.followup.send(embed=payload)

        # ── START ─────────────────────────────────────────────────────────────
        try:
            logger.info("Starting Discord Bot loop...")
            bot.run(bot_token, log_handler=None)
        except Exception as e:
            logger.error(f"Failed to start Discord Bot: {e}", exc_info=True)
