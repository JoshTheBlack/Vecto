import logging
import os
import math
import discord
from discord.ext import commands
from discord import app_commands
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Q
from asgiref.sync import sync_to_async

logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# INTERACTIVE PAGINATION UI
# ---------------------------------------------------------
class S3Paginator(discord.ui.View):
    def __init__(self, query: str, data: list):
        super().__init__(timeout=1200) # View stays active for 20 minutes
        self.query = query
        self.data = data
        self.current_page = 0
        self.per_page = 10
        self.total_pages = math.ceil(len(data) / self.per_page)
        self.update_buttons()

    def create_embed(self):
        embed = discord.Embed(
            title=f"S3 Recovery Search: '{self.query}'",
            description=f"🚨 Found **{len(self.data)}** episodes hosted on S3.",
            color=discord.Color.red()
        )
        
        # Calculate slice for current page
        start = self.current_page * self.per_page
        end = start + self.per_page
        chunk = self.data[start:end]

        for item in chunk:
            embed.add_field(name=item['title'], value=f"[View on {item['network']}]({item['url']})", inline=False)
            
        embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")
        return embed

    def update_buttons(self):
        # Disable buttons if we hit the boundaries
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        # Edit the original message to show the new page
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        # Edit the original message to show the new page
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    # Add this new Close Button block:
    @discord.ui.button(label="✖ Dismiss", style=discord.ButtonStyle.danger, custom_id="close")
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop() # Immediately kills the View listener in the bot's memory
        await interaction.message.delete() # Deletes the embed and buttons from the Discord channel


# ---------------------------------------------------------
# MAIN BOT COMMAND
# ---------------------------------------------------------
class Command(BaseCommand):
    help = 'Starts the Vecto Discord Bot Daemon with Slash Commands'

    def handle(self, *args, **options):
        from pod_manager.models import Episode, NetworkMembership, Network
        
        bot_token = settings.DISCORD_BOT_TOKEN
        
        if not bot_token:
            logger.error("CRITICAL: DISCORD_BOT_TOKEN is not set in environment. Exiting.")
            return

        intents = discord.Intents.default()
        intents.message_content = True 

        bot = commands.Bot(command_prefix="!", intents=intents)

        @bot.event
        async def on_ready():
            logger.info("=======================================================")
            logger.info(f"🤖 [VECTO BOT] Connected to Discord as {bot.user} (ID: {bot.user.id})")
            try:
                synced = await bot.tree.sync()
                logger.info(f"Synced {len(synced)} slash command(s) globally.")
            except Exception as e:
                logger.error(f"Failed to sync slash commands: {e}")
            logger.info("=======================================================")

        # 1. THE SEARCH COMMAND (/search)
        @bot.tree.command(name="search", description="Search the Vecto database for a podcast episode.")
        @app_commands.describe(
            query="The search term", 
            search_by="What field to search in (Defaults to Title)",
            podcast="Optional: Filter exclusively by a specific podcast title"
        )
        @app_commands.choices(search_by=[
            app_commands.Choice(name="Title", value="title"),
            app_commands.Choice(name="Description", value="description"),
            app_commands.Choice(name="Tags", value="tags"),
        ])
        async def search_episode(interaction: discord.Interaction, query: str, search_by: app_commands.Choice[str] = None, podcast: str = None):
            await interaction.response.defer() 
            
            @sync_to_async
            def do_search(q, s_by, p_cast, guild_id):
                networks = Network.objects.filter(discord_server_id=guild_id)
                if not networks.exists():
                    return None

                qs = Episode.objects.filter(podcast__network__in=networks).select_related('podcast', 'podcast__network')
                
                # Apply the optional podcast filter
                if p_cast:
                    qs = qs.filter(podcast__title__icontains=p_cast)
                
                if not s_by or s_by.value == "title":
                    qs = qs.filter(title__icontains=q)
                elif s_by.value == "description":
                    qs = qs.filter(clean_description__icontains=q)
                elif s_by.value == "tags":
                    qs = qs.filter(tags__icontains=q)
                
                return list(qs[:5])
            
            results = await do_search(query, search_by, podcast, str(interaction.guild_id))
            
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
                base_url = f"https://{network.custom_domain}" if network.custom_domain else "http://localhost:8000"
                url = f"{base_url}/episode/{ep.id}/"
                embed.add_field(name=ep.title[:250], value=f"[Listen on {network.name}]({url})", inline=False)
                
            await interaction.followup.send(embed=embed)

        # 2. THE STATS COMMAND (/mystats)
        @bot.tree.command(name="mystats", description="View your private Vecto profile stats.")
        async def my_stats(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=True)

            @sync_to_async
            def get_stats(discord_id):
                membership = NetworkMembership.objects.filter(user__patron_profile__discord_id=discord_id).select_related('user').first()
                if not membership:
                    return None
                
                total_approved = (
                    (membership.edits_title or 0) + 
                    (membership.edits_chapters or 0) + 
                    (membership.edits_tags or 0) + 
                    (membership.edits_descriptions or 0)
                )
                return membership, total_approved

            result = await get_stats(str(interaction.user.id))
            
            if not result:
                await interaction.followup.send("I couldn't find a linked Vecto account! Please log in via Patreon on the website.", ephemeral=True)
                return
            
            membership, total_approved = result
            level = 1; title = "Initiate"
            if total_approved >= 1000: level = 5; title = "Keeper of the Tome"
            elif total_approved >= 500: level = 4; title = "Grand Archivist"
            elif total_approved >= 100: level = 3; title = "Archivist"
            elif total_approved >= 25: level = 2; title = "Scout"

            embed = discord.Embed(title=f"{interaction.user.display_name}'s Vecto Profile", color=discord.Color.blue())
            if membership.discord_image_url:
                embed.set_thumbnail(url=membership.discord_image_url)
            else:
                embed.set_thumbnail(url=interaction.user.display_avatar.url)

            embed.add_field(name="Guild Rank", value=f"Level {level} {title}", inline=False)
            embed.add_field(name="Trust Score", value=str(membership.trust_score), inline=True)
            embed.add_field(name="Total Edits", value=str(total_approved), inline=True)
            
            await interaction.followup.send(embed=embed, ephemeral=True)

        # 3. THE S3 RECOVERY COMMAND (/s3) - MULTI-ACTION (SEARCH & STATS)
        @bot.tree.command(name="s3", description="S3 Recovery Tool: Search episodes or view stats.")
        @app_commands.describe(
            action="Choose between searching episodes or getting stats",
            query="Optional: Episode title to search for (Search mode only)",
            podcast="Optional: Filter by a specific podcast title"
        )
        @app_commands.choices(action=[
            app_commands.Choice(name="Search Episodes", value="search"),
            app_commands.Choice(name="View Stats", value="stats")
        ])
        async def s3_command(interaction: discord.Interaction, action: app_commands.Choice[str], query: str = None, podcast: str = None):
            await interaction.response.defer()

            @sync_to_async
            def do_s3_stats(p_cast, guild_id):
                from django.db.models import Count
                networks = Network.objects.filter(discord_server_id=guild_id)
                if not networks.exists():
                    return None

                qs = Episode.objects.filter(
                    Q(podcast__network__in=networks),
                    Q(audio_url_public__icontains='s3') | Q(audio_url_subscriber__icontains='s3')
                )

                if p_cast:
                    qs = qs.filter(podcast__title__icontains=p_cast)
                    # Group by podcast title and aggregate the counts
                    stats = qs.values('podcast__title').annotate(count=Count('id')).order_by('-count')
                    return list(stats)
                else:
                    return qs.count()

            @sync_to_async
            def do_s3_search(q, p_cast, guild_id):
                networks = Network.objects.filter(discord_server_id=guild_id)
                if not networks.exists():
                    return None

                qs = Episode.objects.filter(
                    Q(podcast__network__in=networks),
                    Q(audio_url_public__icontains='s3') | Q(audio_url_subscriber__icontains='s3')
                ).select_related('podcast', 'podcast__network')

                if p_cast:
                    qs = qs.filter(podcast__title__icontains=p_cast)
                if q:
                    qs = qs.filter(title__icontains=q)
                    
                qs = qs[:500] 
                
                data = []
                for ep in qs:
                    network = ep.podcast.network
                    base_url = f"https://{network.custom_domain}" if network.custom_domain else "http://localhost:8000"
                    data.append({
                        "title": ep.title[:250],
                        "url": f"{base_url}/episode/{ep.id}/",
                        "network": network.name
                    })
                return data

            # ==========================================
            # MODE: STATS
            # ==========================================
            if action.value == "stats":
                result = await do_s3_stats(podcast, str(interaction.guild_id))
                
                if result is None:
                    await interaction.followup.send("This Discord server is not linked to any Vecto Network.", ephemeral=True)
                    return

                embed = discord.Embed(title="S3 Recovery Stats", color=discord.Color.blue())
                
                if podcast:
                    if not result:
                        embed.description = f"No S3-hosted episodes found for podcast matching **'{podcast}'**."
                    else:
                        lines = [f"**{r['count']}** episodes found hosted on S3 in podcast **{r['podcast__title']}**" for r in result]
                        embed.description = "\n".join(lines)[:4096] # Prevent Discord character limit crash
                else:
                    embed.description = f"**{result}** episodes found hosted on S3."

                await interaction.followup.send(embed=embed)

            # ==========================================
            # MODE: SEARCH
            # ==========================================
            elif action.value == "search":
                results = await do_s3_search(query, podcast, str(interaction.guild_id))

                if results is None:
                    await interaction.followup.send("This Discord server is not linked to any Vecto Network.", ephemeral=True)
                    return

                if not results:
                    msg = "No S3-hosted episodes found"
                    if podcast: msg += f" for podcast **'{podcast}'**"
                    if query: msg += f" matching **'{query}'**"
                    await interaction.followup.send(msg + ".")
                    return

                # Fire up the Paginator View
                search_title = f"Search: '{query}'" if query else "All Episodes"
                if podcast: search_title += f" | Podcast: '{podcast}'"
                
                view = S3Paginator(search_title, results)
                await interaction.followup.send(embed=view.create_embed(), view=view)

        # START DAEMON
        try:
            logger.info("Starting Discord Bot loop...")
            bot.run(bot_token, log_handler=None)
        except Exception as e:
            logger.error(f"Failed to start Discord Bot: {e}", exc_info=True)