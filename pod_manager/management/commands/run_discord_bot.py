import logging
import os
import discord
from discord.ext import commands
from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Starts the Vecto Discord Bot Daemon'

    def handle(self, *args, **options):
        bot_token = settings.DISCORD_BOT_TOKEN
        
        if not bot_token:
            logger.error("CRITICAL: DISCORD_BOT_TOKEN is not set in environment. Exiting.")
            return

        # 1. Enable the Message Content Intent
        intents = discord.Intents.default()
        intents.message_content = True 
        # intents.members = True  # Keep this off until we need it

        bot = commands.Bot(command_prefix="!", intents=intents)

        @bot.event
        async def on_ready():
            logger.info(f"=======================================================")
            logger.info(f"🤖 [VECTO BOT] Connected to Discord as {bot.user} (ID: {bot.user.id})")
            logger.info(f"=======================================================")

        # 2. Scaffold the Search Command
        @bot.command(name="search", help="Search for a podcast episode by title.")
        async def search_episode(ctx, *, query: str = None):
            if not query:
                await ctx.send("Please provide a search term! Example: `!search breaking bad`")
                return
            
            # TODO: Add Django ORM logic here in the next phase
            logger.info(f"[Discord] User {ctx.author} searched for: {query}")
            await ctx.send(f"🔍 Searching the Vecto database for: **{query}**...\n*(Logic coming soon!)*")

        # 3. Scaffold the Profile Command
        @bot.command(name="profile", help="View your Vecto profile stats.")
        async def view_profile(ctx):
            # TODO: Fetch user's NetworkMembership based on ctx.author.id
            logger.info(f"[Discord] User {ctx.author} requested their profile.")
            await ctx.send(f"👤 Fetching profile for **{ctx.author.name}**...\n*(Logic coming soon!)*")

        try:
            logger.info("Starting Discord Bot loop...")
            bot.run(bot_token, log_handler=None)
        except Exception as e:
            logger.error(f"Failed to start Discord Bot: {e}", exc_info=True)