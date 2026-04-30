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

        # Explicitly define intents
        intents = discord.Intents.default()
        # intents.members = True  # Uncomment later if we need to scan server member lists

        bot = commands.Bot(command_prefix="!", intents=intents)

        @bot.event
        async def on_ready():
            logger.info(f"=======================================================")
            logger.info(f"🤖 [VECTO BOT] Connected to Discord as {bot.user} (ID: {bot.user.id})")
            logger.info(f"=======================================================")

        try:
            logger.info("Starting Discord Bot loop...")
            bot.run(bot_token, log_handler=None) # We handle logging via Django
        except Exception as e:
            logger.error(f"Failed to start Discord Bot: {e}", exc_info=True)