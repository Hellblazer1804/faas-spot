#!/usr/bin/env python3
"""
Discord Bot Launcher
Simple launcher script to run the Discord bot from the Experiments directory.
"""

import sys
import os

# Add the discord package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'discord'))

# Import and run the bot
from discord.discord_notifier import main

if __name__ == "__main__":
    main()
