"""
Discord Bot Package for FaaS-on-Spot Experiment Notifications
"""

from .discord_integration import DiscordNotifier, init_discord_notifications, get_discord_notifier

__all__ = [
    'DiscordNotifier',
    'init_discord_notifications', 
    'get_discord_notifier'
]

__version__ = "1.0.0"
