#!/usr/bin/env python3
"""
Discord Bot for FaaS-on-Spot Experiment Notifications
Sends real-time updates to a Discord channel during experiment execution.
"""

import discord
import asyncio
import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any

class ExperimentNotifier:
    def __init__(self, token: str, channel_id: int):
        self.token = token
        self.channel_id = channel_id
        self.client = discord.Client(intents=discord.Intents.default())
        self.channel: Optional[discord.TextChannel] = None
        
        # Set up event handlers
        self.client.event(self.on_ready)
        self.client.event(self.on_message)
        
    async def on_ready(self):
        """Called when the bot is ready."""
        print(f"🤖 Discord bot logged in as {self.client.user}")
        
        # Get the target channel
        self.channel = self.client.get_channel(self.channel_id)
        if not self.channel:
            print(f"❌ Could not find channel with ID {self.channel_id}")
            return
        
        print(f"✅ Connected to channel: {self.channel.name}")
        
        # Send startup message
        await self.send_message("🚀 **FaaS-on-Spot Experiment Monitor Started**\n"
                              f"Monitoring experiments and sending notifications to this channel.\n"
                              f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    async def on_message(self, message):
        """Handle incoming messages (for bot commands)."""
        if message.author == self.client.user:
            return
            
        if message.content.startswith('!status'):
            await self.send_status(message)
        elif message.content.startswith('!help'):
            await self.send_help(message)
    
    async def send_status(self, message):
        """Send current bot status."""
        status_msg = f"🤖 **Bot Status**\n"
        status_msg += f"• Connected: ✅\n"
        status_msg += f"• Channel: {self.channel.name if self.channel else 'Unknown'}\n"
        status_msg += f"• Uptime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        status_msg += f"• Latency: {round(self.client.latency * 1000)}ms"
        
        await message.channel.send(status_msg)
    
    async def send_help(self, message):
        """Send help information."""
        help_msg = "📚 **Available Commands**\n"
        help_msg += "• `!status` - Show bot status\n"
        help_msg += "• `!help` - Show this help message\n\n"
        help_msg += "The bot automatically sends notifications for:\n"
        help_msg += "• Experiment start/completion\n"
        help_msg += "• Baseline deployment\n"
        help_msg += "• Errors and warnings\n"
        help_msg += "• Final results summary"
        
        await message.channel.send(help_msg)
    
    async def send_message(self, content: str):
        """Send a message to the configured channel."""
        if not self.channel:
            print("❌ No channel available for sending message")
            return
            
        try:
            await self.channel.send(content)
        except Exception as e:
            print(f"❌ Error sending Discord message: {e}")
    
    async def notify_experiment_start(self, baseline: str, workflow: str):
        """Notify when an experiment starts."""
        embed = discord.Embed(
            title="🧪 Experiment Started",
            description=f"**Baseline:** {baseline}\n**Workflow:** {workflow}",
            color=0x00ff00,
            timestamp=datetime.now()
        )
        embed.add_field(name="Status", value="🔄 Running", inline=True)
        embed.add_field(name="Start Time", value=datetime.now().strftime("%H:%M:%S"), inline=True)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    async def notify_deployment_complete(self, baseline: str, workflow: str):
        """Notify when deployment is complete."""
        embed = discord.Embed(
            title="🚀 Deployment Complete",
            description=f"**Baseline:** {baseline}\n**Workflow:** {workflow}",
            color=0x0099ff,
            timestamp=datetime.now()
        )
        embed.add_field(name="Status", value="✅ Deployed", inline=True)
        embed.add_field(name="Next Step", value="Starting emulator...", inline=True)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    async def notify_emulator_started(self, baseline: str, workflow: str):
        """Notify when emulator starts."""
        embed = discord.Embed(
            title="🌀 Emulator Started",
            description=f"**Baseline:** {baseline}\n**Workflow:** {workflow}",
            color=0xff9900,
            timestamp=datetime.now()
        )
        embed.add_field(name="Status", value="🔄 Running", inline=True)
        embed.add_field(name="Next Step", value="Starting load generator...", inline=True)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    async def notify_loadgen_started(self, baseline: str, workflow: str):
        """Notify when load generator starts."""
        embed = discord.Embed(
            title="📊 Load Generator Started",
            description=f"**Baseline:** {baseline}\n**Workflow:** {workflow}",
            color=0x9932cc,
            timestamp=datetime.now()
        )
        embed.add_field(name="Status", value="🔄 Running", inline=True)
        embed.add_field(name="Target", value="250 requests", inline=True)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    async def notify_experiment_complete(self, baseline: str, workflow: str, duration: float):
        """Notify when an experiment completes successfully."""
        embed = discord.Embed(
            title="✅ Experiment Complete",
            description=f"**Baseline:** {baseline}\n**Workflow:** {workflow}",
            color=0x00ff00,
            timestamp=datetime.now()
        )
        embed.add_field(name="Status", value="✅ Completed", inline=True)
        embed.add_field(name="Duration", value=f"{duration:.1f}s", inline=True)
        embed.add_field(name="Next", value="Cleaning up...", inline=True)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    async def notify_error(self, baseline: str, workflow: str, error_msg: str):
        """Notify when an error occurs."""
        embed = discord.Embed(
            title="❌ Experiment Error",
            description=f"**Baseline:** {baseline}\n**Workflow:** {workflow}",
            color=0xff0000,
            timestamp=datetime.now()
        )
        embed.add_field(name="Status", value="❌ Error", inline=True)
        embed.add_field(name="Error", value=error_msg[:100] + "..." if len(error_msg) > 100 else error_msg, inline=False)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    async def notify_all_complete(self, total_experiments: int, total_duration: float):
        """Notify when all experiments are complete."""
        embed = discord.Embed(
            title="🎉 All Experiments Complete!",
            description=f"Successfully completed {total_experiments} experiments",
            color=0x00ff00,
            timestamp=datetime.now()
        )
        embed.add_field(name="Total Experiments", value=str(total_experiments), inline=True)
        embed.add_field(name="Total Duration", value=f"{total_duration/60:.1f} minutes", inline=True)
        embed.add_field(name="Status", value="✅ All Done", inline=True)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    async def notify_success_rate(self, baseline: str, workflow: str, success_rate: float):
        """Notify when success rate evaluation is complete."""
        embed = discord.Embed(
            title="📊 Success Rate Evaluation Complete",
            description=f"**Baseline:** {baseline}\n**Workflow:** {workflow}",
            color=0x00ff00 if success_rate >= 90 else 0xffff00 if success_rate >= 70 else 0xff6600,
            timestamp=datetime.now()
        )
        
        emoji = "🟢" if success_rate >= 90 else "🟡" if success_rate >= 70 else "🟠"
        embed.add_field(name="Success Rate", value=f"{emoji} {success_rate:.2f}%", inline=True)
        embed.add_field(name="Status", value="✅ Evaluated", inline=True)
        embed.add_field(name="Next", value="Cleaning up...", inline=True)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    async def notify_termination(self, termination_type: str, reason: str, current_experiment: Optional[str] = None):
        """Notify when the experiment runner is terminated."""
        embed = discord.Embed(
            title="🛑 Experiment Runner Terminated",
            description=f"**Termination Type:** {termination_type}\n**Reason:** {reason}",
            color=0xff0000,  # Red for termination
            timestamp=datetime.now()
        )
        
        embed.add_field(name="Status", value="🛑 Terminated", inline=True)
        embed.add_field(name="Time", value=datetime.now().strftime("%H:%M:%S"), inline=True)
        
        if current_experiment:
            embed.add_field(name="Current Experiment", value=current_experiment, inline=True)
        
        if self.channel:
            await self.channel.send(embed=embed)
    
    def run(self):
        """Start the Discord bot."""
        try:
            self.client.run(self.token)
        except Exception as e:
            print(f"❌ Error running Discord bot: {e}")

def create_config_file():
    """Create a sample config file if it doesn't exist."""
    config_path = "discord_config.json"
    if not os.path.exists(config_path):
        sample_config = {
            "discord_token": "YOUR_DISCORD_BOT_TOKEN_HERE",
            "channel_id": 1234567890123456789,
            "enable_notifications": True
        }
        
        with open(config_path, 'w') as f:
            json.dump(sample_config, f, indent=2)
        
        print(f"📝 Created sample config file: {config_path}")
        print("Please edit it with your Discord bot token and channel ID")
        return False
    
    return True

def load_config():
    """Load configuration from file."""
    config_path = "discord_config.json"
    
    if not os.path.exists(config_path):
        print("❌ Config file not found. Run the script first to create it.")
        return None
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        if config.get("discord_token") == "YOUR_DISCORD_BOT_TOKEN_HERE":
            print("❌ Please edit discord_config.json with your actual Discord bot token")
            return None
        
        return config
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return None

def main():
    """Main function to run the Discord bot."""
    print("🤖 FaaS-on-Spot Discord Notifier")
    print("=" * 40)
    
    # Create config file if it doesn't exist
    if not create_config_file():
        return
    
    # Load configuration
    config = load_config()
    if not config:
        return
    
    if not config.get("enable_notifications", True):
        print("⚠️ Notifications are disabled in config")
        return
    
    # Create and run the notifier
    notifier = ExperimentNotifier(
        token=config["discord_token"],
        channel_id=config["channel_id"]
    )
    
    print("🚀 Starting Discord bot...")
    notifier.run()

if __name__ == "__main__":
    main()
