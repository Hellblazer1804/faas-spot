# Discord Bot for FaaS-on-Spot Experiment Notifications

This Discord bot provides real-time notifications during FaaS-on-Spot experiment execution, allowing you to monitor progress from anywhere without constantly checking the terminal.

## Features

- 🧪 **Experiment Start/Completion**: Notifications when each baseline+workflow combination starts and finishes
- 🚀 **Deployment Status**: Updates when workflows are deployed successfully
- 🌀 **Emulator Status**: Notifications when the emulator starts running
- 📊 **Load Generator Status**: Updates when load testing begins
- 📈 **Progress Tracking**: Periodic progress updates (every 5 experiments)
- 📊 **Success Rate Evaluation**: Results after each baseline completes (with color-coded performance)
- ❌ **Error Reporting**: Immediate notifications for any failures or interruptions
- 🛑 **Termination Notifications**: Alerts when experiments are interrupted or terminated
- 🎉 **Completion Summary**: Final summary when all experiments are done

## Setup Instructions

### 1. Create a Discord Bot

1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Click "New Application" and give it a name (e.g., "FaaS-on-Spot Monitor")
3. Go to the "Bot" section and click "Add Bot"
4. Copy the bot token (you'll need this later)

### 2. Set Up Webhook (Recommended)

**Option A: Webhook (Non-blocking, recommended)**
1. In your Discord server, go to the channel where you want notifications
2. Right-click the channel → "Edit Channel" → "Integrations" → "Webhooks"
3. Click "New Webhook" and give it a name
4. Copy the webhook URL

**Option B: Bot Token (Full bot functionality)**
1. Use the bot token from step 1
2. Invite the bot to your server using the OAuth2 URL generator
3. Select "bot" scope and "Send Messages" permission

### 3. Install Dependencies

```bash
cd Faas-on-Spot/Experiments/discord
pip3 install -r requirements_discord.txt
```

### 4. Configure the Bot

Run the bot once to create a configuration file:

```bash
python3 discord_notifier.py
```

This will create `discord_config.json`. Edit it with your credentials:

**For Webhook (Recommended):**
```json
{
  "webhook_url": "https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN",
  "enable_notifications": true
}
```

**For Bot Token:**
```json
{
  "discord_token": "YOUR_BOT_TOKEN_HERE",
  "channel_id": 1234567890123456789,
  "enable_notifications": true
}
```

**Note:** For the bot token method, you need to get the channel ID by:
1. Enabling Developer Mode in Discord (User Settings → Advanced → Developer Mode)
2. Right-clicking the channel → "Copy ID"

## Usage

### Method 1: Integrated with Test Runner (Recommended)

The Discord notifications are automatically integrated into `test_runner.py`. Simply run your experiments normally:

```bash
cd Faas-on-Spot/Experiments
python3 test_runner.py
```

You'll receive notifications for:
- Each experiment start
- Deployment completion
- Emulator start
- Load generator start
- Experiment completion
- Success rate evaluation results (color-coded by performance)
- Progress updates every 5 experiments
- Termination alerts (interrupts, errors, system signals)
- Final completion summary

### Method 2: Standalone Bot

Run the Discord bot separately for full bot functionality:

```bash
python3 discord_notifier.py
```

**Available Commands:**
- `!status` - Show bot status and connection info
- `!help` - Display available commands

### Method 3: Manual Notifications

Import and use the notification functions in your own scripts:

```python
from discord_integration import DiscordNotifier

notifier = DiscordNotifier(webhook_url="YOUR_WEBHOOK_URL")
notifier.notify_experiment_start("static", "wf-1")
notifier.notify_experiment_complete("static", "wf-1", 45.2)
```

## Configuration Options

### Environment Variables

You can also set configuration via environment variables:

```bash
export DISCORD_WEBHOOK_URL="your_webhook_url"
export DISCORD_ENABLED="true"
```

### Notification Frequency

Progress notifications are sent every 5 experiments to avoid spam. You can modify this in `discord_integration.py`:

```python
# Only send progress updates every 5 experiments to avoid spam
if current % 5 == 0 or current == total:
    # ... send notification
```

## Troubleshooting

### Common Issues

1. **"Discord config not found"**
   - Run `python3 discord_notifier.py` first to create the config file
   - Ensure `discord_config.json` exists in the current directory

2. **"Discord webhook error: 404"**
   - Check that your webhook URL is correct
   - Ensure the webhook hasn't been deleted from Discord

3. **"Could not find channel with ID"**
   - Verify the channel ID is correct
   - Ensure the bot has access to the channel
   - Check that the bot has been invited to your server

4. **"Error sending Discord notification"**
   - Check your internet connection
   - Verify the webhook URL is valid
   - Check Discord's status page for any service issues

### Debug Mode

Enable debug logging by modifying the webhook function in `discord_integration.py`:

```python
def _send_webhook(self, content: str, embeds: Optional[list] = None):
    print(f"🔍 Debug: Attempting to send Discord notification")
    print(f"🔍 Debug: Content: {content}")
    # ... rest of the function
```

## Security Considerations

- **Never commit your Discord token or webhook URL** to version control
- **Use webhooks** for simple notifications (they're easier to revoke)
- **Limit bot permissions** to only what's necessary (Send Messages)
- **Rotate tokens** periodically for production use

## Customization

### Adding New Notification Types

To add new notification types, extend the `DiscordNotifier` class:

```python
def notify_custom_event(self, baseline: str, workflow: str, custom_data: str):
    """Notify about a custom event."""
    embed = {
        "title": "🔧 Custom Event",
        "description": f"**Baseline:** {baseline}\n**Workflow:** {workflow}",
        "color": 0x00ffff,
        "fields": [
            {"name": "Custom Data", "value": custom_data, "inline": True}
        ]
    }
    
    self._send_webhook(f"🔧 **{baseline} | {workflow}** custom event", [embed])
```

### Modifying Embed Colors

Each notification type uses different colors:
- 🟢 Green (0x00ff00): Success, completion
- 🔵 Blue (0x0099ff): Information, progress
- 🟠 Orange (0xff9900): Warnings, ongoing processes
- 🟣 Purple (0x9932cc): Load generation
- 🔴 Red (0xff0000): Errors, failures

### Adding Rich Embeds

Enhance notifications with rich Discord embeds:

```python
embed = {
    "title": "Custom Title",
    "description": "Detailed description",
    "color": 0x00ff00,
    "fields": [
        {"name": "Field 1", "value": "Value 1", "inline": True},
        {"name": "Field 2", "value": "Value 2", "inline": True}
    ],
    "footer": {"text": "FaaS-on-Spot Experiment Monitor"},
    "timestamp": datetime.now().isoformat()
}
```

## Support

If you encounter issues:

1. Check the troubleshooting section above
2. Verify your Discord configuration
3. Check the console output for error messages
4. Ensure all dependencies are installed correctly

## License

This Discord bot is part of the FaaS-on-Spot project and follows the same licensing terms.
