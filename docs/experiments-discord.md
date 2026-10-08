# Discord Bot Integration

The Discord bot for experiment notifications is now organized in a dedicated `discord/` folder for better code organization.

## Quick Start

### 1. Setup Discord Bot
```bash
# From the Experiments directory
./setup_discord.sh
```

### 2. Run Experiments with Notifications
```bash
# Discord notifications are automatically integrated
python3 test_runner.py
```

### 3. Run Discord Bot Standalone (Optional)
```bash
python3 run_discord_bot.py
```

## Folder Structure

```
Experiments/
├── discord/                    # Discord bot package
│   ├── __init__.py           # Package initialization
│   ├── discord_notifier.py   # Full Discord bot
│   ├── discord_integration.py # Integration module
│   ├── requirements_discord.txt # Dependencies
│   ├── setup_discord_bot.sh  # Discord setup script
│   └── README_Discord_Bot.md # Detailed documentation
├── test_runner.py             # Main experiment runner (with Discord integration)
├── run_discord_bot.py         # Discord bot launcher
├── setup_discord.sh           # Main setup script
└── README_Discord.md          # This file
```

## How It Works

1. **Integration**: The `test_runner.py` imports from the `discord` package
2. **Notifications**: Automatically sends Discord notifications at each experiment stage
3. **Success Rate Evaluation**: Calls `success_rate.py --use-paths` after each baseline completes
4. **Performance Insights**: Color-coded success rate notifications (🟢≥90%, 🟡≥70%, 🟠<70%)
5. **Termination Alerts**: Notifications when experiments are interrupted or terminated
6. **Non-blocking**: Uses webhooks for fast, non-blocking notifications
7. **Self-contained**: All Discord functionality is in one organized folder

## For More Details

See `discord/README_Discord_Bot.md` for comprehensive setup and usage instructions.
