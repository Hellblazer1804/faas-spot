#!/bin/bash

echo "🤖 FaaS-on-Spot Discord Bot Setup"
echo "=================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.7+ first."
    exit 1
fi

# Check if pip is available
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 is not installed. Please install pip3 first."
    exit 1
fi

echo "✅ Python 3 and pip3 are available"

# Install dependencies
echo "📦 Installing Discord bot dependencies..."
pip3 install -r requirements_discord.txt

if [ $? -ne 0 ]; then
    echo "❌ Failed to install dependencies. Please check the requirements file."
    exit 1
fi

echo "✅ Dependencies installed successfully"

# Create config file if it doesn't exist
if [ ! -f "discord_config.json" ]; then
    echo "📝 Creating Discord configuration file..."
    python3 discord_notifier.py
    echo ""
    echo "🔧 Please edit discord_config.json with your Discord credentials:"
    echo "   - For webhook: Add your webhook URL"
    echo "   - For bot token: Add your bot token and channel ID"
    echo ""
    echo "📖 See README_Discord_Bot.md for detailed setup instructions"
else
    echo "✅ Configuration file already exists"
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit discord_config.json with your Discord credentials"
echo "2. Test the bot: python3 discord_notifier.py"
echo "3. Run experiments with notifications: cd .. && python3 test_runner.py"
echo ""
echo "📖 For detailed instructions, see README_Discord_Bot.md"
