#!/bin/bash

echo "🤖 FaaS-on-Spot Discord Bot Setup"
echo "=================================="

# Check if we're in the right directory
if [ ! -d "discord" ]; then
    echo "❌ Discord folder not found. Please run this from the Experiments directory."
    exit 1
fi

echo "✅ Found Discord bot folder"
echo "🔄 Running Discord bot setup..."

# Run the setup script in the discord folder
cd discord
./setup_discord_bot.sh

echo ""
echo "🎉 Discord bot setup completed!"
echo ""
echo "To run the Discord bot:"
echo "  python3 ../run_discord_bot.py"
echo ""
echo "To run experiments with notifications:"
echo "  python3 test_runner.py"
