#!/usr/bin/env python3
"""
Discord Integration for Test Runner
Provides non-blocking Discord notifications during experiment execution using Webhooks.
"""

import json
import os
from datetime import datetime
from typing import Optional, Dict, Any
import requests

class DiscordNotifier:
    """Simple Discord notifier using webhooks for non-blocking notifications."""
    
    def __init__(self, webhook_url: Optional[str] = None, config_file: str = "discord_config.json"):
        self.webhook_url = webhook_url
        self.config_file = config_file
        self.enabled = False
        self.session = requests.Session()
        self._load_config()
    
    def _load_config(self):
        """Load Discord configuration from a JSON file."""
        if not os.path.exists(self.config_file):
            print("⚠️ Discord config not found. Notifications disabled.")
            return
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
            if config.get("webhook_url"):
                self.webhook_url = config["webhook_url"]
                self.enabled = True
                print("✅ Discord notifications enabled via webhook")
            else:
                print("⚠️ 'webhook_url' not found in config. Notifications disabled.")
        except Exception as e:
            print(f"⚠️ Error loading Discord config: {e}")
    
    def _send_webhook(self, content: str = "", embeds: Optional[list] = None):
        """Send a webhook message to Discord."""
        if not self.enabled or not self.webhook_url: return
        try:
            payload: Dict[str, Any] = {"content": content}
            if embeds: payload["embeds"] = embeds
            
            response = self.session.post(self.webhook_url, json=payload, timeout=10)
            if response.status_code >= 300:
                print(f"⚠️ Discord webhook error: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"⚠️ Error sending Discord notification: {e}")
    
    # *** NEW GENERIC STAGE NOTIFIER ***
    def notify_stage(self, baseline: str, workflow: str, stage_name: str, message: str):
        """Notify about a generic stage in the experiment."""
        if not self.enabled: return
        
        embed = {
            "title": f"🔄 Stage Update: {stage_name}",
            "description": f"**Baseline:** `{baseline}`\n**Workflow:** `{workflow}`",
            "color": 0x5865F2,  # Discord Blurple
            "timestamp": datetime.now().isoformat(),
            "fields": [{"name": "Details", "value": message, "inline": False}]
        }
        self._send_webhook(embeds=[embed])

    # --- Specific Notification Methods (Unchanged) ---
    def notify_experiment_start(self, baseline: str, workflow: str):
        embed = {"title": "🧪 Experiment Started", "description": f"**Baseline:** `{baseline}`\n**Workflow:** `{workflow}`", "color": 0x2ECC71, "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])

    def notify_deployment_complete(self, baseline: str, workflow: str):
        embed = {"title": "🚀 Deployment Complete", "description": f"**Baseline:** `{baseline}`\n**Workflow:** `{workflow}`", "color": 0x3498DB, "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])

    def notify_emulator_started(self, baseline: str, workflow: str):
        embed = {"title": "🌀 Emulator Started", "description": f"**Baseline:** `{baseline}`\n**Workflow:** `{workflow}`", "color": 0xE67E22, "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])

    def notify_loadgen_started(self, baseline: str, workflow: str):
        embed = {"title": "⚡ Loadgen Started", "description": f"**Baseline:** `{baseline}`\n**Workflow:** `{workflow}`", "color": 0x9B59B6, "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])

    def notify_experiment_complete(self, baseline: str, workflow: str, duration: float):
        embed = {"title": "✅ Experiment Complete", "description": f"**Baseline:** `{baseline}`\n**Workflow:** `{workflow}`", "color": 0x2ECC71, "fields": [{"name": "Duration", "value": f"{duration:.2f}s", "inline": True}], "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])

    def notify_success_rate(self, baseline: str, workflow: str, success_rate: float):
        color = 0x2ECC71 if success_rate >= 90 else 0xF1C40F if success_rate >= 70 else 0xE74C3C
        emoji = "🟢" if success_rate >= 90 else "🟡" if success_rate >= 70 else "🔴"
        embed = {"title": "📊 Success Rate Evaluated", "description": f"**Baseline:** `{baseline}`\n**Workflow:** `{workflow}`", "color": color, "fields": [{"name": "Success Rate", "value": f"{emoji} {success_rate:.2f}%"}], "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])
    
    def notify_experiment_summary(self, baseline: str, workflow: str, success_rate: float = None, total_cost: float = None):
        color = 0x2ECC71 if (success_rate and success_rate >= 90) else 0xF1C40F if (success_rate and success_rate >= 70) else 0xE74C3C
        fields = []
        if success_rate is not None:
            emoji = "🟢" if success_rate >= 90 else "🟡" if success_rate >= 70 else "🔴"
            fields.append({"name": "Success Rate", "value": f"{emoji} {success_rate:.2f}%", "inline": True})
        else:
            fields.append({"name": "Success Rate", "value": "❓ N/A", "inline": True})
        if total_cost is not None:
            fields.append({"name": "Total Cost", "value": f"💰 ${total_cost:.6f}", "inline": True})
        else:
            fields.append({"name": "Total Cost", "value": "❓ N/A", "inline": True})
        embed = {"title": "📊 Experiment Summary", "description": f"**Baseline:** `{baseline}`\n**Workflow:** `{workflow}`", "color": color, "fields": fields, "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])

    def notify_termination(self, termination_type: str, reason: str, current_experiment: Optional[str] = None):
        desc = f"**Type:** {termination_type}\n**Reason:** {reason}"
        if current_experiment: desc += f"\n**Last Experiment:** `{current_experiment}`"
        embed = {"title": "🛑 Test Runner Terminated", "description": desc, "color": 0xC0392B, "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])

    def notify_error(self, baseline: str, workflow: str, error_msg: str):
        embed = {"title": "❌ Experiment Error", "description": f"**Baseline:** `{baseline}`\n**Workflow:** `{workflow}`", "color": 0xE74C3C, "fields": [{"name": "Error", "value": error_msg[:1024]}], "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])

    def notify_all_complete(self, total_experiments: int, total_duration: float):
        embed = {"title": "🎉 All Experiments Complete!", "description": f"Finished all **{total_experiments}** experiments.", "color": 0x2ECC71, "fields": [{"name": "Total Duration", "value": f"{total_duration/3600:.2f} hours"}], "timestamp": datetime.now().isoformat()}
        self._send_webhook(embeds=[embed])
    
    def notify_progress(self, current: int, total: int, baseline: str, workflow: str):
        progress = (current / total) * 100
        embed = {"title": f"📈 Progress: {progress:.1f}%", "description": f"Running experiment **{current}** of **{total}**\n**Current:** `{baseline}_{workflow}`", "color": 0x3498DB, "timestamp": datetime.now().isoformat()}
        if current % 5 == 0 or current == 1 or current == total:
             self._send_webhook(embeds=[embed])

# --- Global Notifier Instance ---
discord_notifier = None
def init_discord_notifications(config_file: str = "discord_config.json"):
    global discord_notifier
    discord_notifier = DiscordNotifier(config_file=config_file)
    return discord_notifier

def get_discord_notifier():
    return discord_notifier