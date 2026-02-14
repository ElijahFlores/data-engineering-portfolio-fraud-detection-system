"""
Real-Time Alert System for Fraud Detection
FIXED: Added memory management with LRU cache and TTL
"""

import redis
import json
import time
from datetime import datetime, timedelta
from collections import OrderedDict
import requests
from typing import Dict, List
import os

class FraudAlertSystem:
    def __init__(self, slack_webhook_url=None, max_cache_size=10000):
        """
        Initialize alert system
        FIXED: Added max_cache_size to prevent memory leak
        
        Args:
            slack_webhook_url: Your Slack webhook URL
            max_cache_size: Maximum number of alerted transactions to track
        """
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_client = redis.Redis(
            host=redis_host,
            port=6379,
            decode_responses=True
        )
        
        self.slack_webhook_url = slack_webhook_url
        
        # FIXED: Use OrderedDict with size limit for LRU behavior
        self.alerted_transactions = OrderedDict()
        self.max_cache_size = max_cache_size
        
        # Track alert counts
        self.alert_counts = {
            'total': 0,
            'critical': 0,
            'high': 0,
            'medium': 0,
            'low': 0
        }
        
        # Alert thresholds
        self.CRITICAL_SCORE = 70
        self.HIGH_SCORE = 50
        self.MEDIUM_SCORE = 30
        
    def _add_to_cache(self, txn_id: str):
        """
        Add transaction to alerted cache with LRU eviction
        FIXED: Prevents unbounded memory growth
        """
        if txn_id in self.alerted_transactions:
            # Move to end (most recently used)
            self.alerted_transactions.move_to_end(txn_id)
        else:
            # Add new entry
            self.alerted_transactions[txn_id] = datetime.now()
            
            # Evict oldest if cache is full
            if len(self.alerted_transactions) > self.max_cache_size:
                oldest = next(iter(self.alerted_transactions))
                del self.alerted_transactions[oldest]
    
    def _cleanup_old_alerts(self, max_age_hours=24):
        """
        Remove alerts older than max_age_hours
        FIXED: Additional memory management
        """
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        to_remove = [
            txn_id for txn_id, timestamp in self.alerted_transactions.items()
            if timestamp < cutoff_time
        ]
        for txn_id in to_remove:
            del self.alerted_transactions[txn_id]
        
        if to_remove:
            print(f"🧹 Cleaned up {len(to_remove)} old alerts from cache")
    
    def get_new_frauds(self) -> List[Dict]:
        """Poll Redis for new flagged transactions"""
        try:
            # Periodically cleanup old alerts
            if self.alert_counts['total'] % 100 == 0:
                self._cleanup_old_alerts()
            
            # Get top frauds from sorted set
            fraud_ids = self.redis_client.zrevrange(
                'fraud:leaderboard',
                0, 50,
                withscores=True
            )
            
            new_frauds = []
            for txn_id, score in fraud_ids:
                if txn_id not in self.alerted_transactions:
                    # Get full transaction details
                    fraud_data = self.redis_client.get(f"fraud:{txn_id}")
                    if fraud_data:
                        fraud = json.loads(fraud_data)
                        fraud['transaction_id'] = txn_id
                        fraud['fraud_score'] = score
                        new_frauds.append(fraud)
                        
                        # FIXED: Add to cache with LRU
                        self._add_to_cache(txn_id)
            
            return new_frauds
            
        except Exception as e:
            print(f"❌ Error fetching frauds from Redis: {e}")
            return []
    
    def get_severity(self, score: float) -> tuple:
        """Determine alert severity based on fraud score"""
        if score >= self.CRITICAL_SCORE:
            return "🔴 CRITICAL", "#FF0000"
        elif score >= self.HIGH_SCORE:
            return "🟠 HIGH", "#FF8C00"
        elif score >= self.MEDIUM_SCORE:
            return "🟡 MEDIUM", "#FFD700"
        else:
            return "🟢 LOW", "#32CD32"
    
    def format_slack_message(self, fraud: Dict) -> Dict:
        """Format fraud alert for Slack"""
        severity, color = self.get_severity(fraud['fraud_score'])
        
        reasons_text = "\n".join([f"• {reason}" for reason in fraud['reasons']])
        
        message = {
            "attachments": [
                {
                    "color": color,
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f"{severity} Fraudulent Transaction Detected"
                            }
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Transaction ID:*\n`{fraud['transaction_id']}`"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Fraud Score:*\n{fraud['fraud_score']:.0f}/100"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*User ID:*\n{fraud['user_id']}"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Amount:*\n${fraud['amount']:,.2f}"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Timestamp:*\n{fraud['timestamp']}"
                                }
                            ]
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Fraud Indicators:*\n{reasons_text}"
                            }
                        },
                        {
                            "type": "divider"
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"🤖 Automated detection | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        
        return message
    
    def send_slack_alert(self, fraud: Dict):
        """Send alert to Slack"""
        severity, _ = self.get_severity(fraud['fraud_score'])
        
        # Update alert counts
        self.alert_counts['total'] += 1
        if fraud['fraud_score'] >= self.CRITICAL_SCORE:
            self.alert_counts['critical'] += 1
        elif fraud['fraud_score'] >= self.HIGH_SCORE:
            self.alert_counts['high'] += 1
        elif fraud['fraud_score'] >= self.MEDIUM_SCORE:
            self.alert_counts['medium'] += 1
        else:
            self.alert_counts['low'] += 1
        
        if not self.slack_webhook_url:
            # If no webhook, print to console
            self.print_console_alert(fraud)
            return
        
        try:
            message = self.format_slack_message(fraud)
            response = requests.post(
                self.slack_webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'},
                timeout=5
            )
            
            if response.status_code == 200:
                print(f"✅ {severity} alert sent for {fraud['transaction_id']}")
            else:
                print(f"❌ Failed to send Slack alert: {response.text}")
                # Fallback to console
                self.print_console_alert(fraud)
                
        except Exception as e:
            print(f"❌ Error sending Slack alert: {e}")
            # Fallback to console
            self.print_console_alert(fraud)
    
    def print_console_alert(self, fraud: Dict):
        """Print alert to console (fallback when no Slack webhook)"""
        severity, _ = self.get_severity(fraud['fraud_score'])
        
        print("\n" + "="*80)
        print(f"{severity} FRAUD ALERT")
        print("="*80)
        print(f"Transaction ID: {fraud['transaction_id']}")
        print(f"User ID:        {fraud['user_id']}")
        print(f"Amount:         ${fraud['amount']:,.2f}")
        print(f"Fraud Score:    {fraud['fraud_score']:.0f}/100")
        print(f"Timestamp:      {fraud['timestamp']}")
        print(f"\nFraud Indicators:")
        # FIXED: Safe iteration
        reasons = fraud.get('reasons') or []
        for reason in reasons:
            print(f"  • {reason}")
        print("="*80 + "\n")
    
    def print_statistics(self):
        """Print alert statistics"""
        print("\n" + "="*80)
        print("📊 ALERT STATISTICS")
        print("="*80)
        print(f"Total Alerts:    {self.alert_counts['total']}")
        print(f"  🔴 Critical:   {self.alert_counts['critical']}")
        print(f"  🟠 High:       {self.alert_counts['high']}")
        print(f"  🟡 Medium:     {self.alert_counts['medium']}")
        print(f"  🟢 Low:        {self.alert_counts['low']}")
        print(f"Cache Size:      {len(self.alerted_transactions)} / {self.max_cache_size}")
        print("="*80)
    
    def monitor_and_alert(self, check_interval: int = 5):
        """
        Continuously monitor for new frauds and send alerts
        
        Args:
            check_interval: How often to check for new frauds (seconds)
        """
        print("="*80)
        print("🚨 Fraud Alert System Started")
        print("="*80)
        print(f"📊 Monitoring Redis every {check_interval} seconds")
        print(f"💾 Max cache size: {self.max_cache_size} transactions")
        
        if self.slack_webhook_url:
            print("✅ Slack alerts enabled")
        else:
            print("⚠️  No Slack webhook - alerts will print to console")
            print("💡 To enable Slack: https://api.slack.com/messaging/webhooks")
        
        print("="*80 + "\n")
        
        try:
            stats_counter = 0
            while True:
                new_frauds = self.get_new_frauds()
                
                for fraud in new_frauds:
                    self.send_slack_alert(fraud)
                
                # Print statistics every 10 iterations
                stats_counter += 1
                if stats_counter >= 10:
                    self.print_statistics()
                    stats_counter = 0
                
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            print("\n" + "="*80)
            print("🛑 Alert system stopped")
            self.print_statistics()
            print("="*80)

# Usage
if __name__ == "__main__":
    # Get Slack webhook from environment or use None for console-only
    SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK_URL')
    
    if SLACK_WEBHOOK:
        print(f"✅ Slack webhook configured")
    else:
        print("💡 Set SLACK_WEBHOOK_URL environment variable to enable Slack alerts")
        print("💡 Get webhook from: https://api.slack.com/messaging/webhooks")
    
    alert_system = FraudAlertSystem(slack_webhook_url=SLACK_WEBHOOK)
    alert_system.monitor_and_alert(check_interval=5)
