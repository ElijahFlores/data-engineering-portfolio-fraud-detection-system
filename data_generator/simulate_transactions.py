"""
Real-Time Transaction Data Generator for Fraud Detection
Generates realistic transaction patterns including fraudulent behavior
FIXED: Proper rapid transaction generation, environment-aware Kafka connection
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import numpy as np
import os

class TransactionGenerator:
    def __init__(self):
        # User profiles with spending patterns
        self.users = {
            f"USER_{i:04d}": {
                "avg_transaction": np.random.uniform(20, 500),
                "std_transaction": np.random.uniform(10, 100),
                "usual_merchants": random.sample(self.get_merchants(), k=random.randint(3, 8)),
                "home_location": random.choice(self.get_locations()),
                "device_id": f"DEVICE_{random.randint(1000, 9999)}"
            }
            for i in range(500)
        }
        
        self.fraud_probability = 0.05  # 5% fraud rate
        self.last_user_transaction = {}  # Track for rapid transactions
        
    @staticmethod
    def get_merchants():
        return [
            "Amazon", "Walmart", "Target", "Starbucks", "Shell Gas",
            "McDonald's", "Best Buy", "Home Depot", "CVS Pharmacy",
            "Whole Foods", "Nike Store", "Apple Store", "Costco",
            "7-Eleven", "Uber", "Netflix", "Spotify", "Steam Games",
            "Hotels.com", "Delta Airlines", "Unknown Merchant XYZ",
            "Suspicious Store 123", "Overseas Retailer", "Crypto Exchange"
        ]
    
    @staticmethod
    def get_locations():
        return [
            "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
            "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
            "Dallas, TX", "San Jose, CA", "Austin, TX", "Jacksonville, FL",
            "Lagos, Nigeria", "Moscow, Russia", "Beijing, China",
            "Unknown Location", "VPN Location"
        ]
    
    def generate_normal_transaction(self, user_id, user_profile):
        """Generate a normal transaction based on user's spending pattern"""
        amount = max(5, np.random.normal(
            user_profile["avg_transaction"],
            user_profile["std_transaction"]
        ))
        
        return {
            "transaction_id": f"TXN_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            "user_id": user_id,
            "amount": round(amount, 2),
            "merchant": random.choice(user_profile["usual_merchants"]),
            "location": user_profile["home_location"],
            "timestamp": datetime.now().isoformat(),
            "device_id": user_profile["device_id"],
            "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "is_fraud": False,
            "fraud_type": None
        }
    
    def generate_fraudulent_transaction(self, user_id, user_profile):
        """Generate fraudulent transaction with specific patterns"""
        fraud_patterns = [
            "high_amount",
            "unusual_location", 
            "rapid_transactions",
            "new_device",
            "suspicious_merchant"
        ]
        
        fraud_type = random.choice(fraud_patterns)
        base_txn = self.generate_normal_transaction(user_id, user_profile)
        
        # Apply fraud pattern
        if fraud_type == "high_amount":
            base_txn["amount"] = round(random.uniform(5000, 25000), 2)
            
        elif fraud_type == "unusual_location":
            suspicious_locations = ["Lagos, Nigeria", "Moscow, Russia", "Beijing, China", "Unknown Location"]
            base_txn["location"] = random.choice(suspicious_locations)
            
        elif fraud_type == "rapid_transactions":
            # FIXED: Actually generate burst of transactions
            base_txn["amount"] = round(random.uniform(100, 1000), 2)
            # Mark for rapid generation
            base_txn["_generate_burst"] = True
            
        elif fraud_type == "new_device":
            base_txn["device_id"] = f"NEW_DEVICE_{random.randint(1000, 9999)}"
            
        elif fraud_type == "suspicious_merchant":
            base_txn["merchant"] = random.choice([
                "Unknown Merchant XYZ", "Suspicious Store 123", 
                "Overseas Retailer", "Crypto Exchange"
            ])
        
        base_txn["is_fraud"] = True
        base_txn["fraud_type"] = fraud_type
        
        return base_txn
    
    def generate_transaction(self):
        """Generate a single transaction (normal or fraud)"""
        user_id = random.choice(list(self.users.keys()))
        user_profile = self.users[user_id]
        
        if random.random() < self.fraud_probability:
            return self.generate_fraudulent_transaction(user_id, user_profile)
        else:
            return self.generate_normal_transaction(user_id, user_profile)
    
    def generate_batch(self, count=100):
        """Generate a batch of transactions for testing"""
        return [self.generate_transaction() for _ in range(count)]
    
    def stream_to_kafka(self, bootstrap_servers=None, topic='transactions'):
        """
        Stream transactions to Kafka in real-time
        FIXED: Environment-aware Kafka connection
        """
        # FIXED: Auto-detect environment (Docker vs host)
        if bootstrap_servers is None:
            # Check if running in Docker
            if os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
                bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
            else:
                # Default to localhost for host machine
                bootstrap_servers = 'localhost:9092'
        
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            retries=3
        )
        
        print(f"🚀 Starting transaction stream to Kafka")
        print(f"📡 Kafka broker: {bootstrap_servers}")
        print(f"📊 Topic: {topic}")
        print(f"🎯 Fraud rate: {self.fraud_probability * 100}%")
        print("="*80)
        
        transaction_count = 0
        fraud_count = 0
        
        try:
            while True:
                txn = self.generate_transaction()
                
                # FIXED: Handle rapid transaction bursts
                if txn.get('_generate_burst'):
                    # Generate 3-5 rapid transactions from same user
                    burst_count = random.randint(3, 5)
                    print(f"💥 Generating burst of {burst_count} rapid transactions...")
                    
                    for i in range(burst_count):
                        burst_txn = txn.copy()
                        burst_txn['transaction_id'] = f"TXN_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
                        burst_txn['timestamp'] = datetime.now().isoformat()
                        burst_txn.pop('_generate_burst', None)
                        
                        producer.send(topic, value=burst_txn)
                        transaction_count += 1
                        fraud_count += 1
                        
                        # Small delay between burst transactions (< 1 second)
                        time.sleep(random.uniform(0.1, 0.5))
                    
                    print(f"✅ Sent {burst_count} rapid transactions")
                else:
                    # Normal transaction
                    txn.pop('_generate_burst', None)
                    producer.send(topic, value=txn)
                    transaction_count += 1
                    
                    if txn["is_fraud"]:
                        fraud_count += 1
                        print(f"⚠️  FRAUD #{fraud_count}: {txn['fraud_type']} | "
                              f"User: {txn['user_id']} | Amount: ${txn['amount']:.2f}")
                
                if transaction_count % 100 == 0:
                    print(f"✅ Sent {transaction_count} transactions ({fraud_count} fraudulent)")
                
                # Simulate real-time with random delay
                time.sleep(random.uniform(0.3, 1.0))
                
        except KeyboardInterrupt:
            print(f"\n{'='*80}")
            print(f"🛑 Stopped gracefully")
            print(f"📊 Total: {transaction_count} transactions")
            print(f"🚨 Fraudulent: {fraud_count} ({fraud_count/transaction_count*100:.1f}%)")
            print(f"{'='*80}")
            producer.flush()
            producer.close()

# Usage Examples
if __name__ == "__main__":
    generator = TransactionGenerator()
    
    # Option 1: Generate sample batch for testing
    print("Generating 5 sample transactions...\n")
    sample_batch = generator.generate_batch(5)
    for txn in sample_batch:
        print(json.dumps(txn, indent=2))
        print("-" * 40)
    
    print("\n" + "="*80)
    print("To stream to Kafka, uncomment the line below:")
    print("generator.stream_to_kafka()")
    print("="*80)
    
    # Option 2: Stream to Kafka (uncomment when ready)
    generator.stream_to_kafka()
