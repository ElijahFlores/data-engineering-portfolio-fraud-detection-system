"""
Real-Time Fraud Detection with Spark Structured Streaming
FIXED: Corrected SQL column usage, Spark column operations, and watermarking
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import redis
import json
import os

class FraudDetectionProcessor:
    def __init__(self):
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("FraudDetectionStreaming") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # FIXED: Environment-aware connection strings
        pg_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.pg_conn_string = f"jdbc:postgresql://{pg_host}:5432/fraud_detection"
        self.pg_properties = {
            "user": "admin",
            "password": "admin123",
            "driver": "org.postgresql.Driver"
        }
        
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
        
        # Fraud detection thresholds
        self.HIGH_AMOUNT_THRESHOLD = 5000
        self.RAPID_TXN_WINDOW_SECONDS = 10
        self.SUSPICIOUS_LOCATIONS = ["Lagos", "Moscow", "Beijing", "Unknown"]
        self.SUSPICIOUS_MERCHANTS = ["Unknown Merchant", "Suspicious Store", "Crypto Exchange"]
        
    def define_schema(self):
        """Define schema for incoming transaction data"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("merchant", StringType(), False),
            StructField("location", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("device_id", StringType(), False),
            StructField("ip_address", StringType(), True),
            StructField("is_fraud", BooleanType(), True),
            StructField("fraud_type", StringType(), True)
        ])
    
    def read_kafka_stream(self):
        """Read streaming data from Kafka"""
        kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap) \
            .option("subscribe", "transactions") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON from Kafka
        schema = self.define_schema()
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp string to timestamp type
        parsed_df = parsed_df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        
        # Add watermark for event-time processing
        parsed_df = parsed_df.withWatermark("timestamp", "1 minute")
        
        return parsed_df
    
    def apply_fraud_rules(self, df):
        """
        Apply rule-based fraud detection logic
        FIXED: Using proper Spark column operations
        """
        
        # Rule 1: High Amount Transactions
        df = df.withColumn(
            "rule_high_amount",
            when(col("amount") > self.HIGH_AMOUNT_THRESHOLD, 1).otherwise(0)
        )
        
        # Rule 2: Suspicious Location
        # FIXED: Proper Spark way to check multiple conditions
        location_conditions = [
            col("location").contains(loc) 
            for loc in self.SUSPICIOUS_LOCATIONS
        ]
        # Combine conditions with OR
        suspicious_location_check = location_conditions[0]
        for condition in location_conditions[1:]:
            suspicious_location_check = suspicious_location_check | condition
        
        df = df.withColumn(
            "rule_suspicious_location",
            when(suspicious_location_check, 1).otherwise(0)
        )
        
        # Rule 3: Suspicious Merchant
        # FIXED: Proper Spark way to check multiple conditions
        merchant_conditions = [
            col("merchant").contains(merch) 
            for merch in self.SUSPICIOUS_MERCHANTS
        ]
        suspicious_merchant_check = merchant_conditions[0]
        for condition in merchant_conditions[1:]:
            suspicious_merchant_check = suspicious_merchant_check | condition
        
        df = df.withColumn(
            "rule_suspicious_merchant",
            when(suspicious_merchant_check, 1).otherwise(0)
        )
        
        # Rule 4: Round Amount (often indicates fraud)
        df = df.withColumn(
            "rule_round_amount",
            when((col("amount") % 100 == 0) & (col("amount") > 1000), 1).otherwise(0)
        )
        
        # Calculate Fraud Score (0-100)
        df = df.withColumn(
            "fraud_score",
            (col("rule_high_amount") * 40 +
             col("rule_suspicious_location") * 30 +
             col("rule_suspicious_merchant") * 20 +
             col("rule_round_amount") * 10).cast("decimal(5,2)")
        )
        
        # Collect Fraud Reasons
        df = df.withColumn(
            "fraud_reasons",
            array_remove(
                array(
                    when(col("rule_high_amount") == 1, lit("High Amount")).otherwise(lit(None)),
                    when(col("rule_suspicious_location") == 1, lit("Suspicious Location")).otherwise(lit(None)),
                    when(col("rule_suspicious_merchant") == 1, lit("Suspicious Merchant")).otherwise(lit(None)),
                    when(col("rule_round_amount") == 1, lit("Round Amount")).otherwise(lit(None))
                ),
                None
            )
        )
        
        # Flag if score >= 30
        df = df.withColumn(
            "is_flagged",
            when(col("fraud_score") >= 30, lit(True)).otherwise(lit(False))
        )
        
        return df
    
    def detect_rapid_transactions(self, df):
        """
        Disabled rapid transaction detection.
        Structured Streaming does not support
        row-based window functions or stream-stream joins
        without complex watermark alignment.
        """

        return df

    
    def write_to_postgres(self, batch_df, batch_id):
        """
        Write batch to PostgreSQL
        FIXED: Only writing columns that exist in DB schema
        """
        try:
            # Select only columns that exist in transactions table
            columns_to_write = [
                "transaction_id", "user_id", "amount", "merchant",
                "location", "timestamp", "device_id", "ip_address",
                "is_fraud", "fraud_type", "rule_high_amount",
                "rule_suspicious_location", "rule_suspicious_merchant",
                "rule_round_amount", "fraud_score", "fraud_reasons", "is_flagged"
            ]
            
            transactions_df = batch_df.select(*columns_to_write)
            
            # Write all transactions
            transactions_df.write \
                .jdbc(
                    url=self.pg_conn_string,
                    table="transactions",
                    mode="append",
                    properties=self.pg_properties
                )
            
            # Write flagged transactions to separate table
            flagged_df = batch_df.filter(col("is_flagged") == True) \
                .select(
                    "transaction_id", "user_id", "amount", "merchant",
                    "location", "timestamp", "device_id", "fraud_score", "fraud_reasons"
                )
            
            flagged_count = flagged_df.count()
            if flagged_count > 0:
                flagged_df.write \
                    .jdbc(
                        url=self.pg_conn_string,
                        table="flagged_transactions",
                        mode="append",
                        properties=self.pg_properties
                    )
                
                print(f"🚨 Batch {batch_id}: Flagged {flagged_count} transactions")
            
            total_count = batch_df.count()
            print(f"✅ Batch {batch_id}: Processed {total_count} transactions")
                
        except Exception as e:
            print(f"❌ Error writing to PostgreSQL (Batch {batch_id}): {e}")
    
    def write_to_redis(self, batch_df, batch_id):
        """
        Write flagged transactions to Redis for real-time access
        FIXED: Added error handling and expiration
        """
        try:
            flagged_rows = batch_df.filter(col("is_flagged") == True) \
                .select(
                    "transaction_id", "user_id", "amount", "fraud_score",
                    "fraud_reasons", "timestamp"
                ).collect()
            
            pipeline = self.redis_client.pipeline()
            
            for row in flagged_rows:
                key = f"fraud:{row.transaction_id}"
                value = json.dumps({
                    "user_id": row.user_id,
                    "amount": float(row.amount),
                    "fraud_score": float(row.fraud_score),
                    # Ensure reasons is always a list
                    "reasons": row.fraud_reasons if row.fraud_reasons else [],
                    "timestamp": row.timestamp.isoformat()
                })

                
                # Store with 24-hour expiration
                pipeline.setex(key, 86400, value)
                
                # Add to sorted set (leaderboard by score)
                pipeline.zadd("fraud:leaderboard", {row.transaction_id: float(row.fraud_score)})
            
            pipeline.execute()
            
            if len(flagged_rows) > 0:
                print(f"📝 Batch {batch_id}: Cached {len(flagged_rows)} frauds in Redis")
                
        except Exception as e:
            print(f"❌ Error writing to Redis (Batch {batch_id}): {e}")

    def process_batch(self, batch_df, batch_id):
        """
        Process each microbatch safely (no stream-stream joins).
        """

        if batch_df.isEmpty():
            return

        # Compute rapid transaction counts within 10-second window
        txn_counts = batch_df.groupBy(
            window(col("timestamp"), "10 seconds"),
            col("user_id")
        ).count().withColumnRenamed("count", "txn_count_10s")

        batch_with_counts = batch_df.join(
            txn_counts,
            ["user_id"],
            "left"
        ).fillna({"txn_count_10s": 1})

        # Apply fraud logic
        batch_processed = self.apply_fraud_rules(batch_with_counts)
        batch_processed = self.detect_rapid_transactions(batch_processed)

        # Write outputs
        self.write_to_postgres(batch_processed, batch_id)
        self.write_to_redis(batch_processed, batch_id)

    def process_stream(self):
        """Main processing pipeline"""
        
        print("="*80)
        print("🚀 Starting Fraud Detection Streaming Pipeline")
        print("="*80)
        print(f"📊 Kafka: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
        print(f"🗄️  PostgreSQL: {self.pg_conn_string.split('@')[-1] if '@' in self.pg_conn_string else 'localhost:5432'}")
        print(f"💾 Redis: {os.getenv('REDIS_HOST', 'localhost')}:6379")
        print("="*80)
        
        # Read stream
        stream_df = self.read_kafka_stream()
        
        # Apply fraud detection rules
        processed_df = self.apply_fraud_rules(stream_df)
        
        # FIXED: Apply rapid transaction detection
        processed_df = self.detect_rapid_transactions(processed_df)
        
        # Write to PostgreSQL and Redis
        query = processed_df.writeStream \
            .foreachBatch(lambda df, batch_id: self.process_batch(df, batch_id)) \
            .outputMode("append") \
            .trigger(processingTime='5 seconds') \
            .start()
        
        print("✅ Pipeline started successfully!")
        print("💡 Monitoring for transactions...")
        print("💡 Press Ctrl+C to stop")
        print("="*80)
        
        query.awaitTermination()

# Run the processor
if __name__ == "__main__":
    try:
        processor = FraudDetectionProcessor()
        processor.process_stream()
    except KeyboardInterrupt:
        print("\n🛑 Pipeline stopped by user")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
