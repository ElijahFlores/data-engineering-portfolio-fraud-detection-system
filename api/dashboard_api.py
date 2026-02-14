"""
Real-Time Fraud Detection Dashboard API
FIXED: Corrected SQL queries, INTERVAL syntax, and column names
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
import json
import os

app = FastAPI(
    title="Fraud Detection API",
    description="Real-time fraud detection system API",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# FIXED: Environment-aware configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': 5432,
    'database': 'fraud_detection',
    'user': 'admin',
    'password': 'admin123'
}

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_CLIENT = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

# Pydantic models
class TransactionStats(BaseModel):
    total_transactions: int
    flagged_transactions: int
    fraud_rate: float
    total_amount: float
    flagged_amount: float

class FlaggedTransaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    merchant: str
    location: str
    fraud_score: float
    fraud_reasons: List[str]
    timestamp: str

class UserRiskProfile(BaseModel):
    user_id: str
    total_transactions: int
    flagged_count: int
    total_amount: float
    risk_score: float
    last_transaction: str

class DetectionMetrics(BaseModel):
    true_positives: int
    false_positives: int
    false_negatives: int
    true_negatives: int
    precision: float
    recall: float
    accuracy: float

# Helper functions
def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

@app.get("/")
async def root():
    """API health check"""
    return {
        "status": "healthy",
        "message": "Fraud Detection API is running",
        "timestamp": datetime.now().isoformat(),
        "database": DB_CONFIG['host'],
        "redis": REDIS_HOST
    }

@app.get("/stats/realtime", response_model=TransactionStats)
async def get_realtime_stats(hours: int = Query(1, ge=1, le=24)):
    """
    Get real-time transaction statistics
    FIXED: Proper INTERVAL syntax and is_flagged vs is_fraud
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # FIXED: Use proper PostgreSQL INTERVAL with make_interval
        query = """
            SELECT 
                COUNT(*) as total_transactions,
                SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) as flagged_transactions,
                ROUND(
                    COALESCE(
                        AVG(CASE WHEN is_flagged THEN 1.0 ELSE 0.0 END) * 100, 
                        0
                    ), 
                    2
                ) as fraud_rate,
                COALESCE(SUM(amount), 0) as total_amount,
                COALESCE(SUM(CASE WHEN is_flagged THEN amount ELSE 0 END), 0) as flagged_amount
            FROM transactions
            WHERE timestamp >= NOW() - make_interval(hours => %s)
        """
        
        cursor.execute(query, (hours,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return TransactionStats(
            total_transactions=result['total_transactions'] or 0,
            flagged_transactions=result['flagged_transactions'] or 0,
            fraud_rate=float(result['fraud_rate'] or 0.0),
            total_amount=float(result['total_amount'] or 0),
            flagged_amount=float(result['flagged_amount'] or 0)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/frauds/recent", response_model=List[FlaggedTransaction])
async def get_recent_frauds(limit: int = Query(20, ge=1, le=100)):
    """Get most recent flagged transactions"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                transaction_id,
                user_id,
                amount,
                merchant,
                location,
                fraud_score,
                fraud_reasons,
                timestamp
            FROM flagged_transactions
            ORDER BY flagged_at DESC
            LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        frauds = []
        for row in results:
            frauds.append(FlaggedTransaction(
                transaction_id=row['transaction_id'],
                user_id=row['user_id'],
                amount=float(row['amount']),
                merchant=row['merchant'],
                location=row['location'],
                fraud_score=float(row['fraud_score']),
                fraud_reasons=row['fraud_reasons'] or [],
                timestamp=row['timestamp'].isoformat()
            ))
        
        return frauds
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/frauds/high-risk", response_model=List[FlaggedTransaction])
async def get_high_risk_frauds(min_score: float = Query(70, ge=0, le=100)):
    """Get high-risk flagged transactions"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                transaction_id,
                user_id,
                amount,
                merchant,
                location,
                fraud_score,
                fraud_reasons,
                timestamp
            FROM flagged_transactions
            WHERE fraud_score >= %s
            ORDER BY fraud_score DESC, flagged_at DESC
            LIMIT 50
        """
        
        cursor.execute(query, (min_score,))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        frauds = []
        for row in results:
            frauds.append(FlaggedTransaction(
                transaction_id=row['transaction_id'],
                user_id=row['user_id'],
                amount=float(row['amount']),
                merchant=row['merchant'],
                location=row['location'],
                fraud_score=float(row['fraud_score']),
                fraud_reasons=row['fraud_reasons'] or [],
                timestamp=row['timestamp'].isoformat()
            ))
        
        return frauds
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/risky", response_model=List[UserRiskProfile])
async def get_risky_users(limit: int = Query(10, ge=1, le=50), hours: int = Query(24, ge=1, le=168)):
    """Get users with most flagged transactions"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            WITH user_stats AS (
                SELECT 
                    t.user_id,
                    COUNT(*) as total_transactions,
                    SUM(CASE WHEN t.is_flagged THEN 1 ELSE 0 END) as flagged_count,
                    SUM(t.amount) as total_amount,
                    MAX(t.timestamp) as last_transaction,
                    COALESCE(AVG(CASE WHEN t.is_flagged THEN t.fraud_score ELSE 0 END), 0) as avg_fraud_score
                FROM transactions t
                WHERE t.timestamp >= NOW() - make_interval(hours => %s)
                GROUP BY t.user_id
            )
            SELECT 
                user_id,
                total_transactions,
                flagged_count,
                total_amount,
                ROUND(avg_fraud_score, 2) as risk_score,
                last_transaction
            FROM user_stats
            WHERE flagged_count > 0
            ORDER BY flagged_count DESC, avg_fraud_score DESC
            LIMIT %s
        """
        
        cursor.execute(query, (hours, limit))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        users = []
        for row in results:
            users.append(UserRiskProfile(
                user_id=row['user_id'],
                total_transactions=row['total_transactions'],
                flagged_count=row['flagged_count'],
                total_amount=float(row['total_amount']),
                risk_score=float(row['risk_score']),
                last_transaction=row['last_transaction'].isoformat()
            ))
        
        return users
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/hourly")
async def get_hourly_stats(hours: int = Query(24, ge=1, le=168)):
    """
    Get hourly transaction statistics
    FIXED: Using DATE_TRUNC instead of time_bucket
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour,
                COUNT(*) as transaction_count,
                SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) as fraud_count,
                ROUND(AVG(amount), 2) as avg_amount,
                ROUND(SUM(amount), 2) as total_amount
            FROM transactions
            WHERE timestamp >= NOW() - make_interval(hours => %s)
            GROUP BY DATE_TRUNC('hour', timestamp)
            ORDER BY hour DESC
        """
        
        cursor.execute(query, (hours,))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert datetime to string for JSON serialization
        stats = []
        for row in results:
            stats.append({
                'hour': row['hour'].isoformat(),
                'transaction_count': row['transaction_count'],
                'fraud_count': row['fraud_count'],
                'avg_amount': float(row['avg_amount'] or 0),
                'total_amount': float(row['total_amount'] or 0)
            })
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/merchants/suspicious")
async def get_suspicious_merchants(min_transactions: int = Query(5, ge=1)):
    """Get merchants with highest fraud rates"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                merchant,
                COUNT(*) as total_transactions,
                SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) as fraud_count,
                ROUND(
                    AVG(CASE WHEN is_flagged THEN 1.0 ELSE 0.0 END) * 100, 
                    2
                ) as fraud_rate
            FROM transactions
            GROUP BY merchant
            HAVING COUNT(*) >= %s
            ORDER BY fraud_rate DESC, fraud_count DESC
            LIMIT 15
        """
        
        cursor.execute(query, (min_transactions,))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        merchants = []
        for row in results:
            merchants.append({
                'merchant': row['merchant'],
                'total_transactions': row['total_transactions'],
                'fraud_count': row['fraud_count'],
                'fraud_rate': float(row['fraud_rate'] or 0)
            })
        
        return merchants
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/detection", response_model=DetectionMetrics)
async def get_detection_metrics():
    """
    Get fraud detection accuracy metrics
    FIXED: Using is_flagged for detected fraud
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                SUM(CASE WHEN is_fraud AND is_flagged THEN 1 ELSE 0 END) as true_positives,
                SUM(CASE WHEN NOT is_fraud AND is_flagged THEN 1 ELSE 0 END) as false_positives,
                SUM(CASE WHEN is_fraud AND NOT is_flagged THEN 1 ELSE 0 END) as false_negatives,
                SUM(CASE WHEN NOT is_fraud AND NOT is_flagged THEN 1 ELSE 0 END) as true_negatives
            FROM transactions
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        tp = result['true_positives'] or 0
        fp = result['false_positives'] or 0
        fn = result['false_negatives'] or 0
        tn = result['true_negatives'] or 0
        
        # Calculate metrics
        precision = (tp / (tp + fp)) * 100 if (tp + fp) > 0 else 0.0
        recall = (tp / (tp + fn)) * 100 if (tp + fn) > 0 else 0.0
        accuracy = ((tp + tn) / (tp + fp + fn + tn)) * 100 if (tp + fp + fn + tn) > 0 else 0.0
        
        return DetectionMetrics(
            true_positives=tp,
            false_positives=fp,
            false_negatives=fn,
            true_negatives=tn,
            precision=round(precision, 2),
            recall=round(recall, 2),
            accuracy=round(accuracy, 2)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/redis/stats")
async def get_redis_stats():
    """Get Redis cache statistics"""
    try:
        fraud_count = REDIS_CLIENT.zcard('fraud:leaderboard')
        top_frauds = REDIS_CLIENT.zrevrange('fraud:leaderboard', 0, 4, withscores=True)
        
        return {
            "cached_frauds": fraud_count,
            "top_fraud_scores": [
                {"transaction_id": txn, "score": float(score)} 
                for txn, score in top_frauds
            ],
            "redis_host": REDIS_HOST
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
