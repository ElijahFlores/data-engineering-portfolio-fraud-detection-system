-- SQL Schema for Fraud Detection System
-- File: sql/init.sql
-- FIXED: Added all columns that Spark will write

-- Transactions Table (All transactions with fraud detection results)
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) UNIQUE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    merchant VARCHAR(100) NOT NULL,
    location VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    ip_address VARCHAR(50),
    is_fraud BOOLEAN DEFAULT FALSE,
    fraud_type VARCHAR(50),
    -- FIXED: Added fraud detection columns from Spark
    rule_high_amount INTEGER DEFAULT 0,
    rule_suspicious_location INTEGER DEFAULT 0,
    rule_suspicious_merchant INTEGER DEFAULT 0,
    rule_round_amount INTEGER DEFAULT 0,
    fraud_score DECIMAL(5, 2) DEFAULT 0,
    fraud_reasons TEXT[],
    is_flagged BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Flagged Transactions (Detected by system - for quick access)
CREATE TABLE IF NOT EXISTS flagged_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    merchant VARCHAR(100) NOT NULL,
    location VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    fraud_score DECIMAL(5, 2) NOT NULL,
    fraud_reasons TEXT[],
    flagged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    alert_sent BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
);

-- User Transaction Patterns (For anomaly detection)
CREATE TABLE IF NOT EXISTS user_patterns (
    user_id VARCHAR(50) PRIMARY KEY,
    avg_transaction_amount DECIMAL(10, 2),
    std_transaction_amount DECIMAL(10, 2),
    total_transactions INTEGER DEFAULT 0,
    last_transaction_timestamp TIMESTAMP,
    usual_locations TEXT[],
    usual_merchants TEXT[],
    known_devices TEXT[],
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fraud Statistics (For dashboard - aggregated hourly)
CREATE TABLE IF NOT EXISTS fraud_statistics (
    id SERIAL PRIMARY KEY,
    hour_timestamp TIMESTAMP NOT NULL,
    total_transactions INTEGER DEFAULT 0,
    flagged_transactions INTEGER DEFAULT 0,
    true_frauds INTEGER DEFAULT 0,
    false_positives INTEGER DEFAULT 0,
    avg_fraud_score DECIMAL(5, 2),
    UNIQUE(hour_timestamp)
);

-- Indexes for performance
CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX idx_transactions_is_flagged ON transactions(is_flagged);
CREATE INDEX idx_flagged_timestamp ON flagged_transactions(flagged_at);
CREATE INDEX idx_fraud_stats_timestamp ON fraud_statistics(hour_timestamp);

-- FIXED: Views using correct column names
CREATE OR REPLACE VIEW fraud_detection_summary AS
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as actual_frauds,
    SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) as detected_frauds,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(amount) as avg_transaction_amount,
    MAX(amount) as max_transaction_amount,
    AVG(CASE WHEN is_flagged THEN fraud_score ELSE 0 END) as avg_fraud_score
FROM transactions
GROUP BY DATE(timestamp)
ORDER BY date DESC;

CREATE OR REPLACE VIEW top_fraud_merchants AS
SELECT 
    merchant,
    COUNT(*) as fraud_count,
    AVG(amount) as avg_fraud_amount,
    AVG(fraud_score) as avg_fraud_score
FROM transactions
WHERE is_flagged = TRUE
GROUP BY merchant
ORDER BY fraud_count DESC
LIMIT 10;

-- FIXED: Helper function for hourly aggregation (replaces time_bucket)
CREATE OR REPLACE FUNCTION date_trunc_hour(ts TIMESTAMP)
RETURNS TIMESTAMP AS $$
BEGIN
    RETURN DATE_TRUNC('hour', ts);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Performance monitoring view
CREATE OR REPLACE VIEW fraud_performance_metrics AS
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud AND is_flagged THEN 1 ELSE 0 END) as true_positives,
    SUM(CASE WHEN is_fraud AND NOT is_flagged THEN 1 ELSE 0 END) as false_negatives,
    SUM(CASE WHEN NOT is_fraud AND is_flagged THEN 1 ELSE 0 END) as false_positives,
    SUM(CASE WHEN NOT is_fraud AND NOT is_flagged THEN 1 ELSE 0 END) as true_negatives,
    ROUND(
        SUM(CASE WHEN is_fraud AND is_flagged THEN 1 ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END), 0) * 100, 
        2
    ) as recall_percentage,
    ROUND(
        SUM(CASE WHEN is_fraud AND is_flagged THEN 1 ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END), 0) * 100, 
        2
    ) as precision_percentage
FROM transactions
GROUP BY DATE(timestamp)
ORDER BY date DESC;

COMMENT ON TABLE transactions IS 'All transactions with fraud detection results';
COMMENT ON TABLE flagged_transactions IS 'Quick access table for flagged transactions only';
COMMENT ON VIEW fraud_performance_metrics IS 'Detection accuracy metrics (precision, recall)';
