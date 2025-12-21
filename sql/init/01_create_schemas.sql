-- Create separate database for Airflow metadata
SELECT 'CREATE DATABASE airflow_db' 
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

-- Connect to ecommerce database (default)
\c ecommerce

-- Create schemas for data warehouse layers
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS marts;

-- Create useful extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Grant permissions to airflow user
GRANT ALL PRIVILEGES ON SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA marts TO airflow;

-- Default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO airflow;

-- Create pipeline metadata table
CREATE TABLE IF NOT EXISTS analytics.pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    run_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL,
    records_processed INTEGER,
    execution_time_seconds FLOAT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant access to pipeline_runs
GRANT ALL PRIVILEGES ON TABLE analytics.pipeline_runs TO airflow;
GRANT USAGE, SELECT ON SEQUENCE analytics.pipeline_runs_id_seq TO airflow;

-- Log success
DO $$
BEGIN
    RAISE NOTICE '✅ Database schemas created successfully!';
END $$;
