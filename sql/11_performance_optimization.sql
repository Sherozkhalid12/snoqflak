-- ============================================================================
-- Performance Optimization Script
-- ============================================================================
-- Adds clustering keys and other performance optimizations
-- ============================================================================

USE DATABASE DATA_PIPELINE_DB;

-- ============================================================================
-- Add Clustering to Analytics Tables
-- ============================================================================
-- Clustering improves query performance on large tables by organizing data
-- based on frequently queried columns

USE SCHEMA ANALYTICS;

-- Example: Cluster final data table by date (adjust columns based on your queries)
-- Uncomment and modify based on your actual table structure and query patterns

/*
ALTER TABLE ANALYTICS.FINAL_DATA CLUSTER BY (
    DATE_TRUNC('DAY', ingestion_timestamp),
    category  -- Add other frequently filtered columns
);
*/

-- ============================================================================
-- Check Clustering Effectiveness
-- ============================================================================
-- Run this after adding clustering to verify effectiveness:
-- SELECT SYSTEM$CLUSTERING_INFORMATION('ANALYTICS.FINAL_DATA', '(ingestion_timestamp, category)');

-- ============================================================================
-- Result Cache Configuration
-- ============================================================================
-- Result caching is enabled by default in Snowflake
-- To verify cache usage, query:
-- SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
-- WHERE RESULT_SCAN = TRUE;

-- ============================================================================
-- Query Timeout Configuration
-- ============================================================================
-- Set query timeout at session level (default is 3600 seconds)
-- ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;

-- ============================================================================
-- Warehouse Multi-Cluster Configuration (Optional)
-- ============================================================================
-- Enable multi-cluster for concurrent workloads:
/*
ALTER WAREHOUSE DATA_PIPELINE_WH SET
    MIN_CLUSTER_COUNT = 1,
    MAX_CLUSTER_COUNT = 3,
    SCALING_POLICY = 'STANDARD';  -- Or 'ECONOMY' for cost optimization
*/

-- ============================================================================
-- Performance Monitoring Queries
-- ============================================================================
-- Use these queries to monitor performance:

-- Check clustering information
-- SELECT 
--     TABLE_NAME,
--     CLUSTERING_KEY,
--     ROW_COUNT,
--     BYTES
-- FROM SNOWFLAKE.INFORMATION_SCHEMA.TABLES
-- WHERE TABLE_SCHEMA = 'ANALYTICS'
-- AND CLUSTERING_KEY IS NOT NULL;

-- Monitor query performance
-- SELECT 
--     QUERY_TEXT,
--     EXECUTION_TIME,
--     BYTES_SCANNED,
--     PARTITIONS_SCANNED,
--     PARTITIONS_TOTAL
-- FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
-- WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH'
-- ORDER BY EXECUTION_TIME DESC
-- LIMIT 10;

-- ============================================================================
-- Notes:
-- 1. Clustering is most effective on tables > 1GB
-- 2. Choose clustering keys based on frequent WHERE/JOIN conditions
-- 3. Clustering has maintenance costs - monitor and adjust as needed
-- 4. Result caching is automatic - no configuration needed
-- 5. Multi-cluster is useful for concurrent workloads
-- ============================================================================

