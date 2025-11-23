-- ============================================================================
-- Snowpipe Setup Script
-- ============================================================================
-- Creates Snowpipe for automated file ingestion
-- Requires external stage and file format to be set up first
-- ============================================================================

USE DATABASE DATA_PIPELINE_DB;
USE SCHEMA RAW;

-- Create target table for file ingestion (example)
CREATE TABLE IF NOT EXISTS RAW.FILE_INGESTION_STAGING (
    FILE_NAME VARCHAR(255),
    FILE_ROW_NUMBER NUMBER,
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DATA VARIANT  -- Flexible column for JSON/Parquet data
);

-- Create Snowpipe
CREATE OR REPLACE PIPE FILE_INGESTION_PIPE
    AUTO_INGEST = TRUE  -- Requires S3 event notifications or equivalent
    AS
    COPY INTO RAW.FILE_INGESTION_STAGING
    FROM @FILE_STAGE
    FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
    PATTERN = '.*\\.(csv|parquet)$'
    ON_ERROR = 'CONTINUE'  -- Continue processing even if some files fail
    COMMENT = 'Automated file ingestion pipe';

-- Grant privileges
GRANT OWNERSHIP ON PIPE FILE_INGESTION_PIPE TO ROLE SYSADMIN;
GRANT USAGE ON PIPE FILE_INGESTION_PIPE TO ROLE PUBLIC;

-- Show pipe status
SHOW PIPES LIKE 'FILE_INGESTION_PIPE';

-- ============================================================================
-- Monitoring Snowpipe
-- ============================================================================
-- Check pipe status:
-- SELECT SYSTEM$PIPE_STATUS('FILE_INGESTION_PIPE');

-- View pipe history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
--     DATE_RANGE_START => DATEADD('hours', -24, CURRENT_TIMESTAMP())
-- ));

-- ============================================================================
-- Setup Notes:
-- 1. AUTO_INGEST requires cloud storage event notifications (S3, Azure, GCS)
-- 2. For S3: Set up SQS queue and S3 event notifications
-- 3. For Azure: Configure Azure Event Grid
-- 4. For GCS: Configure Pub/Sub notifications
-- 5. Without AUTO_INGEST, use ALTER PIPE ... REFRESH to trigger ingestion
-- ============================================================================

