-- ============================================================================
-- Snowflake Database and Schema Setup Script
-- ============================================================================
-- This script creates the database and schemas for the data pipeline.
-- Design is flexible to accommodate future growth.
-- ============================================================================

-- Set context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DATA_PIPELINE_WH;

-- Create database
CREATE DATABASE IF NOT EXISTS DATA_PIPELINE_DB
    DATA_RETENTION_TIME_IN_DAYS = 1  -- Time Travel retention
    COMMENT = 'Main database for data pipeline operations';

-- Grant privileges
GRANT OWNERSHIP ON DATABASE DATA_PIPELINE_DB TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE DATA_PIPELINE_DB TO ROLE PUBLIC;

-- Use the database
USE DATABASE DATA_PIPELINE_DB;

-- Create schemas for different data layers
-- RAW: Staging area for ingested data
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw data staging area for ingested files and API data';

-- CLEANED: Data after cleaning and basic transformation
CREATE SCHEMA IF NOT EXISTS CLEANED
    COMMENT = 'Cleaned and standardized data after initial processing';

-- ANALYTICS: Final transformed data ready for consumption
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Final transformed data ready for analytics and reporting';

-- VALIDATION: Schema for validation logs and metadata
CREATE SCHEMA IF NOT EXISTS VALIDATION
    COMMENT = 'Validation logs, pipeline metadata, and monitoring tables';

-- Grant privileges on schemas
GRANT USAGE ON SCHEMA RAW TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA CLEANED TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA VALIDATION TO ROLE PUBLIC;

-- Show created schemas
SHOW SCHEMAS IN DATABASE DATA_PIPELINE_DB;

-- ============================================================================
-- Schema Design Notes:
-- - RAW: Landing zone for all ingested data (files, APIs)
-- - CLEANED: Standardized, deduplicated, validated data
-- - ANALYTICS: Business logic applied, ready for consumption
-- - VALIDATION: Pipeline metadata, logs, validation results
-- ============================================================================

