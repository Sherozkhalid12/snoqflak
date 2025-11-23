-- ============================================================================
-- External Stage Setup Script
-- ============================================================================
-- Creates external stage for file ingestion from cloud storage
-- Supports S3, Azure Blob Storage, and Google Cloud Storage
-- ============================================================================

USE DATABASE DATA_PIPELINE_DB;
USE SCHEMA RAW;

-- ============================================================================
-- S3 External Stage (Uncomment and configure for AWS S3)
-- ============================================================================
/*
CREATE OR REPLACE STAGE FILE_STAGE
    URL = 's3://your-bucket-name/data/raw/'
    CREDENTIALS = (
        AWS_KEY_ID = 'your_access_key_id'
        AWS_SECRET_KEY = 'your_secret_access_key'
    )
    -- OR use IAM role (preferred):
    -- CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::123456789012:role/snowflake-role')
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for S3 file ingestion';
*/

-- ============================================================================
-- Azure Blob Storage External Stage (Uncomment and configure for Azure)
-- ============================================================================
/*
CREATE OR REPLACE STAGE FILE_STAGE
    URL = 'azure://your-storage-account.blob.core.windows.net/container/data/raw/'
    CREDENTIALS = (
        AZURE_SAS_TOKEN = 'your_sas_token'
    )
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for Azure Blob Storage file ingestion';
*/

-- ============================================================================
-- Google Cloud Storage External Stage (Uncomment and configure for GCS)
-- ============================================================================
/*
CREATE OR REPLACE STAGE FILE_STAGE
    URL = 'gcs://your-bucket-name/data/raw/'
    STORAGE_INTEGRATION = your_gcs_integration_name
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for GCS file ingestion';
*/

-- ============================================================================
-- Local Stage (for testing/development)
-- ============================================================================
CREATE OR REPLACE STAGE FILE_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Local stage for file ingestion (testing)';

-- Grant privileges
GRANT USAGE ON STAGE FILE_STAGE TO ROLE PUBLIC;
GRANT READ ON STAGE FILE_STAGE TO ROLE PUBLIC;

-- Show stage
SHOW STAGES IN SCHEMA RAW;

-- ============================================================================
-- Storage Integration Setup (Recommended for production)
-- ============================================================================
-- For S3 with IAM role:
/*
CREATE STORAGE INTEGRATION S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket/data/raw/');
*/

-- ============================================================================
-- Usage Notes:
-- 1. Choose the appropriate stage type based on your cloud provider
-- 2. For production, use Storage Integration with IAM roles (more secure)
-- 3. Update FILE_FORMAT to match your data format (CSV_FORMAT, PARQUET_FORMAT)
-- 4. Test stage access: LIST @FILE_STAGE;
-- ============================================================================

