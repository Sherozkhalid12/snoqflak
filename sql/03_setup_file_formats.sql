-- ============================================================================
-- File Format Setup Script
-- ============================================================================
-- Defines file formats for CSV and Parquet ingestion
-- ============================================================================

USE DATABASE DATA_PIPELINE_DB;
USE SCHEMA RAW;

-- CSV Format
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\134'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('NULL', 'null', '')
    COMMENT = 'Standard CSV format for file ingestion';

-- Parquet Format
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO'
    BINARY_AS_TEXT = FALSE
    TRIM_SPACE = FALSE
    NULL_IF = ('NULL', 'null', '')
    COMMENT = 'Parquet format for file ingestion';

-- JSON Format (for API data)
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON format for API data ingestion';

-- Grant usage
GRANT USAGE ON FILE FORMAT CSV_FORMAT TO ROLE PUBLIC;
GRANT USAGE ON FILE FORMAT PARQUET_FORMAT TO ROLE PUBLIC;
GRANT USAGE ON FILE FORMAT JSON_FORMAT TO ROLE PUBLIC;

-- Show formats
SHOW FILE FORMATS IN SCHEMA RAW;

