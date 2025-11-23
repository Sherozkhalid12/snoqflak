-- ============================================================================
-- Transformation Stored Procedures
-- ============================================================================
-- Stored procedures for data cleaning, transformation, and business logic
-- ============================================================================

USE DATABASE DATA_PIPELINE_DB;
USE SCHEMA CLEANED;

-- ============================================================================
-- Procedure: Clean and standardize data
-- ============================================================================
CREATE OR REPLACE PROCEDURE CLEAN_RAW_DATA(
    SOURCE_TABLE VARCHAR,
    TARGET_TABLE VARCHAR,
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    ROWS_PROCESSED NUMBER;
    ROWS_FAILED NUMBER;
    ERROR_MSG VARCHAR;
BEGIN
    -- Start logging
    INSERT INTO VALIDATION.PIPELINE_LOGS (
        PIPELINE_NAME, RUN_ID, START_TIME, STATUS
    ) VALUES (
        'CLEAN_RAW_DATA', RUN_ID, CURRENT_TIMESTAMP(), 'IN_PROGRESS'
    );
    
    -- Clean and transform data
    BEGIN
        EXECUTE IMMEDIATE '
            INSERT INTO ' || TARGET_TABLE || '
            SELECT
                TRIM(COALESCE(column1, '''')) AS column1_cleaned,
                TRIM(COALESCE(column2, '''')) AS column2_cleaned,
                TRY_TO_DATE(column3) AS date_column,
                TRY_TO_NUMBER(column4) AS numeric_column,
                CURRENT_TIMESTAMP() AS processed_at
            FROM ' || SOURCE_TABLE || '
            WHERE column1 IS NOT NULL  -- Remove completely null rows
        ';
        
        -- Get row count from inserted table
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || TARGET_TABLE INTO ROWS_PROCESSED;
        ROWS_FAILED := 0;
        
        -- Update log
        UPDATE VALIDATION.PIPELINE_LOGS
        SET END_TIME = CURRENT_TIMESTAMP(),
            STATUS = 'SUCCESS',
            ROWS_PROCESSED = ROWS_PROCESSED,
            ROWS_FAILED = ROWS_FAILED,
            EXECUTION_TIME_SECONDS = DATEDIFF(SECOND, START_TIME, CURRENT_TIMESTAMP())
        WHERE RUN_ID = RUN_ID AND PIPELINE_NAME = 'CLEAN_RAW_DATA';
        
        RETURN 'SUCCESS: Processed ' || ROWS_PROCESSED || ' rows';
        
    EXCEPTION
        WHEN OTHER THEN
            ERROR_MSG := SQLERRM;
            ROWS_FAILED := 1;
            
            -- Update log with error
            UPDATE VALIDATION.PIPELINE_LOGS
            SET END_TIME = CURRENT_TIMESTAMP(),
                STATUS = 'FAILED',
                ERROR_MESSAGE = ERROR_MSG,
                EXECUTION_TIME_SECONDS = DATEDIFF(SECOND, START_TIME, CURRENT_TIMESTAMP())
            WHERE RUN_ID = RUN_ID AND PIPELINE_NAME = 'CLEAN_RAW_DATA';
            
            RETURN 'FAILED: ' || ERROR_MSG;
    END;
END;
$$;

-- ============================================================================
-- Procedure: Remove duplicates
-- ============================================================================
CREATE OR REPLACE PROCEDURE REMOVE_DUPLICATES(
    TABLE_NAME VARCHAR,
    KEY_COLUMNS VARCHAR,  -- Comma-separated list of columns
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    ROWS_DELETED NUMBER;
BEGIN
    -- Create temporary table with deduplicated data
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TEMPORARY TABLE ' || TABLE_NAME || '_DEDUP AS
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY ' || KEY_COLUMNS || ' ORDER BY processed_at DESC) AS rn
            FROM ' || TABLE_NAME || '
        ) WHERE rn = 1
    ';
    
    -- Replace original table
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || TABLE_NAME || ' AS
        SELECT * EXCEPT (rn) FROM ' || TABLE_NAME || '_DEDUP
    ';
    
    RETURN 'SUCCESS: Duplicates removed';
END;
$$;

-- ============================================================================
-- Procedure: Apply business transformations
-- ============================================================================
CREATE OR REPLACE PROCEDURE APPLY_BUSINESS_TRANSFORMS(
    SOURCE_TABLE VARCHAR,
    TARGET_TABLE VARCHAR,
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    ROWS_PROCESSED NUMBER;
BEGIN
    -- Example: Calculate derived fields, apply business rules
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || TARGET_TABLE || ' AS
        SELECT
            *,
            -- Example derived fields
            CASE 
                WHEN amount > 1000 THEN ''HIGH_VALUE''
                WHEN amount > 100 THEN ''MEDIUM_VALUE''
                ELSE ''LOW_VALUE''
            END AS value_category,
            DATEDIFF(DAY, created_date, CURRENT_DATE()) AS days_since_creation
        FROM ' || SOURCE_TABLE || '
    ';
    
    -- Get row count
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || TARGET_TABLE INTO ROWS_PROCESSED;
    RETURN 'SUCCESS: Applied business transforms to ' || ROWS_PROCESSED || ' rows';
END;
$$;

-- Grant execute privileges
GRANT USAGE ON PROCEDURE CLEAN_RAW_DATA(VARCHAR, VARCHAR, VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE REMOVE_DUPLICATES(VARCHAR, VARCHAR, VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE APPLY_BUSINESS_TRANSFORMS(VARCHAR, VARCHAR, VARCHAR) TO ROLE PUBLIC;

-- Show procedures
SHOW PROCEDURES IN SCHEMA CLEANED;

