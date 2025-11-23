-- ============================================================================
-- Orchestration Stored Procedures
-- ============================================================================
-- Procedures for task orchestration (called by Snowflake tasks)
-- ============================================================================

USE DATABASE DATA_PIPELINE_DB;
USE SCHEMA VALIDATION;

-- ============================================================================
-- Procedure: Run full pipeline orchestration
-- ============================================================================
CREATE OR REPLACE PROCEDURE RUN_PIPELINE_ORCHESTRATION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    RUN_ID VARCHAR;
    RESULT VARCHAR;
BEGIN
    -- Generate run ID
    RUN_ID := 'RUN_' || REPLACE(REPLACE(TO_VARCHAR(CURRENT_TIMESTAMP()), ' ', '_'), ':', '-');
    
    -- Step 1: Clean raw data
    CALL CLEANED.CLEAN_RAW_DATA('RAW.FILE_INGESTION_STAGING', 'CLEANED.CLEANED_DATA', RUN_ID);
    
    -- Step 2: Remove duplicates
    CALL CLEANED.REMOVE_DUPLICATES('CLEANED.CLEANED_DATA', 'column1,column2', RUN_ID);
    
    -- Step 3: Apply business transforms
    CALL CLEANED.APPLY_BUSINESS_TRANSFORMS('CLEANED.CLEANED_DATA', 'ANALYTICS.FINAL_DATA', RUN_ID);
    
    -- Step 4: Run validations
    CALL VALIDATION.RUN_ALL_VALIDATIONS('ANALYTICS.FINAL_DATA', RUN_ID);
    
    RESULT := 'Pipeline orchestration completed with RUN_ID: ' || RUN_ID;
    RETURN RESULT;
END;
$$;

-- ============================================================================
-- Procedure: Run data quality checks
-- ============================================================================
CREATE OR REPLACE PROCEDURE RUN_DATA_QUALITY_CHECKS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    RUN_ID VARCHAR;
    RESULT VARCHAR;
BEGIN
    -- Generate run ID
    RUN_ID := 'DQ_' || REPLACE(REPLACE(TO_VARCHAR(CURRENT_TIMESTAMP()), ' ', '_'), ':', '-');
    
    -- Run comprehensive validations on all key tables
    CALL VALIDATION.VALIDATE_ROW_COUNT('ANALYTICS.FINAL_DATA', 0, NULL, RUN_ID);
    -- Add more validation calls as needed
    
    RESULT := 'Data quality checks completed with RUN_ID: ' || RUN_ID;
    RETURN RESULT;
END;
$$;

-- Grant execute privileges
GRANT USAGE ON PROCEDURE RUN_PIPELINE_ORCHESTRATION() TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE RUN_DATA_QUALITY_CHECKS() TO ROLE PUBLIC;

-- Show procedures
SHOW PROCEDURES IN SCHEMA VALIDATION;

