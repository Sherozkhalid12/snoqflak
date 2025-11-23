-- ============================================================================
-- Validation Stored Procedures
-- ============================================================================
-- Procedures for data validation and quality checks
-- ============================================================================

USE DATABASE DATA_PIPELINE_DB;
USE SCHEMA VALIDATION;

-- ============================================================================
-- Procedure: Row count validation
-- ============================================================================
CREATE OR REPLACE PROCEDURE VALIDATE_ROW_COUNT(
    TABLE_NAME VARCHAR,
    MIN_ROWS NUMBER DEFAULT 0,
    MAX_ROWS NUMBER DEFAULT NULL,
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    ACTUAL_COUNT NUMBER;
    STATUS VARCHAR(50);
    MSG VARCHAR;
BEGIN
    -- Get row count
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || TABLE_NAME INTO ACTUAL_COUNT;
    
    -- Validate
    IF ACTUAL_COUNT < MIN_ROWS THEN
        STATUS := 'FAIL';
        MSG := 'Row count ' || ACTUAL_COUNT || ' is below minimum ' || MIN_ROWS;
    ELSIF MAX_ROWS IS NOT NULL AND ACTUAL_COUNT > MAX_ROWS THEN
        STATUS := 'FAIL';
        MSG := 'Row count ' || ACTUAL_COUNT || ' exceeds maximum ' || MAX_ROWS;
    ELSE
        STATUS := 'PASS';
        MSG := 'Row count ' || ACTUAL_COUNT || ' is within acceptable range';
    END IF;
    
    -- Log result
    INSERT INTO VALIDATION.VALIDATION_RESULTS (
        RUN_ID, TABLE_NAME, CHECK_NAME, CHECK_TYPE, STATUS,
        EXPECTED_VALUE, ACTUAL_VALUE, MESSAGE
    ) VALUES (
        RUN_ID, TABLE_NAME, 'ROW_COUNT_CHECK', 'THRESHOLD',
        STATUS, MIN_ROWS || '-' || COALESCE(MAX_ROWS::VARCHAR, 'NULL'),
        ACTUAL_COUNT::VARCHAR, MSG
    );
    
    RETURN MSG;
END;
$$;

-- ============================================================================
-- Procedure: Null percentage validation
-- ============================================================================
CREATE OR REPLACE PROCEDURE VALIDATE_NULL_PERCENTAGE(
    TABLE_NAME VARCHAR,
    COLUMN_NAME VARCHAR,
    MAX_NULL_PERCENT NUMBER DEFAULT 10,
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    TOTAL_ROWS NUMBER;
    NULL_ROWS NUMBER;
    NULL_PERCENT NUMBER;
    STATUS VARCHAR(50);
    MSG VARCHAR;
BEGIN
    -- Get counts
    EXECUTE IMMEDIATE '
        SELECT COUNT(*), SUM(CASE WHEN ' || COLUMN_NAME || ' IS NULL THEN 1 ELSE 0 END)
        FROM ' || TABLE_NAME
        INTO TOTAL_ROWS, NULL_ROWS;
    
    NULL_PERCENT := (NULL_ROWS / NULLIF(TOTAL_ROWS, 0)) * 100;
    
    -- Validate
    IF NULL_PERCENT > MAX_NULL_PERCENT THEN
        STATUS := 'FAIL';
        MSG := 'Null percentage ' || NULL_PERCENT || '% exceeds threshold ' || MAX_NULL_PERCENT || '%';
    ELSE
        STATUS := 'PASS';
        MSG := 'Null percentage ' || NULL_PERCENT || '% is within acceptable range';
    END IF;
    
    -- Log result
    INSERT INTO VALIDATION.VALIDATION_RESULTS (
        RUN_ID, TABLE_NAME, CHECK_NAME, CHECK_TYPE, STATUS,
        EXPECTED_VALUE, ACTUAL_VALUE, MESSAGE
    ) VALUES (
        RUN_ID, TABLE_NAME, 'NULL_CHECK_' || COLUMN_NAME, 'THRESHOLD',
        STATUS, MAX_NULL_PERCENT::VARCHAR || '%',
        NULL_PERCENT::VARCHAR || '%', MSG
    );
    
    RETURN MSG;
END;
$$;

-- ============================================================================
-- Procedure: Data freshness validation
-- ============================================================================
CREATE OR REPLACE PROCEDURE VALIDATE_DATA_FRESHNESS(
    TABLE_NAME VARCHAR,
    TIMESTAMP_COLUMN VARCHAR,
    MAX_AGE_HOURS NUMBER DEFAULT 24,
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    MAX_AGE TIMESTAMP_NTZ;
    OLDEST_RECORD TIMESTAMP_NTZ;
    AGE_HOURS NUMBER;
    STATUS VARCHAR(50);
    MSG VARCHAR;
BEGIN
    -- Calculate max age timestamp
    MAX_AGE := DATEADD(HOUR, -MAX_AGE_HOURS, CURRENT_TIMESTAMP());
    
    -- Get oldest record
    EXECUTE IMMEDIATE '
        SELECT MIN(' || TIMESTAMP_COLUMN || ')
        FROM ' || TABLE_NAME
        INTO OLDEST_RECORD;
    
    IF OLDEST_RECORD IS NULL THEN
        STATUS := 'FAIL';
        MSG := 'No records found in table';
    ELSE
        AGE_HOURS := DATEDIFF(HOUR, OLDEST_RECORD, CURRENT_TIMESTAMP());
        
        IF AGE_HOURS > MAX_AGE_HOURS THEN
            STATUS := 'FAIL';
            MSG := 'Data is ' || AGE_HOURS || ' hours old, exceeds threshold of ' || MAX_AGE_HOURS || ' hours';
        ELSE
            STATUS := 'PASS';
            MSG := 'Data is ' || AGE_HOURS || ' hours old, within acceptable range';
        END IF;
    END IF;
    
    -- Log result
    INSERT INTO VALIDATION.VALIDATION_RESULTS (
        RUN_ID, TABLE_NAME, CHECK_NAME, CHECK_TYPE, STATUS,
        EXPECTED_VALUE, ACTUAL_VALUE, MESSAGE
    ) VALUES (
        RUN_ID, TABLE_NAME, 'DATA_FRESHNESS_CHECK', 'THRESHOLD',
        STATUS, MAX_AGE_HOURS::VARCHAR || ' hours',
        COALESCE(AGE_HOURS::VARCHAR, 'NULL'), MSG
    );
    
    RETURN MSG;
END;
$$;

-- ============================================================================
-- Procedure: Run all validations
-- ============================================================================
CREATE OR REPLACE PROCEDURE RUN_ALL_VALIDATIONS(
    TABLE_NAME VARCHAR,
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    RESULT VARCHAR;
    FAILED_CHECKS NUMBER;
BEGIN
    -- Run validations
    CALL VALIDATE_ROW_COUNT(TABLE_NAME, 0, NULL, RUN_ID);
    -- Add more validation calls as needed
    
    -- Check for failures
    SELECT COUNT(*) INTO FAILED_CHECKS
    FROM VALIDATION.VALIDATION_RESULTS
    WHERE RUN_ID = RUN_ID
        AND TABLE_NAME = TABLE_NAME
        AND STATUS = 'FAIL';
    
    IF FAILED_CHECKS > 0 THEN
        RESULT := 'VALIDATION FAILED: ' || FAILED_CHECKS || ' checks failed';
    ELSE
        RESULT := 'VALIDATION PASSED: All checks passed';
    END IF;
    
    RETURN RESULT;
END;
$$;

-- Grant execute privileges
GRANT USAGE ON PROCEDURE VALIDATE_ROW_COUNT(VARCHAR, NUMBER, NUMBER, VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE VALIDATE_NULL_PERCENTAGE(VARCHAR, VARCHAR, NUMBER, VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE VALIDATE_DATA_FRESHNESS(VARCHAR, VARCHAR, NUMBER, VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE RUN_ALL_VALIDATIONS(VARCHAR, VARCHAR) TO ROLE PUBLIC;

-- Show procedures
SHOW PROCEDURES IN SCHEMA VALIDATION;

