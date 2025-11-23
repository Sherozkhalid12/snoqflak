-- ============================================================================
-- Snowflake Tasks Setup
-- ============================================================================
-- Creates tasks for orchestrating the pipeline end-to-end
-- ============================================================================

USE DATABASE DATA_PIPELINE_DB;
USE SCHEMA VALIDATION;

-- ============================================================================
-- Task: Main pipeline orchestration
-- ============================================================================
-- Note: Tasks in Snowflake use SQL statements, not stored procedure calls directly
-- For complex orchestration, consider using a stored procedure and calling it from the task
CREATE OR REPLACE TASK PIPELINE_ORCHESTRATOR
    WAREHOUSE = DATA_PIPELINE_WH
    SCHEDULE = 'USING CRON 0 */6 * * * UTC'  -- Every 6 hours
    COMMENT = 'Main pipeline orchestration task'
AS
    -- Call orchestration stored procedure (create this separately)
    CALL VALIDATION.RUN_PIPELINE_ORCHESTRATION();

-- ============================================================================
-- Task: Data quality monitoring
-- ============================================================================
CREATE OR REPLACE TASK DATA_QUALITY_MONITOR
    WAREHOUSE = DATA_PIPELINE_WH
    SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- Daily at 2 AM UTC
    COMMENT = 'Daily data quality monitoring'
AS
    -- Call data quality monitoring procedure
    CALL VALIDATION.RUN_DATA_QUALITY_CHECKS();

-- Resume tasks (tasks are created in suspended state)
ALTER TASK PIPELINE_ORCHESTRATOR RESUME;
ALTER TASK DATA_QUALITY_MONITOR RESUME;

-- Show tasks
SHOW TASKS IN DATABASE DATA_PIPELINE_DB;

-- ============================================================================
-- Task Management Notes:
-- 1. Tasks are created in SUSPENDED state - resume them to activate
-- 2. Use ALTER TASK ... SUSPEND to pause a task
-- 3. Use ALTER TASK ... RESUME to activate a task
-- 4. Monitor task execution: SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
-- 5. Tasks can be chained using AFTER clause for dependencies
-- ============================================================================

