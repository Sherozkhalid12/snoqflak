# Quick Reference Guide

Quick commands and SQL snippets for common operations.

## Common Commands

### Setup
```bash
# Run full setup
python scripts/setup/run_setup.py

# Or use shell script
./scripts/run_pipeline.sh setup
```

### Run Pipeline
```bash
# Full pipeline
python scripts/orchestration/pipeline_orchestrator.py --step all

# Individual steps
python scripts/orchestration/pipeline_orchestrator.py --step ingestion
python scripts/orchestration/pipeline_orchestrator.py --step transformation
python scripts/orchestration/pipeline_orchestrator.py --step validation
```

### File Ingestion
```bash
# Upload file to stage
python scripts/ingestion/file_ingestion.py data.csv --stage FILE_STAGE

# Upload to S3 (triggers Snowpipe)
python scripts/ingestion/file_ingestion.py data.csv --s3-bucket bucket --s3-key path/data.csv
```

### API Ingestion
```bash
# Ingest all endpoints
python scripts/ingestion/api_ingestion.py --all

# Ingest specific endpoint
python scripts/ingestion/api_ingestion.py --endpoint public_api_example
```

## SQL Quick Reference

### Check Pipeline Status
```sql
-- Recent pipeline runs
SELECT * FROM VALIDATION.PIPELINE_LOGS
ORDER BY START_TIME DESC
LIMIT 10;

-- Validation results
SELECT * FROM VALIDATION.VALIDATION_RESULTS
WHERE RUN_ID = 'RUN_20240101_120000'
ORDER BY CREATED_AT DESC;
```

### Warehouse Management
```sql
-- Check warehouse status
SHOW WAREHOUSES LIKE 'DATA_PIPELINE_WH';

-- Suspend warehouse
ALTER WAREHOUSE DATA_PIPELINE_WH SUSPEND;

-- Resume warehouse
ALTER WAREHOUSE DATA_PIPELINE_WH RESUME;

-- Scale warehouse
ALTER WAREHOUSE DATA_PIPELINE_WH SET WAREHOUSE_SIZE = 'MEDIUM';
```

### Task Management
```sql
-- Show tasks
SHOW TASKS IN DATABASE DATA_PIPELINE_DB;

-- Resume task
ALTER TASK PIPELINE_ORCHESTRATOR RESUME;

-- Suspend task
ALTER TASK PIPELINE_ORCHESTRATOR SUSPEND;

-- Task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
```

### Snowpipe Status
```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('FILE_INGESTION_PIPE');

-- Pipe history
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('hours', -24, CURRENT_TIMESTAMP())
));
```

### Data Quality Checks
```sql
-- Run validation
CALL VALIDATION.VALIDATE_ROW_COUNT('ANALYTICS.FINAL_DATA', 0, NULL, 'TEST_RUN');

-- Check null percentage
CALL VALIDATION.VALIDATE_NULL_PERCENTAGE('ANALYTICS.FINAL_DATA', 'column_name', 10, 'TEST_RUN');

-- Data freshness
CALL VALIDATION.VALIDATE_DATA_FRESHNESS('ANALYTICS.FINAL_DATA', 'ingestion_timestamp', 24, 'TEST_RUN');
```

### Performance Monitoring
```sql
-- Credit usage
SELECT 
    DATE(START_TIME) as DATE,
    SUM(CREDITS_USED) as CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH'
AND START_TIME >= DATEADD('days', -7, CURRENT_DATE())
GROUP BY DATE(START_TIME)
ORDER BY DATE DESC;

-- Query performance
SELECT 
    QUERY_TEXT,
    EXECUTION_TIME,
    BYTES_SCANNED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH'
ORDER BY EXECUTION_TIME DESC
LIMIT 10;
```

### Table Management
```sql
-- List tables
SHOW TABLES IN SCHEMA RAW;
SHOW TABLES IN SCHEMA CLEANED;
SHOW TABLES IN SCHEMA ANALYTICS;

-- Row counts
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT,
    BYTES / (1024*1024) as SIZE_MB
FROM SNOWFLAKE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('RAW', 'CLEANED', 'ANALYTICS')
ORDER BY BYTES DESC;

-- Add clustering
ALTER TABLE ANALYTICS.FINAL_DATA CLUSTER BY (date_column);
```

## Troubleshooting

### Connection Issues
```python
from scripts.utils.snowflake_connector import SnowflakeConnector
connector = SnowflakeConnector()
try:
    result = connector.execute_query("SELECT CURRENT_VERSION()")
    print("Connected!")
finally:
    connector.close()
```

### Check Logs
```sql
-- Pipeline errors
SELECT * FROM VALIDATION.PIPELINE_LOGS
WHERE STATUS = 'FAILED'
ORDER BY START_TIME DESC;

-- Recent validations
SELECT 
    TABLE_NAME,
    CHECK_NAME,
    STATUS,
    MESSAGE
FROM VALIDATION.VALIDATION_RESULTS
WHERE CREATED_AT >= DATEADD('hours', -24, CURRENT_TIMESTAMP())
ORDER BY CREATED_AT DESC;
```

## Configuration Files

- `config/config.yaml` - Main configuration
- `.env` - Credentials (not in git)
- `sql/*.sql` - SQL setup scripts

## File Locations

- **SQL Scripts**: `sql/`
- **Python Scripts**: `scripts/`
- **Configuration**: `config/`
- **Documentation**: `docs/`

