# Testing Guide

Complete guide to test and verify the Snowflake data pipeline is working correctly.

## Quick Test

Run the automated test suite:

```bash
python scripts/test/test_pipeline.py
```

This will test all components and provide a summary report.

## Step-by-Step Manual Testing

### 1. Prerequisites Check

```bash
# Verify Python dependencies
pip list | grep -E "snowflake|pyyaml|pandas|requests"

# Verify configuration
cat .env | grep SNOWFLAKE
cat config/config.yaml | head -20
```

### 2. Connection Test

```python
from scripts.utils.snowflake_connector import SnowflakeConnector

connector = SnowflakeConnector()
try:
    result = connector.execute_query("SELECT CURRENT_VERSION()")
    print("✅ Connection successful!")
    print(result)
finally:
    connector.close()
```

Or run:
```bash
python scripts/test/test_pipeline.py
```

### 3. Setup Verification

```bash
# Run setup (if not done already)
python scripts/setup/run_setup.py
```

Verify in Snowflake SQL editor:
```sql
-- Check warehouse
SHOW WAREHOUSES LIKE 'DATA_PIPELINE_WH';

-- Check database
SHOW DATABASES LIKE 'DATA_PIPELINE_DB';

-- Check schemas
SHOW SCHEMAS IN DATABASE DATA_PIPELINE_DB;

-- Check file formats
SHOW FILE FORMATS IN SCHEMA DATA_PIPELINE_DB.RAW;

-- Check stages
SHOW STAGES IN SCHEMA DATA_PIPELINE_DB.RAW;

-- Check pipes
SHOW PIPES LIKE 'FILE_INGESTION_PIPE';

-- Check procedures
SHOW PROCEDURES IN SCHEMA DATA_PIPELINE_DB.CLEANED;
SHOW PROCEDURES IN SCHEMA DATA_PIPELINE_DB.VALIDATION;

-- Check tables
SHOW TABLES IN SCHEMA DATA_PIPELINE_DB.VALIDATION;

-- Check tasks
SHOW TASKS IN DATABASE DATA_PIPELINE_DB;
```

### 4. File Ingestion Test

#### Option A: Upload to Stage (Manual)

```bash
# Upload sample file
python scripts/ingestion/file_ingestion.py sample_data/sample.csv --stage FILE_STAGE
```

Verify in Snowflake:
```sql
-- List stage contents
LIST @FILE_STAGE;

-- Check if data was ingested (if Snowpipe is configured)
SELECT COUNT(*) FROM RAW.FILE_INGESTION_STAGING;
```

#### Option B: Test Snowpipe (if configured)

```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('FILE_INGESTION_PIPE');

-- View pipe history
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('hours', -24, CURRENT_TIMESTAMP())
));

-- Manually trigger pipe refresh (if auto-ingest disabled)
ALTER PIPE FILE_INGESTION_PIPE REFRESH;
```

### 5. API Ingestion Test

```bash
# Test API ingestion (configure endpoints in config.yaml first)
python scripts/ingestion/api_ingestion.py --endpoint public_api_example

# Or test all endpoints
python scripts/ingestion/api_ingestion.py --all
```

Verify in Snowflake:
```sql
-- Check API data tables
SELECT * FROM RAW.PUBLIC_API_DATA LIMIT 10;
SELECT * FROM RAW.PRIVATE_API_DATA LIMIT 10;
```

### 6. Transformation Test

```sql
-- Test cleaning procedure
CALL CLEANED.CLEAN_RAW_DATA(
    'RAW.FILE_INGESTION_STAGING',
    'CLEANED.CLEANED_DATA',
    'TEST_RUN_001'
);

-- Check cleaned data
SELECT COUNT(*) FROM CLEANED.CLEANED_DATA;

-- Test deduplication
CALL CLEANED.REMOVE_DUPLICATES(
    'CLEANED.CLEANED_DATA',
    'column1,column2',
    'TEST_RUN_001'
);

-- Test business transforms
CALL CLEANED.APPLY_BUSINESS_TRANSFORMS(
    'CLEANED.CLEANED_DATA',
    'ANALYTICS.FINAL_DATA',
    'TEST_RUN_001'
);

-- Check final data
SELECT COUNT(*) FROM ANALYTICS.FINAL_DATA;
```

### 7. Validation Test

```sql
-- Test row count validation
CALL VALIDATION.VALIDATE_ROW_COUNT(
    'ANALYTICS.FINAL_DATA',
    0,
    NULL,
    'TEST_RUN_001'
);

-- Test null percentage validation
CALL VALIDATION.VALIDATE_NULL_PERCENTAGE(
    'ANALYTICS.FINAL_DATA',
    'column_name',
    10,
    'TEST_RUN_001'
);

-- Test data freshness
CALL VALIDATION.VALIDATE_DATA_FRESHNESS(
    'ANALYTICS.FINAL_DATA',
    'ingestion_timestamp',
    24,
    'TEST_RUN_001'
);

-- Run all validations
CALL VALIDATION.RUN_ALL_VALIDATIONS(
    'ANALYTICS.FINAL_DATA',
    'TEST_RUN_001'
);

-- Check validation results
SELECT * FROM VALIDATION.VALIDATION_RESULTS
WHERE RUN_ID = 'TEST_RUN_001'
ORDER BY CREATED_AT DESC;
```

### 8. Pipeline Orchestration Test

```bash
# Test full pipeline
python scripts/orchestration/pipeline_orchestrator.py --step all

# Or test individual steps
python scripts/orchestration/pipeline_orchestrator.py --step ingestion
python scripts/orchestration/pipeline_orchestrator.py --step transformation
python scripts/orchestration/pipeline_orchestrator.py --step validation
```

Check logs:
```sql
-- View pipeline logs
SELECT * FROM VALIDATION.PIPELINE_LOGS
ORDER BY START_TIME DESC
LIMIT 10;

-- Check validation results
SELECT 
    TABLE_NAME,
    CHECK_NAME,
    STATUS,
    MESSAGE,
    CREATED_AT
FROM VALIDATION.VALIDATION_RESULTS
ORDER BY CREATED_AT DESC
LIMIT 20;
```

### 9. Task Execution Test

```sql
-- Check task status
SHOW TASKS IN DATABASE DATA_PIPELINE_DB;

-- View task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- Manually execute task (if needed)
EXECUTE TASK PIPELINE_ORCHESTRATOR;
```

### 10. Performance Test

```sql
-- Check warehouse usage
SELECT 
    WAREHOUSE_NAME,
    STATE,
    AUTO_SUSPEND,
    AUTO_RESUME
FROM SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSES
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH';

-- Check credit usage
SELECT 
    DATE(START_TIME) as DATE,
    SUM(CREDITS_USED) as CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH'
AND START_TIME >= DATEADD('days', -7, CURRENT_DATE())
GROUP BY DATE(START_TIME)
ORDER BY DATE DESC;

-- Check query performance
SELECT 
    QUERY_TEXT,
    EXECUTION_TIME,
    BYTES_SCANNED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH'
ORDER BY EXECUTION_TIME DESC
LIMIT 10;
```

## End-to-End Test Scenario

### Complete Test Flow

1. **Setup** (one-time):
   ```bash
   python scripts/setup/run_setup.py
   ```

2. **Run automated tests**:
   ```bash
   python scripts/test/test_pipeline.py
   ```

3. **Ingest sample data**:
   ```bash
   python scripts/ingestion/file_ingestion.py sample_data/sample.csv --stage FILE_STAGE
   ```

4. **Run transformation**:
   ```sql
   CALL CLEANED.CLEAN_RAW_DATA('RAW.FILE_INGESTION_STAGING', 'CLEANED.CLEANED_DATA', 'E2E_TEST');
   CALL CLEANED.APPLY_BUSINESS_TRANSFORMS('CLEANED.CLEANED_DATA', 'ANALYTICS.FINAL_DATA', 'E2E_TEST');
   ```

5. **Run validation**:
   ```sql
   CALL VALIDATION.RUN_ALL_VALIDATIONS('ANALYTICS.FINAL_DATA', 'E2E_TEST');
   ```

6. **Check results**:
   ```sql
   -- Pipeline logs
   SELECT * FROM VALIDATION.PIPELINE_LOGS WHERE RUN_ID = 'E2E_TEST';
   
   -- Validation results
   SELECT * FROM VALIDATION.VALIDATION_RESULTS WHERE RUN_ID = 'E2E_TEST';
   
   -- Final data
   SELECT COUNT(*) FROM ANALYTICS.FINAL_DATA;
   ```

7. **Run full pipeline**:
   ```bash
   python scripts/orchestration/pipeline_orchestrator.py --step all
   ```

## Troubleshooting Tests

### Connection Issues

**Error: "Invalid account identifier"**
- Check `.env` file for correct `SNOWFLAKE_ACCOUNT`
- Format should be: `orgname-accountname`

**Error: "Authentication failed"**
- Verify username/password in `.env`
- Check if account is locked
- Verify IP whitelist (if configured)

### Setup Issues

**Error: "Insufficient privileges"**
- Ensure using ACCOUNTADMIN role
- Or grant necessary privileges

**Error: "Object already exists"**
- Objects may exist from previous runs
- Scripts use `IF NOT EXISTS` - safe to re-run

### Ingestion Issues

**Files not appearing**
- Verify stage configuration
- Check file format matches data
- Verify Snowpipe status
- Check cloud storage permissions (if using external stage)

**API ingestion failing**
- Verify API credentials
- Check endpoint configuration in `config.yaml`
- Review network connectivity
- Check rate limits

### Transformation Issues

**Procedure errors**
- Verify source table exists
- Check column names match
- Review error messages in logs

### Validation Issues

**Validation failures**
- Check validation thresholds
- Review data quality
- Adjust thresholds in procedures or config

## Success Criteria

✅ All automated tests pass  
✅ Can connect to Snowflake  
✅ All objects created successfully  
✅ File ingestion works  
✅ API ingestion works (if configured)  
✅ Transformations execute successfully  
✅ Validations run and log results  
✅ Pipeline orchestrator runs end-to-end  
✅ Tasks are scheduled and can execute  
✅ Logging and monitoring work correctly  

## Next Steps After Testing

1. **Customize transformations** for your data
2. **Configure API endpoints** in `config/config.yaml`
3. **Set up cloud storage** integration (if using)
4. **Add clustering keys** after analyzing query patterns
5. **Configure alerts** for pipeline failures
6. **Set up monitoring dashboards**

---

**Test Date**: _______________  
**Tested By**: _______________  
**Results**: _______________

