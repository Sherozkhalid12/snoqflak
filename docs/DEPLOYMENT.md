# Deployment Guide

This guide walks you through deploying the Snowflake data pipeline from scratch.

## Prerequisites Checklist

- [ ] Snowflake account with ACCOUNTADMIN or SYSADMIN role
- [ ] Python 3.8+ installed
- [ ] Cloud storage account (S3/Azure/GCS) configured (for file ingestion)
- [ ] API credentials (if using API ingestion)
- [ ] Network access to Snowflake

## Step-by-Step Deployment

### 1. Clone/Download Project

```bash
cd /path/to/your/project
# Ensure all files are present
```

### 2. Install Dependencies

```bash
pip install -r config/requirements.txt
```

### 3. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
# Use your preferred editor
nano .env  # or vim, code, etc.
```

**Required variables:**
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
- `SNOWFLAKE_USER`: Your Snowflake username
- `SNOWFLAKE_PASSWORD`: Your Snowflake password
- `SNOWFLAKE_WAREHOUSE`: `DATA_PIPELINE_WH` (will be created)
- `SNOWFLAKE_DATABASE`: `DATA_PIPELINE_DB` (will be created)
- `SNOWFLAKE_SCHEMA`: `RAW` (will be created)
- `SNOWFLAKE_ROLE`: `ACCOUNTADMIN` or your role

### 4. Configure Pipeline Settings

Edit `config/config.yaml`:

1. **Update Snowflake settings** (if different from defaults)
2. **Configure file ingestion**:
   - Choose cloud provider (S3/Azure/GCS)
   - Update bucket/storage details
   - Configure credentials
3. **Configure API endpoints**:
   - Add your API endpoints
   - Set authentication details
   - Configure schedules

### 5. Configure External Stage

Edit `sql/04_setup_external_stage.sql`:

- Uncomment the section for your cloud provider
- Update bucket/container name
- Configure credentials (prefer IAM roles for production)
- Update file format if needed

### 6. Run Setup Script

```bash
python scripts/setup/run_setup.py
```

This will create:
- Warehouse
- Database and schemas
- File formats
- External stage
- Snowpipe
- Validation tables
- Stored procedures
- Tasks

**Verify setup:**
```sql
-- In Snowflake SQL editor
SHOW WAREHOUSES LIKE 'DATA_PIPELINE_WH';
SHOW DATABASES LIKE 'DATA_PIPELINE_DB';
SHOW SCHEMAS IN DATABASE DATA_PIPELINE_DB;
```

### 7. Test Connection

```python
from scripts.utils.snowflake_connector import SnowflakeConnector

connector = SnowflakeConnector()
try:
    result = connector.execute_query("SELECT CURRENT_VERSION()")
    print("Connection successful!")
    print(result)
finally:
    connector.close()
```

### 8. Test File Ingestion

```bash
# Create a test CSV file
echo "id,name,value
1,Test,100
2,Sample,200" > test_data.csv

# Upload to stage
python scripts/ingestion/file_ingestion.py test_data.csv --stage FILE_STAGE

# Check Snowpipe status (if using Snowpipe)
# In Snowflake: SELECT SYSTEM$PIPE_STATUS('FILE_INGESTION_PIPE');
```

### 9. Test API Ingestion

```bash
# Test a specific endpoint
python scripts/ingestion/api_ingestion.py --endpoint public_api_example

# Or test all enabled endpoints
python scripts/ingestion/api_ingestion.py --all
```

### 10. Run Full Pipeline

```bash
# Using the shell script
chmod +x scripts/run_pipeline.sh
./scripts/run_pipeline.sh all

# Or directly with Python
python scripts/orchestration/pipeline_orchestrator.py --step all
```

### 11. Verify Results

```sql
-- Check pipeline logs
SELECT * FROM VALIDATION.PIPELINE_LOGS
ORDER BY START_TIME DESC
LIMIT 10;

-- Check validation results
SELECT * FROM VALIDATION.VALIDATION_RESULTS
ORDER BY CREATED_AT DESC
LIMIT 10;

-- Check data in target tables
SELECT COUNT(*) FROM ANALYTICS.FINAL_DATA;
```

### 12. Enable Scheduled Tasks

Tasks are created in SUSPENDED state. To enable:

```sql
ALTER TASK PIPELINE_ORCHESTRATOR RESUME;
ALTER TASK DATA_QUALITY_MONITOR RESUME;
```

Verify tasks are running:
```sql
SHOW TASKS IN DATABASE DATA_PIPELINE_DB;
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
```

## Post-Deployment Checklist

- [ ] Warehouse auto-suspend is working
- [ ] File ingestion is processing files
- [ ] API ingestion is fetching data
- [ ] Transformations are running successfully
- [ ] Validations are passing
- [ ] Pipeline logs are being created
- [ ] Tasks are scheduled and running
- [ ] Monitoring queries return expected results

## Troubleshooting Deployment

### Connection Issues

**Error: "Invalid account identifier"**
- Verify `SNOWFLAKE_ACCOUNT` format (usually `orgname-accountname`)
- Check for typos in account name

**Error: "Authentication failed"**
- Verify username and password
- Check if account is locked
- Verify IP whitelist (if configured)

### Setup Script Failures

**Error: "Insufficient privileges"**
- Ensure you're using ACCOUNTADMIN role
- Or grant necessary privileges to your role

**Error: "Object already exists"**
- Objects may already exist from previous runs
- Drop existing objects or modify script to use `IF NOT EXISTS`

### Ingestion Issues

**Files not appearing in Snowflake**
- Verify external stage configuration
- Check cloud storage permissions
- Verify Snowpipe status
- Check file format matches data

**API ingestion failing**
- Verify API credentials
- Check network connectivity
- Review API endpoint configuration
- Check rate limits

## Production Considerations

### Security

1. **Use IAM Roles**: Prefer IAM roles over access keys for cloud storage
2. **Environment Variables**: Never commit credentials to version control
3. **Network Policies**: Configure IP whitelisting if required
4. **Role-Based Access**: Use least privilege principle

### Monitoring

1. Set up alerts for pipeline failures
2. Monitor warehouse credit usage
3. Track data quality metrics
4. Review validation results regularly

### Backup

1. Enable Time Travel for critical tables
2. Consider longer retention periods
3. Document recovery procedures

### Scaling

1. Start with small warehouse size
2. Monitor performance metrics
3. Scale up gradually as needed
4. Use multi-cluster for concurrent workloads

## Next Steps

1. Customize transformations for your business logic
2. Add additional validation checks
3. Configure alerts and notifications
4. Set up monitoring dashboards
5. Document your specific use cases

## Support

For issues:
1. Check the troubleshooting section
2. Review Snowflake query history
3. Check pipeline logs in VALIDATION schema
4. Consult Snowflake documentation

---

**Deployment Date**: _______________
**Deployed By**: _______________
**Environment**: _______________

