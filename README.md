# Snowflake End-to-End Data Pipeline

A production-ready, scalable data pipeline for Snowflake that handles file and API ingestion, transformation, validation, and orchestration.

## 🎯 Features

- **Multi-source Ingestion**: Supports CSV, Parquet files via Snowpipe and API endpoints
- **Automated Transformation**: Stored procedures for cleaning, deduplication, and business logic
- **Data Validation**: Comprehensive validation framework with logging
- **Cost Optimization**: Auto-suspend/resume warehouses, result caching, clustering
- **End-to-End Orchestration**: Single command execution or scheduled tasks
- **Configuration-Driven**: Easy to extend and maintain
- **Production-Ready**: Logging, error handling, monitoring

## 📋 Prerequisites

- Snowflake account with appropriate privileges
- Python 3.8+
- Cloud storage account (S3, Azure Blob, or GCS) for file ingestion (optional)
- API credentials for API endpoints (if using API ingestion)

## 🚀 Quick Start

### 1. Installation

```bash
# Clone or navigate to the project directory
cd /path/to/TESTNEW

# Install Python dependencies
pip install -r config/requirements.txt
```

### 2. Configuration

1. **Copy environment template:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your Snowflake credentials:**
   ```bash
   SNOWFLAKE_ACCOUNT=your_account_identifier
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=DATA_PIPELINE_WH
   SNOWFLAKE_DATABASE=DATA_PIPELINE_DB
   SNOWFLAKE_SCHEMA=RAW
   SNOWFLAKE_ROLE=ACCOUNTADMIN
   ```

3. **Update `config/config.yaml`** with your specific settings:
   - Cloud storage configuration (S3/Azure/GCS)
   - API endpoint details
   - Warehouse sizing
   - Validation thresholds

### 3. Setup Snowflake Objects

Run the setup script to create all necessary Snowflake objects:

```bash
python scripts/setup/run_setup.py
```

This will create:
- Warehouse with auto-suspend/resume
- Database and schemas (RAW, CLEANED, ANALYTICS, VALIDATION)
- File formats (CSV, Parquet, JSON)
- External stage (configure for your cloud provider)
- Snowpipe for automated file ingestion
- Validation tables and logging infrastructure
- Transformation and validation stored procedures
- Scheduled tasks for orchestration

### 4. Configure External Stage

Edit `sql/04_setup_external_stage.sql` and uncomment the appropriate section for your cloud provider (S3, Azure, or GCS). Update with your credentials and bucket details.

### 5. Run the Pipeline

#### Option A: Run Full Pipeline (Single Command)

```bash
python scripts/orchestration/pipeline_orchestrator.py --step all
```

#### Option B: Run Individual Steps

```bash
# Ingestion only
python scripts/orchestration/pipeline_orchestrator.py --step ingestion

# Transformation only
python scripts/orchestration/pipeline_orchestrator.py --step transformation

# Validation only
python scripts/orchestration/pipeline_orchestrator.py --step validation
```

#### Option C: Use Snowflake Tasks (Scheduled)

Tasks are automatically created and scheduled. To manually trigger:

```sql
-- In Snowflake SQL editor
ALTER TASK PIPELINE_ORCHESTRATOR RESUME;
```

## 📁 Project Structure

```
TESTNEW/
├── config/
│   ├── config.yaml              # Main configuration file
│   └── requirements.txt        # Python dependencies
├── sql/
│   ├── 01_setup_warehouse.sql
│   ├── 02_setup_database_schema.sql
│   ├── 03_setup_file_formats.sql
│   ├── 04_setup_external_stage.sql
│   ├── 05_setup_snowpipe.sql
│   ├── 06_setup_validation_tables.sql
│   ├── 07_transformation_procedures.sql
│   ├── 08_validation_procedures.sql
│   └── 09_setup_tasks.sql
├── scripts/
│   ├── utils/
│   │   └── snowflake_connector.py    # Connection utility
│   ├── ingestion/
│   │   ├── api_ingestion.py         # API data ingestion
│   │   └── file_ingestion.py        # File ingestion
│   ├── orchestration/
│   │   └── pipeline_orchestrator.py # End-to-end orchestration
│   └── setup/
│       └── run_setup.py             # Setup script
├── .env.example                    # Environment variables template
├── .gitignore
└── README.md
```

## 🔧 Usage Examples

### File Ingestion

#### Upload file to Snowflake stage:
```bash
python scripts/ingestion/file_ingestion.py data/sample.csv --stage FILE_STAGE
```

#### Upload to S3 (triggers Snowpipe auto-ingest):
```bash
python scripts/ingestion/file_ingestion.py data/sample.csv \
    --s3-bucket your-bucket \
    --s3-key data/raw/sample.csv
```

#### Check Snowpipe status:
```python
from scripts.ingestion.file_ingestion import FileIngestion
ingestion = FileIngestion()
status = ingestion.check_pipe_status()
print(status)
```

### API Ingestion

#### Ingest all enabled endpoints:
```bash
python scripts/ingestion/api_ingestion.py --all
```

#### Ingest specific endpoint:
```bash
python scripts/ingestion/api_ingestion.py --endpoint public_api_example
```

### Pipeline Monitoring

#### Check pipeline status:
```bash
python scripts/orchestration/pipeline_orchestrator.py --run-id RUN_20240101_120000
```

#### Query validation results:
```sql
-- In Snowflake SQL editor
SELECT * FROM VALIDATION.VALIDATION_RESULTS
WHERE RUN_ID = 'RUN_20240101_120000'
ORDER BY CREATED_AT DESC;

-- View pipeline logs
SELECT * FROM VALIDATION.PIPELINE_LOGS
ORDER BY START_TIME DESC
LIMIT 10;
```

## ⚙️ Configuration Guide

### Adding New API Endpoints

Edit `config/config.yaml`:

```yaml
api_ingestion:
  endpoints:
    - name: "new_api"
      url: "https://api.example.com/data"
      method: "GET"
      auth_type: "bearer"
      bearer_token: "${API_BEARER_TOKEN}"  # Use env var
      target_table: "RAW.NEW_API_DATA"
      schedule: "0 */6 * * *"  # Every 6 hours
      enabled: true
```

### Adjusting Warehouse Size

Edit `config/config.yaml`:

```yaml
snowflake:
  warehouse_config:
    size: "MEDIUM"  # X-SMALL, SMALL, MEDIUM, LARGE, X-LARGE, etc.
    auto_suspend: 120  # Increase for less frequent queries
```

Or update directly in Snowflake:

```sql
ALTER WAREHOUSE DATA_PIPELINE_WH SET WAREHOUSE_SIZE = 'MEDIUM';
```

### Customizing Transformations

Edit `sql/07_transformation_procedures.sql` to add your business logic:

```sql
CREATE OR REPLACE PROCEDURE APPLY_BUSINESS_TRANSFORMS(...)
AS
BEGIN
    -- Add your custom transformation logic here
END;
```

## 📊 Performance Optimization

### Warehouse Sizing

- **Start Small**: Begin with X-SMALL or SMALL warehouse
- **Monitor**: Use Snowflake query history to identify bottlenecks
- **Scale Up**: Increase size for large transformations
- **Scale Out**: Use multi-cluster warehouses for concurrent workloads

### Clustering Keys

Add clustering to frequently queried tables:

```sql
ALTER TABLE ANALYTICS.FINAL_DATA CLUSTER BY (date_column, category);
```

### Result Caching

Result caching is enabled by default. To verify:

```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(
    WAREHOUSE_NAME => 'DATA_PIPELINE_WH'
))
WHERE RESULT_SCAN = TRUE;
```

### Cost Monitoring

Monitor warehouse usage:

```sql
SELECT 
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) as TOTAL_CREDITS,
    AVG(AUTO_SUSPEND_AT) as AVG_SUSPEND_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH'
GROUP BY WAREHOUSE_NAME;
```

## 🧪 Testing

### Quick Test

Run the automated test suite to verify everything is working:

```bash
python scripts/test/test_pipeline.py
```

This comprehensive test checks:
- ✅ Snowflake connection
- ✅ Warehouse configuration (auto-suspend/resume)
- ✅ Database and schemas
- ✅ File formats and stages
- ✅ Snowpipe setup
- ✅ Stored procedures
- ✅ Validation tables
- ✅ Tasks
- ✅ End-to-end pipeline

### Manual Testing

See `docs/TESTING_GUIDE.md` for detailed step-by-step testing instructions.

### Test File Ingestion

```bash
# Upload sample file
python scripts/ingestion/file_ingestion.py sample_data/sample.csv --stage FILE_STAGE
```

### Test API Ingestion

```bash
# Test all configured endpoints
python scripts/ingestion/api_ingestion.py --all
```

### Test Full Pipeline

```bash
# Run end-to-end pipeline
python scripts/orchestration/pipeline_orchestrator.py --step all
```

## 🔍 Validation Framework

### Built-in Validations

- **Row Count Check**: Ensures minimum/maximum row counts
- **Null Percentage Check**: Validates data completeness
- **Data Freshness Check**: Ensures data is up-to-date
- **Custom Validations**: Extend `sql/08_validation_procedures.sql`

### Adding Custom Validations

```sql
CREATE OR REPLACE PROCEDURE VALIDATION.CUSTOM_CHECK(
    TABLE_NAME VARCHAR,
    RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Your validation logic here
    -- Log results to VALIDATION.VALIDATION_RESULTS
END;
$$;
```

## 🚨 Troubleshooting

### Connection Issues

- Verify credentials in `.env`
- Check network connectivity to Snowflake
- Ensure IP whitelist allows your IP (if configured)

### Snowpipe Not Ingesting

- Verify external stage configuration
- Check S3/Azure/GCS permissions
- Verify Snowpipe status: `SELECT SYSTEM$PIPE_STATUS('FILE_INGESTION_PIPE');`
- Check pipe history: `SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(...))`

### Performance Issues

- Check warehouse size and scaling
- Review query profiles in Snowflake UI
- Consider adding clustering keys
- Optimize transformation procedures

### Validation Failures

- Check `VALIDATION.VALIDATION_RESULTS` for details
- Review `VALIDATION.PIPELINE_LOGS` for error messages
- Adjust validation thresholds in `config/config.yaml`

## 📈 Scaling Considerations

### Adding New Data Sources

1. **File Sources**: Add to external stage or create new stage
2. **API Sources**: Add endpoint configuration to `config/config.yaml`
3. **Database Sources**: Use Snowflake connectors or replication

### Adding New Databases

The design supports multiple databases. To add:

```sql
CREATE DATABASE NEW_DATABASE;
-- Replicate schema structure
-- Update configuration files
```

### Horizontal Scaling

- Use multi-cluster warehouses for concurrent workloads
- Partition large tables by date or category
- Use Snowflake's automatic scaling features

## 🔐 Security Best Practices

1. **Use Environment Variables**: Never commit credentials
2. **IAM Roles**: Prefer IAM roles over access keys for cloud storage
3. **Least Privilege**: Grant minimum required permissions
4. **Network Policies**: Configure IP whitelisting if needed
5. **Encryption**: Enable encryption at rest and in transit

## 📝 Maintenance

### Regular Tasks

- Monitor pipeline logs weekly
- Review warehouse usage and costs monthly
- Update validation thresholds as data patterns change
- Review and optimize slow queries

### Backup and Recovery

- Snowflake Time Travel: 1 day retention (configurable)
- Consider longer retention for critical data
- Use `UNDROP` commands to recover accidentally dropped objects

## 🤝 Extending the Pipeline

### Adding New Transformations

1. Create stored procedure in `sql/07_transformation_procedures.sql`
2. Add call to orchestration script
3. Update task definition in `sql/09_setup_tasks.sql`

### Adding New Validations

1. Create validation procedure in `sql/08_validation_procedures.sql`
2. Add to `RUN_ALL_VALIDATIONS` procedure
3. Configure thresholds in `config/config.yaml`

### Integrating with dbt

The pipeline can be extended with dbt:

1. Install dbt-snowflake
2. Create dbt models in `dbt/models/`
3. Call dbt run from orchestration script

## 📚 Additional Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Snowpipe Best Practices](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-best-practices.html)
- [Warehouse Sizing Guide](https://docs.snowflake.com/en/user-guide/warehouses-sizing.html)
- [Cost Optimization](https://docs.snowflake.com/en/user-guide/cost-understanding.html)

## 📄 License

This project is provided as-is for production use.

## 🆘 Support

For issues or questions:
1. Check the troubleshooting section
2. Review Snowflake query history and logs
3. Consult Snowflake documentation

---

**Last Updated**: 2024
**Version**: 1.0.0

# snoqflak
