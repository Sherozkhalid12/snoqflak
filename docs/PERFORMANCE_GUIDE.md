# Performance and Cost Optimization Guide

This guide provides specific best practices for optimizing the Snowflake data pipeline for performance, cost, and reliability.

## Warehouse Configuration

### Auto-Suspend and Auto-Resume

The warehouse is configured with:
- **AUTO_SUSPEND = 60 seconds**: Suspends after 60 seconds of inactivity
- **AUTO_RESUME = TRUE**: Automatically resumes when queries are submitted

**Benefits**:
- Reduces costs by suspending idle warehouses
- No manual intervention required
- Seamless operation for end users

**Monitoring**:
```sql
SELECT 
    WAREHOUSE_NAME,
    STATE,
    AUTO_SUSPEND,
    AUTO_RESUME,
    CREATED_ON,
    RESUMED_ON,
    SUSPENDED_ON
FROM SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSES
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH';
```

### Warehouse Sizing

**Guidelines**:
- **X-SMALL**: Development and testing (< 1M rows)
- **SMALL**: Small production workloads (1M - 10M rows)
- **MEDIUM**: Medium workloads (10M - 100M rows)
- **LARGE+**: Large-scale production (100M+ rows)

**Scaling Strategy**:
1. Start with SMALL
2. Monitor query performance
3. Scale up if queries exceed SLA
4. Scale down during off-peak hours

**Dynamic Scaling**:
```sql
-- Scale up for large transformation
ALTER WAREHOUSE DATA_PIPELINE_WH SET WAREHOUSE_SIZE = 'LARGE';

-- Scale down after completion
ALTER WAREHOUSE DATA_PIPELINE_WH SET WAREHOUSE_SIZE = 'SMALL';
```

### Multi-Cluster Warehouses

For concurrent workloads, enable multi-cluster:

```sql
ALTER WAREHOUSE DATA_PIPELINE_WH SET
    MIN_CLUSTER_COUNT = 1,
    MAX_CLUSTER_COUNT = 3,
    SCALING_POLICY = 'STANDARD';
```

## Result Set Caching

Snowflake automatically caches query results for 24 hours.

**Best Practices**:
- Reuse identical queries within cache window
- Structure queries to maximize cache hits
- Monitor cache usage

**Check Cache Usage**:
```sql
SELECT 
    QUERY_TEXT,
    RESULT_SCAN,
    BYTES_SCANNED,
    CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH'
ORDER BY START_TIME DESC
LIMIT 100;
```

## Table Clustering

### When to Use Clustering

- Tables > 1GB
- Frequent filtering on specific columns
- Date-based queries
- High cardinality columns

### Adding Clustering Keys

```sql
-- Example: Cluster by date and category
ALTER TABLE ANALYTICS.FINAL_DATA CLUSTER BY (
    DATE_TRUNC('DAY', ingestion_timestamp),
    category
);

-- Check clustering effectiveness
SELECT SYSTEM$CLUSTERING_INFORMATION('ANALYTICS.FINAL_DATA', '(ingestion_timestamp, category)');
```

### Monitoring Clustering

```sql
SELECT 
    TABLE_NAME,
    CLUSTERING_KEY,
    CLUSTER_BY_KEY,
    ROW_COUNT,
    BYTES
FROM SNOWFLAKE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ANALYTICS'
AND CLUSTERING_KEY IS NOT NULL;
```

## Query Optimization

### Best Practices

1. **Use SELECT * Sparingly**: Select only needed columns
2. **Filter Early**: Apply WHERE clauses before JOINs
3. **Use Appropriate Data Types**: Avoid VARCHAR for numeric data
4. **Partition Large Tables**: Use date-based partitioning
5. **Avoid Functions in WHERE Clauses**: Pre-compute values

### Query Profiling

```sql
-- Enable query profiling
ALTER SESSION SET QUERY_TAG = 'pipeline_transformation';

-- Review query profile in Snowflake UI
-- Or query history:
SELECT 
    QUERY_TEXT,
    EXECUTION_TIME,
    BYTES_SCANNED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TAG = 'pipeline_transformation'
ORDER BY EXECUTION_TIME DESC;
```

## Cost Optimization

### Credit Usage Monitoring

```sql
-- Daily credit usage
SELECT 
    DATE(START_TIME) as USAGE_DATE,
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) as TOTAL_CREDITS,
    AVG(CREDITS_USED) as AVG_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH'
AND START_TIME >= DATEADD('days', -30, CURRENT_DATE())
GROUP BY DATE(START_TIME), WAREHOUSE_NAME
ORDER BY USAGE_DATE DESC;
```

### Cost Reduction Strategies

1. **Right-Size Warehouses**: Use smallest size that meets SLA
2. **Optimize Auto-Suspend**: Balance between cost and performance
3. **Batch Processing**: Group operations to reduce warehouse starts
4. **Use Result Cache**: Avoid re-running identical queries
5. **Time Travel**: Reduce retention period if not needed
6. **Data Compression**: Use appropriate file formats (Parquet)

### Storage Costs

```sql
-- Monitor storage usage
SELECT 
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    BYTES,
    ROW_COUNT,
    BYTES / (1024*1024*1024) as SIZE_GB
FROM SNOWFLAKE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('RAW', 'CLEANED', 'ANALYTICS')
ORDER BY BYTES DESC;
```

## Performance Monitoring

### Key Metrics

1. **Query Execution Time**: Track against SLAs
2. **Warehouse Utilization**: Monitor active vs idle time
3. **Data Freshness**: Ensure data loads on schedule
4. **Validation Pass Rate**: Track data quality

### Monitoring Queries

```sql
-- Pipeline performance summary
SELECT 
    PIPELINE_NAME,
    STATUS,
    AVG(EXECUTION_TIME_SECONDS) as AVG_EXECUTION_TIME,
    AVG(ROWS_PROCESSED) as AVG_ROWS_PROCESSED,
    COUNT(*) as RUN_COUNT,
    SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) as FAILURE_COUNT
FROM VALIDATION.PIPELINE_LOGS
WHERE START_TIME >= DATEADD('days', -7, CURRENT_TIMESTAMP())
GROUP BY PIPELINE_NAME, STATUS
ORDER BY PIPELINE_NAME, STATUS;

-- Warehouse activity
SELECT 
    DATE_TRUNC('hour', START_TIME) as HOUR,
    COUNT(*) as QUERY_COUNT,
    SUM(CREDITS_USED) as CREDITS,
    AVG(EXECUTION_TIME) as AVG_EXECUTION_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE WAREHOUSE_NAME = 'DATA_PIPELINE_WH'
AND START_TIME >= DATEADD('days', -7, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('hour', START_TIME)
ORDER BY HOUR DESC;
```

## Reliability Best Practices

### Error Handling

- All procedures include try-catch blocks
- Errors are logged to VALIDATION.PIPELINE_LOGS
- Pipeline stops on critical failures

### Data Validation

- Row count checks prevent empty loads
- Null percentage checks ensure data quality
- Freshness checks ensure timely updates

### Retry Logic

- API ingestion includes retry logic
- Snowpipe automatically retries failed files
- Tasks can be configured with retry policies

## Scaling for Volume Spikes

### Vertical Scaling

```sql
-- Scale up warehouse
ALTER WAREHOUSE DATA_PIPELINE_WH SET WAREHOUSE_SIZE = 'X-LARGE';

-- Scale down after spike
ALTER WAREHOUSE DATA_PIPELINE_WH SET WAREHOUSE_SIZE = 'SMALL';
```

### Horizontal Scaling

```sql
-- Enable multi-cluster
ALTER WAREHOUSE DATA_PIPELINE_WH SET
    MAX_CLUSTER_COUNT = 5,
    SCALING_POLICY = 'ECONOMY';  -- Or 'STANDARD' for faster scaling
```

### Batch Processing

- Process data in batches to avoid timeouts
- Use Snowflake's automatic query timeout
- Monitor query duration and adjust batch size

## SLA Guidelines

### Recommended SLAs

- **Ingestion**: < 5 minutes for files, < 1 minute for APIs
- **Transformation**: < 15 minutes for typical volumes
- **Validation**: < 2 minutes
- **End-to-End**: < 30 minutes total

### Monitoring SLA Compliance

```sql
SELECT 
    PIPELINE_NAME,
    RUN_ID,
    EXECUTION_TIME_SECONDS,
    CASE 
        WHEN EXECUTION_TIME_SECONDS <= 1800 THEN 'WITHIN_SLA'
        ELSE 'EXCEEDS_SLA'
    END as SLA_STATUS
FROM VALIDATION.PIPELINE_LOGS
WHERE STATUS = 'SUCCESS'
AND START_TIME >= DATEADD('days', -7, CURRENT_TIMESTAMP())
ORDER BY EXECUTION_TIME_SECONDS DESC;
```

## Troubleshooting Performance Issues

### Slow Queries

1. Check query profile in Snowflake UI
2. Review warehouse size and utilization
3. Verify clustering keys are effective
4. Check for full table scans

### High Costs

1. Review warehouse usage patterns
2. Check for unnecessary warehouse starts
3. Verify auto-suspend is working
4. Review storage costs

### Pipeline Failures

1. Check VALIDATION.PIPELINE_LOGS for errors
2. Review validation results
3. Verify data source availability
4. Check warehouse availability

---

**Remember**: Performance optimization is iterative. Monitor, measure, adjust, and repeat.

