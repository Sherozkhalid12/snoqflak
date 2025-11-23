-- ============================================================================
-- Snowflake Warehouse Setup Script
-- ============================================================================
-- This script creates and configures the data pipeline warehouse with
-- performance and cost optimization settings.
-- ============================================================================

-- Set context
USE ROLE ACCOUNTADMIN;

-- Create warehouse with auto-suspend/resume for cost optimization
CREATE WAREHOUSE IF NOT EXISTS DATA_PIPELINE_WH
WITH
    WAREHOUSE_SIZE = 'SMALL'  -- Start small, scale up as needed
    AUTO_SUSPEND = 60  -- Suspend after 60 seconds of inactivity
    AUTO_RESUME = TRUE  -- Automatically resume when queries are submitted
    INITIALLY_SUSPENDED = TRUE  -- Start suspended to save costs
    COMMENT = 'Data pipeline warehouse with auto-suspend enabled for cost optimization';

-- Grant usage to appropriate roles
GRANT USAGE ON WAREHOUSE DATA_PIPELINE_WH TO ROLE SYSADMIN;
GRANT OPERATE ON WAREHOUSE DATA_PIPELINE_WH TO ROLE SYSADMIN;

-- Show warehouse configuration
SHOW WAREHOUSES LIKE 'DATA_PIPELINE_WH';

-- ============================================================================
-- Performance Notes:
-- - AUTO_SUSPEND: Reduces costs by suspending warehouse when idle
-- - AUTO_RESUME: Ensures seamless operation without manual intervention
-- - WAREHOUSE_SIZE: Start with SMALL, monitor performance, scale up if needed
-- - Consider using multi-cluster warehouses for concurrent workloads
-- ============================================================================

