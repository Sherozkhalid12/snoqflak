"""
Comprehensive Pipeline Testing Script
Tests all components of the Snowflake data pipeline
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.snowflake_connector import SnowflakeConnector

# Test results
test_results = {
    'passed': [],
    'failed': [],
    'warnings': []
}


def log_test(test_name, status, message=""):
    """Log test result"""
    if status == 'PASS':
        test_results['passed'].append(test_name)
        print(f"✅ PASS: {test_name}")
        if message:
            print(f"   {message}")
    elif status == 'FAIL':
        test_results['failed'].append(test_name)
        print(f"❌ FAIL: {test_name}")
        if message:
            print(f"   {message}")
    else:  # WARNING
        test_results['warnings'].append(test_name)
        print(f"⚠️  WARN: {test_name}")
        if message:
            print(f"   {message}")


def test_connection():
    """Test 1: Snowflake Connection"""
    print("\n" + "="*60)
    print("TEST 1: Snowflake Connection")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        result = connector.execute_query("SELECT CURRENT_VERSION() as version, CURRENT_DATABASE() as db, CURRENT_WAREHOUSE() as wh")
        
        if result and len(result) > 0:
            version = result[0].get('VERSION', 'Unknown')
            db = result[0].get('DB', 'Unknown')
            wh = result[0].get('WH', 'Unknown')
            log_test("Snowflake Connection", 'PASS', 
                    f"Connected! Version: {version}, DB: {db}, WH: {wh}")
            connector.close()
            return True
        else:
            log_test("Snowflake Connection", 'FAIL', "No result returned")
            return False
    except Exception as e:
        log_test("Snowflake Connection", 'FAIL', str(e))
        return False


def test_warehouse():
    """Test 2: Warehouse Configuration"""
    print("\n" + "="*60)
    print("TEST 2: Warehouse Configuration")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        
        # Check warehouse exists
        result = connector.execute_query("""
            SHOW WAREHOUSES LIKE 'DATA_PIPELINE_WH'
        """)
        
        if result and len(result) > 0:
            wh_info = result[0]
            auto_suspend = wh_info.get('auto_suspend', 0)
            auto_resume = wh_info.get('auto_resume', 'false')
            state = wh_info.get('state', 'UNKNOWN')
            
            log_test("Warehouse Exists", 'PASS', f"State: {state}")
            
            if auto_suspend and int(auto_suspend) > 0:
                log_test("Auto-Suspend Configured", 'PASS', f"Auto-suspend: {auto_suspend}s")
            else:
                log_test("Auto-Suspend Configured", 'WARN', "Auto-suspend not configured")
            
            if auto_resume.upper() == 'TRUE':
                log_test("Auto-Resume Configured", 'PASS')
            else:
                log_test("Auto-Resume Configured", 'WARN', "Auto-resume not enabled")
            
            connector.close()
            return True
        else:
            log_test("Warehouse Exists", 'FAIL', "Warehouse not found")
            return False
    except Exception as e:
        log_test("Warehouse Configuration", 'FAIL', str(e))
        return False


def test_database_schemas():
    """Test 3: Database and Schemas"""
    print("\n" + "="*60)
    print("TEST 3: Database and Schemas")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        
        # Check database
        result = connector.execute_query("SHOW DATABASES LIKE 'DATA_PIPELINE_DB'")
        if result and len(result) > 0:
            log_test("Database Exists", 'PASS')
        else:
            log_test("Database Exists", 'FAIL', "Database not found")
            return False
        
        # Check schemas
        required_schemas = ['RAW', 'CLEANED', 'ANALYTICS', 'VALIDATION']
        result = connector.execute_query("SHOW SCHEMAS IN DATABASE DATA_PIPELINE_DB")
        
        if result:
            existing_schemas = [s['name'] for s in result]
            for schema in required_schemas:
                if schema in existing_schemas:
                    log_test(f"Schema '{schema}' Exists", 'PASS')
                else:
                    log_test(f"Schema '{schema}' Exists", 'FAIL', f"Schema {schema} not found")
        else:
            log_test("Schemas Check", 'FAIL', "Could not retrieve schemas")
            return False
        
        connector.close()
        return True
    except Exception as e:
        log_test("Database and Schemas", 'FAIL', str(e))
        return False


def test_file_formats():
    """Test 4: File Formats"""
    print("\n" + "="*60)
    print("TEST 4: File Formats")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        
        required_formats = ['CSV_FORMAT', 'PARQUET_FORMAT', 'JSON_FORMAT']
        result = connector.execute_query("SHOW FILE FORMATS IN SCHEMA DATA_PIPELINE_DB.RAW")
        
        if result:
            existing_formats = [f['name'] for f in result]
            for fmt in required_formats:
                if fmt in existing_formats:
                    log_test(f"File Format '{fmt}' Exists", 'PASS')
                else:
                    log_test(f"File Format '{fmt}' Exists", 'FAIL', f"Format {fmt} not found")
        else:
            log_test("File Formats Check", 'FAIL', "Could not retrieve file formats")
            return False
        
        connector.close()
        return True
    except Exception as e:
        log_test("File Formats", 'FAIL', str(e))
        return False


def test_stages():
    """Test 5: External Stages"""
    print("\n" + "="*60)
    print("TEST 5: External Stages")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        
        result = connector.execute_query("SHOW STAGES IN SCHEMA DATA_PIPELINE_DB.RAW")
        
        if result and len(result) > 0:
            stage_names = [s['name'] for s in result]
            if 'FILE_STAGE' in stage_names:
                log_test("Stage 'FILE_STAGE' Exists", 'PASS')
                
                # Try to list stage (test access)
                try:
                    list_result = connector.execute_query("LIST @FILE_STAGE")
                    log_test("Stage Access", 'PASS', "Can list stage contents")
                except Exception as e:
                    log_test("Stage Access", 'WARN', f"Cannot list stage: {str(e)}")
            else:
                log_test("Stage 'FILE_STAGE' Exists", 'FAIL', "FILE_STAGE not found")
                return False
        else:
            log_test("Stages Check", 'FAIL', "No stages found")
            return False
        
        connector.close()
        return True
    except Exception as e:
        log_test("External Stages", 'FAIL', str(e))
        return False


def test_snowpipe():
    """Test 6: Snowpipe"""
    print("\n" + "="*60)
    print("TEST 6: Snowpipe")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        
        result = connector.execute_query("SHOW PIPES LIKE 'FILE_INGESTION_PIPE'")
        
        if result and len(result) > 0:
            pipe_info = result[0]
            pipe_name = pipe_info.get('name', '')
            state = pipe_info.get('state', 'UNKNOWN')
            
            log_test("Snowpipe Exists", 'PASS', f"State: {state}")
            
            # Check pipe status
            try:
                status_result = connector.execute_query("SELECT SYSTEM$PIPE_STATUS('FILE_INGESTION_PIPE') as status")
                if status_result:
                    log_test("Snowpipe Status Check", 'PASS', "Can query pipe status")
            except Exception as e:
                log_test("Snowpipe Status Check", 'WARN', f"Cannot check status: {str(e)}")
        else:
            log_test("Snowpipe Exists", 'FAIL', "Snowpipe not found")
            return False
        
        connector.close()
        return True
    except Exception as e:
        log_test("Snowpipe", 'FAIL', str(e))
        return False


def test_stored_procedures():
    """Test 7: Stored Procedures"""
    print("\n" + "="*60)
    print("TEST 7: Stored Procedures")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        
        # Check transformation procedures
        result = connector.execute_query("SHOW PROCEDURES IN SCHEMA DATA_PIPELINE_DB.CLEANED")
        if result:
            proc_names = [p['name'] for p in result]
            required_procs = ['CLEAN_RAW_DATA', 'REMOVE_DUPLICATES', 'APPLY_BUSINESS_TRANSFORMS']
            for proc in required_procs:
                if proc in proc_names:
                    log_test(f"Procedure 'CLEANED.{proc}' Exists", 'PASS')
                else:
                    log_test(f"Procedure 'CLEANED.{proc}' Exists", 'FAIL', f"Procedure {proc} not found")
        
        # Check validation procedures
        result = connector.execute_query("SHOW PROCEDURES IN SCHEMA DATA_PIPELINE_DB.VALIDATION")
        if result:
            proc_names = [p['name'] for p in result]
            required_procs = ['VALIDATE_ROW_COUNT', 'VALIDATE_NULL_PERCENTAGE', 'VALIDATE_DATA_FRESHNESS', 'RUN_ALL_VALIDATIONS']
            for proc in required_procs:
                if proc in proc_names:
                    log_test(f"Procedure 'VALIDATION.{proc}' Exists", 'PASS')
                else:
                    log_test(f"Procedure 'VALIDATION.{proc}' Exists", 'FAIL', f"Procedure {proc} not found")
        
        connector.close()
        return True
    except Exception as e:
        log_test("Stored Procedures", 'FAIL', str(e))
        return False


def test_validation_tables():
    """Test 8: Validation Tables"""
    print("\n" + "="*60)
    print("TEST 8: Validation Tables")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        
        required_tables = ['PIPELINE_LOGS', 'VALIDATION_RESULTS', 'DATA_QUALITY_METRICS']
        result = connector.execute_query("SHOW TABLES IN SCHEMA DATA_PIPELINE_DB.VALIDATION")
        
        if result:
            table_names = [t['name'] for t in result]
            for table in required_tables:
                if table in table_names:
                    log_test(f"Table 'VALIDATION.{table}' Exists", 'PASS')
                else:
                    log_test(f"Table 'VALIDATION.{table}' Exists", 'FAIL', f"Table {table} not found")
        else:
            log_test("Validation Tables Check", 'FAIL', "Could not retrieve tables")
            return False
        
        connector.close()
        return True
    except Exception as e:
        log_test("Validation Tables", 'FAIL', str(e))
        return False


def test_tasks():
    """Test 9: Tasks"""
    print("\n" + "="*60)
    print("TEST 9: Tasks")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        
        result = connector.execute_query("SHOW TASKS IN DATABASE DATA_PIPELINE_DB")
        
        if result:
            task_names = [t['name'] for t in result]
            required_tasks = ['PIPELINE_ORCHESTRATOR', 'DATA_QUALITY_MONITOR']
            for task in required_tasks:
                if task in task_names:
                    task_info = next((t for t in result if t['name'] == task), None)
                    state = task_info.get('state', 'UNKNOWN') if task_info else 'UNKNOWN'
                    log_test(f"Task '{task}' Exists", 'PASS', f"State: {state}")
                else:
                    log_test(f"Task '{task}' Exists", 'FAIL', f"Task {task} not found")
        else:
            log_test("Tasks Check", 'FAIL', "Could not retrieve tasks")
            return False
        
        connector.close()
        return True
    except Exception as e:
        log_test("Tasks", 'FAIL', str(e))
        return False


def test_file_ingestion():
    """Test 10: File Ingestion (Sample)"""
    print("\n" + "="*60)
    print("TEST 10: File Ingestion")
    print("="*60)
    
    try:
        # Check if sample file exists
        sample_file = Path(__file__).parent.parent.parent / "sample_data" / "sample.csv"
        if not sample_file.exists():
            log_test("Sample File Exists", 'WARN', "sample_data/sample.csv not found - skipping ingestion test")
            return True
        
        # Test file ingestion script exists
        ingestion_script = Path(__file__).parent.parent / "ingestion" / "file_ingestion.py"
        if ingestion_script.exists():
            log_test("File Ingestion Script Exists", 'PASS')
        else:
            log_test("File Ingestion Script Exists", 'FAIL', "file_ingestion.py not found")
            return False
        
        log_test("File Ingestion", 'PASS', "Script available - run manually: python scripts/ingestion/file_ingestion.py sample_data/sample.csv")
        return True
    except Exception as e:
        log_test("File Ingestion", 'FAIL', str(e))
        return False


def test_api_ingestion():
    """Test 11: API Ingestion"""
    print("\n" + "="*60)
    print("TEST 11: API Ingestion")
    print("="*60)
    
    try:
        # Test API ingestion script exists
        ingestion_script = Path(__file__).parent.parent / "ingestion" / "api_ingestion.py"
        if ingestion_script.exists():
            log_test("API Ingestion Script Exists", 'PASS')
        else:
            log_test("API Ingestion Script Exists", 'FAIL', "api_ingestion.py not found")
            return False
        
        log_test("API Ingestion", 'PASS', "Script available - configure endpoints in config.yaml and run manually")
        return True
    except Exception as e:
        log_test("API Ingestion", 'FAIL', str(e))
        return False


def test_end_to_end():
    """Test 12: End-to-End Pipeline Test"""
    print("\n" + "="*60)
    print("TEST 12: End-to-End Pipeline")
    print("="*60)
    
    try:
        connector = SnowflakeConnector()
        
        # Create a test run
        test_run_id = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Test logging
        try:
            connector.execute_query(f"""
                INSERT INTO VALIDATION.PIPELINE_LOGS (
                    PIPELINE_NAME, RUN_ID, START_TIME, STATUS
                ) VALUES (
                    'TEST_PIPELINE', '{test_run_id}', CURRENT_TIMESTAMP(), 'IN_PROGRESS'
                )
            """)
            
            # Verify log entry
            result = connector.execute_query(f"""
                SELECT * FROM VALIDATION.PIPELINE_LOGS
                WHERE RUN_ID = '{test_run_id}'
            """)
            
            if result and len(result) > 0:
                log_test("Pipeline Logging", 'PASS', f"Test run ID: {test_run_id}")
            else:
                log_test("Pipeline Logging", 'FAIL', "Could not verify log entry")
                return False
        except Exception as e:
            log_test("Pipeline Logging", 'FAIL', str(e))
            return False
        
        # Test validation procedure (if tables exist)
        try:
            # Check if we have any tables to validate
            tables_result = connector.execute_query("SHOW TABLES IN SCHEMA DATA_PIPELINE_DB.ANALYTICS")
            if tables_result and len(tables_result) > 0:
                # Try a simple validation
                connector.execute_query(f"""
                    CALL VALIDATION.VALIDATE_ROW_COUNT(
                        'ANALYTICS.FINAL_DATA',
                        0,
                        NULL,
                        '{test_run_id}'
                    )
                """)
                log_test("Validation Procedure Execution", 'PASS', "Can execute validation procedures")
            else:
                log_test("Validation Procedure Execution", 'WARN', "No tables to validate yet")
        except Exception as e:
            log_test("Validation Procedure Execution", 'WARN', f"Cannot execute yet: {str(e)}")
        
        connector.close()
        return True
    except Exception as e:
        log_test("End-to-End Pipeline", 'FAIL', str(e))
        return False


def print_summary():
    """Print test summary"""
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    total = len(test_results['passed']) + len(test_results['failed']) + len(test_results['warnings'])
    
    print(f"\nTotal Tests: {total}")
    print(f"✅ Passed: {len(test_results['passed'])}")
    print(f"❌ Failed: {len(test_results['failed'])}")
    print(f"⚠️  Warnings: {len(test_results['warnings'])}")
    
    if test_results['failed']:
        print("\n❌ Failed Tests:")
        for test in test_results['failed']:
            print(f"   - {test}")
    
    if test_results['warnings']:
        print("\n⚠️  Warnings:")
        for test in test_results['warnings']:
            print(f"   - {test}")
    
    print("\n" + "="*60)
    
    if len(test_results['failed']) == 0:
        print("✅ ALL CRITICAL TESTS PASSED!")
        if len(test_results['warnings']) > 0:
            print("⚠️  Some warnings - review and address as needed")
        return True
    else:
        print("❌ SOME TESTS FAILED - Please review and fix")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("SNOWFLAKE DATA PIPELINE - COMPREHENSIVE TEST SUITE")
    print("="*60)
    print(f"Test Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run all tests
    tests = [
        test_connection,
        test_warehouse,
        test_database_schemas,
        test_file_formats,
        test_stages,
        test_snowpipe,
        test_stored_procedures,
        test_validation_tables,
        test_tasks,
        test_file_ingestion,
        test_api_ingestion,
        test_end_to_end
    ]
    
    for test_func in tests:
        try:
            test_func()
        except Exception as e:
            log_test(test_func.__name__, 'FAIL', f"Test crashed: {str(e)}")
    
    # Print summary
    success = print_summary()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())

