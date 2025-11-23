"""
Setup Script
Runs all SQL setup scripts in order
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.utils.snowflake_connector import SnowflakeConnector


def run_setup():
    """Run all setup SQL scripts in order"""
    base_dir = Path(__file__).parent.parent.parent
    sql_dir = base_dir / "sql"
    
    # List of SQL files in execution order
    sql_files = [
        "01_setup_warehouse.sql",
        "02_setup_database_schema.sql",
        "03_setup_file_formats.sql",
        "04_setup_external_stage.sql",
        "05_setup_snowpipe.sql",
        "06_setup_validation_tables.sql",
        "07_transformation_procedures.sql",
        "08_validation_procedures.sql",
        "10_orchestration_procedures.sql",
        "09_setup_tasks.sql",
        "11_performance_optimization.sql"  # Optional: Add clustering after data is loaded
    ]
    
    connector = SnowflakeConnector()
    
    try:
        print("Starting Snowflake setup...")
        
        for sql_file in sql_files:
            file_path = sql_dir / sql_file
            if file_path.exists():
                print(f"\nExecuting {sql_file}...")
                connector.execute_file(str(file_path))
                print(f"✓ Completed {sql_file}")
            else:
                print(f"⚠ Warning: {sql_file} not found, skipping...")
        
        print("\n✓ Setup completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Setup failed: {e}")
        sys.exit(1)
    finally:
        connector.close()


if __name__ == "__main__":
    run_setup()

