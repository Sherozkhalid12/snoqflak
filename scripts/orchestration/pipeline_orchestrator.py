"""
Pipeline Orchestrator
Orchestrates end-to-end pipeline execution
"""

import os
import sys
import uuid
from datetime import datetime
from typing import Dict, Optional
import yaml

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.utils.snowflake_connector import SnowflakeConnector
from scripts.ingestion.api_ingestion import APIIngestion
from scripts.ingestion.file_ingestion import FileIngestion


class PipelineOrchestrator:
    """Orchestrates the complete data pipeline"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize pipeline orchestrator"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.snowflake = SnowflakeConnector(config_path)
        self.run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        
    def log_pipeline_start(self, pipeline_name: str) -> None:
        """Log pipeline start"""
        sql = f"""
        INSERT INTO VALIDATION.PIPELINE_LOGS (
            PIPELINE_NAME, RUN_ID, START_TIME, STATUS
        ) VALUES ('{pipeline_name}', '{self.run_id}', CURRENT_TIMESTAMP(), 'IN_PROGRESS')
        """
        conn = self.snowflake.connect()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
    
    def log_pipeline_end(self, pipeline_name: str, status: str, 
                        rows_processed: int = 0, rows_failed: int = 0,
                        error_message: Optional[str] = None) -> None:
        """Log pipeline end"""
        sql = f"""
        UPDATE VALIDATION.PIPELINE_LOGS
        SET END_TIME = CURRENT_TIMESTAMP(),
            STATUS = '{status}',
            ROWS_PROCESSED = {rows_processed},
            ROWS_FAILED = {rows_failed},
            ERROR_MESSAGE = {'NULL' if error_message is None else f"'{error_message.replace(chr(39), chr(39)+chr(39))}'"},
            EXECUTION_TIME_SECONDS = DATEDIFF(SECOND, START_TIME, CURRENT_TIMESTAMP())
        WHERE RUN_ID = '{self.run_id}' AND PIPELINE_NAME = '{pipeline_name}'
        """
        conn = self.snowflake.connect()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
    
    def run_ingestion(self) -> bool:
        """Run data ingestion (files and APIs)"""
        self.log_pipeline_start('INGESTION')
        
        try:
            # API ingestion
            api_ingestion = APIIngestion()
            api_results = api_ingestion.ingest_all_endpoints()
            api_success = all(api_results.values())
            
            # File ingestion is typically handled by Snowpipe automatically
            # But we can check status if needed
            
            self.log_pipeline_end('INGESTION', 'SUCCESS' if api_success else 'FAILED')
            return api_success
            
        except Exception as e:
            self.log_pipeline_end('INGESTION', 'FAILED', error_message=str(e))
            return False
    
    def run_transformation(self) -> bool:
        """Run data transformation"""
        self.log_pipeline_start('TRANSFORMATION')
        
        try:
            conn = self.snowflake.connect()
            cursor = conn.cursor()
            
            # Clean raw data
            cursor.execute(f"""
                CALL CLEANED.CLEAN_RAW_DATA(
                    'RAW.FILE_INGESTION_STAGING',
                    'CLEANED.CLEANED_DATA',
                    '{self.run_id}'
                )
            """)
            
            # Remove duplicates
            cursor.execute(f"""
                CALL CLEANED.REMOVE_DUPLICATES(
                    'CLEANED.CLEANED_DATA',
                    'column1,column2',
                    '{self.run_id}'
                )
            """)
            
            # Apply business transforms
            cursor.execute(f"""
                CALL CLEANED.APPLY_BUSINESS_TRANSFORMS(
                    'CLEANED.CLEANED_DATA',
                    'ANALYTICS.FINAL_DATA',
                    '{self.run_id}'
                )
            """)
            
            conn.commit()
            cursor.close()
            
            self.log_pipeline_end('TRANSFORMATION', 'SUCCESS')
            return True
            
        except Exception as e:
            self.log_pipeline_end('TRANSFORMATION', 'FAILED', error_message=str(e))
            return False
    
    def run_validation(self) -> bool:
        """Run data validation"""
        self.log_pipeline_start('VALIDATION')
        
        try:
            conn = self.snowflake.connect()
            cursor = conn.cursor()
            
            # Run all validations
            cursor.execute(f"""
                CALL VALIDATION.RUN_ALL_VALIDATIONS(
                    'ANALYTICS.FINAL_DATA',
                    '{self.run_id}'
                )
            """)
            
            # Check validation results
            cursor.execute(f"""
                SELECT COUNT(*) as failed_checks
                FROM VALIDATION.VALIDATION_RESULTS
                WHERE RUN_ID = '{self.run_id}'
                AND STATUS = 'FAIL'
            """)
            result = cursor.fetchone()
            failed_checks = result[0] if result else 0
            
            cursor.close()
            
            status = 'SUCCESS' if failed_checks == 0 else 'FAILED'
            self.log_pipeline_end('VALIDATION', status, error_message=f'{failed_checks} validation checks failed' if failed_checks > 0 else None)
            
            return failed_checks == 0
            
        except Exception as e:
            self.log_pipeline_end('VALIDATION', 'FAILED', error_message=str(e))
            return False
    
    def run_full_pipeline(self) -> Dict[str, bool]:
        """Run the complete pipeline end-to-end"""
        print(f"Starting pipeline run: {self.run_id}")
        
        results = {
            'ingestion': False,
            'transformation': False,
            'validation': False
        }
        
        # Step 1: Ingestion
        print("Step 1: Running ingestion...")
        results['ingestion'] = self.run_ingestion()
        if not results['ingestion']:
            print("Ingestion failed. Stopping pipeline.")
            return results
        
        # Step 2: Transformation
        print("Step 2: Running transformation...")
        results['transformation'] = self.run_transformation()
        if not results['transformation']:
            print("Transformation failed. Stopping pipeline.")
            return results
        
        # Step 3: Validation
        print("Step 3: Running validation...")
        results['validation'] = self.run_validation()
        
        # Summary
        all_success = all(results.values())
        print(f"\nPipeline run {self.run_id} completed.")
        print(f"Results: {results}")
        print(f"Overall status: {'SUCCESS' if all_success else 'FAILED'}")
        
        return results
    
    def get_pipeline_status(self, run_id: Optional[str] = None) -> Dict:
        """Get status of pipeline run"""
        run_id = run_id or self.run_id
        
        sql = f"""
        SELECT 
            PIPELINE_NAME,
            STATUS,
            START_TIME,
            END_TIME,
            ROWS_PROCESSED,
            ROWS_FAILED,
            ERROR_MESSAGE,
            EXECUTION_TIME_SECONDS
        FROM VALIDATION.PIPELINE_LOGS
        WHERE RUN_ID = '{run_id}'
        ORDER BY START_TIME
        """
        
        results = self.snowflake.execute_query(sql)
        return results if results else []


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Orchestrate Snowflake data pipeline')
    parser.add_argument('--step', type=str, choices=['ingestion', 'transformation', 'validation', 'all'],
                       default='all', help='Pipeline step to run')
    parser.add_argument('--run-id', type=str, help='Specific run ID to check status')
    
    args = parser.parse_args()
    
    orchestrator = PipelineOrchestrator()
    
    try:
        if args.run_id:
            status = orchestrator.get_pipeline_status(args.run_id)
            print(status)
            return
        
        if args.step == 'all':
            results = orchestrator.run_full_pipeline()
            sys.exit(0 if all(results.values()) else 1)
        elif args.step == 'ingestion':
            success = orchestrator.run_ingestion()
            sys.exit(0 if success else 1)
        elif args.step == 'transformation':
            success = orchestrator.run_transformation()
            sys.exit(0 if success else 1)
        elif args.step == 'validation':
            success = orchestrator.run_validation()
            sys.exit(0 if success else 1)
    finally:
        orchestrator.snowflake.close()


if __name__ == "__main__":
    main()

