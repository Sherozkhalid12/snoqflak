"""
File Ingestion Script
Handles file uploads and triggers Snowpipe ingestion
"""

import os
import sys
import boto3
from pathlib import Path
from typing import Optional
import yaml
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.snowflake_connector import SnowflakeConnector

load_dotenv()


class FileIngestion:
    """Handles file ingestion into Snowflake"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize file ingestion with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.snowflake = SnowflakeConnector(config_path)
        self.file_config = self.config.get('file_ingestion', {})
        
    def upload_to_stage(self, local_file_path: str, stage_name: str = "FILE_STAGE") -> bool:
        """Upload file to Snowflake stage"""
        try:
            conn = self.snowflake.connect()
            cursor = conn.cursor()
            
            file_name = os.path.basename(local_file_path)
            
            # Upload file to stage
            upload_sql = f"PUT file://{local_file_path} @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cursor.execute(upload_sql)
            
            cursor.close()
            print(f"Successfully uploaded {file_name} to stage {stage_name}")
            return True
            
        except Exception as e:
            print(f"Error uploading file to stage: {e}")
            return False
    
    def trigger_snowpipe(self, pipe_name: str = "FILE_INGESTION_PIPE") -> bool:
        """Manually trigger Snowpipe refresh (if auto-ingest is disabled)"""
        try:
            conn = self.snowflake.connect()
            cursor = conn.cursor()
            
            refresh_sql = f"ALTER PIPE {pipe_name} REFRESH"
            cursor.execute(refresh_sql)
            
            cursor.close()
            print(f"Triggered Snowpipe refresh for {pipe_name}")
            return True
            
        except Exception as e:
            print(f"Error triggering Snowpipe: {e}")
            return False
    
    def check_pipe_status(self, pipe_name: str = "FILE_INGESTION_PIPE") -> Optional[dict]:
        """Check Snowpipe status"""
        try:
            conn = self.snowflake.connect()
            cursor = conn.cursor()
            
            status_sql = f"SELECT SYSTEM$PIPE_STATUS('{pipe_name}')"
            cursor.execute(status_sql)
            result = cursor.fetchone()
            
            cursor.close()
            return result[0] if result else None
            
        except Exception as e:
            print(f"Error checking pipe status: {e}")
            return None
    
    def upload_to_s3(self, local_file_path: str, bucket: str, s3_key: str) -> bool:
        """Upload file to S3 (for Snowpipe auto-ingest)"""
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            
            s3_client.upload_file(local_file_path, bucket, s3_key)
            print(f"Successfully uploaded {local_file_path} to s3://{bucket}/{s3_key}")
            return True
            
        except Exception as e:
            print(f"Error uploading file to S3: {e}")
            return False


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Ingest files into Snowflake')
    parser.add_argument('file_path', type=str, help='Path to file to ingest')
    parser.add_argument('--stage', type=str, default='FILE_STAGE', help='Stage name')
    parser.add_argument('--trigger-pipe', action='store_true', help='Trigger Snowpipe refresh')
    parser.add_argument('--s3-bucket', type=str, help='S3 bucket name (for S3 upload)')
    parser.add_argument('--s3-key', type=str, help='S3 key/path')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.file_path):
        print(f"File not found: {args.file_path}")
        sys.exit(1)
    
    ingestion = FileIngestion()
    
    try:
        success = False
        
        if args.s3_bucket and args.s3_key:
            # Upload to S3 (triggers auto-ingest if configured)
            success = ingestion.upload_to_s3(args.file_path, args.s3_bucket, args.s3_key)
        else:
            # Upload to Snowflake stage
            success = ingestion.upload_to_stage(args.file_path, args.stage)
            
            if success and args.trigger_pipe:
                ingestion.trigger_snowpipe()
        
        sys.exit(0 if success else 1)
    finally:
        ingestion.snowflake.close()


if __name__ == "__main__":
    main()

