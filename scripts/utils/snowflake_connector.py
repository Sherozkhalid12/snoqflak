"""
Snowflake Connection Utility
Handles connection management and common operations
"""

import os
import snowflake.connector
from snowflake.connector import DictCursor
from typing import Dict, Optional, List
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class SnowflakeConnector:
    """Manages Snowflake connections and provides helper methods"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize Snowflake connector with configuration"""
        self.config = self._load_config(config_path)
        self.connection = None
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """Establish connection to Snowflake"""
        if self.connection is None or self.connection.is_closed():
            self.connection = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT') or self.config['snowflake']['account'],
                user=os.getenv('SNOWFLAKE_USER') or self.config['snowflake']['user'],
                password=os.getenv('SNOWFLAKE_PASSWORD') or self.config['snowflake']['password'],
                warehouse=self.config['snowflake']['warehouse'],
                database=self.config['snowflake']['database'],
                schema=self.config['snowflake']['schema'],
                role=self.config['snowflake']['role']
            )
        return self.connection
    
    def execute_query(self, query: str, fetch: bool = True) -> Optional[List[Dict]]:
        """Execute a SQL query and return results"""
        conn = self.connect()
        cursor = conn.cursor(DictCursor)
        
        try:
            cursor.execute(query)
            if fetch:
                return cursor.fetchall()
            return None
        finally:
            cursor.close()
    
    def execute_file(self, file_path: str) -> None:
        """Execute SQL commands from a file"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            with open(file_path, 'r') as f:
                sql = f.read()
                # Split by semicolon and execute each statement
                for statement in sql.split(';'):
                    statement = statement.strip()
                    if statement:
                        cursor.execute(statement)
            conn.commit()
        finally:
            cursor.close()
    
    def close(self) -> None:
        """Close the connection"""
        if self.connection and not self.connection.is_closed():
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

