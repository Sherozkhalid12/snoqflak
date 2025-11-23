"""
API Data Ingestion Script
Fetches data from APIs and loads into Snowflake
"""

import os
import sys
import requests
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import yaml
from dotenv import load_dotenv
import time
import uuid

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.snowflake_connector import SnowflakeConnector

load_dotenv()


class APIIngestion:
    """Handles API data ingestion into Snowflake"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize API ingestion with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.snowflake = SnowflakeConnector(config_path)
        self.api_config = self.config.get('api_ingestion', {})
        
    def _get_auth_headers(self, endpoint_config: Dict) -> Dict:
        """Get authentication headers based on auth type"""
        headers = endpoint_config.get('headers', {}).copy()
        
        auth_type = endpoint_config.get('auth_type', 'none')
        
        if auth_type == 'bearer':
            token = os.getenv('API_BEARER_TOKEN') or endpoint_config.get('bearer_token', '')
            headers['Authorization'] = f'Bearer {token}'
        elif auth_type == 'api_key':
            api_key = os.getenv('API_KEY') or endpoint_config.get('api_key', '')
            headers['X-API-Key'] = api_key
        elif auth_type == 'basic':
            # Basic auth would be handled via requests.auth
            pass
        
        return headers
    
    def _fetch_api_data(self, endpoint_config: Dict) -> Optional[List[Dict]]:
        """Fetch data from API endpoint"""
        url = endpoint_config['url']
        method = endpoint_config.get('method', 'GET')
        headers = self._get_auth_headers(endpoint_config)
        params = endpoint_config.get('params', {})
        
        rate_limit = self.api_config.get('rate_limit', {})
        max_retries = rate_limit.get('retry_attempts', 3)
        retry_delay = rate_limit.get('retry_delay', 5)
        
        for attempt in range(max_retries):
            try:
                # Rate limiting
                requests_per_second = rate_limit.get('requests_per_second', 10)
                time.sleep(1.0 / requests_per_second)
                
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                # Parse JSON response
                data = response.json()
                
                # Handle different response formats
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    # Try common keys
                    if 'data' in data:
                        return data['data'] if isinstance(data['data'], list) else [data['data']]
                    elif 'results' in data:
                        return data['results'] if isinstance(data['results'], list) else [data['results']]
                    else:
                        return [data]
                else:
                    return [{'raw_data': str(data)}]
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(f"Failed to fetch data from {url} after {max_retries} attempts: {e}")
                    return None
        
        return None
    
    def _load_to_snowflake(self, data: List[Dict], target_table: str, run_id: str) -> bool:
        """Load API data into Snowflake table"""
        if not data:
            print("No data to load")
            return False
        
        try:
            conn = self.snowflake.connect()
            cursor = conn.cursor()
            
            # Convert to DataFrame for easier handling
            df = pd.DataFrame(data)
            
            # Add metadata columns
            df['ingestion_timestamp'] = datetime.now()
            df['run_id'] = run_id
            df['source'] = 'API'
            
            # Create table if it doesn't exist
            # This is a simplified version - adjust based on your schema
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                data VARIANT,
                ingestion_timestamp TIMESTAMP_NTZ,
                run_id VARCHAR(255),
                source VARCHAR(50)
            )
            """
            cursor.execute(create_table_sql)
            
            # Insert data
            for _, row in df.iterrows():
                insert_sql = f"""
                INSERT INTO {target_table} (data, ingestion_timestamp, run_id, source)
                VALUES (PARSE_JSON(%s), %s, %s, %s)
                """
                cursor.execute(insert_sql, (
                    json.dumps(row.to_dict()),
                    row['ingestion_timestamp'],
                    row['run_id'],
                    row['source']
                ))
            
            conn.commit()
            cursor.close()
            
            print(f"Successfully loaded {len(data)} rows into {target_table}")
            return True
            
        except Exception as e:
            print(f"Error loading data to Snowflake: {e}")
            return False
    
    def ingest_endpoint(self, endpoint_name: str) -> bool:
        """Ingest data from a specific API endpoint"""
        endpoints = self.api_config.get('endpoints', [])
        endpoint_config = next(
            (ep for ep in endpoints if ep['name'] == endpoint_name and ep.get('enabled', True)),
            None
        )
        
        if not endpoint_config:
            print(f"Endpoint {endpoint_name} not found or disabled")
            return False
        
        print(f"Ingesting data from {endpoint_name}...")
        
        # Generate run ID
        run_id = f"API_{endpoint_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Fetch data
        data = self._fetch_api_data(endpoint_config)
        
        if data is None:
            return False
        
        # Load to Snowflake
        target_table = endpoint_config['target_table']
        success = self._load_to_snowflake(data, target_table, run_id)
        
        return success
    
    def ingest_all_endpoints(self) -> Dict[str, bool]:
        """Ingest data from all enabled API endpoints"""
        results = {}
        endpoints = self.api_config.get('endpoints', [])
        
        for endpoint in endpoints:
            if endpoint.get('enabled', True):
                endpoint_name = endpoint['name']
                results[endpoint_name] = self.ingest_endpoint(endpoint_name)
        
        return results


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Ingest data from APIs into Snowflake')
    parser.add_argument('--endpoint', type=str, help='Specific endpoint to ingest (optional)')
    parser.add_argument('--all', action='store_true', help='Ingest all enabled endpoints')
    
    args = parser.parse_args()
    
    ingestion = APIIngestion()
    
    try:
        if args.endpoint:
            success = ingestion.ingest_endpoint(args.endpoint)
            sys.exit(0 if success else 1)
        elif args.all:
            results = ingestion.ingest_all_endpoints()
            failed = [name for name, success in results.items() if not success]
            if failed:
                print(f"Failed endpoints: {', '.join(failed)}")
                sys.exit(1)
            sys.exit(0)
        else:
            parser.print_help()
            sys.exit(1)
    finally:
        ingestion.snowflake.close()


if __name__ == "__main__":
    main()

