import requests
import os
from dotenv import load_dotenv
load_dotenv()
import snowflake.connector
from datetime import datetime
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

LIMIT = 1000
DS = '2025-09-25'


def fetch_with_retry(url, max_retries=5, initial_wait=60):
    """Fetch URL with exponential backoff retry logic for rate limits."""
    import time
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            data = response.json()
            
            # Check for rate limit error
            if data.get("status") == "ERROR":
                error_msg = data.get("error", "Unknown error")
                if "exceeded the maximum requests" in error_msg or "Rate limited" in error_msg:
                    wait_time = initial_wait * (2 ** attempt)  # Exponential backoff
                    print(f"\n⚠️  Rate limited (attempt {attempt + 1}/{max_retries})")
                    print(f"   Error: {error_msg}")
                    print(f"   Waiting {wait_time} seconds before retry...\n")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"API Error: {error_msg}")
                    return None
            
            # Success
            return data
        
        except requests.exceptions.Timeout:
            wait_time = initial_wait * (2 ** attempt)
            print(f"\n⏱️  Request timeout (attempt {attempt + 1}/{max_retries})")
            print(f"   Waiting {wait_time} seconds before retry...\n")
            time.sleep(wait_time)
        
        except Exception as e:
            print(f"Error fetching URL: {e}")
            return None
    
    print(f"Failed to fetch after {max_retries} retries")
    return None


def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    
    # Fetch first page with retry logic
    data = fetch_with_retry(url)
    if not data or 'results' not in data:
        print("Failed to fetch initial data. Exiting.")
        return
    
    tickers = []
    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker)

    # Fetch subsequent pages
    page_count = 1
    while 'next_url' in data:
        page_count += 1
        next_url = data['next_url']
        if 'apiKey=' not in next_url:
            next_url = next_url + f'&apiKey={POLYGON_API_KEY}'
        
        print(f'requesting page {page_count}: {next_url[:80]}...')
        data = fetch_with_retry(next_url)
        
        if not data:
            print("Failed to fetch page, stopping pagination")
            break
        
        if 'results' not in data:
            print("No results in response, stopping pagination")
            break
        
        for ticker in data.get('results', []):
            ticker['ds'] = DS
            tickers.append(ticker)
        
        print(f'  -> Collected {len(data.get("results", []))} tickers, total: {len(tickers)}')

    example_ticker =  {'ticker': 'ZWS', 
        'name': 'Zurn Elkay Water Solutions Corporation', 
        'market': 'stocks', 
        'locale': 'us', 
        'primary_exchange': 'XNYS', 
        'type': 'CS', 
        'active': True, 
        'currency_name': 'usd', 
        'cik': '0001439288', 
        'composite_figi': 'BBG000H8R0N8', 	'share_class_figi': 'BBG001T36GB5', 	
        'last_updated_utc': '2025-09-11T06:11:10.586204443Z',
        'ds': '2025-09-25'
        }

    fieldnames = list(example_ticker.keys())

    # Load to Snowflake instead of CSV
    load_to_snowflake(tickers, fieldnames)
    print(f'\n✅ Successfully loaded {len(tickers)} rows to Snowflake')



def load_to_snowflake(rows, fieldnames):
    # Build connection kwargs from environment variables
    connect_kwargs = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
    }
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    if account:
        connect_kwargs['account'] = account

    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')
    if warehouse:
        connect_kwargs['warehouse'] = warehouse
    if database:
        connect_kwargs['database'] = database
    if schema:
        connect_kwargs['schema'] = schema
    if role:
        connect_kwargs['role'] = role

    print(connect_kwargs)
    conn = snowflake.connector.connect( 
        user=connect_kwargs['user'],
        password=connect_kwargs['password'],
        account=connect_kwargs['account'],
        database=connect_kwargs['database'],
        schema=connect_kwargs['schema'],
        role=connect_kwargs['role'],
        session_parameters={
        "CLIENT_TELEMETRY_ENABLED": False,
        }
    )
    try:
        cs = conn.cursor()
        try:
            table_name = os.getenv('SNOWFLAKE_TABLE', 'stock_tickers')

            # Define typed schema based on example_ticker
            type_overrides = {
                'ticker': 'VARCHAR',
                'name': 'VARCHAR',
                'market': 'VARCHAR',
                'locale': 'VARCHAR',
                'primary_exchange': 'VARCHAR',
                'type': 'VARCHAR',
                'active': 'BOOLEAN',
                'currency_name': 'VARCHAR',
                'cik': 'VARCHAR',
                'composite_figi': 'VARCHAR',
                'share_class_figi': 'VARCHAR',
                'last_updated_utc': 'TIMESTAMP_NTZ',
                'ds': 'VARCHAR'
            }
            columns_sql_parts = []
            for col in fieldnames:
                col_type = type_overrides.get(col, 'VARCHAR')
                columns_sql_parts.append(f'"{col.upper()}" {col_type}')

            create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ( ' + ', '.join(columns_sql_parts) + ' )'
            cs.execute(create_table_sql)

            column_list = ', '.join([f'"{c.upper()}"' for c in fieldnames])
            placeholders = ', '.join([f'%({c})s' for c in fieldnames])
            insert_sql = f'INSERT INTO {table_name} ( {column_list} ) VALUES ( {placeholders} )'

            # Conform rows to fieldnames
            transformed = []
            for t in rows:
                row = {}
                for k in fieldnames:
                    row[k] = t.get(k, None)
                print(row)
                transformed.append(row)

            if transformed:
                cs.executemany(insert_sql, transformed)
        finally:
            cs.close()
    finally:
        conn.close()


if __name__ == '__main__':
    run_stock_job()
