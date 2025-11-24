import requests
import os
import time
import csv
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

def fetch(url):
    while True:
        response = requests.get(url)
        data = response.json()
        
        if data.get("status") == "ERROR":
            print("\n API: Error:", data.get("error"))
            print("waiting 60 seconds to retry...\n")
            time.sleep(60)
            continue
        return data


url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
response = requests.get(url)
tickers = []

data = response.json()
for ticker_data in data.get('results', []):
    tickers.append(ticker_data)

while data.get('next_url'):
    print('requesting next page', data['next_url'])
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()
    print(data)
    
    # Check if API returned an error
    if data.get("status") == "ERROR":
        print("\nAPI Error:", data.get("error"))
        print("waiting 60 seconds to retry...\n")
        time.sleep(60)
        continue
    
    # Only process results if they exist
    if 'results' in data:
        for ticker_data in data['results']:
            tickers.append(ticker_data)
    else:
        print("No results in response, breaking loop")
        break

# Write to CSV
if tickers:
    csv_file = 'tickers.csv'
    fieldnames = ['ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 'active', 
                  'currency_name', 'cik', 'composite_figi', 'share_class_figi', 'last_updated_utc']
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ticker_data in tickers:
            row = {field: ticker_data.get(field, '') for field in fieldnames}
            writer.writerow(row)
    
    print(f"\nSuccessfully wrote {len(tickers)} tickers to {csv_file}")
else:
    print("No tickers collected, CSV not created")

example_tickers = {'ticker': 'GSY',
 'name': 'Invesco Ultra Short Duration ETF',
 'market': 'stocks',
 'locale': 'us',
 'primary_exchange': 'ARCX',
 'type': 'ETF',
 'active': True,
 'currency_name': 'usd',
 'cik': '0001418144',
 'composite_figi': 'BBG00KJR1SL9',
 'share_class_figi': 'BBG00KJR1T91',
 'last_updated_utc': '2025-11-24T07:05:34.768453578Z'}

