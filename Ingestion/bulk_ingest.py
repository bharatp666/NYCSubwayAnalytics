import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import InsertAllRequest
import argparse


# Initialize argument parser
parser = argparse.ArgumentParser(description='Insert records into BigQuery in bulk.')

# Add arguments
parser.add_argument('--project_id', required=True, help='Google Cloud project ID')

# Parse the arguments
args = parser.parse_args()

# Function to fetch data from the API
def fetch_data(offset,limit):
    url = f'https://data.ny.gov/resource/wujg-7c2s.json?$offset={offset}&$limit={limit}'
    response = requests.get(url)
    return response.json()  # Return JSON response

# Function to fetch the total count of records on DB
def fetch_count():
    url = 'https://data.ny.gov/resource/wujg-7c2s.json?$select=count(*)'
    response = requests.get(url)
    return int(response.json()[0]['count'])

# Generate the range of offsets, ensuring the last value is included
limit =10000
total_count = fetch_count()
offsets = list(range(0, total_count, limit))
if offsets[-1] != total_count:
    offsets.append(total_count)


# Loop through offsets to fetch data
for offset in offsets[:1]:
    try:
        data = fetch_data(offset,limit)  # Fetch data for the current offset
        df = pd.DataFrame(data)
        # Convert columns to the desired data types
        df = df.astype({
          'transit_timestamp': 'datetime64[ns]',   # TIMESTAMP
          'transit_mode': 'string',                # STRING
          'station_complex_id': 'string',          # STRING
          'station_complex': 'string',             # STRING
          'borough': 'string',                     # STRING
          'payment_method': 'string',              # STRING
          'fare_class_category': 'string',         # STRING
          'ridership': 'float64',                    # INTEGER
          'transfers': 'float64',                    # INTEGER
          'latitude': 'float64',                   # FLOAT
          'longitude': 'float64',                  # FLOAT
      })

        df_dict = df.iloc[:,:-4].T.to_dict().values()
        print(f'Fetched {len(df)} records starting from offset {offset}')

  
        # Process the data (e.g., insert into BigQuery) here
        # Initialize BigQuery client
        client = bigquery.Client(project=args.project_id)

        dataset_id = 'nyc_subway_data'
        table_id = 'nyc_subway_data.hourly_trip_data'
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

        # Insert rows into BigQuery
        status = client.insert_rows_json(table, df_dict)

        #del data  # Delete the data to free up memory
        #gc.collect()  # Manually trigger garbage collection
    except Exception as e:
        print(f'Error fetching data starting at offset {offset}: {e}')
