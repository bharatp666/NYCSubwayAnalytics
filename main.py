# import logging
from logger_init import *
from pathlib import Path
import polars as pl
from functions import *
from validation import *
import requests
from joblib import Parallel, delayed
import hashlib
from google.cloud import storage
import time 

# Initialize GCP Logging client and handler
# from google.cloud.logging import Client

# gcp_client = Client()  # Explicitly initialize the client

# Initialize the Cloud Logging client
# gcp_client = gcp_logging.Client()

# Set up Cloud Logging handler
# cloud_handler = gcp_client.get_default_handler()

# gcp_client.setup_logging()
# atexit.register(gcp_client.close)
# Configure the Cloud Logging handler for synchronous transport
# cloud_handler = CloudLoggingHandler(client=gcp_client)

# logging.basicConfig(
#     level=logging.INFO,
#     # filemode='w', 
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('/Users/bharatpremnath/nycsubwayridership/app.logs', mode='w'),  # File logging
#     ]
# )


def has_parquet_files(bucket_name, folder_name):
    """Checks if a folder in the bucket contains any Parquet files."""
    #logging.info("Checking for Parquet files in bucket '%s', folder '%s'", bucket_name, folder_name)
    time.sleep(5)
    gcp_logger.log_text(f"Checking for Parquet files in bucket '{bucket_name}', folder '{folder_name}'", severity=200)

    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=folder_name)

    # Iterate through blobs to find Parquet files
    for blob in blobs:
        if blob.name.endswith('.parquet'):  # Look specifically for Parquet files
            #logging.info("Found Parquet file: %s", blob.name)
            gcp_logger.log_text(f"Found Parquet file: {blob.name}", severity=200)
            return True
    
    #logging.info("No Parquet files found in the folder '%s'.", folder_name)
    gcp_logger.log_text(f"No Parquet files found in the folder '{folder_name}'.", severity=200)
    return False

folder_path = "gs://mi-data-bucket/nycdata"
bucket_name = 'mi-data-bucket'
folder_name = 'nycdata'

def process_files():
    """
    Process NYC daily ridership files and fetch additional data if required.
    """
#    if any(folder_path.glob("*.parquet")):
    if has_parquet_files(bucket_name, folder_name):
        print("There are files in the folder.")
        #logging.info("Files found in the folder.")
        gcp_logger.log_text("Files found in the folder.", severity=200)
        
        # Load existing Parquet files
        df = pl.scan_parquet(f'{folder_path}/*.parquet')
        latest_date = df.select('transit_timestamp').max().collect().to_pandas()['transit_timestamp'].values[0]
        print(latest_date)
        #logging.info(f"Latest date in existing data: {latest_date}")
        gcp_logger.log_text(f"Latest date in existing data: {latest_date}", severity=200)

        # Fetch and process data for missing date ranges
        min_date, max_date = fetch_min_max_dates()
        #logging.info(f"Fetching data for date range: {latest_date} to {max_date}")
        gcp_logger.log_text(f"Fetching data for date range: {latest_date} to {max_date}", severity=200)

        # Add the checkpoint log here to ensure the script continues after the log
        #gcp_logger.log_text("Checkpoint: Successfully logged the date range.", severity=200)

        year_month_list = generate_year_month_list(latest_date, max_date)

        for index in range(0,len(year_month_list)-1):
            start_date = f"{year_month_list[index][0]}-{year_month_list[index][1]}-01"
            end_date = f"{year_month_list[index + 1][0]}-{year_month_list[index + 1][1]}-01"
            print(start_date,end_date)
            dat = process_date_range(start_date,end_date,end_date)
        ended_date = f"{year_month_list[-1][0]}-{year_month_list[-1][1]}-01"
        if ended_date!=max_date:
            dat = process_date_range(ended_date,max_date,max_date)
            print(ended_date,max_date)

            # dat_validation(dat)
            # schema_validation(dat)
            # check_data_completeness(dat)
            # handle_duplicates(dat)


    else:
        print('There are no files in the directory')
        #logging.info("No files found in the folder.")
        gcp_logger.log_text("No files found in the folder.", severity=200)
        
        # Fetch and process all data from the beginning
        min_date, max_date = fetch_min_max_dates()
        #logging.info(f"Fetching data for date range: {min_date} to {max_date}")
        gcp_logger.log_text(f"Fetching data for date range: {min_date} to {max_date}", severity=200)
        year_month_list = generate_year_month_list(min_date, max_date)

        # for i in range(0,len(year_month_list)-1):
        #     dat = process_date_range(year_month_list, i)
        for index in range(0,len(year_month_list)-1):
            start_date = f"{year_month_list[index][0]}-{year_month_list[index][1]}-01"
            end_date = f"{year_month_list[index + 1][0]}-{year_month_list[index + 1][1]}-01"
            print(start_date,end_date)
            dat = process_date_range(start_date,end_date,end_date)
        ended_date = f"{year_month_list[-1][0]}-{year_month_list[-1][1]}-01"
        if ended_date!=max_date:
            dat = process_date_range(ended_date,max_date,max_date)
            print(ended_date,max_date)
            # dat_validation(dat)
            # schema_validation(dat)
            # check_data_completeness(dat)
            # handle_duplicates(dat)

    # except Exception as e:
    #     logging.error(f"Error processing files: {e}")


def process_date_range(start_date,end_date,file_date):
    """
    Fetch data for a specific date range and save it as a Parquet file.

    Args:
        year_month_list (list): List of (year, month) tuples.
    """
    try:
        #logging.info(f"Fetching data for range: {start_date} to {end_date}")
        gcp_logger.log_text(f"Fetching data for range: {start_date} to {end_date}", severity=200)
        data = fetch_data_by_date(start_date, end_date)

        # Convert to Polars DataFrame and save
        df = pl.DataFrame(data)
        output_path = f'{folder_path}/nyc_daily_ridership_{file_date}_.parquet'
        df.write_parquet(output_path)
        #logging.info(f"Data saved to {output_path}")
        gcp_logger.log_text(f"Data saved to {output_path}", severity=200)
        del df
        return data
    except Exception as e:
        #logging.error(f"Error processing range {start_date} to {end_date}: {e}")
        gcp_logger.log_text(f"Error processing range {start_date} to {end_date}: {e}", severity=500)


if __name__== "__main__":
    process_files()