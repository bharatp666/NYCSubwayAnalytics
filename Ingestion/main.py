# import logging
from logger_init import *
from pathlib import Path
import polars as pl
from functions import *
# from validation import * 
import requests
from joblib import Parallel, delayed
import hashlib
from google.cloud import storage
import time 
import argparse


bucket_name = 'mi-buket'
folder_name = 'nycdata'

# Set up argument parsing
# parser = argparse.ArgumentParser(description="Process bucket and folder names.")
# parser.add_argument("--bucket-name", required=True, help="Name of the bucket")
# parser.add_argument("--folder-name", required=True, help="Name of the folder")

# # Parse the command-line arguments
# args = parser.parse_args()

# # Access the arguments
# bucket_name = args.bucket_name
# folder_name = args.folder_name
folder_path = f"gs://{bucket_name}/{folder_name}"


def has_parquet_files(bucket_name, folder_name):
    """Checks if a folder in the bucket contains any Parquet files."""
    gcp_logger.log_text(f"Checking for Parquet files in bucket '{bucket_name}', folder '{folder_name}'", severity=200)

    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=folder_name)

    # Iterate through blobs to find Parquet files
    for blob in blobs:
        if blob.name.endswith('.parquet'):  # Look specifically for Parquet files
            gcp_logger.log_text(f"Found Parquet file: {blob.name}", severity=200)
            return True
    
    gcp_logger.log_text(f"No Parquet files found in the folder '{folder_name}'.", severity=200)
    return False



def process_files():
    """
    Process NYC daily ridership files and fetch additional data if required.
    """

    if has_parquet_files(bucket_name, folder_name):
        print("There are files in the folder.")
        gcp_logger.log_text("Files found in the folder.", severity=200)
        
        # Load existing Parquet files
        df = pl.scan_parquet(f'{folder_path}/*.parquet')
        latest_date = (
            df.select(pl.col('transit_timestamp').max())
            .collect()
            .to_pandas()
            ['transit_timestamp']
            .values[0]
        )
        #df.select('transit_timestamp').max().collect().to_pandas()['transit_timestamp'].values[0]
        print(latest_date)
        gcp_logger.log_text(f"Latest date in existing data: {latest_date}", severity=200)

        # Fetch and process data for missing date ranges
        min_date, max_date = fetch_min_max_dates()


        # Comparing latest date with max_date
        if latest_date != max_date:
            gcp_logger.log_text(f'Fetching data for date range {latest_date} and {max_date}')

            year_month_list = generate_year_month_list(latest_date, max_date)
            print(year_month_list)

            # Fetch the data for start dates other than 1st day of the month
            inter_date = get_date_(f"{year_month_list[0][0]}-{year_month_list[0][1]}-01")
            miin_date = get_date_only(latest_date)
            print(inter_date,miin_date)
            if (miin_date!=inter_date):
                dat = process_date_range(miin_date,inter_date,miin_date)
                print(inter_date,miin_date)


            for index in range(0,len(year_month_list)-1):
                start_date = f"{year_month_list[index][0]}-{year_month_list[index][1]}-01"
                end_date = f"{year_month_list[index + 1][0]}-{year_month_list[index + 1][1]}-01"
                print(start_date,end_date)
                dat = process_date_range(start_date,end_date,start_date)

            # Fetch data for last day other than first day of next month 
            ended_date = get_date_(f"{year_month_list[-1][0]}-{year_month_list[-1][1]}-01")
            maxx_date = get_date_only(max_date)
            if (ended_date!=maxx_date):
                dat = process_date_range(ended_date,maxx_date,ended_date)
                print(ended_date,maxx_date)

        else:
            gcp_logger.log_text(f'The data is upto date, date ranges : {latest_date} , {max_date}')
            return

    else:
        print('There are no files in the directory')

        gcp_logger.log_text("No files found in the folder.", severity=200)
        
        # Fetch and process all data from the beginning
        min_date, max_date = fetch_min_max_dates()

        gcp_logger.log_text(f"Fetching year and month values for date range: {min_date} to {max_date}", severity=200)
        year_month_list = generate_year_month_list(min_date, max_date)

        # Fetch the data for start dates other than 1st day of the month
        inter_date = get_date_(f"{year_month_list[0][0]}-{year_month_list[0][1]}-01")
        miin_date = get_date_only(min_date)
        if (miin_date!=inter_date):
            process_date_range(miin_date,inter_date,miin_date)
            print(inter_date,miin_date)

        # Fetch data in a monthly range 
        for index in range(0,len(year_month_list)-1):
            start_date = f"{year_month_list[index][0]}-{year_month_list[index][1]}-01"
            end_date = f"{year_month_list[index + 1][0]}-{year_month_list[index + 1][1]}-01"
            print(start_date,end_date)
            process_date_range(start_date,end_date,start_date)

        # Fetch data for last day other than first day of next month 
        ended_date = get_date_(f"{year_month_list[-1][0]}-{year_month_list[-1][1]}-01")
        maxx_date = get_date_only(max_date)
        if (ended_date!=max_date):
            dat = process_date_range(ended_date,maxx_date,ended_date)
            print(ended_date,maxx_date)



def process_date_range(start_date,end_date,file_date):
    """
    Fetch data for a specific date range and save it as a Parquet file.

    Args:
        year_month_list (list): List of (year, month) tuples.
    """
    try:
        #logging.info(f"Fetching data for range: {start_date} to {end_date}")
        #gcp_logger.log_text(f"Starting data fetching for date range: {start_date} to {end_date}", severity=200)
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
        gcp_logger.log_text(f"Error fetching data for {start_date} to {end_date}: {e}", severity=500)

if __name__== "__main__":
    process_files()
