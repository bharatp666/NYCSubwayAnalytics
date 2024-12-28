import logging
from pathlib import Path
import polars as pl
from functions import *
from validation import *
import requests
from joblib import Parallel, delayed
import hashlib

logging.basicConfig(
    level=logging.INFO,
    filename='/Users/bharatpremnath/nycsubwayridership/app.logs',
    filemode='a', 
    format='%(asctime)s - %(levelname)s - %(message)s'
)


folder_path = Path("/Users/bharatpremnath/nycdatafiles")

def process_files():
    """
    Process NYC daily ridership files and fetch additional data if required.
    """
    if any(folder_path.glob("*.parquet")):
        print("There are files in the folder.")
        logging.info("Files found in the folder.")
        
        # Load existing Parquet files
        df = pl.scan_parquet(f'{folder_path}/*.parquet')
        latest_date = df.select('transit_timestamp').max().collect().to_pandas()['transit_timestamp'].values[0]
        print(latest_date)
        logging.info(f"Latest date in existing data: {latest_date}")

        # Fetch and process data for missing date ranges
        min_date, max_date = fetch_min_max_dates()
        logging.info(f"Fetching data for date range: {latest_date} to {max_date}")
        year_month_list = generate_year_month_list(latest_date, max_date)

        for i in range(len(year_month_list) - 1):
            dat = process_date_range(year_month_list, i)
            schema_validation(dat)
            check_data_completeness(dat)
            handle_duplicates(dat)


    else:
        print('There are no files in the directory')
        logging.info("No files found in the folder.")
        
        # Fetch and process all data from the beginning
        min_date, max_date = fetch_min_max_dates()
        logging.info(f"Fetching data for date range: {min_date} to {max_date}")
        year_month_list = generate_year_month_list(min_date, max_date)

        for i in range(len(year_month_list) - 1):
            dat = process_date_range(year_month_list, i)
            schema_validation(dat)
            check_data_completeness(dat)
            handle_duplicates(dat)

    # except Exception as e:
    #     logging.error(f"Error processing files: {e}")


def process_date_range(year_month_list, index):
    """
    Fetch data for a specific date range and save it as a Parquet file.

    Args:
        year_month_list (list): List of (year, month) tuples.
        index (int): Current index in the list.
    """
    start_date = f"{year_month_list[index][0]}-{year_month_list[index][1]}-01"
    end_date = f"{year_month_list[index + 1][0]}-{year_month_list[index + 1][1]}-01"
    
    try:
        logging.info(f"Fetching data for range: {start_date} to {end_date}")
        data = fetch_data_by_date(start_date, end_date)

        # Convert to Polars DataFrame and save
        df = pl.DataFrame(data)
        output_path = folder_path / f'nyc_daily_ridership_{end_date}_.parquet'
        df.write_parquet(output_path)
        logging.info(f"Data saved to {output_path}")
        return data
    except Exception as e:
        logging.error(f"Error processing range {start_date} to {end_date}: {e}")


if __name__== "__main__":
    process_files()