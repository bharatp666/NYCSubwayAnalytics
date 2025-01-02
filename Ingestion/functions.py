import requests 
import pandas as pd
import logging
from datetime import datetime 
from logger_init import gcp_logger
from datetime import datetime
from google.cloud import storage

def fetch_min_max_dates():
    '''Query the date of the oldest and latest records from the dataset'''
    # Define the API endpoint with SoQL query to get min and max dates
    url = (
        "https://data.ny.gov/resource/wujg-7c2s.json"
        "?$select=min(transit_timestamp) as min_date, max(transit_timestamp) as max_date"
    )

    # Make the GET request
    response = requests.get(url)

    # Example block of code with logging
    if response.status_code == 200:
        #logging.info("API request successful. Status code: 200")
        gcp_logger.log_text("API request successful. Status code: 200", severity=200)
        data = response.json()
        if data:
            min_date = data[0].get('min_date')
            max_date = data[0].get('max_date')
            if min_date and max_date:
                #logging.info(f"Fetched min_date: {min_date}, max_date: {max_date}")
                gcp_logger.log_text(f"Fetched min_date: {min_date}, max_date: {max_date}", severity=200)

                return min_date, max_date
            else:
                #logging.error("The API response is missing 'min_date' or 'max_date'.")
                gcp_logger.log_text("The API response is missing 'min_date' or 'max_date'.", severity=500)
                raise KeyError("The API response is missing 'min_date' or 'max_date'.")
        else:
            #logging.error("No data returned from the API.")
            gcp_logger.log_text("No data returned from the API.", severity=500)
            raise ValueError("No data returned from the API.")
    else:
        #logging.error(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
        gcp_logger.log_text(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}", severity=500)
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")


def generate_year_month_list(min_date_str, max_date_str):
    """
    Generate a list of (year, month) tuples from min_date to max_date.

    Args:
        min_date_str (str): The starting date in string format (e.g., '2023-01-01').
        max_date_str (str): The ending date in string format (e.g., '2023-12-31').

    Returns:
        list of tuples: A list of (year, month) tuples for each month in the range.
    """

    # Convert string dates to datetime objects
    min_date = pd.to_datetime(min_date_str)
    max_date = pd.to_datetime(max_date_str)

    # Generate a date range with monthly frequency
    date_range = pd.date_range(
        start=min_date.replace(day=1),  # Ensure the start is the 1st of the month
        end=max_date.replace(day=1),    # Ensure the end is the 1st of the month
        freq='MS'                       # Frequency: Month Start
    )

    # Create a list of (year, month) tuples
    year_month_list = [(date.year, date.month) for date in date_range]
    return year_month_list



# Function to fetch data between a start date and end date
def fetch_data_by_date(start_date, end_date):
    """
    Fetch data from the API filtered by date range.

    Args:
        start_date (str): The start date in string format (e.g., '2023-01-01').
        end_date (str): The end date in string format (e.g., '2023-01-31').

    Returns:
        list: The JSON response from the API.

    Raises:
        Exception: If the API request fails.
    """
    # Log the API request details
    #logging.info(f"Fetching data for date range: {start_date} to {end_date}")
    gcp_logger.log_text(f"Fetching data for date range: {start_date} to {end_date}", severity=200)

    # Define the API endpoint with date filtering
    url = (
        f"https://data.ny.gov/resource/wujg-7c2s.json"
        f"?$where=transit_timestamp >= '{start_date}' AND transit_timestamp <= '{end_date}'"
        f" &$limit=5000000"
    )

    try:
        # Make the GET request
        #logging.info(f"Making request to URL: {url}")
        gcp_logger.log_text(f"Making request to URL: {url}", severity=200)
        response = requests.get(url)

        # Handle the response
        if response.status_code == 200:
            gcp_logger.log_text(f"Data successfully fetched for range {start_date} to {end_date}", severity=200)
            #logging.info(f"Data successfully fetched for range {start_date} to {end_date}")
            return response.json()  # Return JSON response
        else:
            #logging.error(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
            gcp_logger.log_text(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}", severity=500)
            raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        #logging.error(f"An error occurred during the request: {e}")
        gcp_logger.log_text(f"An error occurred during the request: {e}", severity=500)
        raise Exception(f"An error occurred during the request: {e}")

def get_date_only(datetime_str):
    # Convert to datetime object
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%f")

    # Format to date-only
    date_only = datetime_obj.date()
    return date_only

def get_date_(date_string):
    # Convert to datetime object
    datetime_obj = datetime.strptime(date_string, "%Y-%m-%d")

    # Format to date-only
    date_only = datetime_obj.date()
    return date_only

def check_bucket_and_folder(bucket_name, folder_name):
    """
    Checks if the bucket and folder exist in Google Cloud Storage
    and logs the results using GCP Logger.
    """
    client = storage.Client()

    try:
        # Check if the bucket exists
        bucket = client.get_bucket(bucket_name)
        gcp_logger.log_text(f"Bucket '{bucket_name}' exists.", severity="INFO")

        # Check if the folder exists (list blobs with the folder prefix)
        blobs = list(bucket.list_blobs(prefix=f"{folder_name}/", max_results=1))
        if blobs:
            gcp_logger.log_text(f"Folder '{folder_name}' exists in bucket '{bucket_name}'.", severity=200)
        else:
            gcp_logger.log_text(f"Folder '{folder_name}' does NOT exist in bucket '{bucket_name}'.", severity=500)
    except Exception as e:
        gcp_logger.log_text(f"Error checking bucket/folder: {e}", severity=500)
