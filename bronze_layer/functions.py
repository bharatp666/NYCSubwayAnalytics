import requests 
import pandas as pd
from datetime import datetime 
from google.cloud import storage
import polars as pl
import os
from dateutil.relativedelta import relativedelta
from logger_init import *
import re


def get_source_dates():
    url = "https://data.ny.gov/resource/wujg-7c2s.json"
    params = {
        "$select": "date_trunc_ym(transit_timestamp) AS first_of_month",
        "$group": "first_of_month",
        "$order": "first_of_month"
    }

    response = requests.get(url, params=params)
    data = response.json()
    return set(pl.DataFrame(data).to_series().to_list())
    


def get_existing_datetimes_from_gcs(bucket_name: str, prefix: str) -> list[str]:
    """
    Extracts datetime strings in the form 'YYYY-MM-DDTHH:MM:SS.fff'
    from filenames like 'ridership_YYYY-MM-DDTHH-MM-SS-fff.parquet'
    Logs key steps and outcomes to Google Cloud Logging.
    """
    client = storage.Client()
    try:
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        gcp_logger.log_text(f"Fetching blobs from bucket: {bucket_name} with prefix: {prefix}", severity="INFO")

        # Match format like: ridership_2020-07-01T00-00-00-000.parquet
        pattern = re.compile(r"ridership_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3})\.parquet")

        datetimes = []
        for blob in blobs:
            match = pattern.search(blob.name)
            if match:
                raw = match.group(1)  # e.g., 2020-07-01T00-00-00-000
                # Convert from dash-safe format to ISO format with milliseconds due to filename compatibility issues
                iso_ts = (
                    raw[:10] + "T" +       # YYYY-MM-DDT
                    raw[11:13] + ":" +     # HH:
                    raw[14:16] + ":" +     # MM:
                    raw[17:19] + "." +     # SS.
                    raw[20:23]             # fff
                )
                datetimes.append(iso_ts)

        gcp_logger.log_text(f"Extracted {len(datetimes)} datetime(s) from GCS.", severity="INFO")
        return set(datetimes)

    except Exception as e:
        err_msg = f"Error while extracting datetimes from GCS: {str(e)}"
        gcp_logger.log_text(err_msg, severity="ERROR")
        return []


def increment_month(timestamp_str: str) -> str:
    """
    Increments a timestamp string of the form 'YYYY-MM-DDTHH:MM:SS.fff' by one month.
    Returns the result in the same format and logs the result.
    """
    try:
        dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
        next_month = dt + relativedelta(months=1)
        result = next_month.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]  # keep milliseconds (.000)
        gcp_logger.log_text(f"Incremented timestamp: {timestamp_str} -> {result}", severity="INFO")
        return result
    except ValueError as e:
        err_msg = f"Error parsing timestamp '{timestamp_str}': {str(e)}"
        gcp_logger.log_text(err_msg, severity="ERROR")
        return None

    
def fetch_data_from_url(start_date, end_date):
    """
    Fetches bulk data from the NYC Transit API for the given date range.
    Logs all major events and errors using Google Cloud Logging.
    """
    url = (
        "https://data.ny.gov/resource/wujg-7c2s.json"
        f"?$where=transit_timestamp >= '{start_date}' AND transit_timestamp < '{end_date}'"
        "&$limit=5000000"
    )

    try:
        print(f"Fetching URL: {url}")
        response = requests.get(url)

        if response.status_code == 200:
            gcp_logger.log_text(
                f"Successfully fetched bulk data from {start_date} to {end_date}",
                severity="INFO"
            )
            return response.json()
        else:
            error_msg = (
                f"Failed to fetch bulk data for {start_date} to {end_date}: "
                f"Status {response.status_code}, Response: {response.text}"
            )
            gcp_logger.log_text(error_msg, severity="ERROR")
            raise Exception(error_msg)

    except requests.exceptions.RequestException as e:
        error_msg = f"Request exception during bulk fetch for {start_date} to {end_date}: {str(e)}"
        gcp_logger.log_text(error_msg, severity="ERROR")
        raise Exception(error_msg)
    
def get_gcs_uri(start: str, BUCKET_NAME: str, GCS_FOLDER: str):
    """
    Constructs a GCS URI with full timestamp-based filename including milliseconds.
    Input format: 'YYYY-MM-DDTHH:MM:SS.fff'
    Filename format: ridership_YYYY-MM-DDTHH-MM-SS-fff.parquet
    """
    try:
        dt = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.%f")
        safe_timestamp = dt.strftime("%Y-%m-%dT%H-%M-%S-%f")[:-3]  # Keep only 3 ms digits
        gcs_uri = f"gs://{BUCKET_NAME}/{GCS_FOLDER}/ridership_{safe_timestamp}.parquet"
        gcp_logger.log_text(f"Constructed GCS URI: {gcs_uri}", severity="INFO")
        return gcs_uri
    except Exception as e:
        gcp_logger.log_text(f"Error constructing GCS URI for start={start}: {e}", severity="ERROR")
        return None
    
def fetch_and_write(start, end, BUCKET_NAME, GCS_FOLDER):
    try:
        data = fetch_data_from_url(start, end)
        df = pl.DataFrame(data)
        gcs_uri = get_gcs_uri(start,BUCKET_NAME,GCS_FOLDER)
        df.write_parquet(gcs_uri)
        msg = f"Written to GCS: {gcs_uri}"
        print(msg)
        gcp_logger.log_text(msg, severity="INFO")
        return msg
    except Exception as e:
        err_msg = f"Failed to write bulk data from {start} to {end}. Error: {str(e)}"
        print(err_msg)
        gcp_logger.log_text(err_msg, severity="ERROR")
        return err_msg