import requests 
import pandas as pd
from datetime import datetime 
from google.cloud import storage
import polars as pl
import os
from dateutil.relativedelta import relativedelta
from logger_init import *
import re

def fetch_min_max_dates():
    """Query the date of the oldest and latest records from the dataset and log the result."""
    url = (
        "https://data.ny.gov/resource/wujg-7c2s.json"
        "?$select=min(transit_timestamp) as min_date, max(transit_timestamp) as max_date"
    )

    try:
        response = requests.get(url)

        if response.status_code != 200:
            error_msg = f"Failed to fetch data: {response.status_code}, {response.text}"
            print(error_msg)
            gcp_logger.log_text(error_msg, severity="ERROR")
            raise Exception(error_msg)

        data = response.json()
        if not data:
            error_msg = "No data returned from the API."
            print(error_msg)
            gcp_logger.log_text(error_msg, severity="ERROR")
            raise ValueError(error_msg)

        min_date_str = data[0].get("min_date")
        max_date_str = data[0].get("max_date")

        if not min_date_str or not max_date_str:
            error_msg = "The API response is missing 'min_date' or 'max_date'."
            print(error_msg)
            gcp_logger.log_text(error_msg, severity="ERROR")
            raise KeyError(error_msg)

        min_date = datetime.strptime(min_date_str, "%Y-%m-%dT%H:%M:%S.%f")
        max_date = datetime.strptime(max_date_str, "%Y-%m-%dT%H:%M:%S.%f")

        msg = f"Fetched min_date: {min_date}, max_date: {max_date}"
        print(msg)
        gcp_logger.log_text(msg, severity="INFO")

        return min_date, max_date

    except Exception as e:
        error_msg = f"Unexpected error while fetching min/max dates: {str(e)}"
        print(error_msg)
        gcp_logger.log_text(error_msg, severity="ERROR")
        raise



def split_date_range(start_datetime, end_datetime):
    """Splits a date range into monthly chunks and logs the chunks to GCP."""
    chunks = []
    log_prefix = f"Splitting date range from {start_datetime} to {end_datetime}"

    try:
        # First partial month: start → start of next month
        start_of_next_month = (start_datetime.replace(day=1) + relativedelta(months=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if start_datetime < start_of_next_month:
            chunk = (start_datetime, min(start_of_next_month, end_datetime))
            chunks.append(chunk)

        # Full months in between
        current = start_of_next_month
        while current + relativedelta(months=1) < end_datetime:
            next_month = current + relativedelta(months=1)
            chunks.append((current, next_month))
            current = next_month

        # Final partial month: start of ending month → end_datetime
        if current < end_datetime:
            chunks.append((current, end_datetime))

        # Logging
        gcp_logger.log_text(f"{log_prefix} — Total chunks: {len(chunks)}", severity="INFO")
        for idx, (start, end) in enumerate(chunks):
            gcp_logger.log_text(f"Chunk {idx + 1}: {start} to {end}", severity="INFO")

        return chunks

    except Exception as e:
        error_msg = f"{log_prefix} — Error during chunking: {str(e)}"
        print(error_msg)
        gcp_logger.log_text(error_msg, severity="ERROR")
        raise

def label_chunks_by_time(chunks):
    """
    Labels each (start, end) chunk as 'first_partial', 'middle', or 'last_partial'
    and logs the labeling process using GCP Logging.
    """
    tagged = []
    try:
        gcp_logger.log_text(f"Labeling {len(chunks)} chunks by time boundaries", severity="INFO")

        for i, (start, end) in enumerate(chunks):
            is_start_partial = start.day != 1 or start.time() != datetime.min.time()
            is_end_partial = end.day != 1 or end.time() != datetime.min.time()

            if i == 0 and is_start_partial:
                label = 'first_partial'
            elif i == len(chunks) - 1 and is_end_partial:
                label = 'last_partial'
            else:
                label = 'middle'

            tagged.append((start, end, label))
            gcp_logger.log_text(f"Chunk {i + 1}: {start} to {end} labeled as {label}", severity="INFO")

        return tagged

    except Exception as e:
        error_msg = f"Error labeling chunks: {str(e)}"
        print(error_msg)
        gcp_logger.log_text(error_msg, severity="ERROR")
        raise


def convert_timechunks_to_strings(tagged_chunks):
    """
    Converts datetime chunks into string format and logs the process.
    """
    try:
        gcp_logger.log_text(f"Converting {len(tagged_chunks)} tagged chunks to string format", severity="INFO")

        string_chunks = [
            (
                start.strftime("%Y-%m-%dT%H:%M:%S.000"),
                end.strftime("%Y-%m-%dT%H:%M:%S.000"),
                label
            )
            for start, end, label in tagged_chunks
        ]

        preview = string_chunks[:3] if string_chunks else []
        gcp_logger.log_text(f"Preview of converted chunks: {preview}", severity="INFO")

        return string_chunks

    except Exception as e:
        error_msg = f"Error converting chunks to strings: {str(e)}"
        print(error_msg)
        gcp_logger.log_text(error_msg, severity="ERROR")
        raise

def fetch_data_by_date_bulk(start_date, end_date, date_position):
    url_middle = (
        f"https://data.ny.gov/resource/wujg-7c2s.json"
        f"?$where=transit_timestamp >= '{start_date}' AND transit_timestamp < '{end_date}'"
        f"&$limit=5000000"
    )
    url_last_partial = (
        f"https://data.ny.gov/resource/wujg-7c2s.json"
        f"?$where=transit_timestamp >= '{start_date}' AND transit_timestamp <= '{end_date}'"
        f"&$limit=5000000"
    )

    try:
        if date_position == 'last_partial':
            url = url_last_partial
        else:
            url = url_middle

        response = requests.get(url)
        if response.status_code == 200:
            gcp_logger.log_text(f"Successfully fetched bulk data from {start_date} to {end_date} for {date_position}", severity="INFO")
            return response.json()
        else:
            error_msg = f"Failed to fetch bulk data: {response.status_code}, {response.text}"
            gcp_logger.log_text(error_msg, severity="ERROR")
            raise Exception(error_msg)

    except requests.exceptions.RequestException as e:
        error_msg = f"Request error during bulk fetch: {e}"
        gcp_logger.log_text(error_msg, severity="ERROR")
        raise Exception(error_msg)


def fetch_data_by_date_incremental(start_date, end_date, date_position):
    url_first_partial = (
        f"https://data.ny.gov/resource/wujg-7c2s.json"
        f"?$where=transit_timestamp > '{start_date}' AND transit_timestamp < '{end_date}'"
        f"&$limit=5000000"
    )
    url_middle = (
        f"https://data.ny.gov/resource/wujg-7c2s.json"
        f"?$where=transit_timestamp >= '{start_date}' AND transit_timestamp < '{end_date}'"
        f"&$limit=5000000"
    )
    url_last_partial = (
        f"https://data.ny.gov/resource/wujg-7c2s.json"
        f"?$where=transit_timestamp >= '{start_date}' AND transit_timestamp <= '{end_date}'"
        f"&$limit=5000000"
    )

    try:
        if date_position == 'first_partial':
            url = url_first_partial
        elif date_position == 'last_partial':
            url = url_last_partial
        else:
            url = url_middle

        response = requests.get(url)
        if response.status_code == 200:
            gcp_logger.log_text(f"Successfully fetched incremental data from {start_date} to {end_date} for {date_position}", severity="INFO")
            return response.json()
        else:
            error_msg = f"Failed to fetch incremental data: {response.status_code}, {response.text}"
            gcp_logger.log_text(error_msg, severity="ERROR")
            raise Exception(error_msg)

    except requests.exceptions.RequestException as e:
        error_msg = f"Request error during incremental fetch: {e}"
        gcp_logger.log_text(error_msg, severity="ERROR")
        raise Exception(error_msg)



def check_gcs_parquet_files(bucket_name: str, folder_name: str):
    """
    Checks for Parquet files in the specified GCS folder (within bucket) and returns the latest transit_timestamp.
    Assumes filenames follow: ridership_YYYY-MM.parquet
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        prefix = folder_name.rstrip("/") + "/"  # ensure trailing slash
        blobs = list(client.list_blobs(bucket, prefix=prefix))

        # Match files only with correct naming inside folder
        pattern = re.compile(r"ridership_(\d{4}-\d{2})\.parquet$")
        parquet_files = [
            blob.name for blob in blobs
            if pattern.match(blob.name.split("/")[-1])  # ensure correct filename
        ]

        if not parquet_files:
            msg = f"No matching Parquet files found in gs://{bucket_name}/{folder_name}"
            print(msg)
            gcp_logger.log_text(msg, severity="INFO")
            return None

        gcp_logger.log_text(
            f"Found {len(parquet_files)} matching Parquet files in gs://{bucket_name}/{folder_name}",
            severity="INFO"
        )

        gcs_paths = [f"gs://{bucket_name}/{name}" for name in parquet_files]
        df = pl.scan_parquet(gcs_paths, allow_missing_columns=True)
        latest_date_str = (
            df.select(pl.col("transit_timestamp").max())
            .collect()
            .to_pandas()["transit_timestamp"]
            .values[0]
        )

        try:
            latest_date = datetime.strptime(latest_date_str, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            latest_date = datetime.strptime(latest_date_str, "%Y-%m-%dT%H:%M:%S")

        log_msg = f"Latest transit_timestamp from GCS data: {latest_date}"
        print(log_msg)
        gcp_logger.log_text(log_msg, severity="INFO")
        return latest_date

    except Exception as e:
        error_msg = f"Error checking GCS Parquet files in folder '{folder_name}': {e}"
        print(error_msg)
        gcp_logger.log_text(error_msg, severity="ERROR")
        return None
    

def get_gcs_uri(start,BUCKET_NAME,GCS_FOLDER):
    # Start is ISO format string: 'YYYY-MM-DDTHH:MM:SS.fff'
    try:
        month_str = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m")
        gcs_uri = f"gs://{BUCKET_NAME}/{GCS_FOLDER}/ridership_{month_str}.parquet"
        gcp_logger.log_text(f"Constructed GCS URI: {gcs_uri}", severity="INFO")
        return gcs_uri
    except Exception as e:
        gcp_logger.log_text(f"Error constructing GCS URI for start={start}: {e}", severity="ERROR")
        raise
    
    return gcs_uri

def fetch_and_write_bulk(start, end, label, BUCKET_NAME,GCS_FOLDER):
    try:
        data = fetch_data_by_date_bulk(start, end, label)
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

def fetch_and_write_incremental(start, end, label, BUCKET_NAME, GCS_FOLDER):
    try:
        data = fetch_data_by_date_incremental(start, end, label)

        # If label is 'first_partial' and no data, skip the write
        if label == "first_partial" and not data:
            msg = f"No data fetched for 'first_partial' from {start} to {end}. Skipping write."
            print(msg)
            gcp_logger.log_text(msg, severity="INFO")
            return msg

        df = pl.DataFrame(data)
        gcs_uri = get_gcs_uri(start, BUCKET_NAME, GCS_FOLDER)
        df.write_parquet(gcs_uri)

        msg = f"Written to GCS: {gcs_uri}"
        print(msg)
        gcp_logger.log_text(msg, severity="INFO")
        return msg

    except Exception as e:
        err_msg = f"Failed to write incremental data from {start} to {end}. Error: {str(e)}"
        print(err_msg)
        gcp_logger.log_text(err_msg, severity="ERROR")
        return err_msg

