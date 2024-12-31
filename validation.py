import pandas as pd
import numpy as np
import logging
import polars as pl
from joblib import Parallel, delayed
import hashlib
from logger_init import gcp_logger
#from google.cloud.logging_v2 import enums
# from prefect import flow, task, get_run_logger
# from prefect.task_runners import ConcurrentTaskRunner

#@task
def schema_validation(data):
    """
    Validate the schema of a Polars DataFrame against a predefined schema.

    Args:
        data: Input data to validate (must be convertible to a Polars DataFrame).

    Returns:
        str: Validation message indicating the schema status.
    """
    schema = ['transit_timestamp', 'transit_mode', 'station_complex_id',
              'station_complex', 'borough', 'payment_method', 'fare_class_category',
              'ridership', 'transfers', 'latitude', 'longitude', 'georeference']

    # Create Polars DataFrame
    try:
        # Schema validation
        #logging.info("Starting schema validation.")
        gcp_logger.log_text("Starting schema validation.", severity=200)
        df = pl.DataFrame(data)
        #logging.info("Data successfully converted to Polars DataFrame.")
        gcp_logger.log_text("Data successfully converted to Polars DataFrame.", severity=200)
    except Exception as e:
        #logging.error(f"Failed to create Polars DataFrame: {e}")
        gcp_logger.log_text(f"Failed to create Polars DataFrame: {e}", severity=500)
        raise ValueError("Invalid data for Polars DataFrame conversion.") from e

    missing_columns = set(schema) - set(df.columns)
    extra_columns = set(df.columns) - set(schema)

    if missing_columns or extra_columns:
        #logging.warning("Change in schema detected.")
        gcp_logger.log_text("Change in schema detected.", severity=400)
        if missing_columns:
            #logging.warning(f"Missing columns: {missing_columns}"
            gcp_logger.log_text(f"Missing columns: {missing_columns}", severity=400)
        if extra_columns:
            #logging.warning(f"Extra columns: {extra_columns}")
            gcp_logger.log_text(f"Extra columns: {extra_columns}", severity=400)
        return "Schema validation failed: Check logs for details."
    else:
        #logging.info("Schema is consistent.")
        gcp_logger.log_text("Schema is consistent.", severity=200)
        return "Schema is consistent."
    
#@task

def check_data_completeness(data):
    """
    Check data completeness by logging the number of missing values for each column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to check for null values.

    Returns:
        str: Message indicating whether the data is complete or has missing values.
    """
    #logging.info("Starting data completeness check.")
    gcp_logger.log_text("Starting data completeness check.", severity=200)
        # Create Polars DataFrame
    try:
        # Schema validation
        df = pl.DataFrame(data)
        #logging.info("Data successfully converted to Polars DataFrame.")
        gcp_logger.log_text("Data successfully converted to Polars DataFrame.", severity=200)
    except Exception as e:
        #logging.error(f"Failed to create Polars DataFrame: {e}")
        gcp_logger.log_text(f"Failed to create Polars DataFrame: {e}", severity=500)
        raise ValueError("Invalid data for Polars DataFrame conversion.") from e
    
    # Count missing values for each column
    missing_values = df.null_count().to_pandas().sum()
    total_missing = missing_values.sum()

    # Log missing values for each column
    for col, missing in zip(df.columns, missing_values):
        if missing > 0:
            #logging.warning(f"Column '{col}' has {missing} missing values.")
            gcp_logger.log_text(f"Column '{col}' has {missing} missing values.", severity=400)

    # Check overall completeness
    if total_missing == 0:
        #logging.info("Data is complete.")
        gcp_logger.log_text("Data is complete.", severity=200)
        return "Data is complete."
    else:
        #logging.warning(f"Data has {total_missing} missing values in total.")
        gcp_logger.log_text(f"Data has {total_missing} missing values in total.", severity=400)
        return f"Data has {total_missing} missing values. Check logs for details."
    


def hash_dict(d):
    """
    Hash a dictionary (including nested structures).

    Args:
        d (dict): Dictionary to be hashed.

    Returns:
        str: MD5 hash of the dictionary.
    """
    try:
        return hashlib.md5(str(sorted(d.items())).encode()).hexdigest()
    except Exception as e:
        #logging.error(f"Error hashing dictionary {d}: {e}")
        gcp_logger.log_text(f"Error hashing dictionary {d}: {e}", severity=500)
        raise

#@task
def handle_duplicates(data):
    """
    Detect duplicates in an array of dictionaries using hashing.

    Args:
        data (list): List of dictionaries to check for duplicates.

    Returns:
        str: Message indicating whether duplicates were found.
    """
    #logging.info("Starting duplicate detection.")
    gcp_logger.log_text("Starting duplicate detection.", severity=200)

    try:
        # Generate hashes for all dictionaries in the data
        dedupe_array = Parallel(n_jobs=-1)(delayed(hash_dict)(x) for x in data)
        orig_len = len(data)
        #logging.info(f"Original data length: {orig_len}")
        gcp_logger.log_text(f"Original data length: {orig_len}", severity=200)

        # Remove duplicates by converting to a unique set of hashes
        dedupe_len = len(np.unique(np.array(dedupe_array)))
        #logging.info(f"Length after deduplication: {dedupe_len}")
        gcp_logger.log_text(f"Length after deduplication: {dedupe_len}", severity=200)

        if dedupe_len != orig_len:
            #logging.warning("Duplicates found in the data.")
            gcp_logger.log_text("Duplicates found in the data.", severity=400)
            return "Duplicates found."
        else:
            #logging.info("No duplicates found.")
            gcp_logger.log_text("No duplicates found.", severity=200)
            return "No duplicates found."
    except Exception as e:
        #logging.error(f"Error during duplicate detection: {e}")
        gcp_logger.log_text(f"Error during duplicate detection: {e}", severity=500)
        raise


#@flow()
# def dat_validation(data):
#     schema_validation(data)
#     check_data_completeness(data)
#     handle_duplicates(data)