import pandas as pd
import numpy as np
import logging
import polars as pl
from joblib import Parallel, delayed
import hashlib

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
        logging.info("Starting schema validation.")
        df = pl.DataFrame(data)
        logging.info("Data successfully converted to Polars DataFrame.")
    except Exception as e:
        logging.error(f"Failed to create Polars DataFrame: {e}")
        raise ValueError("Invalid data for Polars DataFrame conversion.") from e

    missing_columns = set(schema) - set(df.columns)
    extra_columns = set(df.columns) - set(schema)

    if missing_columns or extra_columns:
        logging.warning("Change in schema detected.")
        if missing_columns:
            logging.warning(f"Missing columns: {missing_columns}")
        if extra_columns:
            logging.warning(f"Extra columns: {extra_columns}")
        return "Schema validation failed: Check logs for details."
    else:
        logging.info("Schema is consistent.")
        return "Schema is consistent."
    

def check_data_completeness(data):
    """
    Check data completeness by logging the number of missing values for each column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame to check for null values.

    Returns:
        str: Message indicating whether the data is complete or has missing values.
    """
    logging.info("Starting data completeness check.")
        # Create Polars DataFrame
    try:
        # Schema validation
        df = pl.DataFrame(data)
        logging.info("Data successfully converted to Polars DataFrame.")
    except Exception as e:
        logging.error(f"Failed to create Polars DataFrame: {e}")
        raise ValueError("Invalid data for Polars DataFrame conversion.") from e
    
    # Count missing values for each column
    missing_values = df.null_count().to_pandas().sum()
    total_missing = missing_values.sum()

    # Log missing values for each column
    for col, missing in zip(df.columns, missing_values):
        if missing > 0:
            logging.warning(f"Column '{col}' has {missing} missing values.")

    # Check overall completeness
    if total_missing == 0:
        logging.info("Data is complete.")
        return "Data is complete."
    else:
        logging.warning(f"Data has {total_missing} missing values in total.")
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
        logging.error(f"Error hashing dictionary {d}: {e}")
        raise

def handle_duplicates(data):
    """
    Detect duplicates in an array of dictionaries using hashing.

    Args:
        data (list): List of dictionaries to check for duplicates.

    Returns:
        str: Message indicating whether duplicates were found.
    """
    logging.info("Starting duplicate detection.")

    try:
        # Generate hashes for all dictionaries in the data
        dedupe_array = Parallel(n_jobs=-1)(delayed(hash_dict)(x) for x in np.array(data))
        orig_len = len(data)
        logging.info(f"Original data length: {orig_len}")

        # Remove duplicates by converting to a unique set of hashes
        dedupe_len = len(np.unique(np.array(dedupe_array)))
        logging.info(f"Length after deduplication: {dedupe_len}")

        if dedupe_len != orig_len:
            logging.warning("Duplicates found in the data.")
            return "Duplicates found."
        else:
            logging.info("No duplicates found.")
            return "No duplicates found."
    except Exception as e:
        logging.error(f"Error during duplicate detection: {e}")
        raise