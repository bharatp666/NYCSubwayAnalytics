import requests
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import polars as pl
from joblib import Parallel, delayed
import re
from google.cloud import storage
from functions import *


if __name__ == "__main__":
    # Configuration
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    FOLDER_NAME = os.getenv("FOLDER_NAME")

    if not BUCKET_NAME or not FOLDER_NAME:
        raise ValueError("Environment variables BUCKET_NAME and FOLDER_NAME are required.")
    
    source_timestamps = get_source_dates()
    destination_timestamps = get_existing_datetimes_from_gcs(BUCKET_NAME,FOLDER_NAME)
    delta_timestamps = list(source_timestamps - destination_timestamps)
    fetch_timestamps =[]
    for i in delta_timestamps:
        fetch_timestamps.append((i,increment_month(i)))

    results = Parallel(n_jobs=4)(
        delayed(fetch_and_write)(start, end, BUCKET_NAME, FOLDER_NAME)
        for start, end in fetch_timestamps
    )
