import requests
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import polars as pl
from joblib import Parallel, delayed
import re
from google.cloud import storage
from functions import *
import agrparse



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestion Job Config")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--folder", required=True, help="Folder name")
    parser.add_argument("--n_jobs", type=int, default=1, help="Number of parallel jobs")

    args = parser.parse_args()

    BUCKET_NAME = args.bucket
    FOLDER_NAME = args.folder
    N_JOBS = args.n_jobs

    
    source_timestamps = get_source_dates()
    destination_timestamps = get_existing_datetimes_from_gcs(BUCKET_NAME,FOLDER_NAME)
    delta_timestamps = list(source_timestamps - destination_timestamps)
    if delta_timestamps:
        fetch_timestamps =[]
        for i in delta_timestamps:
            fetch_timestamps.append((i,increment_month(i)))

        results = Parallel(N_JOBS)(
            delayed(fetch_and_write)(start, end, BUCKET_NAME, FOLDER_NAME)
            for start, end in fetch_timestamps
        )
    else:
        gcp_logger.log_text('Data is upto date', severity="INFO")
