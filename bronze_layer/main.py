import requests
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import polars as pl
from joblib import Parallel, delayed
import re
from google.cloud import storage
from functions import *



# if __name__ == "__main__":
#     BUCKET_NAME = os.getenv("BUCKET_NAME")
#     FOLDER_NAME = os.getenv("FOLDER_NAME")
#     N_JOBS = int(os.getenv("N_JOBS", 1))

    
#     source_timestamps = get_source_dates()
#     destination_timestamps = get_existing_datetimes_from_gcs(BUCKET_NAME,FOLDER_NAME)
#     delta_timestamps = list(source_timestamps - destination_timestamps)
#     if delta_timestamps:
#         fetch_timestamps =[]
#         for i in delta_timestamps:
#             fetch_timestamps.append((i,increment_month(i)))

#         results = Parallel(N_JOBS)(
#             delayed(fetch_and_write)(start, end, BUCKET_NAME, FOLDER_NAME)
#             for start, end in fetch_timestamps
#         )
#     else:
#         gcp_logger.log_text('Data is upto date', severity="INFO")

if __name__ == "__main__":
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    FOLDER_NAME = os.getenv("FOLDER_NAME")
    
    # From Cloud Run injected env vars
    TASK_INDEX = int(os.getenv("CLOUD_RUN_TASK_INDEX", "0"))
    TASK_COUNT = int(os.getenv("CLOUD_RUN_TASK_COUNT", "1"))

    source_timestamps = get_source_dates()
    destination_timestamps = get_existing_datetimes_from_gcs(BUCKET_NAME, FOLDER_NAME)
    delta_timestamps = sorted(list(source_timestamps - destination_timestamps))

    if not delta_timestamps:
        gcp_logger.log_text('Data is up to date', severity="INFO")
        exit(0)

    # Partition delta_timestamps across tasks
    total = len(delta_timestamps)
    chunk_size = (total + TASK_COUNT - 1) // TASK_COUNT  # ceiling division
    start_idx = TASK_INDEX * chunk_size
    end_idx = min(start_idx + chunk_size, total)
    my_chunk = delta_timestamps[start_idx:end_idx]

    if not my_chunk:
        gcp_logger.log_text(f"No work assigned to task {TASK_INDEX}", severity="INFO")
        exit(0)

    fetch_timestamps = [(i, increment_month(i)) for i in my_chunk]

    for start, end in fetch_timestamps:
        fetch_and_write(start, end, BUCKET_NAME, FOLDER_NAME)

