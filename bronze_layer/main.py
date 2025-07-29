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
    N_JOBS = int(os.getenv("N_JOBS"))
    META_BUCKET = os.getenv("META_BUCKET")
    META_FOLDER = os.getenv("META_FOLDER")
    
    # From Cloud Run injected env vars
    TASK_INDEX = int(os.getenv("CLOUD_RUN_TASK_INDEX", "0"))
    TASK_COUNT = int(os.getenv("CLOUD_RUN_TASK_COUNT", "1"))

    source_timestamps = get_source_dates()
    destination_timestamps = get_existing_datetimes_from_gcs(BUCKET_NAME, FOLDER_NAME)
    delta_timestamps = sorted(list(source_timestamps - destination_timestamps))

    if delta_timestamps:
        dt_df = pl.DataFrame({"new_timestamps":delta_timestamps})
        dt_df.write_parquet(f"gs://{META_BUCKET}/{META_FOLDER}/new_timestamps.parquet")
        gcp_logger.log_text('Written timestamp metadata',severity = "INFO")

    else:
        gcp_logger.log_text('Data is up to date', severity="INFO")
        exit(0)

    # Task-aware chunk distribution (round-robin)
    my_chunk = [ts for i, ts in enumerate(delta_timestamps) if i % TASK_COUNT == TASK_INDEX]


    if not my_chunk:
        gcp_logger.log_text(f"No work assigned to task {TASK_INDEX}", severity="INFO")
        exit(0)

    fetch_timestamps = [(i, increment_month(i)) for i in my_chunk]


    Parallel(n_jobs=N_JOBS)(
        delayed(fetch_and_write)(start, end, BUCKET_NAME, FOLDER_NAME)
        for start, end in fetch_timestamps
    )

