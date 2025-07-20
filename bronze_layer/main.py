from functions import *
import polars as pl
import os
import time
from joblib import Parallel, delayed
from tqdm import tqdm
from logger_init import *  # your initialized logger
from google.cloud import storage




if __name__ == "__main__":
    # Configuration
    BUCKET_NAME = "mi-buck"
    FOLDER_NAME = "nycdata"

    last_date = check_gcs_parquet_files(BUCKET_NAME,FOLDER_NAME)
    min_date, max_date = fetch_min_max_dates()

    if last_date is None:
        date_ranges = split_date_range(min_date, max_date)
        labelled_dates = label_chunks_by_time(date_ranges)
        str_labelled_dates = convert_timechunks_to_strings(labelled_dates)  

        
        start_time = time.time()

        results = Parallel(n_jobs=3)(
            delayed(fetch_and_write_bulk)(start, end, label,BUCKET_NAME,FOLDER_NAME)
            for start, end, label in tqdm(str_labelled_dates, desc="Processing")
        )

        for res in results:
            print(res)

        total_time = time.time() - start_time
        print(f"\n Total time taken: {total_time:.2f} seconds")
    
    else:
        date_ranges = split_date_range(last_date,max_date)
        labelled_dates = label_chunks_by_time(date_ranges)
        str_labelled_dates = convert_timechunks_to_strings(labelled_dates)  

        start_time = time.time()

        results = Parallel(n_jobs=3)(
            delayed(fetch_and_write_incremental)(start, end, label, BUCKET_NAME, FOLDER_NAME)
            for start, end, label in tqdm(str_labelled_dates, desc="Processing")
        )

        for res in results:
            print(res)

        total_time = time.time() - start_time
        print(f"\n Total time taken: {total_time:.2f} seconds")

