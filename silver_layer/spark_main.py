from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.functions import date_format
from delta.tables import DeltaTable
import pyspark.sql.functions as f
import great_expectations as gx
from logger_spark import *
from spark_utility import *
import pandas as pd
import argparse
import polars as pl
import gcsfs
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--meta_bucket", required=True)
    parser.add_argument("--meta_folder", required=True)
    parser.add_argument("--ingest_bucket", required=True)
    parser.add_argument("--ingest_folder", required=True)
    parser.add_argument("--config_bucket", required=True)
    parser.add_argument("--config_folder", required=True)
    parser.add_argument("--delta_bucket", required=True)
    parser.add_argument("--delta_folder", required=True)
    parser.add_argument("--quarantine_bucket", required=True)
    parser.add_argument("--quarantine_good_folder", required=True)
    parser.add_argument("--quarantine_bad_folder", required=True)
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument("--temp_gcs_bucket",required=True)
    args = parser.parse_args()

    # Build quarantine paths
    quarantine_path_good = f'gs://{args.quarantine_bucket}/{args.quarantine_good_folder}'
    quarantine_path_bad = f'gs://{args.quarantine_bucket}/{args.quarantine_bad_folder}'
    delta_table_path = f'gs://{args.delta_bucket}/{args.delta_folder}/'


    # Load timestamps
    df_timestamps = pl.read_parquet(f"gs://{args.meta_bucket}/{args.meta_folder}/new_timestamps.parquet")['new_timestamps'].to_list()

    filenames = [get_gcs_uri_from_date(i, args.ingest_bucket, args.ingest_folder) for i in df_timestamps]

    # Load config
    fs = gcsfs.GCSFileSystem()
    with fs.open(f'{args.config_bucket}/{args.config_folder}/validation_config.json', 'r') as f:
        config = json.load(f)

    expected_columns = config['expected_columns']
    schema_dict = config["schema"]
    key_columns = config['key_columns']

            # Start Spark session
    spark = SparkSession.builder \
        .appName("RidershipValidationJob") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


    # Read and cast data
    df = spark.read.parquet(*filenames).select(expected_columns)
    df = df.select([col(col_name).cast(dtype) for col_name, dtype in schema_dict.items()])
    df = df.dropDuplicates()

    # Run validation
    validation_summary = get_validations(
        config=config,
        data_source_name='nyc_data',
        data_asset_name='ridership_data',
        suite_name='basic_validation',
        batch_definition_name='full_batch',
        definition_name='full_data',
        df=df
    )

    #df = df.withColumn("transit_timestamp", to_timestamp("transit_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS"))

    if not all(validation_summary['Success']):
        gcp_logger.log_text("Data Validation Failed", severity=500)
        good_data, bad_data = data_isolation(df)

        good_data.write.format('parquet').mode('append').save(quarantine_path_good)
        gcp_logger.log_text(f'Good records written successfully in {quarantine_path_good}', severity=200)

        bad_data.write.format('parquet').mode('append').save(quarantine_path_bad)
        gcp_logger.log_text(f'Bad records written successfully in {quarantine_path_bad}', severity=200)

        raise Exception("Validation failed. Files quarantined.")

    else:
        gcp_logger.log_text("Data Validation Passed", severity=200)
        upsert_data(spark, df, delta_table_path, args.project_id, args.dataset_id, args.temp_gcs_bucket , key_columns)
        gcp_logger.log_text("Upsert to Delta table and BigQuery completed successfully", severity=200)

if __name__ == "__main__":
    main()

