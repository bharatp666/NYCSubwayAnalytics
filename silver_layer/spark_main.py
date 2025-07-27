# from datetime import datetime
# from pyspark.sql import SparkSession
# from delta.tables import DeltaTable
# import pyspark.sql.functions as f
# import great_expectations as gx
# import pandas as pd
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
# from spark_utility import *
# import argparse
# from logger_spark import *


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import pyspark.sql.functions as f
import argparse
from logger_spark import *
from spark_utility import *
from great_expectations.dataset import SparkDFDataset

def main(source_path, good_path, bad_path):
    # Define schema
    schema_struct = StructType([
        StructField("transit_timestamp", TimestampType(), True),
        StructField("transit_mode", StringType(), True),
        StructField("station_complex_id", StringType(), True),
        StructField("station_complex", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("fare_class_category", StringType(), True),
        StructField("ridership", IntegerType(), True),
        StructField("transfers", IntegerType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("georeference", StringType(), True)
    ])

    spark = SparkSession.builder \
        .appName("SimpleSparkValidation") \
        .getOrCreate()

    df_raw = spark.read.format("parquet").load(source_path).dropDuplicates()
    gcp_logger.log_text(f"Read data from: {source_path}", severity=200)

    # Run validations using Great Expectations
    validation_summary = get_validations(
        table="nyc_data",
        dataset="ridership_data",
        expectation_suite="basic_validation",
        batch="full_batch",
        batch_type="full_data",
        df=df_raw
    )

    if validation_summary.empty:
        gcp_logger.log_text("Validation summary is empty — skipping write.", severity=400)
        raise Exception("No validations executed.")

    if not all(validation_summary['Success']):
        gcp_logger.log_text("Validation failed. Isolating good and bad data...", severity=400)

        good_data, bad_data = data_isolation(df_raw)

        # Cast good data to correct types
        for field in schema_struct.fields:
            good_data = good_data.withColumn(field.name, good_data[field.name].cast(field.dataType))
            bad_data = bad_data.withColumn(field.name, bad_data[field.name].cast(field.dataType))

        good_data.write.mode("append").parquet(good_path)
        bad_data.write.mode("append").parquet(bad_path)

        gcp_logger.log_text(f"Good data written to: {good_path}", severity=200)
        gcp_logger.log_text(f"Bad data written to: {bad_path}", severity=200)
    else:
        gcp_logger.log_text("Validation passed. Writing all records to good path.", severity=200)

        df_validated = df_raw
        for field in schema_struct.fields:
            df_validated = df_validated.withColumn(field.name, df_validated[field.name].cast(field.dataType))

        df_validated.write.mode("append").parquet(good_path)
        gcp_logger.log_text(f"All records written to: {good_path}", severity=200)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("source_path", type=str, help="GCS input path for Bronze data")
    parser.add_argument("good_path", type=str, help="GCS output path for validated (good) data")
    parser.add_argument("bad_path", type=str, help="GCS output path for invalid (bad) data")
    args = parser.parse_args()

    main(args.source_path, args.good_path, args.bad_path)