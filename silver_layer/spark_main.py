from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pyspark.sql.functions as f
import great_expectations as gx
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from spark_utility import *
import argparse
from logger_spark import *


# Define schema as StructType

def main(source_path, delta_table_path,quarantine_path_good,quarantine_path_bad):
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

    expected_columns = ["transit_timestamp", 
        "transit_mode", 
        "station_complex_id",
        "station_complex", 
        "borough", 
        "payment_method", 
        "fare_class_category", 
        "ridership", 
        "transfers", 
        "latitude", 
        "longitude", 
        "georeference"]

    spark = SparkSession.builder \
        .appName("ResourceIssueFix") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    df_ = spark.read.format('parquet').load(source_path)
    df = df_.dropDuplicates()
    gcp_logger.log_text(f"Loaded data from source path: {source_path}", severity=200)

    delta_presence = check_delta_existance(spark, delta_table_path)

    if not delta_presence:
        validation_summary = get_validations('nyc_data', 'ridership_data', 'basic_validation', 'full_data', 'full_batch', df)
        gcp_logger.log_text("Performed full data validation", severity=200)
        schema_check_results = validation_summary.loc[validation_summary['Expectation Type'] == 'expect_column_to_exist']

        if all(schema_check_results['Success']):
            gcp_logger.log_text("Schema validation successful",severity=200)
            if not all(validation_summary['Success']):
                gcp_logger.log_text("Data Validation Failed",severity=500)
                good_data, bad_data = data_isolation(df)
                for field in schema_struct.fields:
                    col_name = field.name
                    col_type = field.dataType
                    good_data = good_data.withColumn(col_name, good_data[col_name].cast(col_type))

                good_data.write.format('parquet').mode('append').save(quarantine_path_good)
                gcp_logger.log_text(f'Good records written succesfully in {quarantine_path_good}',severity=200)

                bad_data.write.format('parquet').mode('append').save(quarantine_path_bad)
                gcp_logger.log_text(f'Bad records written succesfully in {quarantine_path_bad}',severity=200) 

                raise 'exit'


            else:
                for field in schema_struct.fields:
                    col_name = field.name
                    col_type = field.dataType
                    df = df.withColumn(col_name, df[col_name].cast(col_type))

                df = df.withColumn('year', f.year(df['transit_timestamp']))

                df.write.format("delta") \
                    .mode("overwrite") \
                    .partitionBy("year") \
                    .save(delta_table_path)

                gcp_logger.log_text("Successfully wrote data to Delta table", severity=200)
        else:
            gcp_logger.log_text("Schema validation failed: Schema changed", severity=400)
            raise ValueError("Schema changed")

    else:
        delta_df = spark.read.format("delta").load(delta_table_path)
        gcp_logger.log_text(f"Read Delta table from path: {delta_table_path}", severity=200)

        latest_date_delta = delta_df.selectExpr("MAX(transit_timestamp)").collect()[0][0]
        latest_date_delta = latest_date_delta.strftime("%Y-%m-%dT%H:%M:%S") + f".{latest_date_delta.microsecond // 1000:03d}"

        latest_date_source = df.selectExpr("MAX(transit_timestamp)").collect()[0][0]

        if latest_date_source >= latest_date_delta:
            gcp_logger.log_text(f"Data discrepancy detected: Source ({latest_date_source}) Delta ({latest_date_delta})", severity=200)

            df_new = df.filter(f.col('transit_timestamp') >= latest_date_delta)

            validation_summary = get_validations('nyc_data', 'ridership_data', 'basic_validation', 'incremental_data', 'full_batch', df_new)
            schema_check_results = validation_summary.loc[validation_summary['Expectation Type'] == 'expect_column_to_exist']

            if all(schema_check_results['Success']):
                gcp_logger.log_text("Schema validation successful",severity=200)
                if not all(validation_summary['Success']):
                    gcp_logger.log_text("Data Validation Failed",severity=500)
                    good_data, bad_data = data_isolation(df_new)
                    for field in schema_struct.fields:
                        col_name = field.name
                        col_type = field.dataType
                        good_data = good_data.withColumn(col_name, good_data[col_name].cast(col_type))
                    
                    good_data.write.format('parquet').mode('append').save(quarantine_path_good)
                    gcp_logger.log_text(f'Good records written succesfully in {quarantine_path_good}',severity=200)

                    bad_data.write.format('parquet').mode('append').save(quarantine_path_bad)
                    gcp_logger.log_text(f'Bad records written succesfully in {quarantine_path_bad}',severity=200)

                    raise 'exit'

                else:
                    for field in schema_struct.fields:
                        col_name = field.name
                        col_type = field.dataType
                        df_new = df_new.withColumn(col_name, df_new[col_name].cast(col_type))

                    df_new = df_new.withColumn('year', f.year(df_new['transit_timestamp']))

                    delta_table = DeltaTable.forPath(spark, delta_table_path)

                    gcp_logger.log_text(f'successfully loaded delta table',severity=200)

                    # Create the merge condition for all columns
                    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in expected_columns])

                    merge_condition_par = merge_condition + ' AND target.year=source.year'

                    delta_table.alias("target").merge(
                        df_new.alias("source"),
                        merge_condition_par
                    ).whenNotMatchedInsertAll().execute()

                    gcp_logger.log_text("Successfully merged new data into Delta table", severity=200)
        else:
            gcp_logger.log_text("Data is up-to-date", severity=200)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "source_path",
        type=str,
        help="The GCS source bucket link (e.g., gs://source-bucket-name/)."
    )
    parser.add_argument(
        "delta_table_path",
        type=str,
        help="The GCS destination bucket link (e.g., gs://destination-bucket-name/)."
    )

    parser.add_argument(
        "quarantine_bucket_good",
        type=str,
        help="The GCS destination bucket link (e.g., gs://quarantine-bucket-name/)."
    )

    parser.add_argument(
        "quarantine_bucket_bad",
        type=str,
        help="The GCS destination bucket link (e.g., gs://quarantine-bucket-name/)."
    )

    args = parser.parse_args()

    main(args.source_path, args.delta_table_path,args.quarantine_path_good,args.quarantine_path_bad)
