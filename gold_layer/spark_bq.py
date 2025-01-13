import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from google.cloud import logging as gcp_logging

def main(delta_table_path, temp_gcs_bucket, project_id, dataset_id, bq_table_id):
    # Initialize GCP Logger
    logging_client = gcp_logging.Client()
    gcp_logger = logging_client.logger("delta_to_bigquery") 
    
    gcp_logger.log_text("Starting Delta to BigQuery data transfer process.", severity=200)
    
    # Initialize Spark session with BigQuery connector
    spark = SparkSession.builder \
            .appName("DeltaToBigQuery") \
            .getOrCreate()
    
    gcp_logger.log_text("Spark session initialized successfully.", severity=200)
    
    bq_table_full_name = f"{project_id}.{dataset_id}.{bq_table_id}"
    
    required_columns = ["transit_timestamp", 
        "transit_mode", 
        "station_complex_id",
        "station_complex", 
        "borough", 
        "payment_method", 
        "fare_class_category", 
        "ridership", 
        "transfers", 
        "latitude", 
        "longitude"]
    
    # Read Delta table
    try:
        delta_df = spark.read.format("delta").load(delta_table_path)
        gcp_logger.log_text(f"Loaded data from Delta table path: {delta_table_path}", severity=200)
    except Exception as e:
        gcp_logger.log_text(f"Failed to load Delta table: {str(e)}", severity=500)
        raise e
    
    # Filter Delta table to include only the required columns
    delta_df = delta_df.select(*required_columns)
    gcp_logger.log_text("Filtered Delta table to include required columns.", severity=200)
    
    # Check if BigQuery table has data
    try:
        bq_df = spark.read.format("bigquery") \
            .option("table", bq_table_full_name) \
            .load()
        bq_table_has_data = bq_df.count() > 0
        gcp_logger.log_text(f"BigQuery table {bq_table_full_name} data check completed. Data exists: {bq_table_has_data}", severity=200)
    except Exception as e:
        gcp_logger.log_text(f"Error accessing BigQuery table: {str(e)}", severity=500)
        bq_table_has_data = False
    
    # If BigQuery table has no data, write all Delta table records to BigQuery
    if not bq_table_has_data:
        try:
            delta_df.write.format("bigquery") \
                .option("table", bq_table_full_name) \
                .option("temporaryGcsBucket", temp_gcs_bucket) \
                .mode("overwrite") \
                .save()
            gcp_logger.log_text("BigQuery table has no data. All records from Delta table written to BigQuery.", severity=200)
        except Exception as e:
            gcp_logger.log_text(f"Failed to write to BigQuery: {str(e)}", severity=500)
            raise e
    else:
        # Fetch unique combinations from BigQuery
        bq_unique_df = bq_df.select(
            col("transit_timestamp").alias("bq_transit_timestamp"),
            col("station_complex_id").alias("bq_station_complex_id"),
            col("fare_class_category").alias("bq_fare_class_category")
        ).distinct()
    
        # Prepare Delta table with the same unique columns
        delta_unique_df = delta_df.select(
            col("transit_timestamp").alias("delta_transit_timestamp"),
            col("station_complex_id").alias("delta_station_complex_id"),
            col("fare_class_category").alias("delta_fare_class_category"),
            "*"
        )
    
        # Perform anti-join to filter out redundant records
        new_records_df = delta_unique_df.join(
            bq_unique_df,
            (
                (delta_unique_df["delta_transit_timestamp"] == bq_unique_df["bq_transit_timestamp"]) &
                (delta_unique_df["delta_station_complex_id"] == bq_unique_df["bq_station_complex_id"]) &
                (delta_unique_df["delta_fare_class_category"] == bq_unique_df["bq_fare_class_category"])
            ),
            "left_anti"  # Select only records that don't exist in BigQuery
        )
    
        new_records_count = new_records_df.count()
        gcp_logger.log_text(f"Identified {new_records_count} new records to be written to BigQuery.", severity=200)
    
        # Write new records to BigQuery or handle case with no new records
        if new_records_count > 0:
            try:
                new_records_df.select(*required_columns).write.format("bigquery") \
                    .option("table", bq_table_full_name) \
                    .option("temporaryGcsBucket", temp_gcs_bucket) \
                    .mode("append") \
                    .save()
                gcp_logger.log_text(f"{new_records_count} new required records written to BigQuery.", severity=200)
            except Exception as e:
                gcp_logger.log_text(f"Failed to write new records to BigQuery: {str(e)}", severity=500)
                raise e
        else:
            gcp_logger.log_text("No new records to write to BigQuery. Skipping write operation.", severity=200)
    
    gcp_logger.log_text("Delta to BigQuery data transfer process completed.", severity=200)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta to BigQuery data transfer")
    parser.add_argument("--delta_table_path", required=True, help="Path to the Delta table in GCS")
    parser.add_argument("--temp_gcs_bucket", required=True, help="Temporary GCS bucket for BigQuery")
    parser.add_argument("--project_id", required=True, help="GCP project ID")
    parser.add_argument("--dataset_id", required=True, help="BigQuery dataset ID")
    parser.add_argument("--bq_table_id", required=True, help="BigQuery table ID")

    args = parser.parse_args()
    
    main(
        delta_table_path=args.delta_table_path,
        temp_gcs_bucket=args.temp_gcs_bucket,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        bq_table_id=args.bq_table_id
    )
