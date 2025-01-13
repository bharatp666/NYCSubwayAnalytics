from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session with BigQuery connector
spark = SparkSession.builder \
        .appName("DeltaToBigQuery") \
        .getOrCreate()

# Define Delta table and BigQuery table paths
delta_table_path = "gs://delta-table-nyc/delta_table/"  # Replace with your Delta table path
temp_gcs_bucket = "temp_bq_bk"
project_id = "lithe-creek-446516-r9"
dataset_id = "nyc_subway_data"
bq_table_id = "hourly_trip_data"
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
delta_df = spark.read.format("delta").load(delta_table_path)

# Filter Delta table to include only the required columns
delta_df = delta_df.select(*required_columns)

# Check if BigQuery table has data
try:
    bq_df = spark.read.format("bigquery") \
        .option("table", bq_table_full_name) \
        .load()

    # Check if the BigQuery table has data
    bq_table_has_data = bq_df.count() > 0
except Exception as e:
    print(f"Error accessing BigQuery table: {e}")
    bq_table_has_data = False

# If BigQuery table has no data, write all Delta table records to BigQuery
if not bq_table_has_data:
    delta_df.write.format("bigquery") \
        .option("table", bq_table_full_name) \
        .option("temporaryGcsBucket", temp_gcs_bucket) \
        .mode("overwrite") \
        .save()
    print("BigQuery table has no data. All records from Delta table written to BigQuery.")
else:
    # Fetch unique combinations from BigQuery
    bq_unique_df = bq_df.select(
        col("transit_timestamp").alias("bq_transit_timestamp"),
        col("station_complex_id").alias("bq_station_complex_id"),
        col("payment_method").alias("bq_payment_method")
    ).distinct()

    # Prepare Delta table with the same unique columns
    delta_unique_df = delta_df.select(
        col("transit_timestamp").alias("delta_transit_timestamp"),
        col("station_complex_id").alias("delta_station_complex_id"),
        col("payment_method").alias("delta_payment_method"),
        "*"
    )

    # Perform anti-join to filter out redundant records
    new_records_df = delta_unique_df.join(
        bq_unique_df,
        (
            (delta_unique_df["delta_transit_timestamp"] == bq_unique_df["bq_transit_timestamp"]) &
            (delta_unique_df["delta_station_complex_id"] == bq_unique_df["bq_station_complex_id"]) &
            (delta_unique_df["delta_payment_method"] == bq_unique_df["bq_payment_method"])
        ),
        "left_anti"  # Select only records that don't exist in BigQuery
    )

    # Write new records to BigQuery or handle case with no new records
    new_records_count = new_records_df.count()
    if new_records_count > 0:
        new_records_df.select(*required_columns).write.format("bigquery") \
            .option("table", bq_table_full_name) \
            .option("temporaryGcsBucket", temp_gcs_bucket) \
            .mode("append") \
            .save()
        print(f"{new_records_count} new required records written to BigQuery.")
    else:
        print("No new records to write to BigQuery. Skipping write operation.")
