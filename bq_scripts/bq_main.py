import argparse
import json
from google.cloud import bigquery
from google.cloud import logging as gcp_logging


gcp_client = gcp_logging.Client()
gcp_logger = gcp_client.logger("bq_upsert_logger")

def upsert_ridership_data(project_id: str, dataset_id: str, staging_table: str, main_table: str, key_columns: list):
    client = bigquery.Client(project=project_id)

    try:
        # Step 1: Create main table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{main_table}` (
            transit_timestamp DATETIME,
            transit_mode STRING,
            station_complex_id STRING,
            station_complex STRING,
            borough STRING,
            payment_method STRING,
            fare_class_category STRING,
            ridership INT64,
            transfers INT64,
            latitude FLOAT64,
            longitude FLOAT64
        )
        """
        gcp_logger.log_text(f"Ensuring main table `{dataset_id}.{main_table}` exists", severity=200)
        client.query(create_table_sql).result()
        gcp_logger.log_text(f"Main table check/creation completed", severity=200)

        # Step 2: MERGE 
        merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])

        merge_sql = f"""
        MERGE `{project_id}.{dataset_id}.{main_table}` AS target
        USING (
          SELECT
            DATETIME(transit_timestamp) AS transit_timestamp,
            transit_mode,
            station_complex_id,
            station_complex,
            borough,
            payment_method,
            fare_class_category,
            ridership,
            transfers,
            latitude,
            longitude
          FROM `{project_id}.{dataset_id}.{staging_table}`
        ) AS source
        ON {merge_condition}
        WHEN NOT MATCHED THEN
          INSERT (
            transit_timestamp,
            transit_mode,
            station_complex_id,
            station_complex,
            borough,
            payment_method,
            fare_class_category,
            ridership,
            transfers,
            latitude,
            longitude
          )
          VALUES (
            source.transit_timestamp,
            source.transit_mode,
            source.station_complex_id,
            source.station_complex,
            source.borough,
            source.payment_method,
            source.fare_class_category,
            source.ridership,
            source.transfers,
            source.latitude,
            source.longitude
          )
        """
        gcp_logger.log_text("Executing merge from staging to main table", severity=200)
        client.query(merge_sql).result()
        gcp_logger.log_text(f"Merge completed successfully into `{dataset_id}.{main_table}`", severity=200)

    except Exception as e:
        gcp_logger.log_text(f"Error during BigQuery upsert: {str(e)}", severity=500)
        raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument("--staging_table", default="ridership_staging_table")
    parser.add_argument("--main_table", default="ridership")
    parser.add_argument("--key_columns_json", required=True, help="JSON string of merge key columns")

    args = parser.parse_args()
    key_columns = json.loads(args.key_columns_json)

    gcp_logger.log_text("BigQuery upsert pipeline started", severity=200)

    upsert_ridership_data(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        staging_table=args.staging_table,
        main_table=args.main_table,
        key_columns=key_columns
    )

    gcp_logger.log_text("BigQuery upsert pipeline finished", severity=200)

if __name__ == "__main__":
    main()
