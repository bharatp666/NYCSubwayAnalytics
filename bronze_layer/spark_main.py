from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pyspark.sql.functions as f
import great_expectations as gx
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from spark_utility import *
import argparse


# Define schema as StructType

def main(source_path,delta_table_path):
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
        .config("spark.executor.instances", "3") \
        .config("spark.executor.memory", "3g") \
        .config("spark.executor.cores", "3") \
        .config("spark.driver.memory", "3g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        


    df = spark.read.format('parquet').load(source_path)

    delta_presence = check_delta_existance(spark,delta_table_path)



    if delta_presence==False:
        
        # Perform data validation
        validation_summary = get_validations('nyc_data','ridership_data','basic_validation','full_data','full_batch',df)
        schema_check_results = validation_summary.loc[validation_summary['Expectation Type']=='expect_column_to_exist']
        
        if all(schema_check_results['Success']):
            if all(validation_summary['Success'])!=True:
                good_data,bad_data = data_isolation(df)
                for field in schema_struct.fields:
                    col_name = field.name
                    col_type = field.dataType
                    good_data = good_data.withColumn(col_name, good_data[col_name].cast(col_type))
            else:
                for field in schema_struct.fields:
                    col_name = field.name
                    col_type = field.dataType
                    df = df.withColumn(col_name, df[col_name].cast(col_type))
                    
                df = df.withColumn('year',f.year(df['transit_timestamp']))
                
                # Write the DataFrame to a partitioned Delta table
                df.write.format("delta") \
                    .mode("overwrite") \
                    .partitionBy("year") \
                    .save(delta_table_path)
                
                print('Success')
        else:
            raise 'Schema changed'
                
    else:
        # Read the Delta table
        delta_df = spark.read.format("delta").load(delta_table_path)
        
        latest_date_delta = delta_df.selectExpr("MAX(transit_timestamp)").collect()[0][0]
        
        latest_date_delta = latest_date_delta.strftime("%Y-%m-%dT%H:%M:%S")+ f".{latest_date_delta.microsecond // 1000:03d}"
        
        latest_date_source = df.selectExpr("MAX(transit_timestamp)").collect()[0][0]
        
        if latest_date_source!=latest_date_delta:
            print(latest_date_source,latest_date_delta)
            
            df_new = df.filter(f.col('transit_timestamp')>latest_date_delta)
            
            validation_summary = get_validations('nyc_data','ridership_data','basic_validation','incremental_data','full_batch',df_new)
            schema_check_results = validation_summary.loc[validation_summary['Expectation Type']=='expect_column_to_exist']
            
            # Perform data validation
            if all(schema_check_results['Success']):
                if all(validation_summary['Success'])!=True:
                    good_data,bad_data = data_isolation(df_new)
                    for field in schema_struct.fields:
                        col_name = field.name
                        col_type = field.dataType
                        good_data = good_data.withColumn(col_name, good_data[col_name].cast(col_type))
                else:
                    for field in schema_struct.fields:
                        col_name = field.name
                        col_type = field.dataType
                        df_new = df_new.withColumn(col_name, df_new[col_name].cast(col_type))

                    df_new = df_new.withColumn('year',f.year(df_new['transit_timestamp']))
                    
                    # Load the Delta table
                    delta_table = DeltaTable.forPath(spark, delta_table_path)
                    
                    # Perform merge to append only new records
                    delta_table.alias("target").merge(
                        df_new.alias("source"),
                        "target.transit_timestamp = source.transit_timestamp AND target.year = source.year"  # Ensure uniqueness with partition column
                    ).whenNotMatchedInsertAll() \
                    .execute()
        else:
            print('Data is upto date')

if __name__=='__main__':
    parser = argparse.ArgumentParser()
        # Adding arguments for the GCS bucket links
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

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(args.source_path, args.delta_table_path)