# Perform data validation using Great Expectations
import great_expectations as gx
import pandas as pd
from delta.tables import DeltaTable
from logger_spark import *

# Perform data validation using Great Expectations

def get_validations(data_source_name,data_asset_name,suite_name,batch_definition_name,definition_name,df):
    
    global df_
    global fare_class_cat
    
    fare_class_cat = ['Metrocard - Fair Fare',
       'OMNY - Seniors & Disability',
       'Metrocard - Seniors & Disability',
       'Metrocard - Full Fare',
       'OMNY - Other',
       'OMNY - Full Fare',
       'Metrocard - Unlimited 7-Day',
       'Metrocard - Unlimited 30-Day',
       'Metrocard - Students',
       'Metrocard - Other',
       'OMNY - Students',
       'OMNY - Fair Fare']
    
    df_ = pd.read_csv('https://raw.githubusercontent.com/bharatp666/NYCSubwayAnalytics/updated2/station_data.csv')
    
    
    
    context = gx.get_context()
    
    gcp_logger.log_text(f'')
    
    # Define the Data Source name

    # Add the Data Source to the Data Context
    data_source = context.data_sources.add_spark(name=data_source_name)
    gcp_logger.log_text(f'gx: Data source created successfully {data_source_name}',severity=200)

    # Define the name of your data asset

    data_asset = data_source.add_dataframe_asset(name=data_asset_name)
    gcp_logger.log_text(f'gx: Data asset created successfully {data_asset_name}',severity=200)

    batch_parameters = {"dataframe": df}
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
    batch_definition_name
    )
    gcp_logger.log_text(f'gx: Batch created successfully {batch_definition_name}',severity=200)

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Create an Expectation Suite
    suite = gx.ExpectationSuite(name=suite_name)
    
    gcp_logger.log_text(f'gx: Expectation suite created successfully {suite_name}',severity=200)
    

    # Add the Expectation Suite to the Data Context
    suite = context.suites.add(suite)
    
    for i in expected_columns:
        expectation = gx.expectations.ExpectColumnToExist(
            column=i
        )
        # Add the previously created Expectation to the Expectation Suite
        suite.add_expectation(expectation)
        gcp_logger.log_text(f'gx: Adding {i} to ExpectColumnToExist', severity=200)

    # Add Expectations to the Suite
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="transit_timestamp",
        regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$"
    ))
    gcp_logger.log_text(f'gx: Adding transit_timestamp to ExpectColumnValuesToMatchRegex', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='transit_mode',
        value_set=['subway', 'tram', 'staten_island_railway']
    ))
    gcp_logger.log_text('gx: Adding transit_mode to ExpectColumnValuesToBeInSet', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='station_complex_id',
        value_set=list(df_['station_complex_id'])
    ))
    gcp_logger.log_text('gx: Adding station_complex_id to ExpectColumnValuesToBeInSet', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='station_complex',
        value_set=list(df_['station_complex'])
    ))
    gcp_logger.log_text('gx: Adding station_complex to ExpectColumnValuesToBeInSet', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='borough',
        value_set=['Brooklyn', 'Manhattan', 'Bronx', 'Queens', 'Staten Island']
    ))
    gcp_logger.log_text('gx: Adding borough to ExpectColumnValuesToBeInSet', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='payment_method',
        value_set=['metrocard', 'omny']
    ))
    gcp_logger.log_text('gx: Adding payment_method to ExpectColumnValuesToBeInSet', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='fare_class_category',
        value_set=fare_class_cat
    ))
    gcp_logger.log_text('gx: Adding fare_class_category to ExpectColumnValuesToBeInSet', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='ridership',
        min_value=1,
        max_value=16217
    ))
    gcp_logger.log_text('gx: Adding ridership to ExpectColumnValuesToBeBetween', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='transfers',
        min_value=0,
        max_value=1450
    ))
    gcp_logger.log_text('gx: Adding transfers to ExpectColumnValuesToBeBetween', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='latitude',
        min_value=40.576126,
        max_value=40.903126
    ))
    gcp_logger.log_text('gx: Adding latitude to ExpectColumnValuesToBeBetween', severity=200)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='longitude',
        min_value=-74.07484,
        max_value=-73.7554
    ))
    gcp_logger.log_text('gx: Adding longitude to ExpectColumnValuesToBeBetween', severity=200)

    # Create Validation Definition
    validation_definition = gx.ValidationDefinition(
        data=batch_definition, suite=suite, name=definition_name
    )
    gcp_logger.log_text('gx: Creating validation definition', severity=200)

    # Add Validation Definition to the Data Context
    validation_definition = context.validation_definitions.add(validation_definition)
    gcp_logger.log_text('gx: Added validation definition to context', severity=200)

    # Validate the Batch
    validation_results = batch.validate(suite)
    gcp_logger.log_text('gx: Validation executed', severity=200)

    # Process Validation Results
    results = [
        {
            "Expectation Type": result["expectation_config"]["type"],
            "Column": result["expectation_config"]["kwargs"].get("column", None),
            "Success": result["success"],
            "Unexpected Count": result["result"].get("unexpected_count", 0),
            "Unexpected Percent": result["result"].get("unexpected_percent", 0.0),
            "Partial Unexpected List": result["result"].get("partial_unexpected_list", [])
        }
        for result in validation_results["results"]
    ]
    gcp_logger.log_text('gx: Processed validation results', severity=200)

    # Convert Results to DataFrame
    res_df = pd.DataFrame(results)
    gcp_logger.log_text('gx: Converted validation results to DataFrame', severity=200)
    
    return res_df



def data_isolation(df):

    regex_pattern_timestamp = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$"
    allowed_transit_modes = ['subway', 'tram', 'staten_island_railway']
    allowed_station_complex_ids = list(df_['station_complex_id'])  # Replace with actual values
    allowed_station_complexes = list(df_['station_complex'])
    allowed_boroughs = ['Brooklyn', 'Manhattan', 'Bronx', 'Queens', 'Staten Island']
    allowed_payment_methods = ['metrocard', 'omny']
    fare_class_cat = fare_class_cat  # Replace with actual values
    ridership_min, ridership_max = 1, 16217
    transfers_min, transfers_max = 0, 1450
    latitude_min, latitude_max = 40.576126, 40.903126
    longitude_min, longitude_max = -74.07484, -73.7554

    # Combine all conditions
    bad_records_condition = (
        ~df["transit_timestamp"].rlike(regex_pattern_timestamp) |
        ~df["transit_mode"].isin(allowed_transit_modes) |
        ~df["station_complex_id"].isin(allowed_station_complex_ids) |
        ~df["station_complex"].isin(allowed_station_complexes) |
        ~df["borough"].isin(allowed_boroughs) |
        ~df["payment_method"].isin(allowed_payment_methods) |
        ~df["fare_class_category"].isin(fare_class_cat) |
        (df["ridership"] < ridership_min) | (df["ridership"] > ridership_max) |
        (df["transfers"] < transfers_min) | (df["transfers"] > transfers_max) |
        (df["latitude"] < latitude_min) | (df["latitude"] > latitude_max) |
        (df["longitude"] < longitude_min) | (df["longitude"] > longitude_max)
    )

    # Filter bad records
    bad_records_df = df.filter(bad_records_condition)
    gcp_logger.log_text(f"Bad records proportion: {bad_records_df.count()/df.count()}", severity=200)

    # Filter good records (optional, if needed)
    good_records_df = df.filter(~bad_records_condition)
    gcp_logger.log_text(f"Good records proportion: {good_records_df.count()/df.count()}", severity=200)

    # Show bad records
    print(f"Bad Records: {bad_records_df.count()}")

    # Show good records (optional)
    print(f"Good Records: {good_records_df.count()}")
    
    return good_records_df, bad_records_df

# Check delta table presence 
def check_delta_existance(spark, delta_table_path):
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        gcp_logger.log_text(f"Delta table exists at path: {delta_table_path}", severity=200)
        return True
    else:
        gcp_logger.log_text(f"Delta table does not exist at path: {delta_table_path}", severity=500)
        return False