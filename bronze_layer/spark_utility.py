# Perform data validation using Great Expectations
import great_expectations as gx
import pandas as pd

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

    # Define the Data Source name

    # Add the Data Source to the Data Context
    data_source = context.data_sources.add_spark(name=data_source_name)

    # Define the name of your data asset

    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

    batch_parameters = {"dataframe": df}
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
    batch_definition_name
    )

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Create an Expectation Suite

    suite = gx.ExpectationSuite(name=suite_name)

    # Add the Expectation Suite to the Data Context
    suite = context.suites.add(suite)
    
    for i in expected_columns:
        expectation = gx.expectations.ExpectColumnToExist ( 
            column = i
        )
        # Add the previously created Expectation to the Expectation Suite
        suite.add_expectation(expectation)
    

    # Create an Expectation to test
    suite.add_expectation (gx.expectations.ExpectColumnValuesToMatchRegex(
    column="transit_timestamp",
    regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$"
    ))


    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeInSet(
    column='transit_mode',
    value_set=['subway','tram','staten_island_railway']
    )

                      )

    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeInSet( 
    column='station_complex_id',
    value_set = list(df_['station_complex_id'])

    )
                      )

    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeInSet (
    column='station_complex',
    value_set = list(df_['station_complex'])
    )
                      )

    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeInSet ( 
    column = 'borough',
    value_set = ['Brooklyn','Manhattan','Bronx','Queens','Staten Island']
    )
                      )

    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeInSet (
    column = 'payment_method',
    value_set = ['metrocard','omny']
    )
                      )

    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeInSet (
    column = 'fare_class_category',
    value_set = fare_class_cat
    )
                      )

    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeBetween(
    column = 'ridership',
    min_value=1,
    max_value=16217
    )
                      )

    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeBetween(
    column = 'transfers',
    min_value=0,
    max_value=1450
    )
                      )

    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeBetween(
    column = 'latitude',
    min_value=40.576126,
    max_value=40.903126
    )
                      )

    suite.add_expectation ( gx.expectations.ExpectColumnValuesToBeBetween(
    column = 'longitude',
    min_value=-74.07484,
    max_value=-73.7554
    )
                      )


    validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name=definition_name
    )


    # Add the Validation Definition to the Data Context
    validation_definition = context.validation_definitions.add(validation_definition)

    validation_results = batch.validate(suite)
    
    results = [
    {
        "Expectation Type": result["expectation_config"]["type"],
        "Column": result["expectation_config"]["kwargs"].get("column", None),
        "Success": result["success"],
        "Unexpected Count": result["result"].get("unexpected_count", 0),
        "Unexpected Percent": result["result"].get("unexpected_percent", 0.0),
        "Partial Unexpected List": result["result"].get("partial_unexpected_list", []),
    }
    for result in validation_results["results"]
    ]

    # Convert to Pandas DataFrame
    res_df = pd.DataFrame(results)
    
    return res_df


# Allowed values and ranges

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

    # Filter good records (optional, if needed)
    good_records_df = df.filter(~bad_records_condition)

    # Show bad records
    print(f"Bad Records:{bad_records_df.count()}")

    # Show good records (optional)
    print(f"Good Records:{good_records_df.count()}")
    
    return good_records_df, bad_records_df