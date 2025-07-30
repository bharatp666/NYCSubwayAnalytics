# Perform data validation using Great Expectations
import great_expectations as gx
import pandas as pd
from delta.tables import DeltaTable
from logger_spark import *

# Perform data validation using Great Expectations

def get_validations(config, data_source_name, data_asset_name, suite_name, batch_definition_name, definition_name, df):
    expected_columns = config['expected_columns']
    regex_validation_timestamp = config['regex_patterns']['transit_timestamp']
    station_data_url = config['external_sources']['station_master_data_url']
    allowed_values = config['allowed_values']
    expected_ranges = config['range_constraints']

    expected_stations = pd.read_csv(station_data_url)

    context = gx.get_context()
    data_source = context.data_sources.add_spark(name=data_source_name)
    gcp_logger.log_text(f'gx: Data source created successfully {data_source_name}', severity=200)

    data_asset = data_source.add_dataframe_asset(name=data_asset_name)
    gcp_logger.log_text(f'gx: Data asset created successfully {data_asset_name}', severity=200)

    batch_parameters = {"dataframe": df}
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
    gcp_logger.log_text(f'gx: Batch created successfully {batch_definition_name}', severity=200)

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    suite = gx.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)


    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="transit_timestamp",
        regex=regex_validation_timestamp
    ))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='transit_mode',
        value_set=allowed_values['transit_mode']
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='station_complex_id',
        value_set=list(expected_stations['station_complex_id'])
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='station_complex',
        value_set=list(expected_stations['station_complex'])
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='borough',
        value_set=allowed_values['borough']
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='payment_method',
        value_set=allowed_values['payment_method']
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='fare_class_category',
        value_set=allowed_values['fare_class_category']
    ))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='ridership',
        min_value=expected_ranges['ridership']['min'],
        max_value=expected_ranges['ridership']['max']
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='transfers',
        min_value=expected_ranges['transfers']['min'],
        max_value=expected_ranges['transfers']['max']
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='latitude',
        min_value=expected_ranges['latitude']['min'],
        max_value=expected_ranges['latitude']['max']
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column='longitude',
        min_value=expected_ranges['longitude']['min'],
        max_value=expected_ranges['longitude']['max']
    ))

    validation_definition = gx.ValidationDefinition(
        data=batch_definition, suite=suite, name=definition_name
    )
    validation_definition = context.validation_definitions.add(validation_definition)

    validation_results = batch.validate(suite)

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

    return pd.DataFrame(results)


def data_isolation(config, df, expected_stations):
    pattern = config['regex_patterns']['transit_timestamp']
    allowed = config['allowed_values']
    ranges = config['range_constraints']

    bad_condition = (
        ~df['transit_timestamp'].rlike(pattern) |
        ~df['transit_mode'].isin(allowed['transit_mode']) |
        ~df['station_complex_id'].isin(list(expected_stations['station_complex_id'])) |
        ~df['station_complex'].isin(list(expected_stations['station_complex'])) |
        ~df['borough'].isin(allowed['borough']) |
        ~df['payment_method'].isin(allowed['payment_method']) |
        ~df['fare_class_category'].isin(allowed['fare_class_category']) |
        (df['ridership'] < ranges['ridership']['min']) | (df['ridership'] > ranges['ridership']['max']) |
        (df['transfers'] < ranges['transfers']['min']) | (df['transfers'] > ranges['transfers']['max']) |
        (df['latitude'] < ranges['latitude']['min']) | (df['latitude'] > ranges['latitude']['max']) |
        (df['longitude'] < ranges['longitude']['min']) | (df['longitude'] > ranges['longitude']['max'])
    )

    bad_df = df.filter(bad_condition)
    good_df = df.filter(~bad_condition)

    gcp_logger.log_text(f"Bad records: {bad_df.count()}, Good records: {good_df.count()}", severity=200)

    return good_df, bad_df


def check_delta_existance(spark, delta_table_path):
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        gcp_logger.log_text(f"Delta table exists at path: {delta_table_path}", severity=200)
        return True
    else:
        gcp_logger.log_text(f"Delta table does not exist at path: {delta_table_path}", severity=500)
        return False

def get_gcs_uri_from_date(date_str, bucket_name, folder_name):
    dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")
    safe_timestamp = dt.strftime("%Y-%m-%dT%H-%M-%S-%f")[:-3]  # Keep only 3 ms digit
    s
    filename = f"ridership_{safe_timestamp}.parquet"
    gcs_uri = f"gs://{bucket_name}/{folder_name}/{filename}"
    return gcs_uri
