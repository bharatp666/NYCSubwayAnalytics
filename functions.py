import requests
import pandas as pd
import logging

# Function to fetch the maximum and minimum transit_timestamp
def fetch_min_max_dates():
    '''Query the date of the oldest and latest records from the dataset'''
    # Define the API endpoint with SoQL query to get min and max dates
    url = (
        "https://data.ny.gov/resource/wujg-7c2s.json"
        "?$select=min(transit_timestamp) as min_date, max(transit_timestamp) as max_date"
    )

    # Make the GET request
    response = requests.get(url)

# # Configure logging
#     logging.basicConfig(
#     level = logging.INFO,
#     filename ='/Users/bharatpremnath/nycsubwayridership/app.logs',
#     filemode = 'w',
#     format = '%(asctime)s - %(levelname)s - %(message)s'
#)

    # Example block of code with logging
    if response.status_code == 200:
        logging.info("API request successful. Status code: 200")
        data = response.json()
        if data:
            min_date = data[0].get('min_date')
            max_date = data[0].get('max_date')
            if min_date and max_date:
                logging.info(f"Fetched min_date: {min_date}, max_date: {max_date}")
                return min_date, max_date
            else:
                logging.error("The API response is missing 'min_date' or 'max_date'.")
                raise KeyError("The API response is missing 'min_date' or 'max_date'.")
        else:
            logging.error("No data returned from the API.")
            raise ValueError("No data returned from the API.")
    else:
        logging.error(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")


# Example usage
# try:
#     min_date, max_date = fetch_min_max_dates()
#     print(f"Minimum Date: {min_date}")
#     print(f"Maximum Date: {max_date}")
# except Exception as e:
#     print(e)


''''''''''''''''''''''''''''''''




def generate_year_month_list(min_date_str, max_date_str):
    """
    Generate a list of (year, month) tuples from min_date to max_date.

    Args:
        min_date_str (str): The starting date in string format (e.g., '2023-01-01').
        max_date_str (str): The ending date in string format (e.g., '2023-12-31').

    Returns:
        list of tuples: A list of (year, month) tuples for each month in the range.
    """

    # Convert string dates to datetime objects
    min_date = pd.to_datetime(min_date_str)
    max_date = pd.to_datetime(max_date_str)

    # Generate a date range with monthly frequency
    date_range = pd.date_range(
        start=min_date.replace(day=1),  # Ensure the start is the 1st of the month
        end=max_date.replace(day=1),    # Ensure the end is the 1st of the month
        freq='MS'                       # Frequency: Month Start
    )

    # Create a list of (year, month) tuples
    year_month_list = [(date.year, date.month) for date in date_range]
    return year_month_list

# Generate the year-month list
# year_month_list = generate_year_month_list(min_date, max_date)
# print("Year-Month List:", year_month_list)



# Function to fetch data between a start date and end date
def fetch_data_by_date(start_date, end_date):
    """
    Fetch data from the API filtered by date range.

    Args:
        start_date (str): The start date in string format (e.g., '2023-01-01').
        end_date (str): The end date in string format (e.g., '2023-01-31').

    Returns:
        list: The JSON response from the API.

    Raises:
        Exception: If the API request fails.
    """
    # Log the API request details
    logging.info(f"Fetching data for date range: {start_date} to {end_date}")

    # Define the API endpoint with date filtering
    url = (
        f"https://data.ny.gov/resource/wujg-7c2s.json"
        f"?$where=transit_timestamp >= '{start_date}' AND transit_timestamp <= '{end_date}'"
        f"&$limit=5000000"
    )

    try:
        # Make the GET request
        logging.info(f"Making request to URL: {url}")
        response = requests.get(url)

        # Handle the response
        if response.status_code == 200:
            logging.info(f"Data successfully fetched for range {start_date} to {end_date}")
            return response.json()  # Return JSON response
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
            raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred during the request: {e}")
        raise Exception(f"An error occurred during the request: {e}")

# Function usage
# start_date = f"{year_month_list[0][0]}-{year_month_list[0][1]}-01"
# end_date = f"{year_month_list[1][0]}-{year_month_list[1][1]}-01"

# try:
#     data = fetch_data_by_date(start_date, end_date)
#     #print(data)
# except Exception as e:
#     print(e)