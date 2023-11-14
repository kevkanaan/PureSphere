from calendar import monthrange
import requests
import pandas as pd

LANDING_ZONE_PATH = "/opt/airflow/data/landing/water-quality/"

def get_analysepc(params=None):
    """
    Makes a GET request to the specified endpoint with optional query parameters.

    Args:
        params (dict, optional): A dictionary of query parameters to include in the request.

    Returns:
        Status code of the response.
    """
    endpoint="https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc.csv"
    response = requests.get(endpoint, params=params)
    response.raise_for_status()
    #print(response.headers)

    # Write the response content to a CSV file
    try:
        with open(LANDING_ZONE_PATH + 'analysispc.csv', 'w', newline='', encoding='utf-8') as csvfile:
            csvfile.write(response.content.decode('utf-8'))
    except FileNotFoundError as e:
        print("Error while writing the file", str(e))

    return response.status_code

def get_operationpc(params=None):
    """
    Makes a GET request to the specified endpoint with optional query parameters.

    Args:
        params (dict, optional): A dictionary of query parameters to include in the request.

    Returns:
        Status code of the response.
    """
    endpoint="https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/operation_pc.csv"
    response = requests.get(endpoint, params=params)
    response.raise_for_status()

    # Write the response content to a CSV file
    try:
        with open(LANDING_ZONE_PATH + 'operationpc.csv', 'w', newline='', encoding='utf-8') as csvfile:
            csvfile.write(response.content.decode('utf-8'))
    except FileNotFoundError:
        print("Error while writing the file")

    return response.status_code

def get_stationpc(params=None):
    """
    Makes a GET request to the specified endpoint with optional query parameters.

    Args:
        params (dict, optional): A dictionary of query parameters to include in the request.

    Returns:
        Status code of the response.
    """
    endpoint="https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/station_pc.csv"
    response = requests.get(endpoint, params=params)
    response.raise_for_status()

    # Write the response content to a CSV file
    try:
        with open(LANDING_ZONE_PATH + 'stationpc.csv', 'w', newline='', encoding='utf-8') as csvfile:
            csvfile.write(response.content.decode('utf-8'))
    except FileNotFoundError:
        print("Error while writing the file")

    return response.status_code

def get_analysepc_location(latitude, longitude, distance):
    """
    Makes a GET request to the specified endpoint with optional query parameters.

    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        distance (int): Distance in km around the location.

    Returns:
        Status code of the response.
    """
    params = {
        "distance": distance,
        "latitude": latitude,
        "longitude": longitude,
        "sort": "desc"
    }
    return get_analysepc(params)

def get_analysepc_filtered_year(year, chemical_components):
    """
    Makes a GET request to the specified endpoint with optional query parameters for each month of the year and combines the results into a single CSV file.

    Args:
        year (int): Year of the measurement.
        chemical_components (str): Chemical components of the measurement.

    Returns:
        Status code of the last response.
    """
    # Initialize an empty DataFrame to store the results
    df = pd.DataFrame()

    # Loop over each month of the year
    for month in range(1, 13):
        print(f"Processing month {month}...")
        # Get the last day of the month
        _, last_day = monthrange(year, month)

        # Format the month and last_day to always be two digits
        month = str(month).zfill(2)
        last_day = str(last_day).zfill(2)

        params = {
            "date_debut_prelevement": f"{year}-{month}-01",
            "date_fin_prelevement": f"{year}-{month}-{last_day}",
            "code_parametre": chemical_components,
            "sort": "desc",
            "size": 20000
        }

        # Make the GET request
        status_code = get_analysepc(params)
        print(f"Response code is {status_code}")
        # Read the results into a DataFrame
        df_month = pd.read_csv(LANDING_ZONE_PATH + 'analysispc.csv', encoding='ISO-8859-1', sep=';')

        # Append the results to the main DataFrame
        df = pd.concat([df, df_month], ignore_index=True)

        print(f"Finished processing month {month}")

    # Write the main DataFrame to a CSV file
    df.to_csv(LANDING_ZONE_PATH + f'analysispc_{year}.csv', index=False)

    return status_code

def get_operationpc_location(latitude, longitude, distance):
    """
    Makes a GET request to the specified endpoint with optional query parameters.

    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        distance (int): Distance in km around the location.

    Returns:
        Status code of the response.
    """
    params = {
        "distance": distance,
        "latitude": latitude,
        "longitude": longitude,
        "sort": "desc"
    }
    return get_operationpc(params)

def get_stationpc_location(latitude, longitude, distance):
    """
    Makes a GET request to the specified endpoint with optional query parameters.

    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        distance (int): Distance in km around the location.

    Returns:
        Status code of the response.
    """
    params = {
        "distance": distance,
        "latitude": latitude,
        "longitude": longitude,
        "size": 20000
    }
    return get_stationpc(params)

def get_analysepc_location_date(latitude, longitude, distance, date):
    """
    Makes a GET request to the specified endpoint with optional query parameters.

    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        distance (int): Distance in km around the location.
        date (str): Date of the measurement Example : 2022-09-01.

    Returns:
        Status code of the response.
    """
    params = {
        "distance": distance,
        "latitude": latitude,
        "longitude": longitude,
        "date": date,
        "sort": "desc"
    }
    return get_analysepc(params)

def main():
    print(get_analysepc_filtered_year(2020,"1319,1350,1383,1386"))

if __name__ == "__main__":
    main()
