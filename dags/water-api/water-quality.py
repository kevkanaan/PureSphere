import requests

def get_analysepc(params=None):
    """
    Makes a GET request to the specified endpoint with optional query parameters.

    Args:
        params (dict, optional): A dictionary of query parameters to include in the request.

    Returns:
        Status code of the response.
    """
    endpoint="https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/analyse_pc.csv"
    response = requests.get(endpoint, params=params, timeout=10)
    response.raise_for_status()

    # Write the response content to a CSV file
    try:
        with open('./data/water-quality/analysispc.csv', 'w', newline='') as csvfile:
            csvfile.write(response.content.decode('utf-8'))
    except:
        print("Error while writing the file")

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
        with open('./data/water-quality/operationpc.csv', 'w', newline='') as csvfile:
            csvfile.write(response.content.decode('utf-8'))
    except:
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
        with open('./data/water-quality/stationpc.csv', 'w', newline='') as csvfile:
            csvfile.write(response.content.decode('utf-8'))
    except:
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
        "longitude": longitude
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
    params = {
        "distance": "5",
        "latitude": "45.782",
        "longitude": "4.893",
        "sort": "desc"
    }

    print(f"Calling Analyse Physico-Chimique , response code is : {get_analysepc(params)}")

    print(f"Calling Operation Physico-Chimique, response code is : {get_operationpc(params)}")
    
    #For stationpc we don't need to sort because the sort is on The Date of the last measurement
    print(f"Calling Station Physico-Chimique, responde code is : {get_stationpc(params)}")

    print(get_analysepc_location(45.782, 4.893, 5))
    print(get_operationpc_location(45.782, 4.893, 5))
    print(get_stationpc_location(45.782, 4.893, 5))

    print(get_analysepc_location_date(45.782, 4.893, 5, "2022-09-01"))

if __name__ == "__main__":
    main()

