import urllib.request
import pandas as pd
import json
from config import hdr


def get_response(url):
    """
    Sends a GET request to the specified URL using urllib.request and returns the response object.

    Parameters:
    url (str): The URL to send the GET request to.

    Returns:
    HTTPResponse: The response object containing the server's response to the request.

    Raises:
     Exception: If an error occurs while sending the request or processing the response, with tag [get_response].
    """
    try:
        req = urllib.request.Request(url, headers=hdr)
        req.get_method = lambda: 'GET'
        response = urllib.request.urlopen(req)
        return response
    except Exception as err:
        raise Exception(f"[get_response] {err}")


def get_lines():
    """
    Retrieves information about the present London Underground lines from the TfL API.

    Returns:
    pandas.DataFrame: DataFrame containing information about Tube lines including id, name and mode.

    Raises:
     Exception: If an error occurs during the API request or processing the response,
     or if the DataFrame for output is empty, with tag [get_lines].
    """
    try:
        url = "https://api.tfl.gov.uk/Line/Mode/tube/Status"
        response = get_response(url)

        if response.getcode() != 200:
            raise Exception(f"Failed to retrieve lines. Status code: {response.getcode()}")

        json_lines = json.load(response)
        lines = []

        for line in json_lines:
            line_id = line.get("id", "NaN")
            name = line.get("name", "NaN")
            mode_name = line.get("modeName", "NaN")

            if "NaN" in (line_id, name, mode_name):
                raise ValueError(f"One or more required fields of a line are missing. " +
                                 f"(id: {line_id},name: {name}, modeName: {mode_name})")
            if mode_name != "tube":
                raise ValueError(f"The modeName of a line is not 'tube'. " +
                                 f"(id: {line_id},name: {name}, modeName: {mode_name})")

            lines.append([line_id, name, mode_name])

        df = pd.DataFrame(lines, columns=["id", "name", "modeName"])

        if df.empty:
            raise ValueError(f"Dataframe for output is empty.")

        return df
    except Exception as err:
        raise Exception(f"[get_lines] {err}")


def get_line_stations(line_id):
    """
    Retrieve information about the stations of a given line ID.

    Parameters:
    line_id (str): The ID of the line to retrieve stations for.

    Returns:
    pandas.DataFrame: A DataFrame containing information about the stations,
                      including Naptan ID, common name, status, and Wi-Fi availability.

    Raises:
     Exception: If an error occurs during the API request or processing the response,
     or if the DataFrame for output is empty, with tag [get_line_stations].
    """
    try:
        url = f"https://api.tfl.gov.uk/Line/{line_id}/StopPoints"
        response = get_response(url)

        if response.getcode() != 200:
            raise Exception(f"Failed to retrieve stations. Status code: {response.getcode()}")

        stop_points = json.load(response)
        stop_data = []

        for stop_point in stop_points:
            naptan_id = stop_point.get('naptanId', 'NaN')
            common_name = stop_point.get('commonName', 'NaN')
            status = stop_point.get('status', 'NaN')

            wifi_status = "NaN"
            if 'additionalProperties' in stop_point:
                for elem in stop_point['additionalProperties']:
                    if elem.get('key') == 'WiFi':
                        wifi_status = elem.get('value')
                        break

            if "NaN" in (naptan_id, common_name, status):
                raise ValueError(
                    f"One or more required fields of a station are missing." +
                    f"(naptan_id: {naptan_id},common_name: {common_name}," +
                    f" status: {status}, wifi_status: {wifi_status})")
            if wifi_status not in ("yes", "no", "NaN"):
                raise ValueError(
                    f"The wifi status of a station is unknown." +
                    f"(naptan_id: {naptan_id},common_name: {common_name}," +
                    f" status: {status}, wifi_status: {wifi_status})")

            stop_data.append([naptan_id, common_name, status, wifi_status])

        df = pd.DataFrame(stop_data, columns=['naptanId', 'commonName', 'status', 'WiFi'])

        if df.empty:
            raise ValueError(f"Dataframe for output is empty.")

        return df
    except Exception as err:
        raise Exception(f"[get_line_stations] {err}")


def get_all_stations(lines_df):
    """
    Retrieve information about stop points for all London Underground lines with Wi-Fi set to "yes".

    Parameters:
    lines_df (pandas.DataFrame): DataFrame containing information about Tube lines.

    Returns:
    tuple: A tuple containing unique 'naptanId' values of stations with Wi-Fi across all lines.

    Raises:
     Exception: If an error occurs while retrieving information about stop points for all London Underground lines.
    """
    try:
        all_lines_df = pd.DataFrame(columns=['naptanId', 'commonName', 'status', 'WiFi'])

        for line_id in lines_df['id']:
            line_stations = get_line_stations(line_id)

            for index, station_row in line_stations.iterrows():
                if station_row['WiFi'] == 'yes' and station_row['naptanId'] not in all_lines_df['naptanId'].tolist():
                    all_lines_df = pd.concat([all_lines_df, station_row.to_frame().T], ignore_index=True)

        naptan_ids_tuple = tuple(all_lines_df['naptanId'])

        if len(naptan_ids_tuple) == 0:
            raise Exception('No stations.')

        return naptan_ids_tuple
    except Exception as err:
        raise Exception(f"[get_all_stations] {err}")


def get_crowding_data(naptan_id):
    """
    Retrieve crowding data for a specified Naptan ID from the TfL API.

    Parameters:
     naptan_id (str): The Naptan ID of the station for which crowding data is to be retrieved.

    Returns:
     float or None: The crowding percentage for the station if available, otherwise None.

    Raises:
     Exception: If an error occurs during the API request or processing the response, with tag [get_crowding_data].
    """
    try:
        url = f"https://api.tfl.gov.uk/crowding/{naptan_id}/Live"
        response = get_response(url)

        crowding = None

        if response.getcode() != 200:
            raise Exception(f"Failed to retrieve crowding of a station {naptan_id}. Status code: {response.getcode()}")

        json_data = json.load(response)
        if json_data['dataAvailable']:
            crowding = float(round(json_data['percentageOfBaseline'], 4))

        return crowding
    except Exception as err:
        raise Exception(f"[get_crowding_data] {err}")
