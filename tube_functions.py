import urllib.request
import pandas as pd
import json
import time
from config import hdr


def get_lines():
    """
    Retrieves information about the present London Underground lines from the TfL API.

    Returns:
        pandas.DataFrame: DataFrame containing information about Tube lines including id, name and mode.

    Raises:
        Exception: If the API request fails.
        ValueError: If one or more required fields are missing or if the modeName is not 'tube'.
    """
    url = "https://api.tfl.gov.uk/Line/Mode/tube/Status"

    req = urllib.request.Request(url, headers=hdr)
    req.get_method = lambda: 'GET'
    response = urllib.request.urlopen(req)

    if response.getcode() != 200:
        raise Exception(f"Failed to retrieve lines. Status code: {response.getcode()}")

    json_lines = json.load(response)
    lines = []

    for line in json_lines:
        id = line.get("id", "NaN")
        name = line.get("name", "NaN")
        modeName = line.get("modeName", "NaN")

        if "NaN" in (id, name, modeName):
            raise ValueError(f"One or more required fields of a line are missing.(id: {id},name: {name}, modeName: {modeName})")
        if modeName != "tube":
            raise ValueError(f"The modeName of a line is not 'tube'.(id: {id},name: {name}, modeName: {modeName})")

        lines.append([id, name, modeName])

    df = pd.DataFrame(lines, columns=["id", "name", "modeName"])

    return df


def get_line_stations(station_id):
    """
    Retrieve information about the stop points of a given station ID.

    Parameters:
    station_id (str): The ID of the station to retrieve stop points for.

    Returns:
    pandas.DataFrame: A DataFrame containing information about the stop points,
                      including Naptan ID, common name, status, and WiFi availability.

    Raises:
    Exception: If failed to retrieve stop points from the API.
    ValueError: If one or more required fields are missing or if the WiFi status is unknown.
    """
    url = f"https://api.tfl.gov.uk/Line/{station_id}/StopPoints"

    req = urllib.request.Request(url, headers=hdr)
    req.get_method = lambda: 'GET'
    response = urllib.request.urlopen(req)

    if response.getcode() != 200:
        raise Exception(f"Failed to retrieve stop points. Status code: {response.getcode()}")

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
                f"One or more required fields of a station are missing.(naptan_id: {naptan_id},common_name: {common_name}, status: {status}, wifi_status: {wifi_status})")
        if wifi_status not in ("yes", "no", "NaN"):
            raise ValueError(
                f"The wifi status of a station is unknown.(naptan_id: {naptan_id},common_name: {common_name}, status: {status}, wifi_status: {wifi_status})")

        stop_data.append([naptan_id, common_name, status, wifi_status])

    df = pd.DataFrame(stop_data, columns=['naptanId', 'commonName', 'status', 'WiFi'])

    return df


def get_all_lines_stations(lines_df):
    """
    Retrieve information about stop points for all London Underground lines.

    Parameters:
    lines_df (pandas.DataFrame): DataFrame containing information about Tube lines.

    Returns:
    pandas.DataFrame: DataFrame containing information about all stations across all lines.
    """
    all_lines_df = pd.DataFrame()

    for line_id in lines_df['id']:
        temp_df = get_line_stations(line_id)
        all_lines_df = pd.concat([all_lines_df, temp_df], ignore_index=True)

    return all_lines_df


def save_wifi_stations(df):
    """
    Save unique Naptan IDs of stations with WiFi available in a list.

    Parameters:
    df (pandas.DataFrame): DataFrame containing information about all lines and stations.

    Returns:
    list: List of unique Naptan IDs of stations with WiFi available.
    """
    wifi_stations = []

    for index, row in df.iterrows():
        if row['WiFi'] == 'yes':
            wifi_stations.append(row['naptanId'])

    unique_wifi_stations = list(set(wifi_stations))

    if len(unique_wifi_stations) == 0:
        raise Exception('No stations.')

    return unique_wifi_stations


def get_crowding_data(naptan_ids, pause_min):
    for naptan_id in naptan_ids:
        try:
            url = f"https://api.tfl.gov.uk/crowding/{naptan_id}/Live"

            req = urllib.request.Request(url, headers=hdr)
            req.get_method = lambda: 'GET'

            response = urllib.request.urlopen(req)
            crowding = None

            if response.getcode() == 200:
                json_data = json.load(response)
                if json_data['dataAvailable']:
                    crowding = round(json_data['percentageOfBaseline'], 4)

            yield naptan_id, crowding

        except Exception as error:
            raise Exception(f"Error fetching crowding data for {naptan_id}: {error}")

        time.sleep(pause_min)
