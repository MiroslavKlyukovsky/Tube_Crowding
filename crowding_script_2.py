import urllib.request
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import psycopg2
from config import db_params, hdr, email_password, smtp_server, smtp_port, smtp_email, recipient_email
from email_informant import EmailInformant

# script must find the newest table and assign it is the value if exists
CURRENT_CROWDING_DATA_TABLE = None


def create_crowding_table(cursor, naptan_ids, timestamp, email_informant):
    global CURRENT_CROWDING_DATA_TABLE
    try:
        print(1)
        create_new_table = False
        if CURRENT_CROWDING_DATA_TABLE is not None:
            print(2)
            cursor.execute(f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{CURRENT_CROWDING_DATA_TABLE}')")
            table_exists = cursor.fetchone()[0]
            if table_exists:
                print(3)
                query = f"SELECT COUNT(*) FROM {CURRENT_CROWDING_DATA_TABLE};"
                cursor.execute(query)
                row_count = cursor.fetchone()[0]

                diff_naptanIds = False
                cursor.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{CURRENT_CROWDING_DATA_TABLE}' AND column_name != 'c_timestamp';")
                existing_columns = [column[0] for column in cursor.fetchall()]
                if existing_columns != naptan_ids:
                    print(4)
                    diff_naptanIds = True

                if row_count >= 100 or diff_naptanIds:
                    print(5)
                    create_new_table = True
                else:
                    create_new_table = False
            else:
                create_new_table = True
        else:
            create_new_table = True

        if create_new_table:
            new_table_name = f"crowding_data_{timestamp.replace('-', '_').replace(':', '_').replace(' ', '_')}"

            column_definitions = ', '.join([f'"{n_id}" NUMERIC(5,4)' for n_id in naptan_ids])

            create_table_query = f"""
                                    CREATE TABLE {new_table_name} (
                                        c_timestamp TIMESTAMP PRIMARY KEY,
                                        {column_definitions}
                                    );
                                  """
            cursor.execute(create_table_query)

            return new_table_name
        else:
            return CURRENT_CROWDING_DATA_TABLE
    except Exception as error:
        raise Exception(f"Error creating table: {error}")


def save_to_postgresql(dumper_state, email_informant):
    global CURRENT_CROWDING_DATA_TABLE
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        for timestamp, data in dumper_state.items():
            naptan_ids = list(data.keys())

            table_name = create_crowding_table(cursor, naptan_ids, timestamp, email_informant)
            if table_name != CURRENT_CROWDING_DATA_TABLE:
                connection.commit()
                CURRENT_CROWDING_DATA_TABLE = table_name
                email_informant.send_email("New table", f"The new table names is: {CURRENT_CROWDING_DATA_TABLE}")

            filtered_naptanIds = [n_id for n_id, value in data.items() if str(value) != 'NaN']

            insert_query = f"""
                            INSERT INTO {CURRENT_CROWDING_DATA_TABLE} (c_timestamp, {', '.join([f'"{n_id}"' for n_id in filtered_naptanIds])})
                            VALUES (%s, {', '.join(['%s' for _ in filtered_naptanIds])})
                        """
            cursor.execute(insert_query, [timestamp] + [data[n_id] for n_id in filtered_naptanIds])

            connection.commit()
    except Exception as error:
        raise Exception(error)
    finally:
        cursor.close()
        connection.close()


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
            raise ValueError(f"One or more required fields are missing.(id: {id},name: {name}, modeName: {modeName})")
        if modeName != "tube":
            raise ValueError(f"The modeName is not 'tube'.(id: {id},name: {name}, modeName: {modeName})")

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
                f"One or more required fields are missing.(naptan_id: {naptan_id},common_name: {common_name}, status: {status}, wifi_status: {wifi_status})")
        if wifi_status not in ("yes", "no", "NaN"):
            raise ValueError(
                f"The wifi status is unknown.(naptan_id: {naptan_id},common_name: {common_name}, status: {status}, wifi_status: {wifi_status})")

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
    all_lines_df = pd.DataFrame()  # Initialize an empty DataFrame to store all stations

    # Iterate over each line ID and retrieve station information
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
    return unique_wifi_stations



dumper = {}

save_interval = timedelta(minutes=30)

last_save_time = datetime.now()

try:
    email_informant = EmailInformant(smtp_server, smtp_port, smtp_email, email_password, recipient_email)
    while True:
        current_datetime = datetime.now()
        date_str = current_datetime.strftime('%Y-%m-%d')
        time_str = current_datetime.strftime('%H:%M:%S')
        timestamp_str = f'{date_str} {time_str}'

        print(f"Data recording for: {timestamp_str}")

        dumper[timestamp_str] = {}

        tube_lines = get_lines()
        all_stations = get_all_lines_stations(tube_lines)
        stations = sorted(save_wifi_stations(all_stations))

        station_number = len(stations)
        counter = 0

        for naptan_id in stations:
            url = f"https://api.tfl.gov.uk/crowding/{naptan_id}/Live"

            req = urllib.request.Request(url, headers=hdr)
            req.get_method = lambda: 'GET'

            try:
                response = urllib.request.urlopen(req)
                crowding = "NaN"

                if response.getcode() == 200:
                    json_data = json.load(response)

                    if json_data['dataAvailable']:
                        crowding = json_data['percentageOfBaseline']
                    else:
                        crowding = "NaN"
                counter += 1
                print(f"{naptan_id} showed its data ({crowding}). [{counter}/{station_number}]")
                dumper[timestamp_str][naptan_id] = crowding

            except Exception as e:
                print(f"Error: {e}")

            time.sleep(0.05)
        time.sleep(90)

        current_time = datetime.now()
        if current_time - last_save_time >= save_interval:
            try:
                save_to_postgresql(dumper, email_informant)
            except Exception as error:
                email_informant.send_email("Something went wrong", str(error))

            dumper, last_save_time = {}, current_time

except KeyboardInterrupt:
    print("Execution interrupted. Exiting the loop.")



