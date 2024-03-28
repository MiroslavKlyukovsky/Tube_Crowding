import urllib.request
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import psycopg2
from config import db_params, hdr


# Function to dynamically create the table based on the number of naptan_id columns
def create_crowding_table(cursor, naptan_ids):
    try:
        print("table")
        column_definitions = ', '.join([f'"{naptan_id}" VARCHAR' for naptan_id in naptan_ids])
        print(column_definitions)

        # Check if the table exists
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'crowding_data')")
        table_exists = cursor.fetchone()[0]
        if table_exists:
            print("Table already exists.")
            return False  # Return False if the table already exists

        # If the table doesn't exist, create it
        create_table_query = f"""
            CREATE TABLE crowding_data (
                timestamp VARCHAR PRIMARY KEY,
                {column_definitions}
            );
        """
        cursor.execute(create_table_query)
        return True  # Return True if the table was created successfully

    except Exception as e:
        print(f"Error creating table: {e}")
        return False  # Return False if there was an error creating the table



def save_to_postgresql(dumper_state):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        for timestamp, data in dumper_state.items():
            naptan_ids = sorted(list(data.keys()))

            create_tab = create_crowding_table(cursor, naptan_ids)
            if create_tab:
                connection.commit()
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'crowding_data' AND column_name != 'timestamp';")
            existing_columns = [column[0] for column in cursor.fetchall()]
            if existing_columns != naptan_ids:
                raise ValueError("The order of naptan_ids does not match the existing table columns.")

            insert_query = f"""
                            INSERT INTO crowding_data (timestamp, {', '.join([f'"{naptan_id}"' for naptan_id in naptan_ids])})
                            VALUES (%s, {', '.join([f"%s" if isinstance(data[naptan_id], str) else f"'%s'" for naptan_id in naptan_ids])})
                        """
            cursor.execute(insert_query, [timestamp] + [data[naptan_id] for naptan_id in naptan_ids])
            print("inserto")
            connection.commit()  # Commit after inserting the data
        print("Data saved to PostgreSQL.")

    except Exception as error:
        print(f"Error saving data to PostgreSQL: {error}")
    finally:
        cursor.close()
        connection.close()
'''
def save_to_postgresql(dumper_state):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        for timestamp, data in dumper_state.items():
            for naptan_id, crowding in data.items():
                query = """
                        INSERT INTO crowding_data (timestamp, naptan_id, crowding)
                        VALUES (%s, %s, %s)
                        """
                cursor.execute(query, (timestamp, naptan_id, crowding))

        connection.commit()
        print("Data saved to PostgreSQL.")

    except Exception as error:
        print(f"Error saving data to PostgreSQL: {error}")

    finally:
        cursor.close()
        connection.close()
'''

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
        print(line.keys())
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

save_interval = timedelta(minutes=6)  # Change this to your desired save interval

last_save_time = datetime.now()

try:
    while True:
        current_datetime = datetime.now()
        date_str = current_datetime.strftime('%Y_%m_%d')
        time_str = current_datetime.strftime('%H_%M')  # Adjust this format as needed
        time_str = f'{date_str}_{time_str}'
        print(f"Data recording for: {time_str}")
        dumper[time_str] = {}

        stations = sorted(save_wifi_stations(get_all_lines_stations(get_lines())))

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
                dumper[time_str][naptan_id] = crowding

            except Exception as e:
                print(f"Error: {e}")

            time.sleep(0.15)

        # Save and reset the dumper based on the save interval
        current_time = datetime.now()
        if current_time - last_save_time >= save_interval:
            save_to_postgresql(dumper)
            dumper, last_save_time = {}, current_time

except KeyboardInterrupt:
    print("Execution interrupted. Exiting the loop.")



