from datetime import datetime, timedelta
import time
import psycopg2
from config import email_password, smtp_server, smtp_port, smtp_email, recipient_email, db_params
from email_informant import EmailInformant
from tube_functions import get_lines, get_all_stations, get_crowding_data
from db_functions import set_up_table, insert_row_in_table


def generate_timestamp():
    current_datetime = datetime.now()
    date_str = current_datetime.strftime('%Y-%m-%d')
    time_str = current_datetime.strftime('%H:%M:%S')
    timestamp_str = f'{date_str} {time_str}'
    return timestamp_str


def main():
    pause_between_stations = 0.005
    pause_between_state_draws = 0
    current_crowding_data_table = None
    max_rows = 5
    save_interval = timedelta(minutes=5)

    last_save_time = datetime.now()
    dumper = {}

    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    email_informant = EmailInformant(smtp_server, smtp_port, smtp_email, email_password, recipient_email)

    while True:
        try:
            connection = psycopg2.connect(**db_params)
            cursor = connection.cursor()

            timestamp_str = generate_timestamp()
            dumper[timestamp_str] = {}

            tube_lines = get_lines()
            naptan_ids = get_all_stations(tube_lines)

            for naptan_id in naptan_ids:
                crowding = get_crowding_data(naptan_id)
                print(naptan_id, crowding)
                dumper[timestamp_str][naptan_id] = crowding
                time.sleep(pause_between_stations)

            time.sleep(pause_between_state_draws)
            print(" -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- ")

            current_time = datetime.now()
            if current_time - last_save_time >= save_interval:
                messages = []
                for timestamp, data in dumper.items():
                    naptan_ids = sorted(list(data.keys()))

                    temp_table = set_up_table(cursor, naptan_ids, timestamp, current_crowding_data_table, max_rows)

                    if temp_table != current_crowding_data_table:
                        connection.commit()
                        current_crowding_data_table = temp_table
                        messages.append(["New table", f"The new table names is: {current_crowding_data_table}"])
                    if insert_row_in_table(cursor, timestamp, data, current_crowding_data_table):
                        connection.commit()

                action_string = '\n'.join([' - '.join(sublist) for sublist in messages])
                email_informant.send_email("Actions at server", action_string)

                dumper, last_save_time = {}, current_time

        except Exception as error:
            print(error)
            email_informant.send_email("At server: ", str(error))
            time.sleep(65)
        finally:
            cursor.close()
            connection.close()


if __name__ == "__main__":
    main()
