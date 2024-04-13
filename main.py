from datetime import datetime, timedelta
import time
from config import email_password, smtp_server, smtp_port, smtp_email, recipient_email, db_params
from email_informant import EmailInformant
from tube_functions import get_lines, get_all_stations, get_crowding_data
from DatabaseHandler import DatabaseHandler
from utils import generate_timestamp


def main():
    pause_between_stations = 0.1
    pause_between_state_draws = 5
    save_interval = timedelta(minutes=15)
    error_limit = 5

    dumper = []
    dumper_counter = 0
    last_save_time = datetime.now()

    email_informant = EmailInformant(smtp_server, smtp_port, smtp_email, email_password, recipient_email)
    database_handler = DatabaseHandler(current_crowding_data_table=None, max_rows=5, db_params=db_params)

    while True:
        try:
            timestamp_str = generate_timestamp()
            dumper.append([timestamp_str, {}])
            dumper_counter += 1

            tube_lines = get_lines()
            naptan_ids = get_all_stations(tube_lines)

            for naptan_id in naptan_ids:
                crowding = get_crowding_data(naptan_id)
                print(naptan_id, crowding)
                dumper[dumper_counter-1][1][naptan_id] = crowding
                time.sleep(pause_between_stations)

            time.sleep(pause_between_state_draws)

            print(" -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- ")

            current_time = datetime.now()
            if current_time - last_save_time >= save_interval:
                database_handler.insert_rows_in_table(dumper=dumper)
                dumper, last_save_time = [], current_time
                dumper_counter = 0

        except Exception as error:
            dumper.pop()
            print(error)
            email_informant.send_email("At server: ", str(error))
            time.sleep(65)
            error_limit -= 1
            if error_limit == 0:
                break

    email_informant.send_email("At server: ", "Program stopped because error limit reached.")


if __name__ == "__main__":
    main()
