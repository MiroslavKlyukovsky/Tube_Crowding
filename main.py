from datetime import datetime
import time
from config import email_password, smtp_server, smtp_port, smtp_email, recipient_email, db_params
from EmailInformant import EmailInformant
from tube_functions import get_lines, get_all_stations, get_crowding_data
from DatabaseHandler import DatabaseHandler
from DataDumper import DataDumper


def main():
    pause_between_stations = 0.008
    pause_between_state_draws = 15
    dumper = DataDumper(save_interval_min=15)

    max_rows_in_commit = 5
    max_rows_in_table = 25
    current_crowding_data_table = None
    database_handler = DatabaseHandler(max_rows_in_commit, current_crowding_data_table, max_rows_in_table, db_params)

    email_informant = EmailInformant(smtp_server, smtp_port, smtp_email, email_password, recipient_email)

    server_error_limit = 3

    dumper.set_save_time(datetime.now())

    while True:
        try:
            tube_lines = get_lines()
            naptan_ids = get_all_stations(tube_lines)
            dumper.create_new_row()

            for naptan_id in naptan_ids:
                crowding = get_crowding_data(naptan_id)
                print(naptan_id, crowding)
                dumper.add_station_to_row(station_name=naptan_id, crowding_data=crowding)
                time.sleep(pause_between_stations)

            time.sleep(pause_between_state_draws)

            print("  -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-  ")

            if dumper.is_time_to_save():
                print("  =================================== FLASH ========================================  ")
                database_handler.insert_dumper(dumper.get_dumper())
                dumper.set_save_time(datetime.now())
                dumper.clear_data()

        except Exception as error:
            dumper.drop_last_row()
            print(error)
            email_informant.send_email("At server: ", str(error))
            time.sleep(65)
            server_error_limit -= 1
            if server_error_limit == 0:
                break

    email_informant.send_email("At server: ", "Program stopped because error limit reached.")


if __name__ == "__main__":
    main()
