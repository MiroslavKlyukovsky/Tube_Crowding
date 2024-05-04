from datetime import datetime, timedelta
import time
from config import email_password, smtp_server, smtp_port, smtp_email, recipient_email, db_params
from EmailInformant import EmailInformant
from tube_functions import get_lines, get_all_stations, get_crowding_data
from DatabaseHandler import DatabaseHandler
from DataDumper import DataDumper
import argparse


def main(pause_between_stations_sec=0.005, pause_between_state_draws_sec=45, save_interval_min=15, max_rows_in_commit=10,
         max_rows_in_table=4000, current_crowding_data_table=None, server_error_limit=5, error_del_time_min=180,
         sleep_af_err_sec=65):
    
    dumper = DataDumper(save_interval_min)

    database_handler = DatabaseHandler(max_rows_in_commit, current_crowding_data_table, max_rows_in_table, db_params)

    email_informant = EmailInformant(smtp_server, smtp_port, smtp_email, email_password, recipient_email)

    server_error_counter = 0
    last_time_error_oc = None
    error_del_time_dif = timedelta(minutes=error_del_time_min)

    dumper.set_save_time(datetime.now())

    while True:
        try:
            tube_lines = get_lines()
            naptan_ids = get_all_stations(tube_lines)
            dumper.create_new_row()

            for naptan_id in naptan_ids:
                crowding = get_crowding_data(naptan_id)
                dumper.add_station_to_row(station_name=naptan_id, crowding_data=crowding)
                time.sleep(pause_between_stations_sec)

            time.sleep(pause_between_state_draws_sec)

            if dumper.is_time_to_save():
                database_handler.insert_dumper(dumper.get_dumper())
                dumper.set_save_time(datetime.now())
                dumper.clear_data()

            if last_time_error_oc and (datetime.now() - last_time_error_oc) >= error_del_time_dif:
                email_informant.send_email("At server: ",
                                           f"Server error counter was set to 0 from {server_error_counter}.")
                server_error_counter = 0

        except Exception as error:
            dumper.drop_last_row()
            email_informant.send_email("At server: ", str(error))
            time.sleep(sleep_af_err_sec)
            server_error_counter += 1
            if server_error_limit == server_error_counter:
                break
            last_time_error_oc = datetime.now()

    email_informant.send_email("At server: ", "Program stopped because error limit reached.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some parameters for main.")

    parser.add_argument("-ps", "--pause_stations", type=float, default=0.005,
                        help="Pause time between getting crowding info of each station in seconds")
    parser.add_argument("-pd", "--pause_draws", type=float, default=45,
                        help="Pause time between drawing stations' states in seconds")
    parser.add_argument("-si", "--save_interval", type=int, default=15,
                        help="Save interval after which a dumper is saved to db in minutes")
    parser.add_argument("-mc", "--max_commit", type=int, default=10,
                        help="Maximum rows in commit")
    parser.add_argument("-mt", "--max_table", type=int, default=4000,
                        help="Maximum rows in table")
    parser.add_argument("-ct", "--crowding_table", type=str, default=None,
                        help="Current crowding data table")
    parser.add_argument("-sl", "--server_limit", type=int, default=5,
                        help="Server error limit after exceeding which server ceases to work")
    parser.add_argument("-ed", "--error_del_time", type=int, default=180,
                        help="Time after which error counter is set to 0 in minutes")
    parser.add_argument("-se", "--sleep_error", type=float, default=65,
                        help="Sleep time after each error in seconds")

    args = parser.parse_args()

    main(args.pause_stations, args.pause_draws, args.save_interval, args.max_commit,
         args.max_table, args.crowding_table, args.server_limit, args.error_del_time,
         args.sleep_error)
