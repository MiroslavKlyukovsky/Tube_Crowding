from datetime import datetime, timedelta
import time
from config import email_password, smtp_server, smtp_port, smtp_email, recipient_email
from email_informant import EmailInformant
from tube_functions import get_lines, get_all_lines_stations, save_wifi_stations, get_crowding_data
from db_functions import save_to_postgresql

# can be strange when dumper has enough data to full several tables
#cursor in finally is asked but not created previously


def generate_timestamp():
    current_datetime = datetime.now()
    date_str = current_datetime.strftime('%Y-%m-%d')
    time_str = current_datetime.strftime('%H:%M:%S')
    timestamp_str = f'{date_str} {time_str}'
    return timestamp_str


def main():
    PAUSE_BETWEEN_STATIONS = 0.01
    PAUSE_BETWEEN_STATE_DRAWS = 60
    CURRENT_CROWDING_DATA_TABLE = None

    save_interval = timedelta(minutes=25)
    last_save_time = datetime.now()
    dumper = {}

    email_informant = EmailInformant(smtp_server, smtp_port, smtp_email, email_password, recipient_email)

    while True:
        try:
            timestamp_str = generate_timestamp()

            dumper[timestamp_str] = {}

            tube_lines = get_lines()
            all_stations = get_all_lines_stations(tube_lines)
            naptan_ids = save_wifi_stations(all_stations)

            for naptan_id, crowding in get_crowding_data(naptan_ids, PAUSE_BETWEEN_STATIONS):
                print(naptan_id, crowding)
                dumper[timestamp_str][naptan_id] = crowding





            time.sleep(PAUSE_BETWEEN_STATE_DRAWS)
            print("FFF")
            current_time = datetime.now()
            if current_time - last_save_time >= save_interval:
                print("LLLLLLLLLLLLLLLoooooooLLLLLLLLLLLLLLLL")
                CURRENT_CROWDING_DATA_TABLE = save_to_postgresql(dumper, email_informant, CURRENT_CROWDING_DATA_TABLE)

                email_informant.send_email("Data inserted", f"Data was inserted in {CURRENT_CROWDING_DATA_TABLE}")
                dumper, last_save_time = {}, current_time
        except Exception as error:
            time.sleep(10)
            print(error)
            email_informant.send_email("Error occured: ", str(error))


if __name__ == "__main__":
    main()
