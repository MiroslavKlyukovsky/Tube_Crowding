from datetime import datetime, timedelta
from utils import generate_timestamp


class DataDumper:
    def __init__(self, save_interval_min):
        self.last_save_time = None
        self.save_interval = timedelta(minutes=save_interval_min)
        self.dumper = []
        self.dumper_counter = 0

    def set_save_time(self, last_save_time):
        self.last_save_time = last_save_time

    def create_new_row(self):
        timestamp_str = generate_timestamp()
        self.dumper.append([timestamp_str, {}])
        self.dumper_counter += 1

    def add_station_to_row(self, station_name, crowding_data):
        self.dumper[self.dumper_counter - 1][1][station_name] = crowding_data

    def clear_data(self):
        self.dumper.clear()
        self.dumper_counter = 0

    def is_time_to_save(self):
        current_time = datetime.now()
        if current_time - self.last_save_time >= self.save_interval:
            return True
        else:
            return False

    def drop_last_row(self):
        if self.dumper_counter != 0:
            self.dumper.pop()
            self.dumper_counter -= 1
