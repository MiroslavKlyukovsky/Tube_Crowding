from datetime import datetime, timedelta
from utils import generate_timestamp


class DataDumper:
    """
        A class to manage data dumping operations.

        Attributes:
        - last_save_time (datetime): Timestamp of the last data save operation.
        - save_interval (timedelta): Time interval between data saves.
        - _dumper (list): List to store data dumps. Note: Internal attribute, avoid direct access.
    """
    def __init__(self, save_interval_min):
        """
            Initializes the DataDumper instance.

            Args:
            - save_interval_min (int): Interval in minutes between data saves.
        """
        self.last_save_time = None
        self.save_interval = timedelta(minutes=save_interval_min)
        self._dumper = []

    def set_save_time(self, last_save_time):
        """
            Sets the timestamp of the last data save operation.

            Args:
            - last_save_time (datetime): Timestamp of the last data save.
        """
        self.last_save_time = last_save_time

    def create_new_row(self):
        """Creates a new row in the dumper list with a timestamp."""
        timestamp_str = generate_timestamp()
        self._dumper.append([timestamp_str, {}])

    def add_station_to_row(self, station_name, crowding_data):
        """
            Adds station crowding data to the latest row in the dumper list.

            Args:
            - station_name (str): Name of the station.
            - crowding_data (float or None): Crowding data for the station.

            Raises:
            - IndexError: If there are no rows in the dumper list to add data to.
        """
        try:
            if crowding_data is not None and not isinstance(crowding_data, float) and crowding_data != 0:
                raise ValueError("Wrong crowding data.")
            self._dumper[-1][1][station_name] = crowding_data
        except Exception as err:
            raise Exception(f"[add_station_to_row] {err}")

    def clear_data(self):
        """Clears all data in the dumper list."""
        self._dumper.clear()

    def is_time_to_save(self):
        """
            Checks if it's time to save data based on the save interval.

            Returns:
            - bool: True if it's time to save data, False otherwise.

            Raises:
            - Exception: If an error occurs while checking the time to save.
        """
        try:
            current_time = datetime.now()
            if current_time - self.last_save_time >= self.save_interval:
                return True
            else:
                return False
        except Exception as err:
            raise Exception(f"[is_time_to_save] {err}")

    def drop_last_row(self):
        """Drops the last row from the dumper list."""
        if len(self._dumper) != 0:
            self._dumper.pop()

    def get_dumper(self):
        """
        Returns the dumper list.

        Returns:
        - list: The dumper list.
        """
        return self._dumper
