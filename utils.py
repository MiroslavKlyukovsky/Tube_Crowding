from datetime import datetime
import pytz

timestamp_format = 'YYYY-MM-DD HH24:MI:SS'


def generate_timestamp(timezone_str='Europe/London'):
    """
        Generates a timestamp string in the format 'YYYY-MM-DD HH:MM:SS' based on the specified timezone.
        If no timezone is provided, 'Europe/London' is used as the default.

        Parameters:
        - timezone_str (str): The timezone in which to generate the timestamp. Default is 'Europe/London'.

        Returns:
        - timestamp_str (str): Timestamp string representing the current date and time in the specified timezone.

        Raises:
        - Exception: If an error occurs while generating the timestamp or if the timezone is invalid.
    """
    try:
        timezone = pytz.timezone(timezone_str)
        current_datetime = datetime.now(timezone)
        date_str = current_datetime.strftime('%Y-%m-%d')
        time_str = current_datetime.strftime('%H:%M:%S')
        timestamp_str = f'{date_str} {time_str}'
        return timestamp_str
    except Exception as err:
        raise Exception(f'[generate_timestamp] {err}')
