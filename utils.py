from datetime import datetime

timestamp_format = 'YYYY-MM-DD HH24:MI:SS'


def generate_timestamp():
    """
        Generates a timestamp string in the format 'YYYY-MM-DD HH:MM:SS' based on the current system time.

        Returns:
        - timestamp_str (str): Timestamp string representing the current date and time.

        Raises:
        - Exception: If an error occurs while generating the timestamp.
    """
    try:
        current_datetime = datetime.now()
        date_str = current_datetime.strftime('%Y-%m-%d')
        time_str = current_datetime.strftime('%H:%M:%S')
        timestamp_str = f'{date_str} {time_str}'
        return timestamp_str
    except Exception as err:
        raise Exception(f'[generate_timestamp] {err}')
