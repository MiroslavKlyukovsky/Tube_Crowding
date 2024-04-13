from datetime import datetime


def generate_timestamp():
    current_datetime = datetime.now()
    date_str = current_datetime.strftime('%Y-%m-%d')
    time_str = current_datetime.strftime('%H:%M:%S')
    timestamp_str = f'{date_str} {time_str}'
    return timestamp_str
