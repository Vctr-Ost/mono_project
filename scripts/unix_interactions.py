import time
from datetime import datetime


def generate_unix_list(unix_time, cnt_days=30):
    """
    Генерує список між unix_time та CURRENT_TS() із періодичністю до 30 днів

    Параметри:
        - unix_time (int) : час останньої транзакції у форматі UNIX
        - cnt_days (int) : максимальна дельта у днях
    
    Повертає:
        - list (int) : ліст із UNIX ts формату        [ [ts_start, ts_end], [ts_start, ts_end] ]
    """

    seconds_to_add = cnt_days * (24 * 60 * 60)
    current_unix_time = int(time.time())

    print(f'unix_time - {type(unix_time)} - {unix_time}')
    print(f'current_unix_time - {type(current_unix_time)} - {current_unix_time}')

    unix_list = []

    while unix_time < current_unix_time:
        append_list = [unix_time]
        unix_time += seconds_to_add

        append_list.append(unix_time)
        unix_list.append(append_list)

    return unix_list


def get_unix():
    """
    Повертає UNIX Timestamp 1 дня поточного місяця
    """
    today = datetime.today()
    first_day_of_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    first_day_unix = int(time.mktime(first_day_of_month.timetuple()))
    return first_day_unix
