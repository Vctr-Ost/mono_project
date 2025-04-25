# %%
import pandas as pd
import json
from dotenv import load_dotenv
import os

from api_interactions import get_query
from postgres_interaction import crt_engine, test_connection, is_postgres_table_exists, add_cols_intp_postgres_tbl, insert_pandas_df, create_schema_if_not_exists
from unix_interactions import generate_unix_list, get_unix


SCHEMA_FOR_DATA = os.getenv('SCHEMA_FOR_DATA')
TABLE_FOR_ROW_DATA = os.getenv('TABLE_FOR_ROW_DATA')


# %%
def get_mono_data(last_trn_unix_ts, account_id, headers):
    """
    Дістає всі транзакції mono по account_id та id карти де timestamp 
        транзакції > last_trn_unix_ts (останній ts транзакції про яку ми знаємо)

    Параметри:
        - last_trn_unix_ts (int) : UNIX Timestamp останньої mono транзакції
        - account_id (str) : Id аккаунту mono
        - headers (str) : Id картки mono

    Повертає:
        - pd.DataFrame : 
    """
    
    unix_list = generate_unix_list(last_trn_unix_ts)
    print(unix_list)

    resp_results = []

    print('start MONO DATA loading')

    for unix_range in unix_list:
        while True:
            url = f'https://api.monobank.ua/personal/statement/{account_id}/{unix_range[0]}/{unix_range[1]}'
            
            resp = get_query(url, headers)
            resp_json = json.loads(resp.text)

            if len(resp_json) < 500:
                resp_results.extend(resp_json)
                break
            else:
                unix_range[1] = str(resp_json[-1]['time'])

    return pd.DataFrame(resp_results)




# %%
def mono_data_loader(pg_conn_data, card_name, account_id, headers, last_trn_unix=0):
    """
    Якщо у функцію не передається last_trn_unix - це значення буде 1 днем поточного місяця
    Функція дістає транзакції по рахунку починаючи із last_trn_unix до поточного моменту
    Потім вона вставляє ці дані у таблицю Postgres DB
        Якщо таблиці немає - вона створиться
        Якщо зʼявились нові колонки - вони додадуться

    Параметри:
        - last_trn_unix (int) - unix_ts останньої транзакції

    Повертає:
        - int - unix_ts останньої транзакції (якщо знайшло нові транзакції)
                або минулий unix_ts
    """

    

    if last_trn_unix == 0:
        last_trn_unix = get_unix()

    resp_df = get_mono_data(last_trn_unix, account_id, headers)


    if len(resp_df) != 0:
        resp_df['accountId'] = account_id
        resp_df['cardName'] = card_name
        pg_engine = crt_engine(pg_conn_data)
        if test_connection(pg_engine):
            create_schema_if_not_exists(pg_engine, SCHEMA_FOR_DATA)

            if is_postgres_table_exists(pg_engine, TABLE_FOR_ROW_DATA, SCHEMA_FOR_DATA):
                print("ADDING COLS")
                add_cols_intp_postgres_tbl(resp_df, pg_engine, TABLE_FOR_ROW_DATA, SCHEMA_FOR_DATA)

            cnt_inserted_rows = insert_pandas_df(resp_df, pg_engine, TABLE_FOR_ROW_DATA, SCHEMA_FOR_DATA)
            print(f'Inserted {cnt_inserted_rows} rows.')

            max_trn_time = int(resp_df['time'].max() + 1)
            print(f'max_trn_time: {max_trn_time}')
            return max_trn_time
    else:
        print(f'returned last_trn_unix == {last_trn_unix}')
        return last_trn_unix


# %%
# mono_data_loader(0)


# %%
