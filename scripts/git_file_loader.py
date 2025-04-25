import pandas as pd
import json
import requests
from dotenv import load_dotenv
import os
from io import StringIO

from api_interactions import get_query
from postgres_interaction import crt_engine, test_connection, is_postgres_table_exists, add_cols_intp_postgres_tbl, insert_pandas_df, create_schema_if_not_exists
load_dotenv()

SCHEMA_FOR_DATA = os.getenv('SCHEMA_FOR_DATA')
TABLE_FOR_MCC_EN = os.getenv('TABLE_FOR_MCC_EN')
TABLE_FOR_MCC_UA = os.getenv('TABLE_FOR_MCC_UA')
TABLE_FOR_MCC_HANDLING_DATA = os.getenv('TABLE_FOR_MCC_HANDLING_DATA')


def mcc_data_normalizer(data_list):
    """
    Функція робить із 'багаторівневого' mcc list однорівневий

    Параметри:
        - data_list (list) - багаторівневий ліст mcc кодів із (https://github.com/Oleksios/Merchant-Category-Codes/tree/main)

    Повертає:
        - list - однорівневий ліст mcc кодів
    """
    data_normalized = []

    for item in data_list:
        data_normalized.append({
            'mcc': item['mcc'],
            'group_type': item['group']['type'],
            'group_description': item['group']['description'],
            'short_description': item['shortDescription'],
            'full_description': item['fullDescription']
        })

    return data_normalized


def mcc_file_loader(pg_conn_data):
    print(pg_conn_data)

    PATH_EN = "https://raw.githubusercontent.com/Oleksios/Merchant-Category-Codes/refs/heads/main/With%20groups/mcc-en.json"
    PATH_UA = "https://raw.githubusercontent.com/Oleksios/Merchant-Category-Codes/refs/heads/main/With%20groups/mcc-uk.json"
    PATH_HANDLING_DATA = "https://raw.githubusercontent.com/Vctr-Ost/mcc_mono_project/refs/heads/main/mcc_handling_list.csv"

    resp_en = get_query(PATH_EN)
    resp_ua = get_query(PATH_UA)
    resp_handling_data = get_query(PATH_HANDLING_DATA)

    resp_data_en = resp_en.json()
    resp_data_ua = resp_ua.json()
    resp_data_handling_data = StringIO(resp_handling_data.text)
    
    data_en = pd.DataFrame(mcc_data_normalizer(resp_data_en))
    data_ua = pd.DataFrame(mcc_data_normalizer(resp_data_ua))
    data_handling = pd.read_csv(resp_data_handling_data, sep=";")

    pg_engine = crt_engine(pg_conn_data)
    if test_connection(pg_engine):
        print(f'---------------------------------------- Creating schema {SCHEMA_FOR_DATA}')
        create_schema_if_not_exists(pg_engine, SCHEMA_FOR_DATA)

        # print(data_en.head())
        # print(data_ua.head())

        cnt_inserted_rows_en = insert_pandas_df(data_en, pg_engine, TABLE_FOR_MCC_EN, SCHEMA_FOR_DATA, 'replace')
        cnt_inserted_rows_ua = insert_pandas_df(data_ua, pg_engine, TABLE_FOR_MCC_UA, SCHEMA_FOR_DATA, 'replace')
        cnt_inserted_rows_ua = insert_pandas_df(data_handling, pg_engine, TABLE_FOR_MCC_HANDLING_DATA, SCHEMA_FOR_DATA, 'replace')


    return f'cnt_inserted_rows_en: {cnt_inserted_rows_en}, cnt_inserted_rows_ua: {cnt_inserted_rows_ua}.'

