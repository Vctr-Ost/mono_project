from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.db import provide_session
from airflow.models import Variable

import os
import json
from dotenv import load_dotenv
load_dotenv()
from main import mono_data_loader
from postgres_interaction import crt_engine, test_connection, is_postgres_table_exists, run_query



pg_conn_data = {
    'PG_USER': os.getenv('POSTGRES_USER'),
    'PG_PASS': os.getenv('POSTGRES_PASSWORD'),
    'PG_HOST': os.getenv('POSTGRES_HOST'),
    'PG_PORT': os.getenv('POSTGRES_PORT'),
    'PG_DB': os.getenv('POSTGRES_DATABASE')
}

ACCOUNT_ID = os.getenv('ACCOUNT_ID')
ACCOUNT_ID_2 = os.getenv('ACCOUNT_ID_2', "{}")
HEADERS = {'X-Token': os.getenv('X_TOKEN')}


def fn_update_staging_data():
    stg_tbl_name = 'stg_transactions'
    stg_schema = 'mono_data'
    pg_engine = crt_engine(pg_conn_data)
    query_base = """
        SELECT
            trn."accountId" AS account_id
            , trn."cardName" AS card_name
            , trn.id AS trn_id
            , trn.time AS trn_unix
            , (TO_TIMESTAMP(trn.time) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC+2' AS ts
            , trn.description AS bank_description
            , trn."originalMcc" AS mcc
            , mcc.group_description AS mcc_group_description
            , mcc.short_description AS mcc_short_description
            , Null AS category
            , Null AS sub_category
            , Null AS comment
            , TRUE AS handle_marker
            , trn.amount / 100.00 AS amount
            , trn."cashbackAmount" / 100.00 AS cashback_amount
            , trn.balance / 100.00 AS rest_balance
        FROM mono_data.raw_transactions trn
        LEFT JOIN mono_data.mcc_en mcc
            ON trn."originalMcc"::text = mcc.mcc
        """

    if test_connection(pg_engine):
        if is_postgres_table_exists(pg_engine, stg_tbl_name, stg_schema):
            query = f"""
                INSERT INTO mono_data.stg_transactions (
                    account_id,
                    card_name,
                    trn_id,
                    trn_unix,
                    ts,
                    bank_description,
                    mcc,
                    mcc_group_description,
                    mcc_short_description,
                    category,
                    sub_category,
                    comment,
                    handle_marker,
                    amount,
                    cashback_amount,
                    rest_balance
                )
                {query_base}
                WHERE trn.time > (SELECT MAX(trn_unix) FROM mono_data.stg_transactions WHERE trn_id NOT LIKE 'custom%')
            """
        else:
            query = f"CREATE TABLE mono_data.stg_transactions AS {query_base}"

        print(query)
        
        run_query(pg_engine, query)
    
    return True


def fn_mono_data_loader():
    card_account_ids = json.loads(ACCOUNT_ID_2)
    


    for card_name, acc_id in card_account_ids.items():

        last_trn_unix = int(Variable.get(f'LAST_TRN_UNIX_{acc_id}', default_var=0))
        
        print(f'FIRST for {card_name}: -------------------------------- {last_trn_unix} - {type(last_trn_unix)}')
        upd_last_trn_unix = mono_data_loader(pg_conn_data, card_name, acc_id, HEADERS, last_trn_unix)
        print(f'LAST for {card_name}: -------------------------------- {upd_last_trn_unix}')

        if upd_last_trn_unix > last_trn_unix :
            Variable.set(f'LAST_TRN_UNIX_{acc_id}', upd_last_trn_unix)
            print(f'LAST_TRN_UNIX_{acc_id} UPDATED ON {upd_last_trn_unix}')
    
    return True



with DAG(
    'get_mono_data_dag',
    description='DAG to extract mono data from last transaction unix_ts',
    schedule='* * * * *',
    # schedule=None,
    # start_date=datetime(2024, 12, 20),
    start_date=datetime(2024, 12, 20),
    catchup=False,
    max_active_runs=1
):

    task_mono_data_loader=PythonOperator(
        task_id='task_mono_data_loader',
        python_callable=fn_mono_data_loader,
    )

    def check_task_mono_data_loader_result(ti):
        result = ti.xcom_pull(task_ids='task_mono_data_loader')
        if result is True:
            fn_update_staging_data()
    
    stg_data_upd = PythonOperator(
        task_id='stg_data_upd',
        python_callable=check_task_mono_data_loader_result,
    )

    task_mono_data_loader >> stg_data_upd
