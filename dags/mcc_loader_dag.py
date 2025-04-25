from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

import os
from dotenv import load_dotenv
load_dotenv()
from git_file_loader import mcc_file_loader


def test_fn():
    print(12345)
    return 'success'

def mcc_loader():

    pg_conn_data = {
        'PG_USER': os.getenv('POSTGRES_USER'),
        'PG_PASS': os.getenv('POSTGRES_PASSWORD'),
        'PG_HOST': os.getenv('POSTGRES_HOST'),
        'PG_PORT': os.getenv('POSTGRES_PORT'),
        'PG_DB': os.getenv('POSTGRES_DATABASE')
    }

    last_trn_unix = mcc_file_loader(pg_conn_data) 

    return last_trn_unix

    # return mono_data_loader(POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE)


with DAG(
    'mcc_loader_dag',
    description='DAG mcc_loader_dag',
    # schedule='* * * * *',
    schedule='@monthly',
    # start_date=datetime(2024, 12, 20),
    start_date=datetime.now(),
    catchup=False,
    max_active_runs=1
):

    task_mono_data_loader=PythonOperator(
        task_id='task_mcc_loader_dag',
        python_callable=mcc_loader,
        # python_callable=test_fn,
)