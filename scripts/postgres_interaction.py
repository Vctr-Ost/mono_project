# %%
from sqlalchemy import create_engine, text, inspect, Integer, Float, String, DateTime
import pandas as pd


# %%
def crt_engine(pg_conn_data):
    """
    Функція створює connection у Postgres DB

    Параметри:
        - postgres credentials (str)
    
    Повертає:
        - sqlalchemy.engine.base.Engine - conn
    """
    print('FN "crt_engine" started.')
    db_connection_str = f'postgresql://{pg_conn_data['PG_USER']}:{pg_conn_data['PG_PASS']}@{pg_conn_data['PG_HOST']}:{pg_conn_data['PG_PORT']}/{pg_conn_data['PG_DB']}'
    
    try:
        engine = create_engine(db_connection_str)
        print('Postgres engine created')
        return engine
    except Exception as e:
        print(f"ERROR: {e}")


# %%
def test_connection(engine):
    """
    Функція перевіряє підключення до engine

    Параметри:
        - engine (sqlalchemy.engine.base.Engine) - conn до postgres DB
    
    Повертає:
        - Bool - булеве значення успішного підʼєднання до postgres DB
    """
    print('FN "test_connection" started.')
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            if result.scalar() == 1:
                print("Connection to Postgres DB successful!")
                return True
            else:
                print("Connection to Postgres DB failed.")
                return False
    except Exception as e:
        print(f"Connection error: {e}")


# %%
def insert_pandas_df(df, engine, tbl_name, schema, if_exists='append'):
    """
    Функція вставляє дані в існуючу таблицю Postgres (або створює таблицю і вставляє дані)

    Параметри:
        - df (pd.DataFrame) - датафрейм із даними
        - engine - Postgres DB engine
        - tbl_name (str) - назва таблиці postgres
        - schema (str) - назва схеми postgres
    
    Повертає:
        - int - к-сть вставлених у таблицю рядків
    """
    print('FN "insert_pandas_df" started.')
    try:
        df.to_sql(tbl_name, 
                engine, 
                schema=schema,
                if_exists=if_exists, 
                index=False)

        print(f'DF inserted in table {schema}.{tbl_name}, len(df)=={len(df)}')
        return len(df)
    except Exception as e:
        print(f'FN insert_pandas_df exception: {e}')


# %%
def is_postgres_table_exists(engine, tbl_name, schema):
    """
    Функція перевіряє чи існує таблиця у Postgres DB

    Параметри:
        - engine - Postgres DB engine
        - tbl_name (str) - назва таблиці postgres
        - schema (str) - назва схеми postgres
    
    Повертає:
        - Bool - маркер наявності таблиці у схемі Postgres DB
    """
    print('FN "is_postgres_table_exists" started.')
    inspector = inspect(engine)

    if tbl_name in inspector.get_table_names(schema=schema):
        print(f"Table '{schema}.{tbl_name}' exists.")
        return True
    else:
        print(f"Table '{schema}.{tbl_name}' NOT exists.")
        return False


# %%
def add_cols_intp_postgres_tbl(df, engine, tbl_name, schema):
    """
    Функція додає колонки із df яких немає у таблиці schema.tbl_name Postgres DB

    Параметри:
        - df (pd.DataFrame) - датафрейм із даними
        - engine - Postgres DB engine
        - tbl_name (str) - назва таблиці postgres
        - schema (str) - назва схеми postgres
    """
    dtype_map = {
        'int64': Integer(),
        'float64': Float(),
        'object': String(),
        'datetime64[ns]': DateTime()
    }

    inspector = inspect(engine)

    postgres_columns = inspector.get_columns(tbl_name, schema=schema)
    postgres_columns = [col['name'] for col in postgres_columns]

    print(f'df.columns - {df.columns}')
    print(f'postgres_columns - {postgres_columns}')

    for col in df.columns:
        if col not in postgres_columns:
            dtype = df[col].dtype
            col_type = dtype_map.get(dtype.name, String())

            try:
                with engine.connect() as connection:
                    query = f'ALTER TABLE {schema}.{tbl_name} ADD COLUMN "{col}" {col_type};'
                    print(f'QUERY - {query}')
                    connection.execute(text(query))
                    print('cols ADDED')
            except Exception as e:
                print(f"Connection error: {e}")


# %%
def create_schema_if_not_exists(engine, schema_name):
    """
    Функція перевіряє чи існує схема у Postgres DB та створює (якщо відсутня)

    Параметри:
        - engine - Postgres DB engine
        - schema_name (str) - назва схеми postgres
    """
    inspector = inspect(engine)

    if schema_name not in inspector.get_schema_names():
        try:
            with engine.connect() as connection:
                create_schema_query = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'
                connection.execute(text(create_schema_query))
                connection.commit()
                print(f"Schema {schema_name} created.")
                return f"Schema {schema_name} created."
        except Exception as e:
            print(f"Schema {schema_name} NOT created. ERROR: {e}")
    else:
        return f'Schema {schema_name} exists.'


# %%
def run_query(engine, query):
    """
    Функція запускає Query у БД

    Параметри:
        - engine - Postgres DB engine
        - query (str) - запит
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            print('Query runned Successfull.')
            return result
    except Exception as e:
        print(f"Query Error. ERROR: {e}")


# %%
