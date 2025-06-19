from datetime import datetime, timedelta
import requests as re
from pathlib import Path
import pandas as pd
import time

from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowFailException
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Table, MetaData


http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host
postges_conn_id = 'postgres_db'
TMP_DATA = r'/opt/airflow/tmp_data/inc_data'
FILE_NAMES = ['user_order_log_inc.csv']
nickname = 'Dmitriidm'
cohort = '7' 

HEADERS = {
        "X-API-KEY": api_key,
        "X-Nickname": nickname,
        "X-Cohort": cohort,
        "X-Project": "True"
    }


def generate_report(ti):
    try:
        generate_report = re.post(f"{base_url}/generate_report", headers = HEADERS)
        generate_report.raise_for_status()
        task_id = generate_report.json()['task_id']
    except Exception as e:
        raise AirflowFailException(f"Ошибка при запросе generate_report: {e}")

    try:
        for _ in range(20):
            get_report = re.get(f"{base_url}/get_report?task_id={task_id}", headers = HEADERS)
            get_report.raise_for_status()
            status = get_report.json()['status']
            if status == 'SUCCESS':
                report_id = get_report.json()['data']['report_id']
                ti.xcom_push(key='report_id', value=report_id)
                print(f'report_id: получен {report_id}')
                break
            else:
                time.sleep(10)

    except Exception as e:
        raise AirflowFailException(f'Ошибка при запросе: get_report {e}')

  
def save_inc_files(file_names, ti):
    report_id = ti.xcom_pull(key='report_id')
    date = datetime.today().date() - timedelta(days=1)
    response = re.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=HEADERS).json()
    increment_id = response['data']['increment_id']

    Path(TMP_DATA).mkdir(exist_ok=True, parents=True)
    for file_name in file_names:
        SAVE_PATH = Path(TMP_DATA).joinpath(file_name)
        url = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{file_name}'
        df = pd.read_csv(url)
        df.to_csv(SAVE_PATH, index=False)
        print(f'Файл {file_name} сохранен успешно.')


def upload_data_staging(file_name, pg_table, pg_schema):
    df = pd.read_csv(f'{TMP_DATA}/{file_name}')
    df=df.drop('id', axis=1)
    df=df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(postges_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    metadata = MetaData()
    table = Table(pg_table, metadata, autoload_with=engine, schema=pg_schema)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = insert(table).values(row.to_dict())
            update_dict = {col: row[col] for col in df.columns if col != 'uniq_id'}
            stmt = stmt.on_conflict_do_update(
                index_elements=['uniq_id'],
                set_=update_dict
            )

            conn.execute(stmt)


args = {
    'owner': 'Dmitriidm',
    'start_date': datetime.today() - timedelta(days=1),
    'end_date': datetime.today() + timedelta(days=1)
}

with DAG (
     dag_id='load_stagigng_inc_data',
     default_args=args,
     catchup=False,
     schedule_interval='@daily'
) as dag:
    
    DDL_staging = SQLExecuteQueryOperator(
        task_id='DDL_staging',
        conn_id=postges_conn_id,
        autocommit=True,
        sql='sql_scripts/DDL_staging.sql'
    )

    DDL_mart = SQLExecuteQueryOperator(
        task_id='DDL_mart',
        conn_id=postges_conn_id,
        autocommit=True,
        sql='sql_scripts/DDL_mart.sql'
    )

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )
    
    save_inc_files = PythonOperator(
        task_id='save_inc_files',
        python_callable=save_inc_files,
        op_kwargs={'file_names': FILE_NAMES}
    )

    upload_data_staging = PythonOperator(
        task_id='upload_data_staging',
        python_callable=upload_data_staging,
        op_kwargs={'file_name': 'user_order_log_inc.csv',
                   'pg_schema': 'staging',
                   'pg_table': 'user_order_log'}
    )
    
    DML_d_city = SQLExecuteQueryOperator(
        task_id='DML_d_city',
        conn_id=postges_conn_id,
        autocommit=True,
        sql='sql_scripts/DML_d_city.sql'
    )

    DML_d_customer = SQLExecuteQueryOperator(
        task_id='DML_d_customer',
        conn_id=postges_conn_id,
        autocommit=True,
        sql='sql_scripts/DML_d_customer.sql'
    )

    DML_d_item = SQLExecuteQueryOperator(
        task_id='DML_d_item',
        conn_id=postges_conn_id,
        autocommit=True,
        sql='sql_scripts/DML_d_item.sql'
    )
    
    DML_f_sales = SQLExecuteQueryOperator(
        task_id='DML_f_sales',
        conn_id=postges_conn_id,
        autocommit=True,
        sql='sql_scripts/DML_f_sales.sql'
    )

    (
        [DDL_staging, DDL_mart]
        >> generate_report
        >> save_inc_files
        >> upload_data_staging
        >> [DML_d_city, DML_d_customer, DML_d_item]
        >> DML_f_sales
    )

    