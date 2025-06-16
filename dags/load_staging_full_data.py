from datetime import datetime, timedelta
import requests as re
from pathlib import Path
import time
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


""" Работа функций:
    generate_report: Получаем report_id.
    save_files: Сохраняем файлы csv в tmp_data/data_full.
"""
TMP_DATA = r'/opt/airflow/tmp_data/full_data' # Путь в контейнере.
NICKNAME = 'Dmitriidm'
COHORT = '7' 
API_KEY = '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
FILE_NAMES = ['customer_research.csv', 'user_order_log.csv', 'user_activity_log.csv', 'price_log.csv']
HEADERS = {
        "X-API-KEY": API_KEY,
        "X-Nickname": NICKNAME,
        "X-Cohort": COHORT,
        "X-Project": "True"
    }


def generate_report():
    generate_report_responce = re.post(
        "https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report",
        headers = HEADERS
    ).json()
    task_id = generate_report_responce['task_id']

    time.sleep(120)

    get_report_responce = re.get(
        f"https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id={task_id}",
        headers = HEADERS   
    ).json()

    result_status = get_report_responce['status']
    report_id = get_report_responce['data']['report_id']
    print(f'{result_status}, {report_id}')
    return report_id

def save_files(file_names):
    report_id = generate_report()
    Path(TMP_DATA).mkdir(exist_ok=True, parents=True) 
    for file_name in file_names:
        SAVE_PATH = Path(TMP_DATA).joinpath(file_name) 
        url = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{report_id}/{file_name}'
        df = pd.read_csv(url)
        df.to_csv(SAVE_PATH, index=False)
        print(f'Файл успешно загружен {file_name}')


args = {
    'owner': 'Dmitriidm',
    'start_date': datetime.today() - timedelta(days=1),
    'end_date': datetime.today() + timedelta(days=1)
}

with DAG(
    'load_staging_full_data',
     default_args=args,
     catchup=False,
     schedule_interval=None
) as dag:
    save_full_data = PythonOperator(
        task_id='save_full_data',
        python_callable=save_files,
        op_kwargs={'file_names': FILE_NAMES}
    )