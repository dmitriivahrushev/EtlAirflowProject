from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



postges_conn_id = 'postgres_db'

args = {
    'owner': 'Dmitriidm',
    'start_date': datetime.today() - timedelta(days=1),
    'end_date': datetime.today() + timedelta(days=1)
}

with DAG (
    dag_id='build_f_customer_retention',
    default_args=args,
    catchup=False,
    schedule_interval='0 1 * * *'
) as dag:
    
    DDL_mart_f_customer_retention = SQLExecuteQueryOperator(
        task_id = 'DDL_mart_f_customer_retention',
        conn_id=postges_conn_id,
        autocommit=True,
        sql='sql_scripts/DDL_mart_f_customer_retention.sql'
    )

    refresh_f_customer_retention = SQLExecuteQueryOperator(
        task_id = 'refresh_f_customer_retention',
        conn_id=postges_conn_id,
        autocommit=True,
        sql='sql_scripts/refresh_f_customer_retention.sql'
    )

    DDL_mart_f_customer_retention >> refresh_f_customer_retention

    