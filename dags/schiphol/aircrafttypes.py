from airflow import DAG
import sys
import os
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

sys.path.append(os.path.dirname(__file__))
from utils.pagination import next_page
from aircrafttypes_help import transform_aircrafttypes, load_aircrafttypes

from pendulum import datetime, duration

with DAG(
    dag_id="schiphol_aircrafttypes",
    start_date=datetime(year=2025, month=11, day=13),
    schedule="29 19 * * *",
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=60)
    },
    catchup=False,
) as dag:
    
    extract = HttpOperator(
        task_id="extract",
        http_conn_id="schiphol_conn",
        endpoint="aircrafttypes",
        method="GET",
        pagination_function=next_page,
        log_response=True,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_aircrafttypes,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_aircraft_table",
        conn_id="schiphol_db",
        sql="sql/create_table_aircrafttypes.sql"
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_aircrafttypes,
        op_kwargs= {"conn_id": "schiphol_db"}
    )

    create_table >> extract >> transform >> load