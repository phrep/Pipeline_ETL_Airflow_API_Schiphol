
import sys
import sys
import os
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow import DAG

sys.path.append(os.path.dirname(__file__))

from flights_help import load_flights, transform_flights
from utils.pagination import next_page

with DAG(
    dag_id="schiphol_flights",
    catchup=False,
) as dag:
    extract = HttpOperator(
        task_id="extract",
        http_conn_id="schiphol_conn",
        endpoint="flights",
        method='GET',
        pagination_function=next_page,
        log_response=True,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_flights,
        
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_flights_table",
        conn_id="schiphol_db",
        sql="sql/create_table_flights.sql",
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_flights,
        op_kwargs={"conn_id": "schiphol_db"},
    )


    create_table >> extract >> transform >> load # type: ignore