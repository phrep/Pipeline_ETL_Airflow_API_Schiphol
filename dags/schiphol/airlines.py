from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
import sys
import os

sys.path.append(os.path.dirname(__file__))
from utils.pagination import next_page
from airlines_help import transform_airlines, load_airlines
with DAG(
    dag_id="schiphol_airlines",
    catchup=False,
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_airlines_table",
        conn_id="schiphol_db",
        sql="sql/create_table_airlines.sql"
    )

    extract = HttpOperator(
        task_id="extract",
        http_conn_id="schiphol_conn",
        endpoint="airlines",
        method="GET",
        pagination_function=next_page,
        log_response=True
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_airlines,
        
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_airlines,
        op_kwargs={"conn_id": "schiphol_db"},
    )


    create_table >> extract >> transform >> load