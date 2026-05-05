from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="create_schiphol_database",
    description='DAG para criar um banco de dados no PostgreSQL e conceder permissões ao usuário airflow',
) as dag:
    
    # task1 
    create_database = SQLExecuteQueryOperator(
        task_id="create_database",
        conn_id="postgresql_conn",
        sql="""
           CREATE DATABASE schiphol_db
           WITH
           OWNER = airflow
           ENCODING = 'UTF8'
           LC_COLLATE = 'en_US.UTF-8'
           LC_CTYPE = 'en_US.UTF-8'
           TEMPLATE = template0;
        """,
        autocommit=True,
    )

    # task2
    grant_permissions = SQLExecuteQueryOperator(
        task_id='grant_permissions',
        conn_id='postgresql_conn',
        sql="""
            GRANT ALL PRIVILEGES ON DATABASE schiphol_db TO airflow;
            GRANT CONNECT ON DATABASE schiphol_db TO airflow;
            GRANT TEMPORARY ON DATABASE schiphol_db TO airflow;
        """,
        autocommit=True,
    )

    create_database >> grant_permissions
