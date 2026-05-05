import json
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


def transform_aircrafttypes(ti:RuntimeTaskInstance):
  
    resultados = []
    aircraft_types :list[dict] = []

    pages :list[str] = ti.xcom_pull(task_ids="extract", key="return_value")
    
    for page in pages:
        page = json.loads(page)
        aircraft_types.extend(page["aircraftTypes"])

    for aircraft in aircraft_types:
        resultados.append(
            {
                "iataMain": str(aircraft.get("iataMain") or "").upper(),
                "iataSub": str(aircraft.get("iataSub") or "").upper(),
                "description": aircraft.get("longDescription"),
            }
        )
    return resultados

def load_aircrafttypes(conn_id:str, **context):
    ti: RuntimeTaskInstance = context["ti"]

    rows: list[dict] = (
        ti.xcom_pull(task_ids="transform", key="return_value") or []
    ) 

    if not rows:
        print("Transform não retornou linhas. Encerrado sem load.")
        return
    
    values = []

    for r in rows:
        iata_main = r.get("iataMain")
        iata_sub = r.get("iataSub")
        description = r.get("description")

        values.append((iata_main, iata_sub, description))

    if not values:
        raise AirflowException("Após validação, não há linhas válidas pra inserir.")
    
    hook = PostgresHook(postgres_conn_id=conn_id)

    sql = """
        INSERT INTO aircrafttypes (iata_main, iata_sub, description)
        VALUES %s
        ON CONFLICT (iata_main, iata_sub, description)
        DO NOTHING;
        """
    
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)

    return len(values)