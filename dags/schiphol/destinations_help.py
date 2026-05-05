
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

from psycopg2.extras import execute_values
from airflow.exceptions import AirflowException
import json


def transform_destinations(ti: RuntimeTaskInstance):

    resultados = []
    destinations = []

    pages: list[str] = ti.xcom_pull(task_ids="extract")


    for page in pages:
        page = json.loads(page)
        destinations.extend(page.get("destinations"))

    for destination in destinations:

        name = destination.get("publicName")
        if name:
            name = name.get("english")

        resultados.append(
            {
                "name": name,
                "country": str(destination.get("country") or "").upper(),
                "iata": str(destination.get("iata") or "").upper(),
                "city": str(destination.get("city") or "").upper(),
            }
        )

    return resultados

def load_destinations(conn_id: str, **context):
    ti: RuntimeTaskInstance = context["ti"]
    rows: list[dict] = (
        ti.xcom_pull(task_ids="transform",  key='return_value') 
        or []
    )

    if not rows:
        # nada a carregar; decide se quer falhar ou apenas logar
        print("Transform não retornou linhas. Encerrando sem load.")
        return 0

    values = []
    for r in rows:

        name = r.get("name")
        country = r.get("country")
        iata = r.get("iata")
        city = r.get("city")

        
        values.append((name, country, iata, city))

    if not values:
        raise AirflowException("Após validação, não há linhas válidas para inserir.")

    hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        INSERT INTO destinations (name, country, iata, city)
        VALUES %s
        """

    # Execução em lote, performática
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)
        conn.commit()

    return len(values)