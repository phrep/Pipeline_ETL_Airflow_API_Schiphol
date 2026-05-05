from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from airflow.exceptions import AirflowException
import json

def transform_airlines(ti: RuntimeTaskInstance):

    resultados = []
    airlines:list[dict] = []

    pages: list[str] = ti.xcom_pull(task_ids="extract")

    for page in pages:
        page = json.loads(page)
        airlines.extend(page.get("airlines"))

    for airline in airlines:
        resultados.append(
            {
                "iata": str(airline.get("iata") or "").upper(),
                "icao": str(airline.get("icao") or "").upper(),
                "nvls": str(airline.get("nvls") or ""),
                "name": airline.get("publicName"),
            }
        )

    return resultados


def load_airlines(conn_id: str, **context):
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
        iata = r.get("iata")
        icao = r.get("icao")
        nvls = r.get("nvls")
        name = r.get("name")

        values.append((iata, icao, nvls, name))

    if not values:
        raise AirflowException("Após validação, não há linhas válidas para inserir.")

    hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        INSERT INTO airlines (iata, icao, nvls, name)
        VALUES %s
        ON CONFLICT (iata, icao, nvls)
        DO NOTHING;
    """

    # Execução em lote, performática
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)
        conn.commit()

    return len(values)