from datetime import datetime, UTC, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

from psycopg2.extras import execute_values
from airflow.exceptions import AirflowException
import json


def atributos_para_dict(flight, lista_atributos):
    resultados = dict()

    for atributo in lista_atributos:
        resultados[atributo] = flight.get(atributo)

    return resultados


def atributos_data_para_dict(flight, lista_atributos):

    resultados = {}

    for atributo in lista_atributos:
        data = flight.get(atributo)

        if data:
            try:
                data = datetime.fromisoformat(data).astimezone(timezone.utc)
            except ValueError:
                data = None

        resultados[atributo] = data

    return resultados


def transform_flights(ti: RuntimeTaskInstance):

    resultados = []

    flights = []
    pages: list[str] = ti.xcom_pull(task_ids="extract")

    for page in pages:
        page = json.loads(page)
        flights.extend(page["flights"])

    for flight in flights:
        aircraftType = flight.get("aircraftType")

        aircraftType_iataMain = None
        aircraftType_iataSub = None
        if aircraftType:
            aircraftType_iataMain = aircraftType.get("iataMain")
            aircraftType_iataSub = aircraftType.get("iataSub")

        # rotas separadas por vírgula
        route = None
        eu = None
        visa = None
        flight_route = flight.get("route")
        if flight_route:
            destinations = flight_route.get("destinations")
            eu = flight_route.get("eu")
            visa = flight_route.get("visa")
            if destinations:
                route = ",".join(destinations)

        # codeshares separados por vírgula
        codeshares = None
        flight_codeshares = flight.get("codeshares")
        if flight_codeshares:
            flight_codeshares = flight_codeshares.get("codeshares")
            if flight_codeshares:
                codeshares = ",".join(flight_codeshares)

        flight_states = None
        public_flight_states = flight.get("publicFlightState")
        if public_flight_states:
            flight_states = public_flight_states.get("flightStates")
            if flight_states:
                flight_states = ",".join(flight_states)

        atributos = {
            "aircraftType_iataMain": aircraftType_iataMain,
            "aircraftType_iataSub": aircraftType_iataSub,
            "route": route,
            "codeshares": codeshares,
            "flightStates": flight_states,
            "eu": eu,
            "visa": visa,
        }

        atributos.update(
            atributos_para_dict(
                flight,
                [
                    "flightDirection",
                    "flightName",
                    "flightNumber",
                    "gate",
                    "pier",
                    "id",
                    "isOperationalFlight",
                    "mainFlight",
                    "prefixIATA",
                    "prefixICAO",
                    "airlineCode",
                    "aircraftRegistration",
                    "serviceType",
                    "terminal",
                ],
            )
        )

        atributos.update(
            atributos_data_para_dict(
                flight,
                [
                    "estimatedLandingTime",
                    "lastUpdatedAt",
                    "actualLandingTime",
                    "scheduleDateTime",
                    "actualOffBlockTime",
                    "expectedTimeBoarding",
                    "expectedTimeGateClosing",
                    "expectedTimeGateOpen",
                    "expectedTimeOnBelt",
                    "expectedSecurityFilter",
                    "publicEstimatedOffBlockTime",
                ],
            )
        )

        resultados.append(atributos)

    return resultados


def load_flights(conn_id: str, **context):
    ti: RuntimeTaskInstance = context["ti"]
    rows: list[dict] = ti.xcom_pull(task_ids="transform", key="return_value") or []

    if not rows:
        # nada a carregar; decide se quer falhar ou apenas logar
        print("Transform não retornou linhas. Encerrando sem load.")
        return 0

    values = []
    for r in rows:

        aircraftType_iataMain = r.get("aircraftType_iataMain")
        aircraftType_iataSub = r.get("aircraftType_iataSub")
        route = r.get("route")
        codeshares = r.get("codeshares")
        flightStates = r.get("flightStates")
        eu = r.get("eu")
        visa = r.get("visa")


        flightDirection = r.get("flightDirection")
        flightName = r.get("flightName")
        flightNumber = r.get("flightNumber")
        gate = r.get("gate")
        pier = r.get("pier")

        isOperationalFlight = r.get("isOperationalFlight")
        mainFlight = r.get("mainFlight")
        prefixIATA = r.get("prefixIATA")
        prefixICAO = r.get("prefixICAO")
        airlineCode = r.get("airlineCode")
        aircraftRegistration = r.get("aircraftRegistration")
        serviceType = r.get("serviceType")
        terminal = r.get("terminal")

        estimatedLandingTime = r.get("estimatedLandingTime")
        lastUpdatedAt = r.get("lastUpdatedAt")
        actualLandingTime = r.get("actualLandingTime")
        scheduleDateTime = r.get("scheduleDateTime")
        actualOffBlockTime = r.get("actualOffBlockTime")
        expectedTimeBoarding = r.get("expectedTimeBoarding")
        expectedTimeGateClosing = r.get("expectedTimeGateClosing")
        expectedTimeGateOpen = r.get("expectedTimeGateOpen")
        expectedTimeOnBelt = r.get("expectedTimeOnBelt")
        expectedSecurityFilter = r.get("expectedSecurityFilter")
        publicEstimatedOffBlockTime = r.get("publicEstimatedOffBlockTime")

        values.append(
            (
                #  aircraftType.*
                aircraftType_iataMain,
                aircraftType_iataSub,
                # listas serializadas por vírgula
                route,
                codeshares,
                flightStates,
                # flags da rota
                eu,
                visa,
                # metadados do voo
                flightDirection,
                flightName,
                flightNumber,
                gate,
                pier,
                isOperationalFlight,
                mainFlight,
                prefixIATA,
                prefixICAO,
                airlineCode,
                aircraftRegistration,
                serviceType,
                terminal,
                # datas/horas
                estimatedLandingTime,
                lastUpdatedAt,
                actualLandingTime,
                scheduleDateTime,
                actualOffBlockTime,
                expectedTimeBoarding,
                expectedTimeGateClosing,
                expectedTimeGateOpen,
                expectedTimeOnBelt,
                expectedSecurityFilter,
                publicEstimatedOffBlockTime,
            )
        )

    if not values:
        raise AirflowException("Após validação, não há linhas válidas para inserir.")

    hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
    INSERT INTO flights (
        aircrafttype_iata_main,
        aircrafttype_iata_sub,
        route,
        codeshares,
        flight_states,
        eu,
        visa,
        flight_direction,
        flight_name,
        flight_number,
        gate,
        pier,
        is_operational_flight,
        main_flight,
        prefix_iata,
        prefix_icao,
        airline_code,
        aircraft_registration,
        service_type,
        terminal,
        estimated_landing_time,
        last_updated_at,
        actual_landing_time,
        schedule_datetime,
        actual_off_block_time,
        expected_time_boarding,
        expected_time_gate_closing,
        expected_time_gate_open,
        expected_time_on_belt,
        expected_security_filter,
        public_estimated_off_block_time
    ) VALUES %s;
    """

    # Execução em lote, performática
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)
        conn.commit()

    return len(values)
