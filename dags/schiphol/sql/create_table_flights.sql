CREATE TABLE IF NOT EXISTS flights (
    -- identificador vindo do payload (use-o como PK para idempotência)
    id SERIAL PRIMARY KEY,

    -- aircraftType.*
    aircrafttype_iata_main VARCHAR(8),
    aircrafttype_iata_sub  VARCHAR(8),

    -- listas serializadas por vírgula
    route         TEXT,   -- ex.: "AMS,CDG,FRA"
    codeshares    TEXT,   -- ex.: "KL1234,DL5678"
    flight_states TEXT,   -- ex.: "EXPECTED,FINAL"

    -- flags da rota
    eu   VARCHAR(8),
    visa BOOLEAN,

    -- metadados do voo
    flight_direction VARCHAR(1), -- tipicamente 'A' (arrivals) ou 'D' (departures)
    flight_name      TEXT,
    flight_number    TEXT,
    gate             TEXT,
    pier             TEXT,
    is_operational_flight BOOLEAN,
    main_flight      TEXT,
    prefix_iata      VARCHAR(8),
    prefix_icao      VARCHAR(8),
    airline_code     VARCHAR(8),
    aircraft_registration TEXT,
    service_type     TEXT,
    terminal         TEXT,

    -- datas/horas
    estimated_landing_time           TIMESTAMPTZ,
    last_updated_at                  TIMESTAMPTZ,
    actual_landing_time              TIMESTAMPTZ,
    schedule_datetime                TIMESTAMPTZ,
    actual_off_block_time            TIMESTAMPTZ,
    expected_time_boarding           TIMESTAMPTZ,
    expected_time_gate_closing       TIMESTAMPTZ,
    expected_time_gate_open          TIMESTAMPTZ,
    expected_time_on_belt            TIMESTAMPTZ,
    expected_security_filter         TEXT,        -- este campo é textual no payload da Schiphol
    public_estimated_off_block_time  TIMESTAMPTZ
);
