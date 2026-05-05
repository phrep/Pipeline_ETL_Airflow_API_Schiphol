CREATE TABLE IF NOT EXISTS airlines (
    id    SERIAL PRIMARY KEY,
    iata  VARCHAR(8) NOT NULL,   -- pode ser string vazia
    icao  VARCHAR(8) NOT NULL,   -- código ICAO (único)
    nvls  VARCHAR(8) NOT NULL,   -- dígitos ou string vazia
    name  TEXT NOT NULL,
    UNIQUE (iata, icao, nvls),
    CHECK (
        iata = UPPER(iata) AND icao = UPPER(icao)
    )
);