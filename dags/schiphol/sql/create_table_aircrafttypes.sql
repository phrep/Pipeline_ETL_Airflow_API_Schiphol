CREATE TABLE IF NOT EXISTS aircrafttypes (
    id SERIAL PRIMARY KEY,
    iata_main VARCHAR(8) NOT NULL,
    iata_sub VARCHAR(8) NOT NULL,
    description TEXT,
    UNIQUE (iata_main, iata_sub, description),
    CHECK (
        iata_sub = UPPER(iata_sub) AND iata_main = UPPER(iata_main)
    )
);