CREATE TABLE IF NOT EXISTS destinations (
    id      SERIAL PRIMARY KEY,
    name    TEXT NOT NULL,
    country TEXT,
    city    TEXT,
    iata    VARCHAR(8),
    CHECK (iata = UPPER(iata))
);
