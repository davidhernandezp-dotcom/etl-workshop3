-- Workshop 3 - Esquema de la base de datos (SQLite)
-- Se ejecuta automaticamente al iniciar el consumer (IF NOT EXISTS: es seguro repetirlo).

-- RAW / STAGING: el mensaje de Kafka tal cual llego (trazabilidad, auditoria, reproceso).
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    kafka_topic       TEXT    NOT NULL,
    kafka_partition   INTEGER NOT NULL,
    kafka_offset      INTEGER NOT NULL,
    kafka_key         TEXT,
    raw_payload       TEXT    NOT NULL,      -- valor original del mensaje, sin modificar
    received_at       TEXT    NOT NULL,      -- cuando lo recibio el consumer
    processing_status TEXT    NOT NULL DEFAULT 'RECEIVED',
        -- RECEIVED | VALID | INVALID_SCHEMA | INVALID_VALUES | DUPLICATE_EVENT | PREDICTION_ERROR
    status_detail     TEXT,                  -- motivo del rechazo / error
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)   -- un mismo mensaje no se guarda dos veces
);

-- DIMENSIONES
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id   INTEGER PRIMARY KEY,           -- YYYYMMDD de event_time
    full_date TEXT    NOT NULL,
    year      INTEGER NOT NULL,
    quarter   INTEGER NOT NULL,
    month     INTEGER NOT NULL,
    day       INTEGER NOT NULL
);

-- HECHOS
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_event_id         INTEGER NOT NULL UNIQUE REFERENCES raw_happiness_events(raw_event_id),
    country_id           INTEGER NOT NULL REFERENCES dim_country(country_id),
    date_id              INTEGER NOT NULL REFERENCES dim_date(date_id),
    event_id             TEXT    NOT NULL,
    observation_year     INTEGER NOT NULL,   -- anio historico de la observacion (campo "year" del evento)
    actual_score         REAL    NOT NULL,
    predicted_score      REAL    NOT NULL,
    prediction_error     REAL    NOT NULL,   -- predicted - actual (con signo)
    abs_error            REAL    NOT NULL,   -- |predicted - actual|
    event_time           TEXT    NOT NULL,
    prediction_timestamp TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_status   ON raw_happiness_events(processing_status);
CREATE INDEX IF NOT EXISTS idx_fact_country ON fact_predictions(country_id);
CREATE INDEX IF NOT EXISTS idx_fact_year    ON fact_predictions(observation_year);

-- VISTAS PARA POWER BI (consultas de lectura sobre el modelo; Power BI las ve como tablas)
DROP VIEW IF EXISTS vw_predictions;
CREATE VIEW vw_predictions AS
SELECT f.prediction_id,
       f.raw_event_id,
       f.event_id,
       c.country_name,
       f.observation_year,
       f.actual_score,
       f.predicted_score,
       f.prediction_error,
       f.abs_error,
       f.event_time,
       f.prediction_timestamp,
       substr(f.prediction_timestamp, 1, 16) AS prediction_minute,
       d.full_date AS event_date
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
JOIN dim_date d    ON d.date_id    = f.date_id;

DROP VIEW IF EXISTS vw_event_status;
CREATE VIEW vw_event_status AS
SELECT raw_event_id, kafka_partition, kafka_offset, kafka_key,
       processing_status, status_detail, received_at
FROM raw_happiness_events;
