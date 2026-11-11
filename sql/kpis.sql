-- Workshop 3 - KPIs del dashboard (el dashboard consulta la base de datos, no CSV)

-- KPI 1. Error promedio de prediccion
SELECT ROUND(AVG(abs_error), 4)        AS mae,
       ROUND(AVG(prediction_error), 4) AS bias_promedio,
       ROUND(SQRT(AVG(prediction_error * prediction_error)), 4) AS rmse,
       COUNT(*)                        AS total_predicciones
FROM fact_predictions;

-- KPI 2. Predicciones por pais
SELECT c.country_name, COUNT(*) AS n_predicciones, ROUND(AVG(f.abs_error), 4) AS mae
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name
ORDER BY n_predicciones DESC, c.country_name;

-- KPI 3. Predicho vs real
SELECT c.country_name, f.observation_year, f.actual_score, f.predicted_score, f.prediction_error
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
ORDER BY f.prediction_id;

-- KPI 4a. Tendencia por anio historico de la observacion
SELECT observation_year,
       ROUND(AVG(actual_score), 3)    AS avg_actual,
       ROUND(AVG(predicted_score), 3) AS avg_predicted,
       ROUND(AVG(abs_error), 4)       AS mae,
       COUNT(*)                       AS n
FROM fact_predictions
GROUP BY observation_year
ORDER BY observation_year;

-- KPI 4b. Tendencia en el tiempo de procesamiento (por minuto de prediction_timestamp)
SELECT substr(prediction_timestamp, 1, 16) AS minuto,
       ROUND(AVG(abs_error), 4) AS mae,
       COUNT(*) AS n
FROM fact_predictions
GROUP BY substr(prediction_timestamp, 1, 16)
ORDER BY minuto;

-- Calidad del stream: eventos por estado de procesamiento
SELECT processing_status, COUNT(*) AS n
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY n DESC;

-- Detalle de rechazados
SELECT raw_event_id, processing_status, status_detail, raw_payload
FROM raw_happiness_events
WHERE processing_status NOT IN ('VALID', 'RECEIVED')
ORDER BY raw_event_id;

-- Trazabilidad: de la prediccion al evento original
SELECT f.prediction_id, f.raw_event_id, r.kafka_partition, r.kafka_offset, r.raw_payload
FROM fact_predictions f
JOIN raw_happiness_events r ON r.raw_event_id = f.raw_event_id
LIMIT 20;
