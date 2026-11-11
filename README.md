# Workshop 3 — Streaming ETL with Apache Kafka and Machine Learning

**Course:** ETL (G01) · Workshop 3 · **Team:** [TEAM MEMBER NAMES]

An event-driven ETL pipeline that predicts the **World Happiness Score** in real time. A *producer* publishes historical records to Kafka, a *consumer* validates each event against an event contract, generates a prediction with a pre-trained model, and stores everything in SQLite. A Power BI dashboard queries the database.

## 1. Architecture

```
OFFLINE (batch)
  data/raw/2015..2019.csv → EDA → cleaning/harmonization → features → training
        → data/processed/happiness_unified.csv · models/model.pkl (StandardScaler + model)
        → data/streaming/stream_events.csv (30 % test split)

STREAMING
  stream_events.csv → producer.py (JSON, key = country, configurable delay)
        → Kafka · topic happiness-events (3 partitions, RF=1)
        → consumer.py (group happiness-ml-team)
             1. stores the original message in raw_happiness_events
             2. validates the event contract
             3. builds features in the same order used for training
             4. loads model.pkl and predicts
             5. stores fact_predictions (+ dim_country, dim_date)
        → SQLite (storage/happiness.db) → SQL views → Power BI (ODBC) / sql/kpis.sql
```

Kafka **retains** events while the container exists (log, offsets, replay). SQLite **persists** the application's results (raw events, rejected events and predictions). These are two different kinds of persistence.

## 2. Data cleaning decisions (Part A, steps 1-2)
- The five CSV files have different schemas (`Country` / `Country or region`, `Family` / `Social support`, `Trust…` / `Perceptions of corruption`, dotted names in 2017, etc.). They are renamed with an explicit mapping to a `snake_case` unified schema: `country, year, happiness_score, gdp, family, health, freedom, generosity, corruption`.
- `year` is taken from the file name (no CSV contains it).
- 6 country aliases were unified (Hong Kong S.A.R., Taiwan Province of China, Macedonia → North Macedonia, Northern Cyprus, Somaliland Region, Trinidad & Tobago).
- 1 row dropped (UAE-2018, missing `corruption`): imputing would invent a value, and losing one row has negligible cost. Result: **781 rows**.
- No duplicates. Outliers in `corruption` / `generosity` were **kept** (real, plausible values).
- Excluded columns: `Region`, `Rank`, standard error, confidence intervals, whiskers and `Dystopia Residual` (not present in all five years and/or *target leakage*).

## 3. Feature engineering and model
- Features: `gdp, family, health, freedom, generosity, corruption`. Target: `happiness_score` (in the event: `actual_happiness_score`, **used for evaluation only**).
- `country` and `year` are not features (they identify the event).
- `Pipeline(StandardScaler → model)` is serialized as a whole in `models/model.pkl`; the column order is stored in `models/features.json`.
- 70/30 split (`random_state=42`). `LinearRegression` and `RandomForestRegressor` are compared and the one with the lowest RMSE is saved. Metrics are in `models/metrics.json` (MAE, RMSE, R²).
- The stream uses the **30 % test split** (rows not seen during training), so the dashboard error is honest. `--source full` streams the whole dataset.

## 4. Kafka pipeline and event contract
Topic `happiness-events` · 3 partitions · replication factor 1 · key = `country` · consumer group `happiness-ml-team`.

```json
{"event_id":"E0001","country":"Colombia","year":2019,"gdp":1.2,"family":0.8,"health":0.9,
 "freedom":0.6,"generosity":0.3,"corruption":0.1,"actual_happiness_score":6.2,"event_time":"2026-11-06T15:30:00"}
```

| Field | Type | Rule |
|---|---|---|
| event_id | string | non-empty, unique |
| country | string | non-empty (also the Kafka key) |
| year | int | 2000-2100 |
| gdp, family, health, freedom, generosity, corruption | number | finite, 0 ≤ x ≤ 5 |
| actual_happiness_score | number | 0-10, **never** a model input |
| event_time | ISO-8601 | parseable timestamp |

Processing statuses in `raw_happiness_events.processing_status`:

| Status | When |
|---|---|
| `VALID` | passed the contract and was predicted |
| `INVALID_SCHEMA` | unreadable JSON, missing/empty field or wrong data type |
| `INVALID_VALUES` | correct type but invalid value (negative, NaN, out of range, bad timestamp) |
| `DUPLICATE_EVENT` | `event_id` already processed |
| `PREDICTION_ERROR` | the model failed |

Invalid events are stored in the raw table, skipped from prediction, and **never crash** the consumer. A valid JSON is not necessarily a valid event: parsing says "I can read it", validation says "I can use it".

## 5. Database schema (`sql/create_tables.sql`)
- `raw_happiness_events` (staging): original message + partition/offset + status. `UNIQUE(topic, partition, offset)` prevents storing a redelivered message twice.
- `dim_country(country_id, country_name)`, `dim_date(date_id, full_date, year, quarter, month, day)`.
- `fact_predictions`: `prediction_id, raw_event_id (FK → raw), country_id, date_id, event_id, observation_year, actual_score, predicted_score, prediction_error (predicted − actual), abs_error, event_time, prediction_timestamp`.
- Views for BI: `vw_predictions` (flattened facts + dimensions) and `vw_event_status`.

## 6. Dashboard (Power BI)
Power BI connects through **ODBC to `storage/happiness.db`** (not CSV files) and reads the views `vw_predictions` and `vw_event_status`. The DAX measures are in `dashboards/powerbi/measures.dax` and the dashboard file is `dashboards/powerbi/workshop3.pbix`.

| KPI | Visual | What it shows |
|---|---|---|
| 1. Average prediction error | Cards: MAE, RMSE, Bias | MAE = mean of \|predicted − actual\|; RMSE penalizes large errors; bias keeps the sign (≈ 0 means no systematic over/under-estimation) |
| 2. Predictions by country | Bar chart | Number of predictions per country (and MAE in the tooltip) |
| 3. Predicted vs actual | Scatter plot | One point per event, colored by absolute error; the closer to the diagonal, the better |
| 4. Trends over time | Two line charts | Average actual vs predicted score by historical year, and MAE by processing time |
| Stream quality | Bar chart + table (page 2) | Events per processing status and the reason for each rejection |

**Requirements to open the `.pbix`:** the 64-bit *SQLite3 ODBC Driver* and a user DSN named `happiness_dw` pointing to `storage/happiness.db`. With the consumer stopped, click Home → Refresh. The same queries are available in plain SQL in `sql/kpis.sql`.

**Connection steps:** install the 64-bit SQLite3 ODBC Driver → open "ODBC Data Sources (64-bit)" → User DSN → Add → *SQLite3 ODBC Driver* → Data Source Name `happiness_dw`, Database Name = full path to `storage/happiness.db`. In Power BI Desktop: Get data → ODBC → `happiness_dw` (no username or password) → load `vw_predictions` and `vw_event_status`.

## 7. How to run

**Requirements:** Docker Desktop running, Python 3.12. Run all commands from the project root.

```powershell
# 1) Python environment (Windows PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
# macOS/Linux: source .venv/bin/activate

# 2) (Optional) regenerate the model: run notebooks/eda.ipynb, then notebooks/model_training.ipynb
#    (jupyter lab notebooks/). models/model.pkl is already included.

# 3) Kafka
docker compose up -d
docker compose ps
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic happiness-events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic happiness-events --bootstrap-server localhost:9092

# 4) Terminal A — consumer (start it first)
python kafka/consumer.py

# 5) Terminal B — producer
python kafka/producer.py --with-invalid        # omit --with-invalid to send only valid events

# 6) Check the consumer group (lag)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group happiness-ml-team

# 7) Stop the consumer with Ctrl+C and open the dashboard in Power BI
```

To start from scratch: delete `storage/happiness.db`, run `docker compose down` and `docker compose up -d` (the topic must be created again).

## 8. Project structure
```
data/{raw,processed,streaming}   notebooks/{eda,model_training}.ipynb   kafka/{producer,consumer,contract,db}.py
models/{model.pkl,features.json,metrics.json}   sql/{create_tables,kpis}.sql
dashboards/powerbi/{measures.dax,workshop3.pbix}
docker-compose.yml   requirements.txt   README.md
```

## 9. Results and limitations

**Model (30 % test split, 235 rows):**

| Model | MAE | RMSE | R² |
|---|---|---|---|
| LinearRegression | 0.419 | 0.546 | 0.765 |
| RandomForestRegressor (selected) | 0.410 | 0.536 | 0.774 |

**Pipeline run (`--with-invalid`):** 241 events processed: 235 `VALID`, 3 `INVALID_SCHEMA`, 2 `INVALID_VALUES` and 1 `DUPLICATE_EVENT`. The 6 rejected events were stored in `raw_happiness_events` with their reason, and the consumer never stopped. The dashboard MAE (0.410) matches the training evaluation because the stream uses exactly the 30 % test split.

**Limitations:**
- The split is random by row: the same country can appear in training (another year) and in testing, so the error may be slightly optimistic.
- The stream is a ~1-minute simulation, so the trend over processing time has few points.
- SQLite allows a single writer at a time: stop the consumer before refreshing Power BI.
- The model is deliberately simple; the goal of the workshop is pipeline integration, not accuracy.
