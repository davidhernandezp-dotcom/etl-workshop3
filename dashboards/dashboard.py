"""
Dashboard de KPIs (Streamlit). Consulta la BASE DE DATOS SQLite (storage/happiness.db), NO archivos CSV.
Ejecutar desde la raiz del proyecto:  streamlit run dashboards/dashboard.py
"""
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).resolve().parent.parent / "storage" / "happiness.db"

st.set_page_config(page_title="World Happiness - Streaming ML", layout="wide")
st.title("World Happiness — Predicciones en tiempo real (Kafka + ML)")


def query(sql):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn)


if not DB_PATH.exists():
    st.error("No existe storage/happiness.db. Ejecuta primero el consumer y el producer.")
    st.stop()

if st.button("Actualizar datos"):
    st.rerun()

pred = query("""
    SELECT f.prediction_id, c.country_name AS country, f.observation_year AS year,
           f.actual_score, f.predicted_score, f.prediction_error, f.abs_error,
           f.event_time, f.prediction_timestamp
    FROM fact_predictions f JOIN dim_country c ON c.country_id = f.country_id
    ORDER BY f.prediction_id""")
status = query("SELECT processing_status AS estado, COUNT(*) AS eventos FROM raw_happiness_events GROUP BY 1")

if pred.empty:
    st.warning("Aun no hay predicciones en la base de datos.")
    st.stop()

# ---- KPI 1: error promedio ----
mae = pred["abs_error"].mean()
rmse = (pred["prediction_error"] ** 2).mean() ** 0.5
c1, c2, c3, c4 = st.columns(4)
c1.metric("Predicciones", f"{len(pred):,}")
c2.metric("Error absoluto promedio (MAE)", f"{mae:.3f}")
c3.metric("RMSE", f"{rmse:.3f}")
c4.metric("Sesgo promedio (pred - real)", f"{pred['prediction_error'].mean():+.3f}")
st.caption("KPI 1 · MAE = promedio de |predicho − real|. El sesgo conserva el signo: cercano a 0 indica que el modelo no sobre/subestima sistemáticamente.")

st.divider()
left, right = st.columns(2)

# ---- KPI 2: predicciones por pais ----
with left:
    st.subheader("KPI 2 · Predicciones por país (top 20)")
    by_country = pred.groupby("country").agg(predicciones=("prediction_id", "count"), mae=("abs_error", "mean")) \
                     .sort_values(["predicciones", "mae"], ascending=[False, False]).head(20)
    st.bar_chart(by_country["predicciones"])
    st.dataframe(by_country.round(3), use_container_width=True)

# ---- KPI 3: predicho vs real ----
with right:
    st.subheader("KPI 3 · Predicho vs real")
    st.scatter_chart(pred, x="actual_score", y="predicted_score")
    st.caption("Cuanto más cerca de la diagonal y = x, mejor la predicción.")

st.divider()
# ---- KPI 4: tendencias ----
t1, t2 = st.columns(2)
with t1:
    st.subheader("KPI 4a · Tendencia por año histórico")
    by_year = pred.groupby("year").agg(real=("actual_score", "mean"), predicho=("predicted_score", "mean"))
    st.line_chart(by_year)
with t2:
    st.subheader("KPI 4b · Error en el tiempo de procesamiento")
    pred["minuto"] = pred["prediction_timestamp"].str.slice(0, 16)
    by_min = pred.groupby("minuto").agg(mae=("abs_error", "mean"))
    st.line_chart(by_min)

st.divider()
st.subheader("Calidad del stream · eventos por estado")
st.dataframe(status, use_container_width=True)
rejected = query("""SELECT raw_event_id, processing_status AS estado, status_detail AS motivo
                    FROM raw_happiness_events WHERE processing_status NOT IN ('VALID','RECEIVED') ORDER BY raw_event_id""")
if not rejected.empty:
    st.write("Eventos rechazados (guardados en la tabla raw, omitidos de la predicción):")
    st.dataframe(rejected, use_container_width=True)
