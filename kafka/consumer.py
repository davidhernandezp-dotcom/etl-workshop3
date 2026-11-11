"""
Consumer (group `happiness-ml-team`): inferencia en tiempo real.

Flujo por mensaje:
  1. guardar el mensaje ORIGINAL en raw_happiness_events (status RECEIVED)
  2. parsear JSON + validar contra el contrato (contract.py)
  3. armar el vector de features con el MISMO orden del entrenamiento (models/features.json)
  4. predecir con models/model.pkl (preprocesamiento + modelo)
  5. guardar en fact_predictions (+ dim_country, dim_date) y marcar el raw como VALID
Los eventos invalidos se guardan en raw con su estado, se omiten de la prediccion y NO tumban el pipeline.

Uso:
  python kafka/consumer.py
  python kafka/consumer.py --max-idle 30     # termina solo tras 30 s sin mensajes
"""
import argparse
import json
import time
from pathlib import Path

import joblib
import pandas as pd

import db
from contract import (DUPLICATE_EVENT, INVALID_SCHEMA, PREDICTION_ERROR, VALID,
                      parse_json, validate_event)

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / "models" / "model.pkl"
FEATURES_PATH = BASE_DIR / "models" / "features.json"
TOPIC = "happiness-events"
GROUP_ID = "happiness-ml-team"


def load_model():
    model = joblib.load(MODEL_PATH)
    features = json.loads(FEATURES_PATH.read_text(encoding="utf-8"))["features"]
    return model, features


def process_message(conn, model, features, topic, partition, offset, key, raw_value):
    """Procesa UN mensaje. Devuelve (status, detalle). Nunca lanza excepciones por datos malos."""
    payload = raw_value.decode("utf-8", errors="replace") if isinstance(raw_value, (bytes, bytearray)) else str(raw_value)

    # 1) RAW primero, antes de cualquier validacion o transformacion
    raw_id = db.insert_raw(conn, topic, partition, offset, key, payload)
    if raw_id is None:
        return "SKIPPED", "mensaje ya almacenado (reentrega)"

    # 2) parseo + contrato
    event, err = parse_json(raw_value)
    if err:
        db.set_status(conn, raw_id, INVALID_SCHEMA, err)
        return INVALID_SCHEMA, err
    status, detail = validate_event(event, features)
    if status != VALID:
        db.set_status(conn, raw_id, status, detail)
        return status, detail
    if db.event_id_already_valid(conn, event["event_id"]):
        detail = f"event_id duplicado: {event['event_id']}"
        db.set_status(conn, raw_id, DUPLICATE_EVENT, detail)
        return DUPLICATE_EVENT, detail

    # 3-4) mismas columnas y orden que en el entrenamiento; actual_happiness_score NO entra
    try:
        X = pd.DataFrame([[event[f] for f in features]], columns=features)
        predicted = float(model.predict(X)[0])
    except Exception as exc:                                   # error del modelo, no del dato
        detail = f"{type(exc).__name__}: {exc}"
        db.set_status(conn, raw_id, PREDICTION_ERROR, detail)
        return PREDICTION_ERROR, detail

    # 5) cargar resultado
    error = db.insert_prediction(conn, raw_id, event, predicted)
    db.set_status(conn, raw_id, VALID, "Evento valido")
    return VALID, f"{event['country']} {event['year']}: real={event['actual_happiness_score']:.3f} " \
                  f"pred={predicted:.3f} error={error:+.3f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--max-idle", type=float, default=None, help="salir tras N s sin mensajes")
    args = ap.parse_args()

    from confluent_kafka import Consumer
    model, features = load_model()
    conn = db.connect()
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    counts, last_msg = {}, time.time()
    print(f"Consumer '{GROUP_ID}' escuchando '{TOPIC}' (Ctrl+C para detener)...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if args.max_idle and time.time() - last_msg > args.max_idle:
                    print("Sin mensajes nuevos, finalizando.")
                    break
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            last_msg = time.time()
            key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
            try:
                status, detail = process_message(conn, model, features, msg.topic(), msg.partition(),
                                                 msg.offset(), key, msg.value())
            except Exception as exc:        # ultima red de seguridad: ningun mensaje debe tumbar el consumer
                status, detail = "UNEXPECTED_ERROR", repr(exc)
            counts[status] = counts.get(status, 0) + 1
            tag = "OK " if status == VALID else "-- "
            print(f"{tag}[{status}] p={msg.partition()} off={msg.offset()} | {detail}")
    except KeyboardInterrupt:
        print("\nConsumer detenido.")
    finally:
        consumer.close()
        conn.close()
        print("Resumen:", counts)


if __name__ == "__main__":
    main()
