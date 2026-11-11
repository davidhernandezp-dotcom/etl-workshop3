"""
Producer: lee el dataset historico preparado registro por registro y lo publica
como eventos JSON en el topic `happiness-events` (key = country).

El CSV es una fuente ACOTADA; la pausa entre registros solo simula llegada en el tiempo.

Uso:
  python kafka/producer.py                    # stream del 30% de prueba, 0.2 s entre eventos
  python kafka/producer.py --delay 0.05       # mas rapido
  python kafka/producer.py --with-invalid     # intercala eventos malos para probar la validacion
  python kafka/producer.py --source full      # todo el dataset unificado (781 filas)
"""
import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

BASE_DIR = Path(__file__).resolve().parent.parent
SOURCES = {
    "test": BASE_DIR / "data" / "streaming" / "stream_events.csv",
    "full": BASE_DIR / "data" / "processed" / "happiness_unified.csv",
}
TOPIC = "happiness-events"


def build_event(n, row):
    return {
        "event_id": f"E{n:04d}",
        "country": str(row["country"]),
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row["happiness_score"]),
        "event_time": datetime.now().isoformat(timespec="seconds"),   # cuando el producer emite el evento
    }


def invalid_variants(event):
    """Eventos defectuosos para observar la validacion del consumer (JSON valido != evento valido)."""
    missing = {k: v for k, v in event.items() if k != "gdp"}
    wrong_type = {**event, "health": "alto"}
    negative = {**event, "freedom": -0.5}
    bad_time = {**event, "event_time": "ayer por la tarde"}
    null_country = {**event, "country": ""}
    return [("missing gdp", missing), ("health string", wrong_type), ("freedom negativa", negative),
            ("event_time invalido", bad_time), ("country vacio", null_country)]


def delivery_report(err, msg):
    if err is not None:
        print(f"  !! entrega fallida: {err}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--delay", type=float, default=0.2, help="segundos entre eventos")
    ap.add_argument("--source", choices=SOURCES, default="test")
    ap.add_argument("--limit", type=int, default=None, help="maximo de eventos")
    ap.add_argument("--with-invalid", action="store_true", help="intercalar eventos invalidos")
    args = ap.parse_args()

    df = pd.read_csv(SOURCES[args.source])
    if args.limit:
        df = df.head(args.limit)
    producer = Producer({"bootstrap.servers": args.bootstrap})
    print(f"Publicando {len(df)} eventos en '{TOPIC}' desde {SOURCES[args.source].name}")

    def send(event, label=""):
        producer.produce(TOPIC, key=str(event.get("country") or "UNKNOWN_COUNTRY"),
                         value=json.dumps(event), callback=delivery_report)
        producer.poll(0)
        print(f"Sent{label}: key={event.get('country')!r} id={event.get('event_id')} year={event.get('year')}")
        time.sleep(args.delay)

    bad_injected = 0
    for n, (_, row) in enumerate(df.iterrows(), start=1):
        event = build_event(n, row)
        send(event)
        if args.with_invalid and n % 40 == 0:                 # un evento defectuoso cada 40
            label, bad = invalid_variants(event)[bad_injected % 5]
            bad = {**bad, "event_id": f"BAD{n:04d}"}
            if "event_id" not in bad:
                bad["event_id"] = f"BAD{n:04d}"
            send(bad, f" [INVALIDO: {label}]")
            bad_injected += 1
        if args.with_invalid and n == 60:                     # evento duplicado (mismo event_id)
            send(event, " [DUPLICADO]")

    producer.flush()
    print("Todos los eventos fueron enviados.")


if __name__ == "__main__":
    main()
