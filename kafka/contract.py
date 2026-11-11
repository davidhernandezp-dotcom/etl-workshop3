"""
Contrato del evento `happiness-events` (Workshop 3).

Kafka solo transporta el registro; la aplicacion es la responsable de interpretar
y validar los campos de negocio. Las reglas de abajo salen del contrato, no de
condiciones arbitrarias escondidas en el codigo.

| Campo                  | Tipo            | Regla                                              |
|------------------------|-----------------|----------------------------------------------------|
| event_id               | string          | no vacio; unico (trazabilidad)                     |
| country                | string          | no vacio; tambien es la key de Kafka               |
| year                   | integer         | anio historico (2000-2100)                         |
| gdp, family, health,   | number          | finito y >= 0 (compatible con el esquema unificado)|
| freedom, generosity,   |                 |                                                    |
| corruption             |                 |                                                    |
| actual_happiness_score | number          | 0-10; SOLO para evaluar, nunca es feature          |
| event_time             | ISO-8601 string | timestamp parseable                                |

Estados de procesamiento:
  INVALID_SCHEMA  -> JSON ilegible, campo faltante/vacio o tipo de dato incorrecto
  INVALID_VALUES  -> tipo correcto pero valor fuera de las reglas (negativo, NaN, fuera de rango, fecha invalida)
"""
import json
import math
from datetime import datetime

FEATURES_DEFAULT = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
REQUIRED_FIELDS = ["event_id", "country", "year", *FEATURES_DEFAULT, "actual_happiness_score", "event_time"]

VALID = "VALID"
INVALID_SCHEMA = "INVALID_SCHEMA"
INVALID_VALUES = "INVALID_VALUES"
DUPLICATE_EVENT = "DUPLICATE_EVENT"
PREDICTION_ERROR = "PREDICTION_ERROR"

FEATURE_MIN, FEATURE_MAX = 0.0, 5.0     # rango muy holgado: en los datos reales el maximo es ~2.1
SCORE_MIN, SCORE_MAX = 0.0, 10.0
YEAR_MIN, YEAR_MAX = 2000, 2100


def _is_number(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def parse_json(raw_bytes):
    """Decodifica y parsea. Devuelve (evento | None, error | None)."""
    try:
        text = raw_bytes.decode("utf-8") if isinstance(raw_bytes, (bytes, bytearray)) else str(raw_bytes)
        event = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, f"JSON ilegible: {exc}"
    if not isinstance(event, dict):
        return None, "El mensaje es JSON valido pero no es un objeto"
    return event, None


def validate_event(event, features=None):
    """
    Valida un evento contra el contrato.
    Devuelve (status, detalle) con status VALID, INVALID_SCHEMA o INVALID_VALUES.
    Es la UNICA fuente de reglas: el consumer no valida nada por su cuenta.
    """
    features = features or FEATURES_DEFAULT

    # ---- 1. Esquema: campos requeridos, vacios y tipos ----
    for field in REQUIRED_FIELDS:
        if field not in event:
            return INVALID_SCHEMA, f"Campo faltante: {field}"
        if event[field] is None or event[field] == "":
            return INVALID_SCHEMA, f"Campo vacio: {field}"
    # features que exige el modelo (por si el modelo usara mas que el contrato minimo)
    for f in features:
        if f not in event:
            return INVALID_SCHEMA, f"Falta feature requerida por el modelo: {f}"

    for field in ("event_id", "country", "event_time"):
        if not isinstance(event[field], str) or not event[field].strip():
            return INVALID_SCHEMA, f"{field} debe ser un string no vacio"
    if not isinstance(event["year"], int) or isinstance(event["year"], bool):
        return INVALID_SCHEMA, "year debe ser un entero"
    for field in {*FEATURES_DEFAULT, *features, "actual_happiness_score"}:
        if not _is_number(event[field]):
            return INVALID_SCHEMA, f"{field} debe ser numerico (llego {type(event[field]).__name__})"

    # ---- 2. Valores ----
    for field in {*FEATURES_DEFAULT, *features, "actual_happiness_score"}:
        if not math.isfinite(event[field]):
            return INVALID_VALUES, f"{field} no es un numero finito"
    for field in {*FEATURES_DEFAULT, *features}:
        if not FEATURE_MIN <= event[field] <= FEATURE_MAX:
            return INVALID_VALUES, f"{field}={event[field]} fuera del rango [{FEATURE_MIN}, {FEATURE_MAX}]"
    if not SCORE_MIN <= event["actual_happiness_score"] <= SCORE_MAX:
        return INVALID_VALUES, f"actual_happiness_score={event['actual_happiness_score']} fuera de [{SCORE_MIN}, {SCORE_MAX}]"
    if not YEAR_MIN <= event["year"] <= YEAR_MAX:
        return INVALID_VALUES, f"year={event['year']} fuera de [{YEAR_MIN}, {YEAR_MAX}]"
    try:
        datetime.fromisoformat(event["event_time"])
    except ValueError:
        return INVALID_VALUES, f"event_time no es un timestamp ISO-8601: {event['event_time']!r}"

    return VALID, "Evento valido"
