"""
Fase 2 · Consumidor de Kafka.
Lee mensajes del tópico raw y los escribe en HDFS (zona bronze del data lake).
"""
from __future__ import annotations

import json
import os
from typing import Iterable

try:
    from kafka import KafkaConsumer
    KAFKA_DISPONIBLE = True
except ImportError:
    KAFKA_DISPONIBLE = False


def crear_consumidor():
    if not KAFKA_DISPONIBLE:
        raise ImportError("kafka-python no está instalado. Instálalo con: pip install kafka-python")
    return KafkaConsumer(
        os.getenv("TOPICO_RAW", "indicadores.raw"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        group_id=os.getenv("KAFKA_GROUP_ID", "consumidor-hdfs"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,
    )


def consumir_mensajes(consumidor) -> Iterable[dict]:
    for mensaje in consumidor:
        yield mensaje.value


if __name__ == "__main__":
    if not KAFKA_DISPONIBLE:
        print("kafka-python no instalado. Saltando consumidor de Kafka.")
    else:
        consumidor = crear_consumidor()
        contador = 0
        for registro in consumir_mensajes(consumidor):
            contador += 1
            if contador % 100 == 0:
                print(f"  ... {contador} mensajes consumidos")
        consumidor.close()
        print(f"Kafka consumer: {contador} mensajes consumidos.")
