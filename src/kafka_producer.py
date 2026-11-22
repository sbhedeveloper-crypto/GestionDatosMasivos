"""
Fase 2 · Productor de Kafka.
Lee los archivos JSONL de la zona raw y publica cada registro en un tópico de Kafka.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

try:
    from kafka import KafkaProducer
    KAFKA_DISPONIBLE = True
except ImportError:
    KAFKA_DISPONIBLE = False


def crear_productor():
    if not KAFKA_DISPONIBLE:
        raise ImportError("kafka-python no está instalado. Instálalo con: pip install kafka-python")
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def publicar_archivo(archivo: Path, productor) -> int:
    """Publica todas las líneas de un JSONL en el tópico de Kafka."""
    enviados = 0
    with archivo.open("r", encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            registro = json.loads(linea)
            clave = str(registro.get("_ingesta", {}).get("fecha_ingesta", "default"))
            futuro = productor.send(os.getenv("TOPICO_RAW", "indicadores.raw"), key=clave, value=registro)
            futuro.get(timeout=10)
            enviados += 1
    productor.flush()
    return enviados


def ejecutar() -> dict:
    """Publica todos los JSONL de la zona raw."""
    if not KAFKA_DISPONIBLE:
        print("kafka-python no instalado. Saltando productor de Kafka.")
        return {}
    productor = crear_productor()
    resumen: dict = {}
    RAW_DIR = Path(os.getenv("RAW_DIR", "datos/raw"))
    for archivo in sorted(RAW_DIR.glob("*.jsonl")):
        n = publicar_archivo(archivo, productor)
        resumen[archivo.name] = n
    productor.close()
    return resumen


if __name__ == "__main__":
    resumen = ejecutar()
    if resumen:
        total = sum(resumen.values())
        print(f"Kafka producer: {total} mensajes publicados en {len(resumen)} archivos.")
        for nombre, n in resumen.items():
            print(f"  - {nombre}: {n} mensajes")
