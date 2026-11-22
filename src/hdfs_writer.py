"""
Fase 3 · Escritura en HDFS (Data Lake).
Recibe registros de Kafka y los almacena en HDFS como archivos Parquet/JSONL particionados.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

try:
    from hdfs import InsecureClient
    HDFS_DISPONIBLE = True
except ImportError:
    HDFS_DISPONIBLE = False


HDFS_URL = os.getenv("WEBHDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HADOOP_USER_NAME", "root")
HDFS_BRONZE_DIR = os.getenv("HDFS_BRONZE_DIR", "/datalake/bronze/indicadores")


def crear_cliente_hdfs():
    if not HDFS_DISPONIBLE:
        raise ImportError("hdfs no está instalado. Instálalo con: pip install hdfs")
    return InsecureClient(HDFS_URL, user=HDFS_USER)


def escribir_en_hdfs(registros: list, nombre_archivo: str, cliente=None) -> str:
    """
    Escribe una lista de registros en HDFS como JSONL.
    Devuelve la ruta HDFS del archivo escrito.
    """
    if cliente is None:
        cliente = crear_cliente_hdfs()
    ruta_hdfs = f"{HDFS_BRONZE_DIR}/{nombre_archivo}"
    contenido = "\n".join(json.dumps(r, ensure_ascii=False) for r in registros)
    with cliente.write(ruta_hdfs, encoding="utf-8", overwrite=True) as writer:
        writer.write(contenido)
    return ruta_hdfs


def listar_bronze(cliente=None) -> list:
    if cliente is None:
        cliente = crear_cliente_hdfs()
    return cliente.list(HDFS_BRONZE_DIR, status=True)


if __name__ == "__main__":
    if not HDFS_DISPONIBLE:
        print("hdfs no instalado. Saltando escritura en HDFS.")
    else:
        cliente = crear_cliente_hdfs()
        try:
            archivos = cliente.list(HDFS_BRONZE_DIR)
            print(f"Archivos en HDFS bronze ({HDFS_BRONZE_DIR}):")
            for a in archivos:
                print(f"  - {a}")
        except Exception as exc:
            print(f"No se pudo listar HDFS: {exc}")
