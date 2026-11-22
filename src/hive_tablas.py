"""
Fase 5 · Tablas externas en Hive sobre HDFS.
Define el esquema y crea tablas externas que apuntan a los datos curados en HDFS.
"""
from __future__ import annotations

import os

try:
    from pyhive import hive
    HIVE_DISPONIBLE = True
except ImportError:
    HIVE_DISPONIBLE = False


HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
HIVE_USER = os.getenv("HIVE_USER", "root")


def conectar_hive():
    if not HIVE_DISPONIBLE:
        raise ImportError("pyhive no está instalado. Instálalo con: pip install pyhive")
    return hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER)


DDL_TABLA_INDICADORES = """
CREATE EXTERNAL TABLE IF NOT EXISTS indicadores_curados (
    indicador STRING,
    pais STRING,
    valor DOUBLE,
    fecha DATE,
    fuente STRING,
    fecha_curado DATE
)
PARTITIONED BY (anio INT, mes INT, dia INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS PARQUET
LOCATION '/datalake/silver/indicadores'
TBLPROPERTIES ('skip.header.line.count'='1')
"""


def crear_tablas(conn) -> None:
    cursor = conn.cursor()
    cursor.execute(DDL_TABLA_INDICADORES)
    cursor.execute("MSCK REPAIR TABLE indicadores_curados")
    conn.commit()
    print("Tabla Hive 'indicadores_curados' creada/reparada.")


def consultar_ejemplo(conn) -> list:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT indicador, pais, AVG(valor) AS valor_promedio, COUNT(*) AS n
        FROM indicadores_curados
        GROUP BY indicador, pais
        ORDER BY valor_promedio DESC
        LIMIT 10
        """
    )
    return cursor.fetchall()


if __name__ == "__main__":
    if not HIVE_DISPONIBLE:
        print("pyhive no instalado. Saltando tablas Hive.")
    else:
        conn = conectar_hive()
        try:
            crear_tablas(conn)
            filas = consultar_ejemplo(conn)
            print(f"Consulta ejemplo: {len(filas)} filas")
            for fila in filas:
                print(f"  {fila}")
        finally:
            conn.close()
