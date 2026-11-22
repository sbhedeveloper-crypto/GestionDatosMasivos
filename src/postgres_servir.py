"""
Fase 6 · Zona de servicio en PostgreSQL.
Carga los datos curados desde HDFS/Silver a PostgreSQL para consumo por Power BI / Streamlit.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pg8000
from sqlalchemy import create_engine, text

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "indicadores")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline")

SILVER_PARQUET = os.getenv("SILVER_PARQUET", "datos/servicio/indicadores_curados.parquet")


def engine_postgres():
    """
    Crea el motor SQLAlchemy usando pg8000 (driver puro Python, funciona en Windows
    sin necesidad de instalar PostgreSQL local ni pg_config).
    """
    url = (
        f"postgresql+pg8000://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(url)


def conectar_pg8000():
    """
    Devuelve una conexión pg8000 directa (útil para pd.read_sql_query sin SQLAlchemy).
    """
    return pg8000.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def crear_esquema(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS indicadores_servicio (
        indicador VARCHAR(100),
        pais VARCHAR(100),
        valor DOUBLE PRECISION,
        fecha DATE,
        fuente VARCHAR(100),
        fecha_curado DATE,
        anio INT,
        mes INT,
        dia INT,
        PRIMARY KEY (indicador, pais, fecha, fuente)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("Esquema PostgreSQL 'indicadores_servicio' listo.")


def cargar_desde_parquet(engine, ruta_parquet: str) -> int:
    df = pd.read_parquet(ruta_parquet)
    for col in ("anio", "mes", "dia"):
        if col not in df.columns:
            df[col] = None
    df.to_sql("indicadores_servicio", engine, if_exists="append", index=False, method="multi")
    print(f"Cargados {len(df)} registros en PostgreSQL.")
    return len(df)


def consultar_ultimos(engine, limite: int = 50) -> pd.DataFrame:
    """
    Consulta los últimos registros usando un engine SQLAlchemy.
    """
    query = f"""
    SELECT indicador, pais, valor, fecha, fuente, fecha_curado
    FROM indicadores_servicio
    ORDER BY fecha DESC, indicador
    LIMIT {limite};
    """
    return pd.read_sql(query, engine)


def consultar_ultimos_pg8000(limite: int = 50) -> pd.DataFrame:
    """
    Consulta los últimos registros usando pg8000 directamente (sin SQLAlchemy).
    Ideal para pandas en Windows.
    """
    conn = conectar_pg8000()
    try:
        query = f"""
        SELECT indicador, pais, valor, fecha, fuente, fecha_curado
        FROM indicadores_servicio
        ORDER BY fecha DESC, indicador
        LIMIT {limite};
        """
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()


def consultar_agregados_pg8000() -> pd.DataFrame:
    """
    Consulta agregados por indicador, país, año y mes usando pg8000 directamente.
    """
    conn = conectar_pg8000()
    try:
        query = """
        SELECT indicador, pais, anio, mes,
               AVG(valor) AS valor_promedio,
               MIN(valor) AS valor_min,
               MAX(valor) AS valor_max,
               COUNT(*) AS n_registros
        FROM indicadores_servicio
        GROUP BY indicador, pais, anio, mes
        ORDER BY indicador, pais, anio, mes;
        """
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()


if __name__ == "__main__":
    engine = engine_postgres()
    crear_esquema(engine)
    if Path(SILVER_PARQUET).exists():
        cargar_desde_parquet(engine, SILVER_PARQUET)
    df = consultar_ultimos(engine)
    print(df.head())