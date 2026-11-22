"""
Fase 7 · Generación de tablero estático (HTML) o datos para Streamlit.
Exporta consultas agregadas desde PostgreSQL a CSV/JSON para el tablero.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from postgres_servir import engine_postgres, consultar_ultimos

SALIDA_DIR = Path(os.getenv("SALIDA_DIR", "datos/servicio"))
SALIDA_DIR.mkdir(parents=True, exist_ok=True)


def exportar_csv(engine) -> Path:
    df = consultar_ultimos(engine, limite=1000)
    ruta = SALIDA_DIR / "indicadores_servicio.csv"
    df.to_csv(ruta, index=False, encoding="utf-8")
    print(f"Exportado CSV: {ruta} ({len(df)} filas)")
    return ruta


def exportar_json_agregado(engine) -> Path:
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
    df = pd.read_sql(query, engine)
    ruta = SALIDA_DIR / "indicadores_agregados.json"
    df.to_json(ruta, orient="records", force_ascii=False, date_format="iso")
    print(f"Exportado JSON agregado: {ruta} ({len(df)} filas)")
    return ruta


if __name__ == "__main__":
    engine = engine_postgres()
    exportar_csv(engine)
    exportar_json_agregado(engine)
