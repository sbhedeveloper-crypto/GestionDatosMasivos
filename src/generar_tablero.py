"""
Fase 7 · Generación de datos para el tablero Streamlit.
Lee los JSONL de la zona raw (descargados de datos.gov.co) y genera el CSV
de servicio que lee el tablero. No requiere PostgreSQL ni conexión remota.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

RAW_DIR = Path(os.getenv("RAW_DIR", "datos/raw"))
SALIDA_DIR = Path(os.getenv("SALIDA_DIR", "datos/servicio"))
SALIDA_DIR.mkdir(parents=True, exist_ok=True)


def _leer_todos_los_jsonl() -> list[dict]:
    """Lee todos los archivos JSONL de datos/raw/ (cualquier subcarpeta)."""
    registros: list[dict] = []
    jsonl_files = sorted(RAW_DIR.rglob("*.jsonl"))
    if not jsonl_files:
        print(f"⚠️  No se encontraron archivos JSONL en {RAW_DIR}")
        return registros

    for archivo in jsonl_files:
        with archivo.open("r", encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if not linea:
                    continue
                try:
                    registro = json.loads(linea)
                    registros.append(registro)
                except json.JSONDecodeError:
                    continue
    return registros


def _normalizar_dataframe(registros: list[dict]) -> pd.DataFrame:
    """Normaliza los registros a un DataFrame con las columnas esperadas por el tablero."""
    if not registros:
        return pd.DataFrame(
            columns=[
                "indicador", "pais", "valor", "fecha", "fuente",
                "fecha_curado", "anio", "mes", "dia",
            ]
        )

    filas = []
    for r in registros:
        filas.append(
            {
                "indicador": r.get("indicador", ""),
                "pais": r.get("pais", ""),
                "valor": r.get("valor", None),
                "fecha": r.get("fecha", ""),
                "fuente": r.get("fuente", ""),
                "fecha_curado": r.get("_fecha_ingesta", ""),
                "anio": r.get("anio", None),
                "mes": r.get("mes", None),
                "dia": r.get("dia", None),
            }
        )

    df = pd.DataFrame(filas)
    if "fecha" in df.columns and not df.empty:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df


def generar_csv() -> Path:
    """Genera el CSV de servicio a partir de los JSONL de la zona raw."""
    registros = _leer_todos_los_jsonl()
    df = _normalizar_dataframe(registros)

    if df.empty:
        print("⚠️  No hay datos para escribir en el CSV.")
        ruta = SALIDA_DIR / "indicadores_servicio.csv"
        pd.DataFrame().to_csv(ruta, index=False, encoding="utf-8")
        return ruta

    ruta = SALIDA_DIR / "indicadores_servicio.csv"
    df.to_csv(ruta, index=False, encoding="utf-8")
    print(f"✅ CSV generado: {ruta} ({len(df)} filas)")
    return ruta


def generar_json_agregado() -> Path:
    """Genera un JSON con agregados por indicador, país, año y mes."""
    registros = _leer_todos_los_jsonl()
    df = _normalizar_dataframe(registros)

    if df.empty or "valor" not in df.columns:
        ruta = SALIDA_DIR / "indicadores_agregados.json"
        pd.DataFrame().to_json(ruta, orient="records", force_ascii=False)
        return ruta

    agg = (
        df.dropna(subset=["valor"])
        .groupby(["indicador", "pais", "anio", "mes"], dropna=False)["valor"]
        .agg(["mean", "min", "max", "count"])
        .reset_index()
        .rename(
            columns={
                "mean": "valor_promedio",
                "min": "valor_min",
                "max": "valor_max",
                "count": "n_registros",
            }
        )
    )

    ruta = SALIDA_DIR / "indicadores_agregados.json"
    agg.to_json(ruta, orient="records", force_ascii=False, date_format="iso")
    print(f"✅ JSON agregado generado: {ruta} ({len(agg)} filas)")
    return ruta


if __name__ == "__main__":
    generar_csv()
    generar_json_agregado()