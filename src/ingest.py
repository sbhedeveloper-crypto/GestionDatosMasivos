"""
Fase 1 · Ingesta de datos desde fuentes abiertas (API).
Extrae registros y los escribe en la zona raw como JSONL.
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import requests

RAW_DIR = Path(os.getenv("RAW_DIR", "datos/raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)


def fetch_json(url: str, params: dict | None = None, timeout: int = 30) -> list[dict]:
    """Realiza una petición GET y devuelve la lista de resultados."""
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        # Algunas APIs envuelven los resultados bajo una clave
        for key in ("records", "results", "data"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return [data]
    return data if isinstance(data, list) else [data]


def write_jsonl(records: Iterable[dict], filename: str) -> Path:
    """Escribe un iterable de diccionarios en un archivo JSONL (una línea por registro)."""
    out_path = RAW_DIR / filename
    with out_path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return out_path


# ---------------------------------------------------------------------------
# Fuente: datos.gov.co (Socrata / SODA API) — TRM diaria
# Dataset: 32sa-8pi3 (Superintendencia Financiera de Colombia)
# Documentación: https://dev.socrata.com/
# ---------------------------------------------------------------------------
TRM_URL = "https://www.datos.gov.co/resource/32sa-8pi3.json"
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")  # opcional, aumenta rate limit
TRM_TAMANO_PAGINA = 1000


def fetch_socrata(
    url: str,
    desde: str,
    hasta: str,
    pagina: int = 1,
    tamano_pagina: int = TRM_TAMANO_PAGINA,
    token: str | None = None,
) -> list[dict]:
    """
    Descarga registros de la API Socrata de datos.gov.co con paginación.
    Filtra por rango de fechas usando la cláusula $where.
    """
    headers = {}
    if token:
        headers["X-App-Token"] = token

    offset = (pagina - 1) * tamano_pagina
    params = {
        "$where": f"fecha >= '{desde}T00:00:00' AND fecha <= '{hasta}T23:59:59'",
        "$limit": tamano_pagina,
        "$offset": offset,
        "$order": "fecha ASC",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ingest_indicadores(
    fuente: str = "datos.gov.co",
    desde: str | None = None,
    hasta: str | None = None,
) -> Path:
    """
    Orquesta la ingesta de TRM desde datos.gov.co (Socrata).
    - Descarga registros paginados de la API real.
    - Añade metadatos de ingesta (fecha, fuente, fecha_corte).
    - Escribe en datos/raw/<fuente>_<fecha>.jsonl
    """
    desde = desde or (date.today() - timedelta(days=30)).isoformat()
    hasta = hasta or date.today().isoformat()

    todos_los_registros: list[dict] = []
    pagina = 1

    while True:
        registros = fetch_socrata(
            url=TRM_URL,
            desde=desde,
            hasta=hasta,
            pagina=pagina,
            tamano_pagina=TRM_TAMANO_PAGINA,
            token=SOCRATA_APP_TOKEN,
        )
        if not registros:
            break
        todos_los_registros.extend(registros)
        if len(registros) < TRM_TAMANO_PAGINA:
            break
        pagina += 1

    for record in todos_los_registros:
        record["_ingesta"] = {
            "fuente": fuente,
            "fecha_ingesta": date.today().isoformat(),
            "fecha_desde": desde,
            "fecha_hasta": hasta,
        }

    filename = f"{fuente}_{desde}_a_{hasta}.jsonl"
    return write_jsonl(todos_los_registros, filename)


if __name__ == "__main__":
    archivo = ingest_indicadores()
    print(f"Ingesta completada: {archivo}")
