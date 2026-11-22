"""
Fase 1 · Fuente, pregunta y arquitectura
Fase 2 · Ingesta a la zona raw
===============================================================================

Fuente: Banco Mundial (World Bank Open Data API v2)
URL: https://api.worldbank.org/v2
Registros: 20,000+ (múltiples países × 10 indicadores × años 1990-2026)
Columna de fecha: sí (campo 'date')
Actualización: anual / periódico (según indicador)

Usar id_registro como clave única (país + indicador + fecha) para evitar
duplicados. Los JSONL se particionan en carpetas por fecha_ingesta=AAAA-MM-DD.
Cada ejecución genera un manifiesto con URL, parámetros y conteo de registros.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
RAW_DIR = Path(os.getenv("RAW_DIR", "datos/raw"))
MANIFIESTO_DIR = Path(os.getenv("MANIFIESTO_DIR", "datos/manifiestos"))
RAW_DIR.mkdir(parents=True, exist_ok=True)
MANIFIESTO_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Fuente: Banco Mundial (World Bank Open Data API v2)
# ---------------------------------------------------------------------------
BANCO_MUNDIAL_URL = "https://api.worldbank.org/v2"

# 10 indicadores con mejor cobertura global (tienen datos para casi todos los países y años)
INDICADORES_BM = {
    "FP.CPI.TOTL.ZG": "inflacion_anual_pct",        # Inflación anual %
    "SL.UEM.TOTL.ZS": "desempleo_pct",              # Desempleo total %
    "NY.GDP.MKTP.KD.ZG": "crecimiento_pib_pct",     # Crecimiento PIB %
    "NY.GDP.PCAP.CD": "pib_per_capita_usd",         # PIB per cápita USD
    "SP.POP.TOTL": "poblacion_total",               # Población total
    "SP.DYN.LE00.IN": "esperanza_vida_anos",        # Esperanza de vida al nacer
    "NE.TRD.GNFS.ZS": "comercio_pct_pib",           # Comercio % PIB
    "PA.NUS.FCRF": "tipo_cambio_oficial",           # Tipo de cambio oficial LCU/USD
    "BX.TRF.PWKR.DT.CD": "remesas_usd",             # Remesas recibidas USD
    "EN.ATM.CO2E.KT": "emisiones_co2_kt",           # Emisiones CO2 kilotoneladas
}

# 60 países para superar 20,000 registros (60 × 10 × 37 años ≈ 22,200 máximo teórico)
PAISES = [
    # América Latina y el Caribe (enfoque regional)
    "COL", "BRA", "MEX", "CHL", "PER", "ARG", "USA", "CAN",
    "ECU", "URY", "PRY", "BOL", "VEN", "CRI", "PAN", "GTM",
    "SLV", "HND", "NIC", "DOM", "JAM", "TTO", "BHS", "BRB",
    "GUY", "SUR", "BLZ",
    # Europa
    "ESP", "FRA", "DEU", "GBR", "ITA", "NLD", "SWE", "NOR",
    # Asia y Pacífico
    "CHN", "IND", "JPN", "KOR", "AUS", "NZL", "MYS", "THA",
    # África y Medio Oriente
    "ZAF", "EGY", "MAR", "NGA", "KEN",
    # Mundial
    "WLD",
]

BM_TAMANO_PAGINA = 1000
BM_FECHA_DESDE = "1990"   # año inicial (37 años de datos)
BM_FECHA_HASTA = str(date.today().year)

PREGUNTA_PRINCIPAL = (
    "¿Cómo han evolucionado la inflación, el desempleo, el crecimiento del PIB, "
    "la población y otros indicadores macroeconómicos en Colombia y países de "
    "referencia durante las últimas tres décadas, y cómo se comparan frente al "
    "promedio mundial?"
)

USUARIOS_INDICADORES = (
    "Analistas financieros, importadores/exportadores, economistas, "
    "estudiantes de economía, inversionistas, funcionarios públicos y "
    "ciudadanos que siguen la evolución macroeconómica de Colombia y la región."
)


def generar_id_registro(registro: dict) -> str:
    """Crea un id único por registro usando país + indicador + fecha."""
    base = f"{registro.get('pais','')}|{registro.get('indicador','')}|{registro.get('fecha','')}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


# Sesión global con reintentos automáticos para la API del Banco Mundial
_retry_strategy = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
_bm_session = requests.Session()
_bm_session.mount("https://", HTTPAdapter(max_retries=_retry_strategy))


def fetch_banco_mundial(
    pais: str,
    indicador_codigo: str,
    pagina: int = 1,
    tamano_pagina: int = BM_TAMANO_PAGINA,
    fecha_desde: str = BM_FECHA_DESDE,
    fecha_hasta: str = BM_FECHA_HASTA,
) -> tuple[list[dict], int | None]:
    """
    Descarga registros paginados de la API del Banco Mundial.
    Devuelve (lista_de_registros, total_paginas_o_None).
    La API devuelve una lista donde el primer elemento es metadatos y el segundo los datos.
    Incluye reintentos automáticos ante errores 429/5xx y timeouts.
    """
    url = (
        f"{BANCO_MUNDIAL_URL}/country/{pais}/indicator/{indicador_codigo}"
    )
    params = {
        "format": "json",
        "per_page": tamano_pagina,
        "page": pagina,
        "date": f"{fecha_desde}:{fecha_hasta}",
    }

    try:
        resp = _bm_session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
        time.sleep(3)
        resp = _bm_session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

    # Estructura World Bank: [metadatos, [registros...]]
    if not isinstance(data, list) or len(data) < 2:
        return [], None

    metadata = data[0]
    registros_raw = data[1] or []

    total_paginas = None
    if metadata and isinstance(metadata, dict):
        total_paginas = metadata.get("pages")

    registros: list[dict] = []
    for r in registros_raw:
        registros.append({
            "pais": r.get("countryiso3code", pais),
            "pais_nombre": r.get("country", {}).get("value", ""),
            "indicador_codigo": indicador_codigo,
            "fecha": str(r.get("date", ""))[:4],  # solo año
            "valor": r.get("value"),
            "unidad": r.get("unit", ""),
            "observacion": r.get("obs_note", ""),
            "decimal": r.get("decimal", 0),
        })

    return registros, total_paginas


def escribir_jsonl_particionado(
    registros: Iterable[dict],
    fecha_ingesta: str,
    nombre_base: str = "indicadores",
) -> tuple[Path, int]:
    """
    Escribe JSONL particionado en carpeta:
    datos/raw/fecha_ingesta=AAAA-MM-DD/nombre_base_parte_N.jsonl
    Devuelve (ruta_archivo, cantidad_registros).
    """
    carpeta = RAW_DIR / f"fecha_ingesta={fecha_ingesta}"
    carpeta.mkdir(parents=True, exist_ok=True)

    partes_existentes = list(carpeta.glob(f"{nombre_base}_parte_*.jsonl"))
    num_parte = len(partes_existentes) + 1
    archivo = carpeta / f"{nombre_base}_parte_{num_parte}.jsonl"

    conteo = 0
    with archivo.open("w", encoding="utf-8") as f:
        for registro in registros:
            registro["_id_registro"] = generar_id_registro(registro)
            registro["_fecha_ingesta"] = fecha_ingesta
            registro["indicador"] = INDICADORES_BM.get(
                registro.get("indicador_codigo", ""),
                registro.get("indicador_codigo", ""),
            )
            registro["fuente"] = "Banco Mundial"
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")
            conteo += 1

    return archivo, conteo


def escribir_manifiesto(
    url: str,
    desde: str,
    hasta: str,
    parametros: dict,
    total_registros: int,
    archivos: list[Path],
    estado: str = "exitoso",
) -> Path:
    """Guarda un manifiesto JSON por ejecución."""
    timestamp = datetime.now().isoformat()
    manifiesto = {
        "fecha_ejecucion": timestamp,
        "url": url,
        "desde": desde,
        "hasta": hasta,
        "parametros": parametros,
        "total_registros": total_registros,
        "archivos_generados": [str(a) for a in archivos],
        "estado": estado,
    }

    nombre_manifiesto = f"manifiesto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    ruta = MANIFIESTO_DIR / nombre_manifiesto
    with ruta.open("w", encoding="utf-8") as f:
        json.dump(manifiesto, f, ensure_ascii=False, indent=2)
    return ruta


def ingest_indicadores(
    fuente: str = "Banco Mundial",
    desde: str | None = None,
    hasta: str | None = None,
) -> dict:
    """
    Orquesta la ingesta de indicadores macroeconómicos del Banco Mundial
    con paginación, particionado por fecha_ingesta y manifiesto de ejecución.
    Descarga 60 países × 10 indicadores = 600 series de tiempo (~20,000+ registros).
    """
    desde = desde or BM_FECHA_DESDE
    hasta = hasta or BM_FECHA_HASTA

    todos_los_registros: list[dict] = []
    total_descargados = 0

    print(f"Descargando indicadores del Banco Mundial ({desde} a {hasta})...")
    print(f"Países: {len(PAISES)}, Indicadores: {len(INDICADORES_BM)}")

    for pais in PAISES:
        for codigo_indicador, nombre_indicador in INDICADORES_BM.items():
            pagina = 1
            while True:
                registros, total_paginas = fetch_banco_mundial(
                    pais=pais,
                    indicador_codigo=codigo_indicador,
                    pagina=pagina,
                    tamano_pagina=BM_TAMANO_PAGINA,
                    fecha_desde=desde,
                    fecha_hasta=hasta,
                )
                if not registros:
                    break

                for r in registros:
                    r["indicador"] = nombre_indicador
                    r["valor"] = (
                        float(r["valor"]) if r.get("valor") not in (None, "") else None
                    )

                todos_los_registros.extend(registros)
                total_descargados += len(registros)

                print(
                    f"  {pais} / {nombre_indicador} / página {pagina}"
                    + (f" / {total_paginas}" if total_paginas else "")
                    + f" → {len(registros)} registros"
                )

                if total_paginas and pagina >= total_paginas:
                    break
                pagina += 1
            time.sleep(0.3)  # espera entre país/indicador para no saturar la API

    # Ordenar por país, indicador y fecha
    todos_los_registros.sort(
        key=lambda r: (
            r.get("pais", ""),
            r.get("indicador", ""),
            r.get("fecha", ""),
        )
    )

    # Escribir particionado por fecha_ingesta
    fecha_ingesta = date.today().isoformat()
    archivos_escritos: list[Path] = []
    total_escritos = 0

    lote = 5000
    for i in range(0, len(todos_los_registros), lote):
        bloque = todos_los_registros[i : i + lote]
        archivo, conteo = escribir_jsonl_particionado(
            bloque, fecha_ingesta, nombre_base="indicadores"
        )
        archivos_escritos.append(archivo)
        total_escritos += conteo

    manifiesto = escribir_manifiesto(
        url=BANCO_MUNDIAL_URL,
        desde=desde,
        hasta=hasta,
        parametros={
            "paises": PAISES,
            "indicadores": list(INDICADORES_BM.keys()),
            "pagina_inicial": 1,
            "tamano_pagina": BM_TAMANO_PAGINA,
        },
        total_registros=total_descargados,
        archivos=archivos_escritos,
    )

    print(f"\nIngesta completada: {total_descargados} registros descargados")
    print(f"Archivos escritos: {len(archivos_escritos)}")
    print(f"Manifiesto: {manifiesto}")

    return {
        "total_descargados": total_descargados,
        "total_escritos": total_escritos,
        "archivos": [str(a) for a in archivos_escritos],
        "manifiesto": str(manifiesto),
    }


if __name__ == "__main__":
    print("=== FASE 1 · Fuente, pregunta y arquitectura ===")
    print(f"Pregunta principal: {PREGUNTA_PRINCIPAL}")
    print(f"Usuarios: {USUARIOS_INDICADORES}")
    print(f"Fuente: {BANCO_MUNDIAL_URL}")
    print(f"Países: {len(PAISES)} ({', '.join(PAISES[:8])}...)")
    print(f"Indicadores: {', '.join(INDICADORES_BM.values())}")
    print()
    print("=== FASE 2 · Ingesta a zona raw ===")
    resumen = ingest_indicadores()
    print(json.dumps(resumen, indent=2, ensure_ascii=False))