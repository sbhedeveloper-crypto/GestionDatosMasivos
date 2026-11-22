"""
Fase 7 · Tablero en Streamlit.
Lee de la zona de servicio (PostgreSQL vía pg8000) y muestra indicadores económicos.
Ejecutar con:  streamlit run app/streamlit_app.py
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# Configuración de la página
st.set_page_config(
    page_title="Tablero de Indicadores Económicos",
    page_icon="📊",
    layout="wide",
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "indicadores")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline")

CSV_FALLBACK = Path("datos/servicio/indicadores_servicio.csv")


@st.cache_data(ttl=300)
def cargar_datos_desde_postgres() -> pd.DataFrame:
    """Carga los datos desde PostgreSQL (zona de servicio) usando pg8000."""
    import pg8000

    conn = pg8000.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    query = """
        SELECT indicador, pais, valor, fecha, fuente, fecha_curado,
               anio, mes, dia
        FROM indicadores_servicio
        ORDER BY fecha DESC, indicador, pais;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    df["fecha"] = pd.to_datetime(df["fecha"])
    return df


@st.cache_data(ttl=300)
def cargar_datos_desde_csv() -> pd.DataFrame:
    """Carga desde CSV de respaldo si no hay PostgreSQL disponible."""
    if not CSV_FALLBACK.exists():
        return pd.DataFrame()
    df = pd.read_csv(CSV_FALLBACK)
    df["fecha"] = pd.to_datetime(df["fecha"])
    return df


def cargar_datos() -> pd.DataFrame:
    try:
        return cargar_datos_desde_postgres()
    except Exception:
        st.warning("No se pudo conectar a PostgreSQL. Usando CSV de respaldo.")
        return cargar_datos_desde_csv()


# ----------------------------- Encabezado -----------------------------
st.title("📊 Tablero de Indicadores Económicos")
st.markdown(
    "Pipeline de datos masivos: **Ingesta → Kafka → HDFS → PySpark → Hive → PostgreSQL → Streamlit**"
)
st.divider()

# ----------------------------- Carga de datos -----------------------------
with st.sidebar:
    st.divider()
    if st.button("🔄 Refrescar datos", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

df = cargar_datos()

if df.empty:
    st.error("No hay datos disponibles en la zona de servicio.")
    st.stop()

# ----------------------------- Filtros -----------------------------
st.sidebar.header("🔎 Filtros")

indicadores_disponibles = sorted(df["indicador"].unique())
indicador_sel = st.sidebar.multiselect(
    "Indicador(es)",
    options=indicadores_disponibles,
    default=indicadores_disponibles[:3],
)

paises_disponibles = sorted(df["pais"].unique())
pais_sel = st.sidebar.multiselect(
    "País(es)",
    options=paises_disponibles,
    default=paises_disponibles[:5],
)

fecha_min = df["fecha"].min().date()
fecha_max = df["fecha"].max().date()
rango_fecha = st.sidebar.date_input(
    "Rango de fechas",
    value=(fecha_min, fecha_max),
    min_value=fecha_min,
    max_value=fecha_max,
)

# Aplicar filtros
df_filtrado = df[
    (df["indicador"].isin(indicador_sel))
    & (df["pais"].isin(pais_sel))
    & (df["fecha"].dt.date >= rango_fecha[0])
    & (df["fecha"].dt.date <= rango_fecha[1])
]

if df_filtrado.empty:
    st.warning("No hay datos con los filtros seleccionados.")
    st.stop()

# ----------------------------- Métricas clave -----------------------------
st.subheader("📈 Resumen por indicador")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Registros totales", len(df_filtrado))
with col2:
    st.metric("Indicadores", df_filtrado["indicador"].nunique())
with col3:
    st.metric("Países", df_filtrado["pais"].nunique())
with col4:
    fecha_reciente = df_filtrado["fecha"].max().strftime("%d/%m/%Y")
    st.metric("Fecha más reciente", fecha_reciente)

st.divider()

# ----------------------------- Gráfico de series de tiempo -----------------------------
st.subheader("📉 Evolución temporal de indicadores")

for indicador in indicador_sel:
    df_ind = df_filtrado[df_filtrado["indicador"] == indicador].sort_values("fecha")
    if df_ind.empty:
        continue
    fig = px.line(
        df_ind,
        x="fecha",
        y="valor",
        color="pais",
        title=f"{indicador} por país",
        labels={"valor": "Valor", "fecha": "Fecha", "pais": "País"},
        markers=True,
    )
    fig.update_layout(hovermode="x unified", height=350)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ----------------------------- Gráfico de barras (promedio por país) -----------------------------
st.subheader("🏆 Promedio por país (rango seleccionado)")

indicador_barra = st.selectbox(
    "Selecciona indicador para barras",
    options=indicador_sel,
    index=0,
)
df_barra = (
    df_filtrado[df_filtrado["indicador"] == indicador_barra]
    .groupby("pais", as_index=False)["valor"]
    .mean()
    .sort_values("valor", ascending=False)
)
fig_barra = px.bar(
    df_barra,
    x="pais",
    y="valor",
    title=f"Promedio de {indicador_barra} por país",
    labels={"valor": "Valor promedio", "pais": "País"},
    color="valor",
    color_continuous_scale="Blues",
)
fig_barra.update_layout(height=400)
st.plotly_chart(fig_barra, use_container_width=True)

st.divider()

# ----------------------------- Tabla de datos -----------------------------
st.subheader("📋 Datos detallados (zona de servicio)")
st.dataframe(
    df_filtrado.sort_values("fecha", ascending=False),
    use_container_width=True,
    hide_index=True,
    column_config={
        "fecha": st.column_config.DateColumn("Fecha"),
        "valor": st.column_config.NumberColumn("Valor", format="%.4f"),
        "fecha_curado": st.column_config.DateColumn("Curado el"),
    },
)

# ----------------------------- Descarga -----------------------------
st.divider()
st.subheader("⬇️ Descargar datos filtrados")
csv_bytes = df_filtrado.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Descargar CSV",
    data=csv_bytes,
    file_name="indicadores_filtrados.csv",
    mime="text/csv",
)

st.caption(
    "Fuente: zona de servicio (PostgreSQL vía pg8000) alimentada por el pipeline de datos masivos. "
    "Si no hay conexión a PostgreSQL, se usa el CSV de respaldo en datos/servicio/."
)
