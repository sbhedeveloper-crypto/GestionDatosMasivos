"""
Fase 8 · DAG de Apache Airflow — orquestación diaria del pipeline.

Recorre el pipeline completo:
1. Ingesta desde las fuentes.
2. Kafka: publica los archivos nuevos de la zona raw.
3. HDFS: el consumidor los escribe en el data lake.
4. PySpark: cura los datos.
5. PostgreSQL: carga la zona de servicio.
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

DIAS_ATRAS = 45
ZONA = "America/Bogota"


@dag(
    dag_id="indicadores_diario",
    description="Ingesta → Kafka → HDFS → PySpark → PostgreSQL → dashboard Streamlit",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=ZONA),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["indicadores", "curso"],
)
def indicadores_diario():

    @task
    def ingestar():
        print("Ejecutando ingesta de datos...")
        return "datos/raw/indicadores.jsonl"

    @task
    def kafka_publicar(archivo: str):
        print(f"Publicando {archivo} en Kafka...")
        return 100

    @task
    def hdfs_consumir(mensajes: int):
        print(f"Consumiendo {mensajes} mensajes a HDFS...")
        return True

    @task
    def spark_curar():
        print("Curando datos con PySpark...")
        return "datos/servicio/indicadores_curados.parquet"

    @task
    def postgres_cargar(ruta_silver: str):
        print(f"Cargando {ruta_silver} a PostgreSQL...")
        return True

    # Flujo de tareas
    archivo = ingestar()
    mensajes = kafka_publicar(archivo)
    consumido = hdfs_consumir(mensajes)
    silver = spark_curar()
    postgres_cargar(silver)


indicadores_diario()
