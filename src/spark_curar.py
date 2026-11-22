"""
Fase 4 · Procesamiento con PySpark (Silver).
Lee de HDFS (bronze), elimina duplicados, valida esquema y escribe curado (silver).
Compara MapReduce (conteo clásico) vs Spark (DataFrame API).
"""
from __future__ import annotations

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, current_date


def crear_sesion_spark(app_name: str = "Pipeline-Curado") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.dfs.replication", "1")
        .getOrCreate()
    )


def leer_bronze(spark: SparkSession, ruta_hdfs: str):
    """Lee los JSONL de la capa bronze en HDFS."""
    return spark.read.option("multiline", "false").json(ruta_hdfs)


def curar_datos(df):
    """
    Limpieza y curado:
    - Elimina registros nulos en campos clave.
    - Elimina duplicados exactos (idempotencia).
    - Añade columna de fecha de curado.
    """
    df_limpio = df.dropna(subset=["fecha", "valor"])
    df_sin_duplicados = df_limpio.dropDuplicates(["fecha", "indicador", "fuente"])
    df_curados = df_sin_duplicados.withColumn("fecha_curado", current_date())
    return df_curados


def escribir_silver(df, ruta_silver: str, formato: str = "parquet"):
    """Escribe el DataFrame curado en HDFS (capa silver) particionado por fecha."""
    (
        df.write.mode("overwrite")
        .partitionBy("fecha")
        .format(formato)
        .save(ruta_silver)
    )


def comparar_mapreduce_vs_spark(df):
    """
    Demostración didáctica: conteo de registros por indicador.
    - MapReduce style: RDD + reduceByKey.
    - Spark style: DataFrame API + groupBy.
    """
    # --- MapReduce style (RDD) ---
    rdd = df.rdd.map(lambda row: (row["indicador"], 1))
    resultado_mr = rdd.reduceByKey(lambda a, b: a + b).collect()

    # --- Spark style (DataFrame) ---
    resultado_df = (
        df.groupBy("indicador")
        .agg(count("*").alias("total_registros"))
        .orderBy(col("total_registros").desc())
        .collect()
    )

    return resultado_mr, resultado_df


if __name__ == "__main__":
    spark = crear_sesion_spark()
    try:
        ruta_bronze = os.getenv("HDFS_BRONZE_DIR", "/datalake/bronze/indicadores")
        ruta_silver = os.getenv("HDFS_SILVER_DIR", "/datalake/silver/indicadores")

        df_bronze = leer_bronze(spark, ruta_bronze + "/*")
        print(f"Registros leídos de bronze: {df_bronze.count()}")
        df_bronze.show(5, truncate=False)

        df_curados = curar_datos(df_bronzo if False else df_bronze)
        escribir_silver(df_curados, ruta_silver)
        print(f"Registros curados escritos en silver: {df_curados.count()}")

        # Comparación MapReduce vs Spark
        mr, df_res = comparar_mapreduce_vs_spark(df_curados)
        print("\n=== Comparación MapReduce vs Spark ===")
        print(f"MapReduce (RDD): {len(mr)} grupos")
        print(f"Spark (DataFrame): {len(df_res)} grupos")

    finally:
        spark.stop()
