# Evidencias del proyecto

Esta carpeta contiene las salidas y capturas requeridas por la rúbrica del proyecto:

## Kafka (tópico y consumidor)
- Captura del tópico `indicadores.raw` creado.
- Captura del consumidor leyendo mensajes y escribiéndolos en HDFS.

## HDFS
- Resultado de `hdfs dfs -ls /datalake/bronze/indicadores/` mostrando los archivos JSONL escritos.
- Captura de la interfaz web del NameNode (http://localhost:9870).

## MapReduce vs Spark
- Resultado del conteo de registros por indicador usando RDD (estilo MapReduce).
- Resultado del conteo usando DataFrame API (Spark).
- Comparación de tiempos y cantidad de grupos.

## Idempotencia
- Ejecutar el curado dos veces sobre los mismos datos y verificar que no hay duplicados en la capa silver.

## Streaming tras reiniciar
- Reiniciar Kafka y HDFS, volver a publicar mensajes y verificar que el consumidor los lee correctamente desde el offset guardado.

> Nota: Agregar aquí las capturas de pantalla y archivos de salida generados durante la ejecución del pipeline.
