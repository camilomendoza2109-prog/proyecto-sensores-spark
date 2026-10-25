from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, min, max, round,
    to_timestamp, hour
)
import os

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisBatchSensores") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Rutas de entrada y salida
archivo_entrada = "/tmp/datos_sensores/dataset_sensores.csv"
carpeta_salida = "/home/hadoop/resultados_batch"

print("\n========== 1. CARGA DE DATOS ==========")

# Cargar el conjunto de datos
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(archivo_entrada)

print("Esquema original:")
df.printSchema()

total_original = df.count()
print(f"Total de registros originales: {total_original}")

print("\nPrimeros cinco registros:")
df.show(5, truncate=False)

print("\n========== 2. LIMPIEZA DE DATOS ==========")

# Convertir las columnas a los tipos apropiados
df = df.withColumn("sensor_id", col("sensor_id").cast("integer")) \
       .withColumn("temperature", col("temperature").cast("double")) \
       .withColumn("humidity", col("humidity").cast("double")) \
       .withColumn("timestamp", to_timestamp(col("timestamp")))

# Contar valores faltantes antes de limpiar
print("Valores faltantes por columna:")
df.select([
    count(columna).alias(columna)
    for columna in df.columns
]).show()

# Eliminar filas con valores nulos
df_sin_nulos = df.dropna(
    subset=["sensor_id", "temperature", "humidity", "timestamp"]
)

# Conservar mediciones dentro de los rangos definidos para el ejercicio
df_limpio = df_sin_nulos.filter(
    (col("temperature") >= 0) &
    (col("temperature") <= 50) &
    (col("humidity") >= 0) &
    (col("humidity") <= 100)
)

total_limpio = df_limpio.count()
eliminados = total_original - total_limpio

print(f"Registros originales: {total_original}")
print(f"Registros después de la limpieza: {total_limpio}")
print(f"Registros descartados: {eliminados}")

print("\n========== 3. ANÁLISIS EXPLORATORIO ==========")

print("Estadísticas generales:")
df_limpio.select(
    round(avg("temperature"), 2).alias("temperatura_promedio"),
    round(min("temperature"), 2).alias("temperatura_minima"),
    round(max("temperature"), 2).alias("temperatura_maxima"),
    round(avg("humidity"), 2).alias("humedad_promedio"),
    round(min("humidity"), 2).alias("humedad_minima"),
    round(max("humidity"), 2).alias("humedad_maxima")
).show(truncate=False)

print("Cantidad de mediciones por sensor:")
df_limpio.groupBy("sensor_id") \
    .agg(count("*").alias("cantidad_mediciones")) \
    .orderBy("sensor_id") \
    .show()

print("Promedios de temperatura y humedad por sensor:")
resumen_sensor = df_limpio.groupBy("sensor_id").agg(
    count("*").alias("cantidad_mediciones"),
    round(avg("temperature"), 2).alias("temperatura_promedio"),
    round(avg("humidity"), 2).alias("humedad_promedio")
).orderBy("sensor_id")

resumen_sensor.show(truncate=False)

print("Promedios por hora:")
resumen_hora = df_limpio.withColumn(
    "hora", hour("timestamp")
).groupBy("hora").agg(
    round(avg("temperature"), 2).alias("temperatura_promedio"),
    round(avg("humidity"), 2).alias("humedad_promedio"),
    count("*").alias("cantidad_mediciones")
).orderBy("hora")

resumen_hora.show(24, truncate=False)

print("\n========== 4. GUARDAR RESULTADOS ==========")

# Guardar los resultados en formato CSV
df_limpio.write.mode("overwrite") \
    .option("header", True) \
    .csv(carpeta_salida + "/datos_limpios")

resumen_sensor.write.mode("overwrite") \
    .option("header", True) \
    .csv(carpeta_salida + "/resumen_por_sensor")

resumen_hora.write.mode("overwrite") \
    .option("header", True) \
    .csv(carpeta_salida + "/resumen_por_hora")

print(f"Resultados guardados en: {carpeta_salida}")
print("Procesamiento por lotes finalizado correctamente.")

spark.stop()
