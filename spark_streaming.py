from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir la estructura de los mensajes JSON
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Leer datos en tiempo real desde Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir el contenido JSON
json_stream = kafka_stream.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Convertir timestamp a tipo temporal de Spark
sensor_data = json_stream \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .filter(col("event_time").isNotNull())

# Calcular promedios por sensor en ventanas de un minuto
aggregated_stream = sensor_data \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

# Mostrar resultados en la consola
query = aggregated_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
