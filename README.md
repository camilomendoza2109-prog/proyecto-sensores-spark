# Análisis de datos ambientales con Apache Spark y Apache Kafka

## 1. Descripción del proyecto

Este proyecto implementa un sistema de análisis de datos ambientales utilizando Apache Spark y Apache Kafka. Se simulan mediciones de temperatura y humedad provenientes de cinco sensores, con el propósito de estudiar el procesamiento de datos por lotes y en tiempo real.

El conjunto de datos utilizado para el procesamiento por lotes contiene 100.000 registros simulados. Para el procesamiento en tiempo real, se generan eventos de sensores y se envían al tópico `sensor_data` de Kafka.

**Nota:** los datos son simulados con fines académicos; no corresponden a mediciones ambientales reales.

## 2. Objetivo general

Implementar técnicas de procesamiento y análisis de grandes volúmenes de datos mediante Apache Spark y Apache Kafka, aplicadas a mediciones ambientales simuladas de temperatura y humedad.

## 3. Tecnologías utilizadas

- Python.
- Apache Spark 3.5.6.
- Apache Kafka 3.9.2.
- Spark Structured Streaming.
- Java 11.
- Pandas y Matplotlib para el análisis y la visualización de resultados.

## 4. Estructura del proyecto

- `dataset_sensores.csv`: conjunto de datos simulado para el procesamiento por lotes.
- `spark_batch.py`: limpieza, transformación y análisis de los datos mediante Spark.
- `productor.py`: genera eventos simulados y los envía a Kafka.
- `spark_streaming.py`: consume y procesa los eventos mediante Spark Structured Streaming.
- `resultados/resumen_por_sensor.csv`: resumen de mediciones agrupadas por sensor.
- `resultados/resumen_por_hora.csv`: resumen de mediciones agrupadas por hora.
- `resultados/temperatura_por_sensor.png`: gráfica de temperatura promedio por sensor.
- `resultados/humedad_por_sensor.png`: gráfica de humedad promedio por sensor.

## 5. Procesamiento por lotes (Batch)

El procesamiento por lotes carga el archivo CSV, revisa la estructura de los datos, elimina registros que no cumplen los criterios de calidad y calcula estadísticas descriptivas.

En la ejecución realizada se obtuvieron los siguientes resultados:

- Registros iniciales: 100.000.
- Registros conservados después de la limpieza: 97.446.
- Registros descartados: 2.554.
- Temperatura promedio: aproximadamente 26,49 °C.
- Humedad promedio: aproximadamente 54,97 %.

Los resultados se agrupan por sensor y por hora, y se representan mediante gráficas.

## 6. Procesamiento en tiempo real

El productor genera eventos simulados con los campos `sensor_id`, `temperature`, `humidity` y `timestamp`, y los publica en el tópico `sensor_data` de Kafka.

Spark Structured Streaming consume los mensajes, interpreta sus campos y calcula promedios de temperatura y humedad por sensor utilizando ventanas de tiempo de un minuto. Los resultados se muestran en la terminal.

## 7. Requisitos previos

Para ejecutar el proyecto se requiere:

- Java 11.
- Apache Spark 3.5.6.
- Apache Kafka 3.9.2.
- Python 3.
- El paquete `kafka-python` para el productor.
- El conector de Kafka para Spark, compatible con Spark 3.5.6 y Scala 2.12.

Las rutas de instalación y los comandos pueden necesitar ajustes según el equipo donde se ejecute el proyecto.

## 8. Ejecución

### Procesamiento por lotes

Ajustar la ruta del conjunto de datos dentro del script y ejecutar:

```bash
spark-submit spark_batch.py
```

### Procesamiento en tiempo real

1. Iniciar el servidor de Kafka en modo KRaft.
2. Crear el tópico `sensor_data` si todavía no existe.
3. Activar el entorno virtual de Python que contiene `kafka-python`.
4. Ejecutar el productor:

```bash
python productor.py
```

5. En otra terminal, ejecutar Spark Streaming:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 spark_streaming.py
```

El productor y Spark Streaming deben ejecutarse mientras el servidor de Kafka permanece activo.

## 9. Conclusión

El proyecto permite comparar el procesamiento por lotes con el procesamiento en tiempo real. Apache Spark facilita la transformación y el análisis de los datos, mientras que Apache Kafka permite transportar los eventos generados por los sensores simulados. La visualización de resultados ayuda a interpretar los promedios de temperatura y humedad por sensor.
