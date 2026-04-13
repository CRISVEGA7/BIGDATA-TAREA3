================================================================================
TAREA 3 - PROCESAMIENTO EN TIEMPO REAL - SPARK STREAMING CONSUMIDOR
================================================================================
Curso: Big Data - 202016911
Universidad Nacional Abierta y a Distancia (UNAD)

Descripción:
    Consumidor Spark Structured Streaming que lee mensajes JSON desde el tópico
    Kafka 'svigila_events', parsea los datos y realiza agregaciones en ventanas
    de tiempo.
    
    Procesamiento en tiempo real:
        - Lectura continua desde Kafka.
        - Parseo del JSON según esquema definido.
        - Agregación por ventana de 1 minuto y nombre de evento.
        - Cálculo del total de casos por evento en cada ventana.
        - Salida de resultados en consola cada minuto.
    
Autor: [CRISTOFHER MATHIAS CALDERON VEGA] - Grupo: [34]
Fecha: Abril 2026

Instrucciones de ejecución:
    1. Asegúrate de que ZooKeeper y Kafka estén corriendo.
    2. Verifica que el productor Kafka esté enviando datos.
    3. Ejecutar: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer_sivigila.py
================================================================================
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder \
    .appName("KafkaSparkStreamingSIVIGILA") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("nombre_evento", StringType(), True),
    StructField("departamento_ocurrencia", StringType(), True),
    StructField("semana_epidemiologica", IntegerType(), True),
    StructField("ano", IntegerType(), True),
    StructField("conteo_casos", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "svigila_events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

windowed_stats = parsed_df.groupBy(
    window(col("timestamp").cast("timestamp"), "1 minute"),
    col("nombre_evento")
).agg(_sum("conteo_casos").alias("total_casos_en_ventana"))

query = windowed_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("=== INICIANDO SPARK STREAMING. ESPERANDO DATOS DE KAFKA... ===")
query.awaitTermination()
