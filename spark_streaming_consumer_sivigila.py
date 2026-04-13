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
