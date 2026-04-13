================================================================================
TAREA 3 - PROCESAMIENTO BATCH CON APACHE SPARK
================================================================================
Curso: Big Data - 202016911
Universidad Nacional Abierta y a Distancia (UNAD)

Descripción:
    Script de procesamiento batch en PySpark que realiza análisis exploratorio
    de datos (EDA) sobre el conjunto histórico SIVIGILA de vigilancia en salud
    pública de Colombia.
    
    Operaciones realizadas:
        1. Carga del archivo CSV con esquema definido.
        2. Limpieza de registros nulos en el campo 'conteo'.
        3. Cálculo del Top 10 de eventos con mayor número de casos acumulados.
        4. Tendencia anual de casos para el evento 'DENGUE'.
        5. Generación de gráficos PNG (barras y líneas).
        6. Exportación de resultados agregados en formato CSV.
    
Autor: CRISTOFHER MATHIAS CALDERON VEGA - GRUPO:34
Fecha: Abril 2026

Instrucciones de ejecución:
    1. Asegúrate de que Apache Spark esté instalado y configurado.
    2. Coloca el archivo CSV de SIVIGILA en el mismo directorio.
    3. Ejecutar: spark-submit batch_analysis_sivigila.py
================================================================================
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import matplotlib.pyplot as plt

# Inicializar Spark
spark = SparkSession.builder \
    .appName("AnalisisBatchSIVIGILA") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=== INICIO DEL PROCESAMIENTO BATCH ===")

# Ruta del archivo CSV (debe estar en el mismo directorio)
data_path = "./Datos_de_Vigilancia_en_Salud_Publica_de_Colombia.csv"

# Definir esquema
schema = StructType([
    StructField("cod_eve", IntegerType(), True),
    StructField("nombre_evento", StringType(), True),
    StructField("semana", IntegerType(), True),
    StructField("ano", IntegerType(), True),
    StructField("cod_dpto_o", IntegerType(), True),
    StructField("cod_mun_o", IntegerType(), True),
    StructField("departamento_ocurrencia", StringType(), True),
    StructField("municipio_ocurrencia", StringType(), True),
    StructField("conteo", IntegerType(), True)
])

# Cargar CSV
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(data_path)

print("Registros totales cargados: ", df_raw.count())

# Limpieza
df_clean = df_raw.na.drop(subset=["conteo"])
df_clean = df_clean.filter(col("nombre_evento").isNotNull() & (col("nombre_evento") != ""))
print("Registros después de limpieza: ", df_clean.count())

# Análisis: Top 10 eventos con más casos
print("\nTop 10 eventos con mayor número de casos:")
top_events_df = df_clean.groupBy("nombre_evento") \
    .agg(_sum("conteo").alias("total_casos")) \
    .orderBy(desc("total_casos")) \
    .limit(10)
top_events_df.show(truncate=False)

# Tendencia anual de DENGUE
evento_especifico = "DENGUE"
print(f"\nTendencia anual de casos para '{evento_especifico}':")
annual_trend_df = df_clean.filter(col("nombre_evento") == evento_especifico) \
    .groupBy("ano") \
    .agg(_sum("conteo").alias("casos_anuales")) \
    .orderBy("ano")
annual_trend_df.show(truncate=False)

# Visualización (guardar gráficos)
top_events_pd = top_events_df.toPandas()
plt.figure(figsize=(10, 6))
plt.barh(top_events_pd['nombre_evento'], top_events_pd['total_casos'], color='skyblue')
plt.xlabel('Total de Casos')
plt.title('Top 10 Eventos de Salud Pública con Mayor Número de Casos')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig('top_10_eventos.png')
print("Gráfico guardado como 'top_10_eventos.png'")

annual_trend_pd = annual_trend_df.toPandas()
plt.figure(figsize=(10, 6))
plt.plot(annual_trend_pd['ano'], annual_trend_pd['casos_anuales'], marker='o', linestyle='-')
plt.xlabel('Año')
plt.ylabel('Número de Casos')
plt.title(f'Tendencia Anual de Casos de {evento_especifico}')
plt.grid(True)
plt.tight_layout()
plt.savefig('tendencia_anual_dengue.png')
print("Gráfico guardado como 'tendencia_anual_dengue.png'")

# Guardar resultados en CSV
output_dir = "./resultados_batch"
os.makedirs(output_dir, exist_ok=True)
top_events_df.write.mode("overwrite").csv(os.path.join(output_dir, "top_eventos.csv"), header=True)
annual_trend_df.write.mode("overwrite").csv(os.path.join(output_dir, "tendencia_dengue.csv"), header=True)

print(f"Resultados guardados en '{output_dir}'")
print("=== FIN DEL PROCESAMIENTO BATCH ===")
spark.stop()
