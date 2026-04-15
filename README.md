# 📊 Tarea 3 - Procesamiento de Datos con Apache Spark
> **Código fuente del procesamiento batch:** [`batch_analysis_sivigila_pro.py`](batch_analysis_sivigila_pro.py)
>
> ![Spark](https://img.shields.io/badge/Apache_Spark-3.5.3-E25A1C?logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.8.0-231F20?logo=apachekafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)
![Status](https://img.shields.io/badge/Status-Entregado-brightgreen)

Repositorio para la **Tarea 3** del curso **Big Data (202016911)** de la Universidad Nacional Abierta y a Distancia (UNAD).

## 📝 Descripción del Proyecto

Este proyecto implementa un pipeline de análisis de datos de vigilancia en salud pública de Colombia (SIVIGILA) utilizando **Apache Spark**:

- **Procesamiento Batch**: Análisis histórico de eventos epidemiológicos para identificar tendencias y patrones.
- **Procesamiento en Tiempo Real**: Simulación de streaming con **Apache Kafka** y **Spark Structured Streaming** para monitoreo continuo.

## 📂 Contenido del Repositorio

| Archivo | Descripción |
| :--- | :--- |
| `batch_analysis_sivigila.py` | Script PySpark para carga, limpieza, análisis exploratorio y visualización de datos históricos de SIVIGILA. |
| `kafka_producer_sivigila.py` | Productor Kafka que simula reportes de eventos de salud en tiempo real. |
| `spark_streaming_consumer_sivigila.py` | Consumidor Spark Structured Streaming que agrega datos en ventanas de 1 minuto. |

## 🛠️ Tecnologías Utilizadas

- Apache Spark 3.5.3 (PySpark)
- Apache Kafka 3.8.0
- Python 3.10
- Librerías: `kafka-python`, `pyspark`, `matplotlib`, `pandas`
- markdown
## 📊 Visualizaciones del Análisis Batch

A continuación se presentan los gráficos generados a partir del procesamiento de datos históricos de SIVIGILA:

| # | Gráfico | Descripción |
|---|---------|-------------|
| 1 | `01_top10_eventos.png` | Top 10 eventos de salud pública con mayor incidencia acumulada (2007-2022). |
| 2 | `02_tendencia_dengue.png` | Tendencia anual de casos de DENGUE en Colombia. |
| 3 | `03_top10_departamentos.png` | Top 10 departamentos con mayor número de casos notificados. |
| 4 | `04_distribucion_semanal_2022.png` | Distribución de casos por semana epidemiológica - Año 2022. |
| 5 | `05_evolucion_top5_eventos.png` | Evolución anual de los 5 eventos con mayor incidencia histórica. |
| 6 | `06_total_casos_por_anio.png` | Volumen total de casos notificados al SIVIGILA por año. |
| 7 | `07_top10_municipios.png` | Top 10 municipios con mayor número de casos notificados. |
| 8 | `08_mapa_calor_dengue.png` | Mapa de calor de casos de DENGUE por semana y año. |
| 9 | `09_estacionalidad_promedio.png` | Estacionalidad promedio semanal con banda de desviación estándar. |
| 10 | `10_comparativa_dengue_vs_evento2.png` | Comparativa anual de DENGUE vs el segundo evento más frecuente. |

## 📈 Resultados Obtenidos

- **Análisis Batch**: Se identificó que el evento **DENGUE** es el de mayor incidencia acumulada, con tendencia creciente en los últimos años.
- **Streaming**: Las agregaciones por ventana permiten visualizar en tiempo real los eventos con mayor número de casos reportados por minuto.

## 🎥 Video Demostrativo

El video explicativo de la ejecución de los scripts se encuentra disponible en el siguiente enlace:

🔗 [Enlace al video en YouTube / Google Drive]

## 👨‍🎓 Autor

- **Nombre:** CRISTOFHER MATHIAS CALDERON VEGA
- **Curso:** BIG DATA - (202016911A_2201)
- **Fecha:** Abril 2026

- ## 🚀 Instrucciones de ejecución

### 1. Clonar el repositorio
```bash
git clone https://github.com/CRISVEGA7/BIGDATA-TAREA3.git
cd BIGDATA-TAREA3

### 2. Procesamiento Batch (Spark)
**Requisitos previos:**
- Apache Spark 3.5.3 instalado.
- Python 3.10 con librerías: `pyspark`, `matplotlib`, `seaborn`, `pandas`.
