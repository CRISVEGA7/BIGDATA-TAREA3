# 📊 Tarea 3 - Procesamiento de Datos con Apache Spark

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
