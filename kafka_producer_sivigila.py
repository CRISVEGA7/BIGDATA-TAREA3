import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'svigila_events'

EVENTOS = [
    "DENGUE", "MALARIA", "CHIKUNGUNYA", "ZIKA", "LEISHMANIASIS",
    "ACCIDENTE OFIDICO", "INTOXICACION POR PLAGUICIDAS", "VIOLENCIA DE GENERO",
    "MORTALIDAD MATERNA", "TUBERCULOSIS"
]
DEPARTAMENTOS = ["ANTIOQUIA", "BOGOTA", "VALLE", "SANTANDER", "CUNDINAMARCA",
                 "ATLANTICO", "BOLIVAR", "NARINO", "MAGDALENA", "TOLIMA"]

def generar_reporte():
    fecha_base = datetime.now() - timedelta(days=random.randint(0, 7))
    semana_epi = fecha_base.isocalendar()[1]
    ano_epi = fecha_base.year
    return {
        "nombre_evento": random.choice(EVENTOS),
        "departamento_ocurrencia": random.choice(DEPARTAMENTOS),
        "semana_epidemiologica": semana_epi,
        "ano": ano_epi,
        "conteo_casos": random.randint(1, 20),
        "timestamp": int(time.time())
    }

print(f"Iniciando productor para topic '{TOPIC_NAME}'...")
try:
    while True:
        reporte = generar_reporte()
        future = producer.send(TOPIC_NAME, value=reporte)
        record_metadata = future.get(timeout=10)
        print(f"Enviado: {reporte} | particion {record_metadata.partition} offset {record_metadata.offset}")
        time.sleep(0.5)
except KeyboardInterrupt:
    print("Productor detenido.")
finally:
    producer.close()
