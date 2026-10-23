"""
Productor de Kafka (basado en el Anexo 3 del curso).
Simula lecturas en tiempo real de 10 estaciones meteorológicas del IDEAM
y las publica en el topic 'sensor_data'.

Ejecución:  python3 kafka_producer.py
"""
import json
import random
import time

from kafka import KafkaProducer

# Estaciones reales del dataset del IDEAM con su temperatura base aproximada (°C)
ESTACIONES = {
    1: ("BOGOTÁ D.C.", 14), 2: ("ANTIOQUIA", 22), 3: ("VALLE DEL CAUCA", 24),
    4: ("ATLÁNTICO", 29), 5: ("BOLÍVAR", 29), 6: ("CUNDINAMARCA", 17),
    7: ("NARIÑO", 13), 8: ("META", 27), 9: ("SANTANDER", 23), 10: ("LA GUAJIRA", 30),
}


def generate_sensor_data():
    sensor_id = random.randint(1, 10)
    departamento, base = ESTACIONES[sensor_id]
    return {
        "sensor_id": sensor_id,
        "departamento": departamento,
        "temperature": round(random.gauss(base, 1.5), 2),
        "humidity": round(random.uniform(40, 90), 2),
        "timestamp": int(time.time()),
    }


producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                         value_serializer=lambda x: json.dumps(x).encode("utf-8"))

while True:
    sensor_data = generate_sensor_data()
    producer.send("sensor_data", value=sensor_data)
    print(f"Sent: {sensor_data}")
    time.sleep(1)
