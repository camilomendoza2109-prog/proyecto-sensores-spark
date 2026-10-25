import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Conexión con el servidor Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "sensor_data"

print("Productor de sensores iniciado. Presiona Ctrl+C para detenerlo.")

try:
    while True:
        data = {
            "sensor_id": random.randint(1, 5),
            "temperature": round(random.uniform(18.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 80.0), 2),
            "timestamp": datetime.now().isoformat()
        }

        producer.send(topic, value=data)
        print(f"Enviado: {data}")

        time.sleep(1)

except KeyboardInterrupt:
    print("\nProductor detenido.")

finally:
    producer.flush()
    producer.close()
