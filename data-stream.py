# Adım 1: Kafka ve Docker ile Veri Akışı Oluşturma

# Docker ile Kafka başlatma adımları (önceki örnekteki gibi)

# Sahte veri üreten Kafka üretici
from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic = 'data_stream'

while True:
    data = {'value': random.randint(1, 100)}
    producer.send(topic, value=data)
    time.sleep(1)
