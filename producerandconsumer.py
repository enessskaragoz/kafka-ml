# Adım 3: Kafka Üretici ve Tüketici Oluşturma

# Kafka'dan gelen verileri işleyerek modeli güncelleyen üretici
from kafka import KafkaConsumer

consumer = KafkaConsumer('data_stream',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    data = message.value
    update_model(data)  # Modeli güncelle
