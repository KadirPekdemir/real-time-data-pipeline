from kafka import KafkaConsumer
from pymongo import MongoClient
import json

def connect_mongo(uri="mongodb://localhost:27017/", db_name="music_db", collection_name="music_logs"):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

def main():
    # MongoDB bağlantısı
    collection = connect_mongo()

    # Kafka consumer tanımla
    consumer = KafkaConsumer(
        'music_logs',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='music-mongo-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Kafka’dan mesajlar okunuyor ve MongoDB’ye yazılıyor...")

    for message in consumer:
        data = message.value
        # Mesajı MongoDB'ye kaydet
        collection.insert_one(data)
        print(f"MongoDB’ye kaydedildi: {data}")

if __name__ == "__main__":
    main()
