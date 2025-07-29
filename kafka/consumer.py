from kafka import KafkaConsumer
import json

def main():
    consumer = KafkaConsumer(
        'music_logs',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',  # en eski mesajdan başla
        group_id='music-log-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Kafka consumer başladı, mesajlar dinleniyor...")

    for message in consumer:
        event = message.value
        print(f"Received event: {event}")

if __name__ == "__main__":
    main()
