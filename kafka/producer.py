from kafka import KafkaProducer
import json
import requests
import time
from config import API_KEY

def get_recent_tracks(user='rj'):
    url = f"http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user={user}&api_key={API_KEY}&format=json&limit=5"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get('recenttracks', {}).get('track', [])
    else:
        print("API Error:", response.status_code)
        return []

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    while True:
        tracks = get_recent_tracks()
        print("Tracks:", tracks) 
        for track in tracks:
            event = {
                'artist': track['artist']['#text'],
                'track': track['name'],
                'timestamp': time.time()
            }
            print("Sending:", event)
            producer.send('music_logs', value=event)
            producer.flush()  
        time.sleep(10)

if __name__ == "__main__":
    main()
