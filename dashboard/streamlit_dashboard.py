import streamlit as st
from pymongo import MongoClient
import pandas as pd
import time

# MongoDB bağlantısı
def get_mongo_collection():
    client = MongoClient("mongodb://localhost:27017/")
    db = client.music_db
    collection = db.music_logs
    return collection

# MongoDB’den veri çek
def fetch_data(collection):
    cursor = collection.find().sort('_id', -1).limit(100)  # Son 100 kayıt
    data = list(cursor)
    if data:
        df = pd.DataFrame(data)
        # _id sütununu string yapalım
        df['_id'] = df['_id'].astype(str)
        return df
    else:
        return pd.DataFrame()

def main():
    st.title("🎵 Gerçek Zamanlı Müzik Dinleme Dashboard")

    collection = get_mongo_collection()

    placeholder = st.empty()

    while True:
        df = fetch_data(collection)
        with placeholder.container():
            st.write(f"Son güncellenme: {pd.Timestamp.now()}")
            if not df.empty:
                st.dataframe(df)
                if 'artist' in df.columns:
                    artist_counts = df['artist'].value_counts()
                    st.bar_chart(artist_counts)
                else:
                    st.write("Veride 'artist' sütunu bulunamadı, grafik gösterilemiyor.")
            else:
                st.write("Henüz veri yok.")
        time.sleep(5)  # 5 saniyede bir yenile

if __name__ == "__main__":
    main()
