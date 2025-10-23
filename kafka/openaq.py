import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
import os

# --- CẤU HÌNH ---
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'air-quality-raw'  # Dùng chung topic dữ liệu thô
OPENAQ_API = "https://api.openaq.org/v2/measurements"
API_CALL_INTERVAL_SECONDS = 300  # 5 phút một lần (Theo giới hạn 300 requests/5 phút)

# Tham số API: Lấy các bản ghi PM2.5 của Việt Nam
API_PARAMS = {
    'country': 'VN',
    'parameter': 'pm25',
    'limit': 1000, # Lấy tối đa 1000 bản ghi mỗi lần gọi
    'sort': 'desc'
}

def get_openaq_data():
    """Lấy dữ liệu PM2.5 từ OpenAQ API cho Việt Nam."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Đang gọi API OpenAQ...")
    try:
        response = requests.get(OPENAQ_API, params=API_PARAMS)
        response.raise_for_status() 
        data = response.json()
        
        # OpenAQ trả về một mảng kết quả trong trường 'results'
        results = data.get("results", [])
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Lấy thành công {len(results)} bản ghi từ OpenAQ.")
        return results
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Lỗi kết nối OpenAQ API: {e}")
        return []

def send_openaq_data_to_kafka(producer, records):
    """Gửi từng bản ghi OpenAQ tới Kafka."""
    count = 0
    for record in records:
        try:
            # Bổ sung metadata nguồn để dễ dàng xử lý sau này
            record['data_source'] = 'OpenAQ'
            record['ingestion_timestamp'] = datetime.now().isoformat()
            
            producer.send(KAFKA_TOPIC, value=record)
            count += 1
        except Exception as e:
            print(f"  > Lỗi khi gửi bản ghi OpenAQ: {e}")
            
    print(f"  > Đã gửi {count} bản ghi OpenAQ tới Kafka.")

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3
    )
    print("--- OpenAQ Producer đã kết nối tới Kafka. ---")
    
    while True:
        records = get_openaq_data()
        send_openaq_data_to_kafka(producer, records)
        producer.flush()
        
        print(f"Đang đợi {API_CALL_INTERVAL_SECONDS} giây để lặp lại...")
        time.sleep(API_CALL_INTERVAL_SECONDS)