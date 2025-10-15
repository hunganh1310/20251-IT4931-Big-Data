import json
import time
from kafka import KafkaProducer
from datetime import datetime

# Cấu hình Kafka Producer
# Dùng localhost vì đã ánh xạ cổng 9092 qua Docker Compose
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Tên topic Kafka
topic_name = 'air-quality-raw'

def generate_air_quality_data():
    """Tạo dữ liệu demo về chất lượng không khí"""
    data = {
        'location_id': 'hanoi_station_01',
        'timestamp': datetime.now().isoformat(),
        'pm25': 55.7,
        'pm10': 80.2,
        'co': 1.5,
        'o3': 45.1,
        'aqi': 120,
        'status': 'Unhealthy for Sensitive Groups'
    }
    return data

print(f"Bắt đầu gửi dữ liệu tới topic '{topic_name}'...")
try:
    for i in range(5): # Gửi 5 bản ghi demo
        demo_data = generate_air_quality_data()
        future = producer.send(topic_name, value=demo_data)
        
        # Chờ bản ghi được gửi thành công
        record_metadata = future.get(timeout=10)
        print(f"Đã gửi bản ghi thành công đến topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
        
        time.sleep(2) # Đợi 2 giây trước khi gửi bản ghi tiếp theo

    producer.flush() # Đảm bảo tất cả các bản ghi đã được gửi đi
    print("\nQuá trình gửi dữ liệu demo hoàn tất.")

except Exception as e:
    print(f"Đã xảy ra lỗi: {e}")
finally:
    producer.close()