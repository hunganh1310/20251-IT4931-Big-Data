import os
import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

# --- 1. CẤU HÌNH CÁC HẰNG SỐ ---

# Biến môi trường hoặc thay thế trực tiếp
# Tốt nhất nên lưu API token trong biến môi trường để bảo mật
AQICN_API_TOKEN = os.environ.get("AQICN_API_TOKEN", "c1d76934e85089689f985da727e49fa8529b1861")
AQICN_HANOI_API = f"https://api.waqi.info/feed/hanoi/?token={AQICN_API_TOKEN}"

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'air-quality-raw'

# Khoảng thời gian giữa các lần gọi API (tính bằng giây)
# AQICN cập nhật mỗi giờ, nên 3600 giây là hợp lý
API_CALL_INTERVAL_SECONDS = 3600

# --- 2. HÀM GỌI API VÀ LẤY DỮ LIỆU ---

def get_air_quality_data():
    """
    Gọi API AQICN để lấy dữ liệu chất lượng không khí tại Hà Nội.
    Trả về dữ liệu JSON nếu thành công, ngược lại trả về None.
    """
    print(f"[{datetime.now()}] Đang gọi API từ: {AQICN_HANOI_API}")
    try:
        response = requests.get(AQICN_HANOI_API)
        # Kiểm tra mã trạng thái HTTP
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "ok":
            print(f"[{datetime.now()}] Lấy dữ liệu thành công.")
            return data.get("data")
        else:
            print(f"[{datetime.now()}] Lỗi từ API AQICN: {data.get('data')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] Lỗi khi kết nối tới API: {e}")
        return None

# --- 3. HÀM GỬI DỮ LIỆU TỚI KAFKA ---

def send_data_to_kafka(producer, data):
    """
    Gửi dữ liệu đã được xử lý tới Kafka.
    """
    if data:
        try:
            # Thêm timestamp vào dữ liệu để dễ xử lý về sau
            data['ingestion_timestamp'] = datetime.now().isoformat()
            
            # Gửi dữ liệu dưới dạng JSON đã mã hóa
            future = producer.send(KAFKA_TOPIC, value=data)
            
            # Chờ để đảm bảo dữ liệu đã được gửi thành công
            record_metadata = future.get(timeout=10)
            
            print(f"[{datetime.now()}] Đã gửi bản ghi thành công tới topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
            
            return True
        except Exception as e:
            print(f"[{datetime.now()}] Lỗi khi gửi dữ liệu tới Kafka: {e}")
            return False
    else:
        print(f"[{datetime.now()}] Không có dữ liệu để gửi.")
        return False

# --- 4. CHƯƠNG TRÌNH CHÍNH ---

if __name__ == "__main__":
    producer = None
    try:
        # Khởi tạo Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3
        )

        print("Đã kết nối thành công tới Kafka Producer.")
        
        while True:
            # Lấy dữ liệu từ API
            aqi_data = get_air_quality_data()
            
            # Gửi dữ liệu tới Kafka
            send_data_to_kafka(producer, aqi_data)
            
            print(f"[{datetime.now()}] Đang đợi {API_CALL_INTERVAL_SECONDS} giây để lặp lại...")
            time.sleep(API_CALL_INTERVAL_SECONDS)

    except Exception as e:
        print(f"[{datetime.now()}] Đã xảy ra lỗi không mong muốn, chương trình sẽ thoát: {e}")
    finally:
        if producer:
            producer.close()
            print("Đã đóng kết nối Kafka Producer.")