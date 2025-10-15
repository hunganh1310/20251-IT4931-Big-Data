import json
from kafka import KafkaConsumer

# Cấu hình Kafka Consumer
# Consumer sẽ lắng nghe từ topic 'air-quality-raw'
consumer = KafkaConsumer(
    'air-quality-raw',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', # Bắt đầu đọc từ bản ghi đầu tiên trong topic
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Bắt đầu lắng nghe dữ liệu từ Kafka...")

try:
    # Lặp qua các bản ghi nhận được
    for message in consumer:
        record_value = message.value
        print(f"\nĐã nhận bản ghi mới từ partition {message.partition}, offset {message.offset}:")
        print(json.dumps(record_value, indent=2))
        
except KeyboardInterrupt:
    print("\nConsumer đã bị dừng bởi người dùng.")
finally:
    consumer.close()