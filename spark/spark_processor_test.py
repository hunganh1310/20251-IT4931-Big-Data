from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

# --- 1. KHỞI TẠO SPARK SESSION ---
# Phải chỉ định các Kafka packages để Spark có thể kết nối
def create_spark_session():
    return (
        SparkSession.builder.appName("AirQualityProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"  # Thay đổi version theo Spark
            "org.apache.kafka:kafka-clients:3.5.1"               # Thay đổi version theo Kafka
        )
        # Sử dụng local[1] để chạy trên máy đơn giản
        .master("local[2]")
        .getOrCreate()
    )

# --- 2. ĐỊNH NGHĨA SCHEMA ---
# Định nghĩa schema cho dữ liệu AQICN và OpenAQ (chung)
# Vì dữ liệu từ 2 API khác nhau, chúng ta chỉ định các trường chung và quan trọng
raw_schema = StructType([
    # Trường từ Kafka
    StructField("value", StringType()),
    StructField("timestamp", TimestampType()),
    
    # Trường từ AQICN/OpenAQ (sau khi parse JSON)
    StructField("region_city", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("aqi", StringType(), True), # AQI từ AQICN (là string, cần convert)
    StructField("pm25", DoubleType(), True), # Ví dụ PM2.5 từ OpenAQ (là double)
    StructField("data_source", StringType(), True), # Nguồn dữ liệu
])

# --- 3. HÀM XỬ LÝ DỮ LIỆU CHÍNH ---
def process_stream(spark):
    
    # Đọc dữ liệu từ Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "air-quality-raw")
        .option("startingOffsets", "latest") # Bắt đầu đọc từ bản ghi mới nhất
        .load()
    )

    # Chuyển đổi giá trị (value) từ binary sang string
    df_string = raw_stream.selectExpr("CAST(value AS STRING)", "timestamp")
    
    # Phân tích chuỗi JSON thành các cột
    # Dùng hàm from_json với schema đã định nghĩa (Hoặc dùng một schema đơn giản hơn)
    
    # Do 2 Producers gửi dữ liệu có cấu trúc khác nhau, việc parse JSON trực tiếp khó khăn.
    # Chúng ta chỉ chọn các trường cơ bản:
    
    parsed_stream = df_string.withColumn(
        "json_data",
        from_json(col("value"), raw_schema) # Đây là bước phức tạp, cần schema chính xác của TỪNG nguồn
    )
    
    # --- BƯỚC LÀM SẠCH VÀ CHUẨN HÓA CƠ BẢN ---
    cleaned_stream = parsed_stream.select(
        col("timestamp").alias("kafka_timestamp"), # Thời điểm Kafka nhận
        col("json_data.data_source").alias("source"),
        col("json_data.region_city").alias("city"),
        col("json_data.aqi").cast("int").alias("current_aqi") # Chuyển AQI từ string sang int
    ).filter(
        col("current_aqi").isNotNull() # Lọc bỏ bản ghi không có AQI hợp lệ
    )
    
    # --- TÍNH TOÁN (WINDOW AGGREGATION) ---
    # Tính AQI trung bình trên cửa sổ 10 phút (Tumbling Window)
    windowed_avg = cleaned_stream.withWatermark("kafka_timestamp", "1 minute") \
        .groupBy(
            window(col("kafka_timestamp"), "10 minutes"),
            col("city")
        ).agg(
            avg("current_aqi").alias("avg_aqi_10min")
        )

    # --- XUẤT KẾT QUẢ (Sink) ---
    # 1. Xuất kết quả ra console (Dùng cho Demo/Test)
    query_console = windowed_avg.writeStream.outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()
        
    # 2. (Tương lai) Xuất kết quả vào TimescaleDB
    # query_db = windowed_avg.writeStream.foreachBatch(write_to_timescale).start()

    query_console.awaitTermination()

if __name__ == "__main__":
    spark = create_spark_session()
    process_stream(spark)