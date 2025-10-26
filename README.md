# 20251-IT4931-HUST: Big Data Project 
## Air Quality Monitoring & Forecasting System 

Hệ thống giám sát và dự báo chất lượng không khí thời gian thực cho Việt Nam (tập trung Hà Nội) sử dụng **Kappa Architecture** và công nghệ Big Data.

##  Mục Lục

- [ Tổng Quan](#-tổng-quan)
- [ Nguồn Dữ Liệu](#️-nguồn-dữ-liệu)
- [ Kiến Trúc](#️-kiến-trúc)
- [ Tech Stack](#️-tech-stack)
- [ Machine Learning](#-machine-learning)
- [ Quick Start](#-quick-start)
- [ Dashboard](#-dashboard)
- [ Monitoring & Alerts](#-monitoring--alerts)
- [ Resource Requirements](#-resource-requirements)
- [ Implementation Timeline](#-implementation-timeline)
- [ API Endpoints](#-api-endpoints)

---

##  Tổng Quan

**Mục tiêu:** Xây dựng pipeline end-to-end để:
- Thu thập dữ liệu chất lượng không khí thời gian thực
- Xử lý và phân tích dữ liệu streaming
- Dự báo PM2.5 ngắn hạn (1-3 giờ)
- Phát hiện bất thường cảm biến
- Hiển thị dashboard trực quan với cảnh báo

**Kiến trúc:** `Kafka → Spark Streaming → TimescaleDB/MinIO → FastAPI → Grafana`

**Triển khai:** Hoàn toàn trên Kubernetes (containerized, production-ready)

##  Nguồn Dữ Liệu

### Chính
- **AQICN API** - 67+ trạm VN, 17 trạm Hà Nội, cập nhật mỗi giờ
- **Hoàn toàn MIỄN PHÍ** cho mục đích học thuật

### Phụ
- **OpenAQ** - Dữ liệu mở, cross-validation
- **Open-Meteo** - Dự báo 5 ngày, dữ liệu thời tiết
- Tất cả đều **MIỄN PHÍ**, không cần API key (trừ AQICN)

##  Kiến Trúc

### Tại Sao Chọn Kappa (không phải Lambda)?

| Tiêu Chí | Lambda | Kappa  |
|----------|--------|----------|
| Số pipeline | 2 (Batch + Speed) | 1 (Stream duy nhất) |
| Độ phức tạp | Cao | Thấp |
| Bảo trì | Khó (2 codebase) | Dễ (1 codebase) |
| Tính nhất quán | Khó đảm bảo | Dễ đảm bảo |

**Ưu điểm:** Đơn giản hơn, dễ debug, phù hợp với dữ liệu streaming tự nhiên

### Luồng Xử Lý

```
Data Sources (AQICN, OpenAQ, Weather)
          ↓
   Python Producers
          ↓
   Kafka Topics (4 topics)
          ↓
Spark Structured Streaming
   • Data validation & cleaning
   • Window aggregations (10-min, 1-hour)
   • Joins (sensor + weather)
   • ML (forecast + anomaly detection)
   • Alerting
          ↓
Storage Layer
   • MinIO: Raw data (Parquet)
   • TimescaleDB: Real-time metrics
          ↓
    FastAPI (REST + WebSocket)
          ↓
 Grafana Dashboard + Alerts
```

##  Tech Stack

| Component | Công Nghệ | Lý Do |
|-----------|-----------|-------|
| **Message Queue** | Kafka | Chuẩn công nghiệp, tài liệu phong phú |
| **Stream Processing** | Spark Structured Streaming | Yêu cầu môn học, ecosystem trưởng thành |
| **Time-Series DB** | TimescaleDB | SQL quen thuộc, tối ưu time-series, 20K-80K writes/sec |
| **Object Storage** | MinIO | S3-compatible, đơn giản (45MB binary), Kubernetes-friendly |
| **API** | FastAPI | Python async, WebSocket, tự động tạo docs |
| **Dashboard** | Grafana | Tốt nhất cho time-series, hỗ trợ TimescaleDB |
| **Orchestration** | Kubernetes | Yêu cầu môn học, production-like |

##  Machine Learning

### Models
1. **Random Forest** - Baseline nhanh (R² = 0.90-0.94)
2. **XGBoost**  - Model chính (R² = 0.94-0.97, cân bằng tốt)
3. **LSTM** - Tùy chọn nâng cao (R² = 0.94-0.96, training chậm)

### Features
- **Lag features** (40-50%): pm25_lag_1h, pm25_lag_3h, pm25_lag_24h
- **Rolling stats** (20-30%): rolling mean/std
- **Time features** (15-25%): hour, day_of_week, season
- **Weather** (10-20%): temperature, humidity, wind speed

### Anomaly Detection
- **Isolation Forest** - Nhanh, real-time (90-95% accuracy)
- **LSTM Autoencoder** - Chính xác cao (95-99.5% accuracy)
- **Mục đích:** Phát hiện sensor drift, lỗi thiết bị, vấn đề chất lượng dữ liệu

##  Quick Start

```bash
# 1. Clone repo
git clone https://github.com/<user>/<repo>.git && cd <repo>

# 2. Deploy core services
kubectl apply -f k8s/minio.yaml
helm install kafka bitnami/kafka -f k8s/kafka-values.yaml
kubectl apply -f k8s/timescaledb.yaml

# 3. Run application services
kubectl apply -f k8s/producer-deployment.yaml
kubectl apply -f k8s/spark-master-worker.yaml

# 4. Submit Spark streaming job
spark-submit --packages org.postgresql:postgresql:42.2.14 \
  --conf "spark.sql.streaming.checkpointLocation=s3a://bucket/checkpoints" \
  processing/spark_job.py

# 5. Deploy Grafana dashboard
kubectl apply -f k8s/grafana-deployment.yaml
kubectl port-forward svc/grafana 3000:80
# Visit: http://localhost:3000 (admin/admin)
```

##  Dashboard

### Các Thành Phần Chính
1. **AQI Gauge** - Hiển thị AQI hiện tại với màu EPA chuẩn
2. **Bản đồ địa lý** - Các trạm màu theo mức độ ô nhiễm
3. **Time-series charts** - PM₂.₅, PM₁₀, AQI theo thời gian
4. **Forecast panel** - Dự báo 1-3 giờ với confidence interval
5. **Alert table** - Cảnh báo active và lịch sử
6. **Sensor health** - Trạng thái và anomaly detection

### EPA AQI Color Standards (BẮT BUỘC)

| AQI | Mức | Màu | Hex |
|-----|-----|-----|-----|
| 0-50 | Tốt | Xanh lá | `#00E400` |
| 51-100 | Trung bình | Vàng | `#FFFF00` |
| 101-150 | Không tốt cho nhóm nhạy cảm | Cam | `#FF7E00` |
| 151-200 | Không tốt | Đỏ | `#FF0000` |
| 201-300 | Rất không tốt | Tím | `#99004C` |
| 301-500 | Nguy hại | Nâu đỏ | `#4C0026` |

##  Monitoring & Alerts

### Prometheus Alerts
- **HighPM25**: PM2.5 > 150 µg/m³ trong 5 phút
- **HazardousAQI**: AQI > 300 trong 2 phút
- **SensorOffline**: Không nhận dữ liệu trong 10 phút
- **KafkaConsumerLag**: Độ trễ Kafka consumer cao
- **SparkJobFailed**: Spark job thất bại

### Metrics
- Kafka: lag, throughput, consumer status
- Spark: processing time, records/sec, failures
- Database: query latency, connections
- System: CPU, memory, disk, network

##  Resource Requirements

**Cho 100 sensors (demo/development):**
- **CPU:** ~40 cores
- **RAM:** ~80GB
- **Storage:** ~1TB
- **Cost:** $1,000-2,000/tháng (có thể dùng free credits sinh viên)

**Deployment options:**
- **Development:** Minikube (local, free)
- **Production:** GKE/EKS/AKS với student credits

##  Implementation Timeline

| Phase | Thời Gian | Nội Dung | Output |
|-------|-----------|----------|--------|
| **Phase 1** | Week 1-2 | Kafka + producers | Data vào Kafka |
| **Phase 2** | Week 3-4 | Spark streaming + TimescaleDB | Data processed trong DB |
| **Phase 3** | Week 5-6 | MinIO + ML models | Predictions hoạt động |
| **Phase 4** | Week 7-8 | Grafana + API + alerts | Dashboard hoàn chỉnh |
| **Phase 5** | Week 9-10 | Documentation + demo | Báo cáo & presentation |

##  API Endpoints

| Method | Endpoint | Mô Tả |
|--------|----------|-------|
| GET | `/current/{location}` | Dữ liệu hiện tại |
| GET | `/historical/{location}` | Dữ liệu lịch sử |
| GET | `/forecast/{location}` | Dự báo 1-3 giờ |
| GET | `/alerts` | Cảnh báo active |
| WS | `/ws/realtime` | Live updates |

---
