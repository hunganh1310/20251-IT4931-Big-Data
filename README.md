# Air Quality Monitoring & Forecasting System
## Vietnam / Hanoi - Big Data Project

---

### HUST_IT4931
**Đại học Bách Khoa Hà Nội**  
**Môn học:** Lưu trữ và xử lý dữ liệu lớn  
**Nhóm:** 

---

## Tổng Quan Dự Án

Hệ thống giám sát và dự báo chất lượng không khí thời gian thực cho **Hà Nội** sử dụng **Kappa Architecture** và công nghệ Big Data.

### Mục Tiêu
- Thu thập dữ liệu chất lượng không khí tại Việt Nam (Hà Nội)
- Xử lý và phân tích dữ liệu streaming real-time
- Dự báo PM2.5 ngắn hạn (1-3 giờ) sử dụng Machine Learning
- Phát hiện bất thường cảm biến và cảnh báo sớm
- Hiển thị dashboard trực quan với EPA color standards

### Pipeline Tổng Quan
```
AQICN API → Kafka → Spark Streaming → TimescaleDB/MinIO → FastAPI → Grafana
```

---

## Nguồn Dữ Liệu

- **AQICN API** (chính): 67+ trạm VN, cập nhật mỗi giờ 
- **OpenAQ**: Cross-validation 
- **Open-Meteo**: Weather & forecast data 

---

## Kiến Trúc Hệ Thống

### Kappa Architecture

**Tại sao chọn Kappa?**
- Đơn giản hơn Lambda (1 pipeline thay vì 2)
- Dễ bảo trì (1 codebase)
- Nhất quán dữ liệu
- Phù hợp với streaming data

### Luồng Xử Lý

```
┌─────────────────────┐
│   Data Sources      │  AQICN, OpenAQ, Weather APIs
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  Python Producers   │  Thu thập & validate
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│   Kafka Topics      │  4 topics: raw, weather, enriched, alerts
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  Spark Streaming    │  Cleaning, aggregation, ML, alerting
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  Storage Layer      │  MinIO (raw) + TimescaleDB (metrics)
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  FastAPI + Grafana  │  Dashboard & API
└─────────────────────┘
```

---

## Tech Stack

| Component | Technology | Lý Do |
|-----------|-----------|-------|
| **Message Queue** | Kafka | Chuẩn công nghiệp |
| **Stream Processing** | Spark Structured Streaming | Yêu cầu môn học |
| **Time-Series DB** | TimescaleDB | SQL, tối ưu time-series |
| **Object Storage** | MinIO | S3-compatible, K8s-friendly |
| **API** | FastAPI | Python async, WebSocket |
| **Dashboard** | Grafana | Time-series visualization |
| **Orchestration** | Kubernetes | Production-ready |

---

## Machine Learning

- **Model:** XGBoost (R² = 0.94-0.97)
- **Features:** Lag values, rolling stats, time patterns, weather data
- **Anomaly Detection:** Isolation Forest (real-time), LSTM Autoencoder (accuracy cao)
- **Output:** Dự báo PM2.5 1-3 giờ tới

---

## Dashboard & Monitoring

### Dashboard Components
1. AQI Gauge (EPA color-coded)
2. Geographic map (color-coded stations)
3. Time-series charts (PM₂.₅, PM₁₀, AQI)
4. Forecast panel (1-3h predictions)
5. Alert table & sensor health

### Alerts
- **HighPM25**: PM2.5 > 150 µg/m³
- **HazardousAQI**: AQI > 300
- **SensorOffline**: No data trong 10 phút

---

## Implementation Timeline

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| **1. Foundation** | Week 1-2 | Kafka + data ingestion |
| **2. Processing** | Week 3-4 | Spark streaming + DB |
| **3. ML** | Week 5-6 | Forecast models |
| **4. Visualization** | Week 7-8 | Dashboard + API |
| **5. Documentation** | Week 9-10 | Report + demo |

---

## Resources

- **Development:** Minikube (local, free)
- **Production:** GKE/EKS/AKS với student credits
- **Requirements:** ~40 cores, ~80GB RAM, ~1TB storage

---

**Status:** In Development