
# Air Quality Monitoring & Forecasting System
## Vietnam / Hanoi - Big Data Project

---

### HUST_IT4931
**Đại học Bách Khoa Hà Nội**  
**Môn học:** Lưu trữ và xử lý dữ liệu lớn  
**Mã lớp:** 162302
**Nhóm: 3** 

---

## Tổng Quan Dự Án

Hệ thống giám sát và dự báo chất lượng không khí thời gian thực cho **Hà Nội** sử dụng **Kappa Architecture** và công nghệ Big Data.

### Mục Tiêu
- Thu thập dữ liệu chất lượng không khí tại Việt Nam (Hà Nội)
- Xử lý và phân tích dữ liệu streaming real-time
- Dự báo PM2.5 ngắn hạn (1-3 giờ) sử dụng Machine Learning
- Phát hiện bất thường cảm biến và cảnh báo sớm
- Hiển thị dashboard trực quan với EPA color standards
## File Structure & Components
```
.
├── data/
│   └── city_metadata.json          # Metadata mô tả thông tin từng thành phố

├── img/
│   └── architecture.png            # Sơ đồ kiến trúc hệ thống (Kappa Architecture)

├── k8s/                            # Kubernetes manifests để triển khai hệ thống
│   ├── cronjob-analytics-daily.yaml
│   ├── cronjob-analytics-hourly-*.yaml
│   ├── deployment-archive-*.yaml
│   ├── deployment-realtime-*.yaml
│   ├── deploy-to-eks.sh            # Script deploy lên Amazon EKS
│   └── namespace.yaml

├── scripts/                        # Các entrypoint chạy pipeline
│   ├── produce_*.py                # Producer cho từng thành phố
│   ├── run_analytics_daily.py
│   ├── run_analytics_hourly_*.py
│   ├── run_archive_*.py
│   └── run_realtime_*.py

├── src/
│   ├── common/
│   │   ├── config.py               # Load config, logger, env
│   │   └── __init__.py
│   │
│   ├── ingestion/                  # Thành phần lấy & đưa dữ liệu vào hệ thống
│   │   ├── api_client.py
│   │   ├── produce_city.py
│   │   └── producer.py             # Kafka producer abstraction
│   │
│   └── processing/                 # Data processing layer
│       ├── analytics_daily.py
│       ├── analytics_hourly.py
│       └── archive.py

├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── LICENSE
├── README.md
├── docker-compose.yaml             # Local development environment
└── requirements.txt                # Python dependencies


```