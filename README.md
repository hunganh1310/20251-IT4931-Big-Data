
# Air Quality Monitoring & Forecasting System
## Vietnam / Hanoi - Big Data Project

---

### HUST_IT4931
**Đại học Bách Khoa Hà Nội**  
**Môn học:** Lưu trữ và xử lý dữ liệu lớn  
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
│   └── city_metadata.json

├── img/
│   └── architecture.png

├── k8s/
│   ├── cronjob-analytics-daily.yaml
│   ├── cronjob-analytics-hourly-canth...
│   ├── cronjob-analytics-hourly-danang.yaml
│   ├── cronjob-analytics-hourly-hanoi.yaml
│   ├── deploy-to-eks.sh
│   ├── deployment-archive-cantho.yaml
│   ├── deployment-archive-danang.yaml
│   ├── deployment-archive-hanoi.yaml
│   ├── deployment-realtime-cantho.yaml
│   ├── deployment-realtime-danang.yaml
│   ├── deployment-realtime-hanoi.yaml
│   └── namespace.yaml

├── scripts/
│   ├── produce_cantho.py
│   ├── produce_danang.py
│   ├── produce_hanoi.py
│   ├── run_analytics_daily.py
│   ├── run_analytics_hourly_cantho.py
│   ├── run_analytics_hourly_danang.py
│   ├── run_analytics_hourly_hanoi.py
│   ├── run_archive_cantho.py
│   ├── run_archive_danang.py
│   ├── run_archive_hanoi.py
│   ├── run_realtime_cantho.py
│   ├── run_realtime_danang.py
│   └── run_realtime_hanoi.py

├── src/
│   ├── common/
│   │   ├── config.py
│   │   └── __init__.py
│   │
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── api_client.py
│   │   ├── produce_city.py
│   │   └── producer.py
│   │
│   └── processing/
│       ├── __init__.py
│       ├── analytics_daily.py
│       ├── analytics_hourly.py
│       └── archive.py

├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── LICENSE
├── README.md
├── docker-compose.yaml
└── requirements.txt

```