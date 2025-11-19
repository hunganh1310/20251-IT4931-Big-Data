# 🚀 Quick Start Guide - Air Quality Pipeline

## Prerequisites
- Python 3.8+
- Docker Desktop (for Windows)
- AQICN API Token (get from https://aqicn.org/data-platform/token/)
- 8GB+ RAM available

---

## Step 1: Environment Configuration

### 1.1 Create `.env` file
```bash
copy .env.example .env
```

### 1.2 Edit `.env` and add your AQICN token:
```env
AQICN_TOKEN=your_actual_token_here
```
> **Get your token:** https://aqicn.org/data-platform/token/

All other settings have working defaults.

---

## Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

**Key packages:**
- `kafka-python`: Kafka producer
- `pyspark==3.5.0`: Stream processing
- `psycopg2-binary`: PostgreSQL connector
- `requests`: API calls

---

## Step 3: Start Infrastructure (Docker)

### 3.1 Start Docker Desktop
Make sure Docker Desktop is running on Windows.

### 3.2 Launch all services:
```bash
docker-compose up -d
```

This starts:
- **Kafka** (port 29092) - Message queue
- **Zookeeper** (port 2181) - Kafka coordination
- **TimescaleDB** (port 5432) - Time-series database
- **MinIO** (port 9000/9001) - Object storage
- **Redpanda Console** (port 8080) - Kafka UI

### 3.3 Verify services:
```bash
docker ps
```
You should see 5 containers running.

### 3.4 Access UIs:
- **Kafka Console:** http://localhost:8080
- **MinIO Console:** http://localhost:9001 (minioadmin/minioadmin)

---

## Step 4: Run the Pipeline

### 🎯 Quick Test (Single City)

#### Terminal 1 - Start Producer (Hanoi)
```bash
python scripts\produce_hanoi.py
```
**What it does:**
- Fetches data from AQICN API every 10 seconds
- Publishes to Kafka topic: `raw.airquality.hanoi`
- Logs each API call and Kafka send

**Expected output:**
```
INFO | Calling AQICN API for HANOI
INFO | Fetched data for HANOI
INFO | Sent record to Kafka topic raw.airquality.hanoi for hanoi
INFO | Sleeping for 10 seconds
```

#### Terminal 2 - Start Spark Streaming (Hanoi)
```bash
python scripts\run_spark.py
```
**What it does:**
- Reads from Kafka topics for all 7 cities
- Parses JSON data
- Writes to TimescaleDB tables: `{city}_measurements`
- Processes every 10 seconds

**Expected output:**
```
INFO | Starting AQICN Spark Streaming job for HANOI
INFO | Database table initialized successfully
INFO | Batch 0: Wrote 1 records to PostgreSQL
```

---

## Step 5: Verify Data

### Option A: PostgreSQL Command Line
```bash
docker exec -it timescaledb psql -U airquality -d airquality
```

```sql
-- Check tables
\dt

-- View recent data
SELECT city, aqi, pm25, temperature, measurement_time 
FROM hanoi_measurements 
ORDER BY processed_time DESC 
LIMIT 10;

-- Check all cities
SELECT city, COUNT(*) as records, MAX(processed_time) as latest
FROM hanoi_measurements
GROUP BY city;
```

### Option B: Check in Kafka Console
1. Open http://localhost:8080
2. Navigate to **Topics**
3. Click on `raw.airquality.hanoi`
4. View messages

---

## 🏙️ Run All Cities

### Start All Producers (7 cities)
```bash
# Terminal 1
python scripts\produce_hanoi.py

# Terminal 2
python scripts\produce_saigon.py

# Terminal 3
python scripts\produce_danang.py

# ... and so on for other cities
```

**Available cities:**
- `produce_hanoi.py` - Hanoi
- `produce_saigon.py` - Ho Chi Minh City
- `produce_danang.py` - Da Nang
- `produce_haiphong.py` - Hai Phong
- `produce_cantho.py` - Can Tho
- `produce_nhatrang.py` - Nha Trang
- `produce_vungtau.py` - Vung Tau

### Start Spark for All Cities
```bash
python scripts\run_spark.py
```
This spawns 7 parallel processes (one per city).

---

## 🔗 Joint Processing (Advanced)

Process data from multiple cities simultaneously with spatial correlation:

```bash
python scripts\run_joint_streaming.py
```

**What it does:**
- Combines data from all city streams
- Creates unified table: `city_comparison`
- Enables cross-city analysis

---

## 📊 Understanding the Data Flow

### 1. Producer Script (`produce_hanoi.py`)
```
AQICN API → Python Script → Kafka Topic
```
- Fetches: `https://api.waqi.info/feed/hanoi/`
- Publishes to: `raw.airquality.hanoi`
- Frequency: Every 10 seconds

### 2. Spark Streaming (`run_spark.py`)
```
Kafka Topic → Spark Transformation → PostgreSQL
```
- Reads: `raw.airquality.hanoi`
- Parses JSON schema
- Writes: `hanoi_measurements` table

### 3. Database Schema
```sql
CREATE TABLE hanoi_measurements (
    id SERIAL,
    city TEXT,
    aqi INTEGER,              -- Air Quality Index
    pm25 DOUBLE PRECISION,    -- PM2.5 particles
    pm10 DOUBLE PRECISION,    -- PM10 particles
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    measurement_time TIMESTAMP,
    processed_time TIMESTAMP
);
```

---

## 🛠️ Troubleshooting

### Problem: Docker containers won't start
**Solution:**
```bash
# Check Docker Desktop is running
docker --version

# Restart Docker Desktop
# Then retry:
docker-compose down
docker-compose up -d
```

### Problem: "AQICN_TOKEN is not configured"
**Solution:**
- Make sure `.env` file exists
- Check token is correctly set: `AQICN_TOKEN=your_token_here`
- No quotes or spaces around the token

### Problem: "Connection refused to Kafka"
**Solution:**
```bash
# Wait 30 seconds for Kafka to be ready
# Check Kafka is running:
docker logs kafka

# Test connection:
telnet localhost 29092
```

### Problem: No data in database
**Solution:**
```bash
# 1. Check producer is sending data
#    Should see: "Sent record to Kafka topic..."

# 2. Check Spark is processing
#    Should see: "Batch X: Wrote Y records..."

# 3. Check Kafka has messages
#    Visit http://localhost:8080

# 4. Manually check database
docker exec -it timescaledb psql -U airquality -d airquality -c "SELECT COUNT(*) FROM hanoi_measurements;"
```

---

## 📈 Monitoring

### Kafka Messages
- **UI:** http://localhost:8080
- View topics, messages, consumer groups

### Database Stats
```sql
-- Count records per city
SELECT city, COUNT(*) 
FROM hanoi_measurements 
GROUP BY city;

-- Check data freshness
SELECT city, MAX(measurement_time) as latest_measurement
FROM hanoi_measurements
GROUP BY city;

-- View AQI trends
SELECT 
    DATE_TRUNC('hour', measurement_time) as hour,
    AVG(aqi) as avg_aqi,
    AVG(pm25) as avg_pm25
FROM hanoi_measurements
WHERE measurement_time > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;
```

---

## 🧹 Cleanup

### Stop all services
```bash
docker-compose down
```

### Remove all data
```bash
docker-compose down -v
```
⚠️ This deletes all data in TimescaleDB and MinIO!

---

## 🎓 Next Steps

1. **Add more producers** - Run all 7 city producers
2. **Test joint processing** - `run_joint_streaming.py`
3. **Implement cold storage** - `run_cold_storage.py`
4. **Build dashboard** - Connect Grafana to TimescaleDB
5. **Add ML forecasting** - PM2.5 prediction models

---

## 📚 Key Files Reference

| File | Purpose |
|------|---------|
| `scripts/produce_*.py` | Data ingestion from AQICN API |
| `scripts/run_spark.py` | Individual city streaming |
| `scripts/run_joint_streaming.py` | Multi-city joint processing |
| `src/ingestion/api_client.py` | API wrapper |
| `src/ingestion/producer.py` | Kafka producer |
| `src/processing/spark_stream.py` | Spark streaming logic |
| `src/common/config.py` | Configuration management |
| `docker-compose.yaml` | Infrastructure definition |

---

## 🆘 Need Help?

- Check logs: `docker logs <container_name>`
- Kafka UI: http://localhost:8080
- Database: `docker exec -it timescaledb psql -U airquality -d airquality`
