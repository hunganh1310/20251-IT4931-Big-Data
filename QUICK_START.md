# ⚡ Quick Test Commands

## Prerequisites Setup (One-time)
```bash
# 1. Install missing package
pip install psycopg2-binary

# 2. Edit .env file and add your AQICN token
# Get token from: https://aqicn.org/data-platform/token/
notepad .env

# 3. Start Docker Desktop (manually)
# 4. Start infrastructure
docker-compose up -d

# 5. Verify setup
python verify_setup.py
```

---

## 🚀 Quick Test - Single City Pipeline

### Terminal 1: Start Producer (Data Ingestion)
```bash
python scripts\produce_hanoi.py
```
**Expected:** See "Sent record to Kafka topic..." every 10 seconds

### Terminal 2: Start Spark Streaming (Processing)
```bash
# For single city
python scripts\run_spark.py
```
**Expected:** See "Batch X: Wrote Y records to PostgreSQL"

---

## 🔍 Verify Data

### Check Kafka Messages
http://localhost:8080
- Topics → `raw.airquality.hanoi`

### Check Database
```bash
docker exec -it timescaledb psql -U airquality -d airquality

# In psql:
SELECT * FROM hanoi_measurements ORDER BY processed_time DESC LIMIT 5;
\q
```

---

## 🏙️ Full Pipeline - All Cities

### Option 1: Run All Producers (7 terminals)
```bash
python scripts\produce_hanoi.py
python scripts\produce_saigon.py
python scripts\produce_danang.py
python scripts\produce_haiphong.py
python scripts\produce_cantho.py
python scripts\produce_nhatrang.py
python scripts\produce_vungtau.py
```

### Option 2: Joint Processing (Recommended)
```bash
# Terminal 1: Start one producer
python scripts\produce_hanoi.py

# Terminal 2: Joint streaming
python scripts\run_joint_streaming.py
```

---

## 🛑 Stop Everything

```bash
# Stop producers: Ctrl+C in each terminal
# Stop Spark: Ctrl+C
# Stop Docker:
docker-compose down
```

---

## 📊 Useful Queries

### Count Records
```sql
SELECT city, COUNT(*) as records 
FROM hanoi_measurements 
GROUP BY city;
```

### Recent AQI Values
```sql
SELECT city, aqi, pm25, measurement_time 
FROM hanoi_measurements 
ORDER BY measurement_time DESC 
LIMIT 10;
```

### Average AQI Last Hour
```sql
SELECT 
    city,
    AVG(aqi)::INTEGER as avg_aqi,
    AVG(pm25)::NUMERIC(5,1) as avg_pm25
FROM hanoi_measurements
WHERE measurement_time > NOW() - INTERVAL '1 hour'
GROUP BY city;
```

---

## 🐛 Common Issues

### "AQICN_TOKEN is not configured"
➜ Edit `.env` file and add your token

### "Connection refused" (Kafka)
➜ Wait 30 seconds for Kafka to start
➜ Run: `docker logs kafka`

### "Docker daemon not running"
➜ Start Docker Desktop application

### No data in database
➜ Check producer is running (Terminal 1)
➜ Check Spark is running (Terminal 2)
➜ Check Kafka UI: http://localhost:8080

---

## 📈 System Architecture

```
┌──────────────┐
│  AQICN API   │  (api.waqi.info)
└──────┬───────┘
       │ HTTP GET every 10s
       ↓
┌──────────────┐
│  Producer    │  (produce_hanoi.py)
│  Python      │
└──────┬───────┘
       │ Kafka Protocol
       ↓
┌──────────────┐
│    Kafka     │  Topic: raw.airquality.hanoi
│ Port: 29092  │
└──────┬───────┘
       │ Spark Streaming
       ↓
┌──────────────┐
│    Spark     │  (spark_stream.py)
│  Processing  │  • Parse JSON
│              │  • Transform data
└──────┬───────┘  • Batch writes
       │ JDBC
       ↓
┌──────────────┐
│ TimescaleDB  │  Table: hanoi_measurements
│ Port: 5432   │  (Time-series optimized)
└──────────────┘
```

---

## 🎯 Test Checklist

- [ ] Docker containers running (`docker ps`)
- [ ] AQICN token configured in `.env`
- [ ] Producer sending data (Terminal 1)
- [ ] Spark processing data (Terminal 2)
- [ ] Data visible in Kafka UI (http://localhost:8080)
- [ ] Data stored in TimescaleDB (`psql` query)

---

## 📚 Files You'll Use

| File | Purpose | When to Use |
|------|---------|-------------|
| `verify_setup.py` | Check prerequisites | Before starting |
| `scripts/produce_hanoi.py` | Ingest Hanoi data | Terminal 1 |
| `scripts/run_spark.py` | Process all cities | Terminal 2 |
| `scripts/run_joint_streaming.py` | Joint processing | Advanced |
| `.env` | Configuration | Edit once |
| `SETUP_GUIDE.md` | Detailed docs | Troubleshooting |

---

## 💡 Pro Tips

1. **Start small:** Test with 1 city (Hanoi) first
2. **Check logs:** Look for error messages in terminals
3. **Use Kafka UI:** Verify messages are being published
4. **Database first:** Query database to confirm data flow
5. **One step at a time:** Get producer working, then Spark

---

## 🆘 Getting Help

1. Check `SETUP_GUIDE.md` for detailed instructions
2. Run `python verify_setup.py` to diagnose issues
3. Check Docker logs: `docker logs <container_name>`
4. View Kafka messages: http://localhost:8080
