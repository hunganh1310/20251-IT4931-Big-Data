# spark_stream_joint.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, current_timestamp, to_timestamp, explode,
    floor, expr, when, udf
)
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType,
)
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from ..common import logger, settings
from ..schemas import aqicn_schema, openaq_schema

def get_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("AQ Joint Streaming") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def normalize_aqicn(df_raw: DataFrame) -> DataFrame:
    """Normalize AQICN data to unified schema - matches spark_stream_aqicn.py logic"""
    schema = aqicn_schema()
    df = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    df_norm = df.select(
        expr("'aqicn'").alias("source"),
        col("data.payload.idx").alias("station_id"),
        col("data.payload.city.name").alias("station_name"),
        col("data.payload.city.geo.0").alias("latitude"),
        col("data.payload.city.geo.1").alias("longitude"),
        to_timestamp(col("data.payload.time.s"), "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
        col("data.payload.iaqi.pm25.v").alias("pm25"),
        col("data.payload.iaqi.pm10.v").alias("pm10"),
        col("data.payload.aqi").alias("aqi"),
        col("data.payload.dominentpol").alias("dominant_pollutant"),
        col("kafka_timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("processed_time")
    )

    return df_norm

def normalize_openaq(df_raw: DataFrame) -> DataFrame:
    """Normalize OpenAQ data to unified schema - matches spark_stream_openaq.py logic"""
    schema = openaq_schema()
    df = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    # explode results array - matches openaq individual processing
    df_exploded = df.select(
        col("data.country").alias("query_country"),
        col("data.parameter").alias("query_parameter"),
        explode(col("data.data.results")).alias("location"),
        col("kafka_timestamp")
    )

    df_norm = df_exploded.select(
        expr("'openaq'").alias("source"),
        col("location.id").alias("station_id"),
        col("location.name").alias("station_name"),
        col("location.coordinates.latitude").alias("latitude"),
        col("location.coordinates.longitude").alias("longitude"),
        to_timestamp(col("kafka_timestamp")).alias("timestamp"),
        expr("NULL").cast(DoubleType()).alias("pm25"),
        expr("NULL").cast(DoubleType()).alias("pm10"),
        expr("NULL").cast(IntegerType()).alias("aqi"),
        expr("NULL").cast(StringType()).alias("dominant_pollutant"),
        col("kafka_timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("processed_time")
    )

    return df_norm

def add_geo_buckets(df: DataFrame, precision: int = 2) -> DataFrame:
    # precision: number of decimal places to bucket (2 -> ~1km). Use floor to avoid rounding issues.
    factor = float(10 ** precision)
    return df.withColumn("lat_bucket", floor(col("latitude") * factor) / factor) \
             .withColumn("lon_bucket", floor(col("longitude") * factor) / factor)

PM25_BREAKPOINTS = [
    (0.0, 12.0, 0, 50),
    (12.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 150.4, 151, 200),
    (150.5, 250.4, 201, 300),
    (250.5, 350.4, 301, 400),
    (350.5, 500.4, 401, 500),
]

PM10_BREAKPOINTS = [
    (0, 54, 0, 50),
    (55, 154, 51, 100),
    (155, 254, 101, 150),
    (255, 354, 151, 200),
    (355, 424, 201, 300),
    (425, 504, 301, 400),
    (505, 604, 401, 500),
]

def _linear_interpolate(Cp, Clow, Chigh, Ilow, Ihigh):
    return ((Ihigh - Ilow) / (Chigh - Clow)) * (Cp - Clow) + Ilow

def compute_aqi_single(pm25_val, pm10_val):
    # Compute AQI for PM2.5 and PM10; return max of available (more conservative)
    results = []
    if pm25_val is not None:
        try:
            c = float(pm25_val)
            for (cl, ch, il, ih) in PM25_BREAKPOINTS:
                if cl <= c <= ch:
                    aqi_p = _linear_interpolate(c, cl, ch, il, ih)
                    results.append(int(round(aqi_p)))
                    break
        except Exception:
            pass
    if pm10_val is not None:
        try:
            c = float(pm10_val)
            for (cl, ch, il, ih) in PM10_BREAKPOINTS:
                if cl <= c <= ch:
                    aqi_p = _linear_interpolate(c, cl, ch, il, ih)
                    results.append(int(round(aqi_p)))
                    break
        except Exception:
            pass
    if not results:
        return None
    return max(results)

compute_aqi_udf = udf(compute_aqi_single, IntegerType())

def create_joint_table():
    try:
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
            user=settings.db_user,
            password=settings.db_password
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS air_quality_enriched (
            id SERIAL,
            source TEXT,
            station_id TEXT,
            station_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            pm25 DOUBLE PRECISION,
            pm10 DOUBLE PRECISION,
            aqi INTEGER,
            dominant_pollutant TEXT,
            event_time TIMESTAMP,
            kafka_timestamp TIMESTAMP,
            processed_time TIMESTAMP NOT NULL,
            PRIMARY KEY (id, processed_time)
        );
        """
        cursor.execute(create_table_query)
        # create hypertable on processed_time
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'air_quality_enriched');"
        )
        is_hypertable = cursor.fetchone()[0]
        if not is_hypertable:
            cursor.execute("SELECT create_hypertable('air_quality_enriched', 'processed_time', if_not_exists => TRUE);")
            logger.info("Created hypertable for air_quality_enriched")
        cursor.close()
        conn.close()
        logger.info("Database table initialized successfully")
    except Exception as e:
        logger.error(f"Error creating joint table: {e}")
        raise

def write_to_postgres(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    jdbc_url = f"jdbc:postgresql://{settings.db_host}:{settings.db_port}/{settings.db_name}"
    connection_properties = {
        "user": settings.db_user,
        "password": settings.db_password,
        "driver": "org.postgresql.Driver"
    }
    # select only desired columns
    cols = [
        "source", "station_id", "station_name", "latitude", "longitude",
        "pm25", "pm10", "aqi", "dominant_pollutant", "event_time",
        "kafka_timestamp", "processed_time"
    ]
    batch_df.select(*cols).write.jdbc(url=jdbc_url, table="air_quality_enriched", mode="append", properties=connection_properties)
    logger.info(f"Batch {batch_id}: wrote {batch_df.count()} records to air_quality_enriched")

# Main pipeline
def main():
    logger.info("Starting Joint Spark Streaming job (AQICN + OpenAQ)")

    # ensure DB table exists
    create_joint_table()

    spark = get_spark_session()

    # read streams
    aqicn_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", settings.aqicn_topic) \
        .option("startingOffsets", "latest") \
        .load()

    openaq_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", settings.openaq_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # normalize
    aqicn_norm = normalize_aqicn(aqicn_raw)
    openaq_norm = normalize_openaq(openaq_raw)

    # add geo buckets for approximate spatial join
    aqicn_b = add_geo_buckets(aqicn_norm, precision=2).withWatermark("timestamp", "5 minutes")
    openaq_b = add_geo_buckets(openaq_norm, precision=2).withWatermark("timestamp", "5 minutes")

    # perform spatial join on geo buckets + optional temporal correlation
    # since OpenAQ provides metadata (no real-time measurements), 
    # we mainly use spatial join for station enrichment
    join_condition = [
        col("a.lat_bucket") == col("b.lat_bucket"),
        col("a.lon_bucket") == col("b.lon_bucket")
    ]
    
    # Add temporal join if both streams have valid timestamps
    join_condition.append(
        col("a.timestamp").between(
            col("b.timestamp") - expr("INTERVAL 5 minutes"),
            col("b.timestamp") + expr("INTERVAL 5 minutes")
        )
    )

    joined = aqicn_b.alias("a").join(
        openaq_b.alias("b"),
        join_condition,
        how="leftOuter"
    )

    enriched = joined.select(
        col("a.source").alias("source"),
        col("a.station_id").cast(StringType()).alias("station_id"),
        when(col("a.station_name").isNotNull(), col("a.station_name")).otherwise(col("b.station_name")).alias("station_name"),
        when(col("a.latitude").isNotNull(), col("a.latitude")).otherwise(col("b.latitude")).alias("latitude"),
        when(col("a.longitude").isNotNull(), col("a.longitude")).otherwise(col("b.longitude")).alias("longitude"),
        col("a.pm25").alias("pm25"),
        col("a.pm10").alias("pm10"),
        when(col("a.aqi").isNotNull(), col("a.aqi")).otherwise(compute_aqi_udf(col("a.pm25"), col("a.pm10"))).alias("aqi"),
        col("a.dominant_pollutant").alias("dominant_pollutant"),
        col("a.timestamp").alias("event_time"),
        col("a.kafka_timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("processed_time")
    )

    # write enriched stream to postgres using foreachBatch
    query = enriched.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", f"{settings.spark_checkpoint_location}/joint_aq") \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping joint streaming job")
        query.stop()
        spark.stop()

if __name__ == "__main__":
    main()
