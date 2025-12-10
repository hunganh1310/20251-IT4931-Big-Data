from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, current_timestamp, to_timestamp, udf, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

from src.common import logger, settings


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("AQICN Streaming") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_schema() -> StructType:
    return StructType([
        StructField("city", StringType(), True),
        StructField("payload", StructType([
            StructField("aqi", IntegerType(), True),
            StructField("idx", IntegerType(), True),
            StructField("dominentpol", StringType(), True),
            StructField("iaqi", StructType([
                StructField("pm25", StructType([
                    StructField("v", DoubleType(), True)
                ]), True),
                StructField("pm10", StructType([
                    StructField("v", DoubleType(), True)
                ]), True),
                StructField("t", StructType([
                    StructField("v", DoubleType(), True)
                ]), True),
                StructField("h", StructType([
                    StructField("v", DoubleType(), True)
                ]), True)
            ]), True),
            StructField("time", StructType([
                StructField("s", StringType(), True),
                StructField("tz", StringType(), True),
                StructField("v", IntegerType(), True)
            ]), True),
            StructField("city", StructType([
                StructField("name", StringType(), True),
                StructField("geo", StructType([
                    StructField("0", DoubleType(), True),
                    StructField("1", DoubleType(), True)
                ]), True)
            ]), True)
        ]), True)
    ])


def load_city_metadata(spark: SparkSession) -> DataFrame:
    metadata_path = os.path.join(os.getcwd(), "data", "city_metadata.json")

    if not os.path.exists(metadata_path):
        raise ValueError(f"City metadata file not found at {metadata_path}")

    df = spark.read.json(metadata_path)

    logger.info(f"Loaded city metadata from {metadata_path}")

    return df


@udf(StringType())
def health_category_udf(aqi):
    if aqi is None:
        return "Unknown"
    if aqi <= 50:
        return "Good"
    elif aqi <= 100:
        return "Moderate"
    elif aqi <= 150:
        return "Unhealthy for Sensitive"
    elif aqi <= 200:
        return "Unhealthy"
    elif aqi <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"


@udf(StringType())
def pollution_severity_udf(pm25, pm10):
    if pm25 is None and pm10 is None:
        return "Unknown"
    pm_max = max(pm25 or 0, pm10 or 0)
    if pm_max < 50:
        return "Low"
    elif pm_max < 100:
        return "Medium"
    elif pm_max < 150:
        return "High"
    else:
        return "Severe"


def add_advanced_features(df: DataFrame, city: str) -> DataFrame:
    df_enriched = df.withColumn("health_status", health_category_udf(col("aqi"))) \
        .withColumn("pollution_level", pollution_severity_udf(col("pm25"), col("pm10")))

    return df_enriched


def read_from_kafka(spark: SparkSession, city: str) -> DataFrame:
    kafka_options = {
        "kafka.bootstrap.servers": settings.kafka_bootstrap_servers,
        "subscribe": f"{settings.aqicn_topic}.{city}",
        "startingOffsets": "earliest"
    }

    if settings.kafka_security_protocol != "PLAINTEXT":
        kafka_options["kafka.security.protocol"] = settings.kafka_security_protocol
        kafka_options["kafka.sasl.mechanism"] = settings.kafka_sasl_mechanism
        kafka_options["kafka.sasl.jaas.config"] = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{settings.kafka_sasl_username}" password="{settings.kafka_sasl_password}";'

    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    return df


def parse_data(df_raw: DataFrame, schema: StructType) -> DataFrame:
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    df_clean = df_parsed.select(
        col("data.city").alias("city"),
        col("data.payload.aqi").alias("aqi"),
        col("data.payload.idx").alias("station_id"),
        col("data.payload.dominentpol").alias("dominant_pollutant"),
        col("data.payload.iaqi.pm25.v").alias("pm25"),
        col("data.payload.iaqi.pm10.v").alias("pm10"),
        col("data.payload.iaqi.t.v").alias("temperature"),
        col("data.payload.iaqi.h.v").alias("humidity"),
        to_timestamp(col("data.payload.time.s"), "yyyy-MM-dd HH:mm:ss").alias("measurement_time"),
        col("data.payload.city.name").alias("station_name"),
        col("data.payload.city.geo.0").alias("latitude"),
        col("data.payload.city.geo.1").alias("longitude"),
        col("kafka_timestamp"),
        current_timestamp().alias("processed_time")
    )

    return df_clean


def create_table(city: str):
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

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {city}_measurements (
            id SERIAL,
            city TEXT,
            station_id INTEGER,
            station_name TEXT,
            aqi INTEGER,
            dominant_pollutant TEXT,
            pm25 DOUBLE PRECISION,
            pm10 DOUBLE PRECISION,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            measurement_time TIMESTAMP,
            kafka_timestamp TIMESTAMP,
            processed_time TIMESTAMP NOT NULL,
            health_status TEXT,
            pollution_level TEXT,
            city_name TEXT,
            country TEXT,
            region TEXT,
            population BIGINT,
            timezone TEXT,
            aqi_threshold_good INTEGER,
            aqi_threshold_moderate INTEGER,
            aqi_threshold_unhealthy_sensitive INTEGER,
            aqi_threshold_unhealthy INTEGER,
            aqi_threshold_very_unhealthy INTEGER,
            PRIMARY KEY (id, processed_time)
        );
        """
        cursor.execute(create_table_query)

        cursor.execute(
            f"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '{city}_measurements' AND schemaname = 'public');"
        )
        table_exists = cursor.fetchone()[0]

        if table_exists:
            cursor.execute(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = '{city}_measurements'
                );
                """
            )
            is_hypertable = cursor.fetchone()[0]

            if not is_hypertable:
                cursor.execute(
                    f"SELECT create_hypertable('{city}_measurements', 'processed_time', if_not_exists => TRUE);"
                )
                logger.info(f"Created hypertable for {city}_measurements")

        cursor.close()
        conn.close()
        logger.info("Database table initialized successfully")

    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise


def write_to_postgres(batch_df: DataFrame, batch_id: int, city: str):
    if batch_df.isEmpty():
        return

    jdbc_url = f"jdbc:postgresql://{settings.db_host}:{settings.db_port}/{settings.db_name}"
    connection_properties = {
        "user": settings.db_user,
        "password": settings.db_password,
        "driver": "org.postgresql.Driver"
    }

    batch_df.write \
        .jdbc(url=jdbc_url, table=f"{city}_measurements", mode="append", properties=connection_properties)

    logger.info(f"Batch {batch_id}: Wrote {batch_df.count()} records to PostgreSQL")


def write_to_database(df: DataFrame, city: str):
    def write_batch(batch_df: DataFrame, batch_id: int):
        write_to_postgres(batch_df, batch_id, city)

    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_batch) \
        .trigger(processingTime="10 minutes") \
        .start()

    return query


def main(city: str):
    logger.info("Starting AQICN Spark Streaming job for %s", city.upper())

    create_table(city)

    spark = get_spark_session()
    schema = define_schema()

    city_metadata = load_city_metadata(spark).cache()

    df_raw = read_from_kafka(spark, city)
    df_parsed = parse_data(df_raw, schema)
    df_enriched = add_advanced_features(df_parsed, city)

    df_with_metadata = df_enriched.join(
        broadcast(city_metadata),
        df_enriched.city == city_metadata.city,
        "left"
    ).drop(city_metadata.city)

    query = write_to_database(df_with_metadata, city)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping AQICN streaming job")
        query.stop()
        spark.stop()
