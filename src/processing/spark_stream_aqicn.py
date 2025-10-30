from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from ..common import logger, settings


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
                StructField("o3", StructType([
                    StructField("v", DoubleType(), True)
                ]), True),
                StructField("no2", StructType([
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


def read_from_kafka(spark: SparkSession) -> DataFrame:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", settings.aqicn_topic) \
        .option("startingOffsets", "earliest") \
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
        col("data.payload.iaqi.o3.v").alias("o3"),
        col("data.payload.iaqi.no2.v").alias("no2"),
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


def create_table():
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
        CREATE TABLE IF NOT EXISTS aqicn_measurements (
            id SERIAL,
            city TEXT,
            station_id INTEGER,
            station_name TEXT,
            aqi INTEGER,
            dominant_pollutant TEXT,
            pm25 DOUBLE PRECISION,
            pm10 DOUBLE PRECISION,
            o3 DOUBLE PRECISION,
            no2 DOUBLE PRECISION,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            measurement_time TIMESTAMP,
            kafka_timestamp TIMESTAMP,
            processed_time TIMESTAMP NOT NULL,
            PRIMARY KEY (id, processed_time)
        );
        """
        cursor.execute(create_table_query)

        cursor.execute(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'aqicn_measurements' AND schemaname = 'public');"
        )
        table_exists = cursor.fetchone()[0]

        if table_exists:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'aqicn_measurements'
                );
                """
            )
            is_hypertable = cursor.fetchone()[0]

            if not is_hypertable:
                cursor.execute(
                    "SELECT create_hypertable('aqicn_measurements', 'processed_time', if_not_exists => TRUE);"
                )
                logger.info("Created hypertable for aqicn_measurements")

        cursor.close()
        conn.close()
        logger.info("Database table initialized successfully")

    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise


def write_to_postgres(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    jdbc_url = f"jdbc:postgresql://{settings.db_host}:{settings.db_port}/{settings.db_name}"
    connection_properties = {
        "user": settings.db_user,
        "password": settings.db_password,
        "driver": "org.postgresql.Driver"
    }

    batch_df.write \
        .jdbc(url=jdbc_url, table="aqicn_measurements", mode="append", properties=connection_properties)

    logger.info(f"Batch {batch_id}: Wrote {batch_df.count()} records to PostgreSQL")


def write_to_database(df: DataFrame):
    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .trigger(processingTime="10 seconds") \
        .start()

    return query


def main():
    logger.info("Starting AQICN Spark Streaming job")

    create_table()

    spark = get_spark_session()
    schema = define_schema()
    df_raw = read_from_kafka(spark)
    df_parsed = parse_data(df_raw, schema)

    query = write_to_database(df_parsed)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping AQICN streaming job")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
