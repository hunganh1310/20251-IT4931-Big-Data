from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp, unix_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.common import logger, settings


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("City Comparison Streaming") \
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
                StructField("s", StringType(), True)
            ]), True)
        ]), True)
    ])


def read_from_kafka(spark: SparkSession, city: str) -> DataFrame:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", f"{settings.aqicn_topic}.{city}") \
        .option("startingOffsets", "earliest") \
        .load()

    return df


def parse_data(df_raw: DataFrame, schema: StructType, city: str) -> DataFrame:
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    df_clean = df_parsed.select(
        col("data.payload.aqi").alias(f"{city}_aqi"),
        col("data.payload.iaqi.pm25.v").alias(f"{city}_pm25"),
        col("data.payload.iaqi.pm10.v").alias(f"{city}_pm10"),
        col("data.payload.iaqi.t.v").alias(f"{city}_temperature"),
        col("data.payload.iaqi.h.v").alias(f"{city}_humidity"),
        to_timestamp(col("data.payload.time.s"), "yyyy-MM-dd HH:mm:ss").alias(f"{city}_measurement_time"),
        col("kafka_timestamp")
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
        CREATE TABLE IF NOT EXISTS city_comparison (
            id SERIAL,
            hanoi_aqi INTEGER,
            hanoi_pm25 DOUBLE PRECISION,
            hanoi_pm10 DOUBLE PRECISION,
            hanoi_temperature DOUBLE PRECISION,
            hanoi_humidity DOUBLE PRECISION,
            hanoi_measurement_time TIMESTAMP,
            saigon_aqi INTEGER,
            saigon_pm25 DOUBLE PRECISION,
            saigon_pm10 DOUBLE PRECISION,
            saigon_temperature DOUBLE PRECISION,
            saigon_humidity DOUBLE PRECISION,
            saigon_measurement_time TIMESTAMP,
            danang_aqi INTEGER,
            danang_pm25 DOUBLE PRECISION,
            danang_pm10 DOUBLE PRECISION,
            danang_temperature DOUBLE PRECISION,
            danang_humidity DOUBLE PRECISION,
            danang_measurement_time TIMESTAMP,
            haiphong_aqi INTEGER,
            haiphong_pm25 DOUBLE PRECISION,
            haiphong_pm10 DOUBLE PRECISION,
            haiphong_temperature DOUBLE PRECISION,
            haiphong_humidity DOUBLE PRECISION,
            haiphong_measurement_time TIMESTAMP,
            cantho_aqi INTEGER,
            cantho_pm25 DOUBLE PRECISION,
            cantho_pm10 DOUBLE PRECISION,
            cantho_temperature DOUBLE PRECISION,
            cantho_humidity DOUBLE PRECISION,
            cantho_measurement_time TIMESTAMP,
            nhatrang_aqi INTEGER,
            nhatrang_pm25 DOUBLE PRECISION,
            nhatrang_pm10 DOUBLE PRECISION,
            nhatrang_temperature DOUBLE PRECISION,
            nhatrang_humidity DOUBLE PRECISION,
            nhatrang_measurement_time TIMESTAMP,
            vungtau_aqi INTEGER,
            vungtau_pm25 DOUBLE PRECISION,
            vungtau_pm10 DOUBLE PRECISION,
            vungtau_temperature DOUBLE PRECISION,
            vungtau_humidity DOUBLE PRECISION,
            vungtau_measurement_time TIMESTAMP,
            join_time TIMESTAMP,
            processed_time TIMESTAMP NOT NULL,
            PRIMARY KEY (id, processed_time)
        );
        """
        cursor.execute(create_table_query)

        cursor.execute(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'city_comparison' AND schemaname = 'public');"
        )
        table_exists = cursor.fetchone()[0]

        if table_exists:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'city_comparison'
                );
                """
            )
            is_hypertable = cursor.fetchone()[0]

            if not is_hypertable:
                cursor.execute(
                    "SELECT create_hypertable('city_comparison', 'processed_time', if_not_exists => TRUE);"
                )
                logger.info("Created hypertable for city_comparison")

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
        .jdbc(url=jdbc_url, table="city_comparison", mode="append", properties=connection_properties)

    logger.info(f"Batch {batch_id}: Wrote {batch_df.count()} records to PostgreSQL")


def write_to_database(df: DataFrame):
    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", f"{settings.spark_checkpoint_location}/city_comparison") \
        .trigger(processingTime="30 seconds") \
        .start()

    return query


def main():
    logger.info("Starting multi-city comparison streaming job")

    create_table()

    spark = get_spark_session()
    schema = define_schema()

    cities = ["hanoi", "saigon", "danang", "haiphong", "cantho", "nhatrang", "vungtau"]

    raw_streams = {city: read_from_kafka(spark, city) for city in cities}
    parsed_streams = {city: parse_data(raw_streams[city], schema, city) for city in cities}

    aliased_streams = {}
    for city in cities:
        aliased_streams[city] = parsed_streams[city].withColumn(
            "join_time",
            from_unixtime(((unix_timestamp("kafka_timestamp") + 5) / 10).cast("long") * 10).cast("timestamp")
        ).withWatermark("join_time", "2 minutes")

    joined_df = aliased_streams["hanoi"]
    for city in ["saigon", "danang", "haiphong", "cantho", "nhatrang", "vungtau"]:
        joined_df = joined_df.join(aliased_streams[city], "join_time", "outer")

    result_df = joined_df.select(
        col("hanoi_aqi"),
        col("hanoi_pm25"),
        col("hanoi_pm10"),
        col("hanoi_temperature"),
        col("hanoi_humidity"),
        col("hanoi_measurement_time"),
        col("saigon_aqi"),
        col("saigon_pm25"),
        col("saigon_pm10"),
        col("saigon_temperature"),
        col("saigon_humidity"),
        col("saigon_measurement_time"),
        col("danang_aqi"),
        col("danang_pm25"),
        col("danang_pm10"),
        col("danang_temperature"),
        col("danang_humidity"),
        col("danang_measurement_time"),
        col("haiphong_aqi"),
        col("haiphong_pm25"),
        col("haiphong_pm10"),
        col("haiphong_temperature"),
        col("haiphong_humidity"),
        col("haiphong_measurement_time"),
        col("cantho_aqi"),
        col("cantho_pm25"),
        col("cantho_pm10"),
        col("cantho_temperature"),
        col("cantho_humidity"),
        col("cantho_measurement_time"),
        col("nhatrang_aqi"),
        col("nhatrang_pm25"),
        col("nhatrang_pm10"),
        col("nhatrang_temperature"),
        col("nhatrang_humidity"),
        col("nhatrang_measurement_time"),
        col("vungtau_aqi"),
        col("vungtau_pm25"),
        col("vungtau_pm10"),
        col("vungtau_temperature"),
        col("vungtau_humidity"),
        col("vungtau_measurement_time"),
        col("join_time"),
        current_timestamp().alias("processed_time")
    )

    query = write_to_database(result_df)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping city comparison streaming job")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
