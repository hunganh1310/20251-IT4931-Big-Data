from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, explode, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from ..common import logger, settings


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("OpenAQ Streaming") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_schema() -> StructType:
    return StructType([
        StructField("country", StringType(), True),
        StructField("parameter", StringType(), True),
        StructField("data", StructType([
            StructField("meta", StructType([
                StructField("name", StringType(), True),
                StructField("page", IntegerType(), True),
                StructField("limit", IntegerType(), True),
                StructField("found", StringType(), True)
            ]), True),
            StructField("results", ArrayType(
                StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("locality", StringType(), True),
                    StructField("timezone", StringType(), True),
                    StructField("country", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("code", StringType(), True),
                        StructField("name", StringType(), True)
                    ]), True),
                    StructField("isMobile", BooleanType(), True),
                    StructField("isMonitor", BooleanType(), True),
                    StructField("sensors", ArrayType(
                        StructType([
                            StructField("id", IntegerType(), True),
                            StructField("name", StringType(), True),
                            StructField("parameter", StructType([
                                StructField("id", IntegerType(), True),
                                StructField("name", StringType(), True),
                                StructField("units", StringType(), True),
                                StructField("displayName", StringType(), True)
                            ]), True)
                        ])
                    ), True),
                    StructField("coordinates", StructType([
                        StructField("latitude", DoubleType(), True),
                        StructField("longitude", DoubleType(), True)
                    ]), True),
                    StructField("datetimeFirst", StructType([
                        StructField("utc", StringType(), True),
                        StructField("local", StringType(), True)
                    ]), True),
                    StructField("datetimeLast", StructType([
                        StructField("utc", StringType(), True),
                        StructField("local", StringType(), True)
                    ]), True)
                ])
            ), True)
        ]), True)
    ])


def read_from_kafka(spark: SparkSession) -> DataFrame:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", settings.openaq_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    return df


def parse_data(df_raw: DataFrame, schema: StructType) -> DataFrame:
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    df_exploded = df_parsed.select(
        col("data.country").alias("query_country"),
        col("data.parameter").alias("query_parameter"),
        explode(col("data.data.results")).alias("location"),
        col("kafka_timestamp")
    )

    df_clean = df_exploded.select(
        col("location.id").alias("location_id"),
        col("location.name").alias("location_name"),
        col("location.locality").alias("locality"),
        col("location.country.code").alias("country_code"),
        col("location.country.name").alias("country_name"),
        col("location.timezone").alias("timezone"),
        col("location.isMobile").alias("is_mobile"),
        col("location.isMonitor").alias("is_monitor"),
        col("location.coordinates.latitude").alias("latitude"),
        col("location.coordinates.longitude").alias("longitude"),
        to_timestamp(col("location.datetimeFirst.utc")).alias("first_measurement"),
        to_timestamp(col("location.datetimeLast.utc")).alias("last_measurement"),
        col("query_country"),
        col("query_parameter"),
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
        CREATE TABLE IF NOT EXISTS openaq_locations (
            id SERIAL,
            location_id INTEGER,
            location_name TEXT,
            locality TEXT,
            country_code TEXT,
            country_name TEXT,
            timezone TEXT,
            is_mobile BOOLEAN,
            is_monitor BOOLEAN,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            first_measurement TIMESTAMP,
            last_measurement TIMESTAMP,
            query_country TEXT,
            query_parameter TEXT,
            kafka_timestamp TIMESTAMP,
            processed_time TIMESTAMP NOT NULL,
            PRIMARY KEY (id, processed_time)
        );
        """
        cursor.execute(create_table_query)

        cursor.execute(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'openaq_locations' AND schemaname = 'public');"
        )
        table_exists = cursor.fetchone()[0]

        if table_exists:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'openaq_locations'
                );
                """
            )
            is_hypertable = cursor.fetchone()[0]

            if not is_hypertable:
                cursor.execute(
                    "SELECT create_hypertable('openaq_locations', 'processed_time', if_not_exists => TRUE);"
                )
                logger.info("Created hypertable for openaq_locations")

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
        .jdbc(url=jdbc_url, table="openaq_locations", mode="append", properties=connection_properties)

    logger.info(f"Batch {batch_id}: Wrote {batch_df.count()} records to PostgreSQL")


def write_to_database(df: DataFrame):
    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .trigger(processingTime="10 seconds") \
        .start()

    return query


def main():
    logger.info("Starting OpenAQ Spark Streaming job")

    create_table()

    spark = get_spark_session()
    schema = define_schema()
    df_raw = read_from_kafka(spark)
    df_parsed = parse_data(df_raw, schema)

    query = write_to_database(df_parsed)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping OpenAQ streaming job")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
