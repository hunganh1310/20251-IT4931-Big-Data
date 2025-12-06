from pyspark.sql import SparkSession, DataFrame
import clickhouse_connect

from src.common import logger, settings


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("Hourly Analytics") \
        .master("local[*]") \
        .config("spark.jars.packages",
            "org.postgresql:postgresql:42.6.0,"
            "com.clickhouse:clickhouse-jdbc:0.6.5,"
            "org.apache.httpcomponents.client5:httpclient5:5.3.1,"
            "org.apache.httpcomponents.core5:httpcore5:5.2.1"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        secure=True,
        connect_timeout=60,
        send_receive_timeout=300
    )


def create_staging_table(client, city: str):
    staging_table = f"{city}_staging"
    create_query = f"""
    CREATE TABLE IF NOT EXISTS airquality.{staging_table} (
        city String,
        aqi Nullable(Int32),
        pm25 Nullable(Float64),
        pm10 Nullable(Float64),
        temperature Nullable(Float64),
        humidity Nullable(Float64),
        measurement_time DateTime
    ) ENGINE = MergeTree()
    ORDER BY (city, measurement_time);
    """
    client.command(create_query)
    logger.info(f"Staging table {staging_table} created or already exists")


def create_analytics_table(client, city: str):
    analytics_table = f"{city}_analytics"
    create_query = f"""
    CREATE TABLE IF NOT EXISTS airquality.{analytics_table} (
        city String,
        hour_start DateTime,
        aqi_avg Nullable(Float64),
        aqi_max Nullable(Int32),
        aqi_min Nullable(Int32),
        aqi_stddev Nullable(Float64),
        pm25_avg Nullable(Float64),
        pm25_max Nullable(Float64),
        pm25_min Nullable(Float64),
        pm10_avg Nullable(Float64),
        pm10_max Nullable(Float64),
        pm10_min Nullable(Float64),
        temperature_avg Nullable(Float64),
        humidity_avg Nullable(Float64),
        record_count Int32
    ) ENGINE = MergeTree()
    ORDER BY (city, hour_start);
    """
    client.command(create_query)
    logger.info(f"Analytics table {analytics_table} created or already exists")


def read_from_timescale(spark: SparkSession, city: str) -> DataFrame:
    jdbc_url = f"jdbc:postgresql://{settings.db_host}:{settings.db_port}/{settings.db_name}"
    connection_properties = {
        "user": settings.db_user,
        "password": settings.db_password,
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.jdbc(
        url=jdbc_url,
        table=f"{city}_measurements",
        properties=connection_properties
    )

    df_selected = df.select(
        "city",
        "aqi",
        "pm25",
        "pm10",
        "temperature",
        "humidity",
        "measurement_time"
    )

    return df_selected


def write_to_staging(df: DataFrame, city: str):
    staging_table = f"{city}_staging"
    jdbc_url = f"jdbc:clickhouse:https://{settings.clickhouse_host}:{settings.clickhouse_port}/{settings.clickhouse_db}?ssl=true"

    connection_properties = {
        "user": settings.clickhouse_user,
        "password": settings.clickhouse_password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "ssl": "true"
    }

    logger.info(f"Writing raw data to staging table: {staging_table}")

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", staging_table) \
        .option("driver", connection_properties["driver"]) \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("ssl", connection_properties["ssl"]) \
        .option("isolationLevel", "NONE") \
        .mode("append") \
        .save()

    logger.info(f"Successfully wrote to staging table: {staging_table}")


def aggregate_and_merge(client, city: str):
    staging_table = f"{city}_staging"
    analytics_table = f"{city}_analytics"

    create_temp_view_query = f"""
    CREATE OR REPLACE VIEW airquality.{city}_temp_agg AS
    SELECT
        city,
        toStartOfHour(measurement_time) AS hour_start,
        avg(aqi) AS aqi_avg,
        max(aqi) AS aqi_max,
        min(aqi) AS aqi_min,
        stddevPop(aqi) AS aqi_stddev,
        avg(pm25) AS pm25_avg,
        max(pm25) AS pm25_max,
        min(pm25) AS pm25_min,
        avg(pm10) AS pm10_avg,
        max(pm10) AS pm10_max,
        min(pm10) AS pm10_min,
        avg(temperature) AS temperature_avg,
        avg(humidity) AS humidity_avg,
        count(*) AS record_count
    FROM airquality.{staging_table}
    GROUP BY city, hour_start
    """
    client.command(create_temp_view_query)
    logger.info("Temp view created with aggregations")

    delete_query = f"""
    ALTER TABLE airquality.{analytics_table}
    DELETE WHERE (city, hour_start) IN (
        SELECT city, hour_start FROM airquality.{city}_temp_agg
    )
    """
    client.command(delete_query)
    logger.info("Deleted overlapping records")
    
    insert_query = f"""
    INSERT INTO airquality.{analytics_table}
    SELECT * FROM airquality.{city}_temp_agg
    """
    client.command(insert_query)
    logger.info("Inserted aggregated data")

    drop_view_query = f"DROP VIEW IF EXISTS airquality.{city}_temp_agg"
    client.command(drop_view_query)
    logger.info("Dropped temp view")

    drop_staging_query = f"DROP TABLE IF EXISTS airquality.{staging_table}"
    client.command(drop_staging_query)
    logger.info(f"Dropped staging table: {staging_table}")


def main(city: str):
    logger.info(f"Starting hourly analytics for {city.upper()}")

    client = get_clickhouse_client()

    try:
        create_staging_table(client, city)
        create_analytics_table(client, city)

        spark = get_spark_session()
        df_timescale = read_from_timescale(spark, city)
        write_to_staging(df_timescale, city)
        spark.stop()

        aggregate_and_merge(client, city)

        logger.info(f"Hourly analytics completed for {city.upper()}")

    finally:
        client.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        city = sys.argv[1]
    else:
        city = "hanoi"
    main(city)
