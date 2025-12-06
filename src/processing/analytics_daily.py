import clickhouse_connect

from src.common import logger, settings


CITIES = ["hanoi", "danang", "cantho"]


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


def create_combined_daily_table(client):
    create_query = """
    CREATE TABLE IF NOT EXISTS airquality.daily_aggregates (
        city String,
        date Date,
        aqi_avg Nullable(Float64),
        aqi_max Nullable(Int32),
        aqi_min Nullable(Int32),
        pm25_avg Nullable(Float64),
        pm25_max Nullable(Float64),
        pm25_min Nullable(Float64),
        pm10_avg Nullable(Float64),
        pm10_max Nullable(Float64),
        pm10_min Nullable(Float64),
        temperature_avg Nullable(Float64),
        humidity_avg Nullable(Float64),
        total_records Int64
    ) ENGINE = MergeTree()
    ORDER BY (date, city);
    """
    client.command(create_query)
    logger.info("Combined daily aggregates table created or already exists")


def aggregate_all_cities_daily(client):
    logger.info("Creating daily aggregates for all cities in one query")

    union_parts = []
    for city in CITIES:
        union_parts.append(f"SELECT * FROM airquality.{city}_analytics")

    union_query = " UNION ALL ".join(union_parts)

    create_temp_view_query = f"""
    CREATE OR REPLACE VIEW airquality.daily_temp AS
    SELECT
        city,
        toDate(hour_start) AS date,
        avg(aqi_avg) AS aqi_avg,
        max(aqi_max) AS aqi_max,
        min(aqi_min) AS aqi_min,
        avg(pm25_avg) AS pm25_avg,
        max(pm25_max) AS pm25_max,
        min(pm25_min) AS pm25_min,
        avg(pm10_avg) AS pm10_avg,
        max(pm10_max) AS pm10_max,
        min(pm10_min) AS pm10_min,
        avg(temperature_avg) AS temperature_avg,
        avg(humidity_avg) AS humidity_avg,
        sum(record_count) AS total_records
    FROM (
        {union_query}
    )
    GROUP BY city, date
    """
    client.command(create_temp_view_query)
    logger.info("Temp view created with all cities")

    delete_query = """
    ALTER TABLE airquality.daily_aggregates
    DELETE WHERE (city, date) IN (
        SELECT city, date FROM airquality.daily_temp
    )
    """
    client.command(delete_query)
    logger.info("Deleted overlapping records")

    insert_query = """
    INSERT INTO airquality.daily_aggregates
    SELECT * FROM airquality.daily_temp
    """
    client.command(insert_query)
    logger.info("Inserted daily aggregates")

    drop_view_query = "DROP VIEW IF EXISTS airquality.daily_temp"
    client.command(drop_view_query)
    logger.info("Dropped temp view")


def main():
    logger.info("Starting daily analytics aggregation for all cities")

    client = get_clickhouse_client()

    try:
        create_combined_daily_table(client)
        aggregate_all_cities_daily(client)

        logger.info("Daily analytics aggregation completed for all cities")

    finally:
        client.close()


if __name__ == "__main__":
    main()
