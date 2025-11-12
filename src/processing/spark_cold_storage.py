from src.common import settings, logger
from minio import Minio
from datetime import datetime
import io

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("Cold-archiver") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_minio_client() -> Minio:
    client = Minio(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_user,
        secret_key=settings.minio_password,
        secure=False
    )

    bucket_name = settings.minio_bucket
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Created bucket: {bucket_name}")

    return client


def read_from_kafka(spark: SparkSession, city: str) -> DataFrame:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", f"{settings.aqicn_topic}.{city}") \
        .option("startingOffsets", "earliest") \
        .load()

    df_raw = df.select(
        col("value").cast("string").alias("json_data"),
        col("timestamp").alias("kafka_timestamp"),
        col("offset").alias("kafka_offset")
    )

    return df_raw


def write_batch_to_minio(batch_df: DataFrame, batch_id: int, city: str):
    if batch_df.isEmpty():
        return

    client = create_minio_client()

    rows = batch_df.collect()

    for row in rows:
        try:
            timestamp_ms = row['kafka_timestamp']
            current_date = datetime.fromtimestamp(timestamp_ms / 1000)

            object_name = (
                f"aqicn/{city}/{current_date.year}/{current_date.month:02d}/"
                f"{current_date.day:02d}/batch_{batch_id}_offset_{row['kafka_offset']}.json"
            )
            json_data = row['json_data']
            data_bytes = json_data.encode('utf-8')
            data_stream = io.BytesIO(data_bytes)

            client.put_object(
                bucket_name=settings.minio_bucket,
                object_name=object_name,
                data=data_stream,
                length=len(data_bytes),
                content_type='application/json'
            )

            logger.info(f"Archived batch {batch_id} offset {row['kafka_offset']} to {object_name}")

        except Exception as e:
            logger.error(f"Error archiving batch {batch_id} offset {row['kafka_offset']}: {e}")
            continue


def write_to_minio(df: DataFrame, city: str):
    def write_batch(batch_df: DataFrame, batch_id: int):
        write_batch_to_minio(batch_df, batch_id, city)

    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", f"{settings.spark_checkpoint_location}/cold_storage_{city}") \
        .trigger(processingTime="30 seconds") \
        .start()

    return query


def main(city: str):
    logger.info(f"Starting cold storage archiver for {city.upper()}")

    spark = get_spark_session()
    df_raw = read_from_kafka(spark, city)
    query = write_to_minio(df_raw, city)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info(f"Stopping cold storage archiver for {city}")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        city = sys.argv[1]
    else:
        city = "hanoi"  # default
    main(city)
