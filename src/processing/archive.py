from src.common import settings, logger
import boto3
import json
from datetime import datetime

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


def create_s3_client():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region
    )

    try:
        s3_client.head_bucket(Bucket=settings.aws_s3_bucket)
        logger.info(f"S3 bucket {settings.aws_s3_bucket} exists")
    except:
        logger.warning(f"S3 bucket {settings.aws_s3_bucket} not accessible or does not exist")

    return s3_client


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

    df_raw = df.select(
        col("value").cast("string").alias("json_data"),
        col("timestamp").alias("kafka_timestamp"),
        col("offset").alias("kafka_offset")
    )

    return df_raw


def write_batch_to_s3(batch_df: DataFrame, batch_id: int, city: str):
    if batch_df.isEmpty():
        return

    s3_client = create_s3_client()
    rows = batch_df.collect()

    if not rows:
        return

    try:
        kafka_ts = rows[0]['kafka_timestamp']
        current_date = kafka_ts if isinstance(kafka_ts, datetime) else datetime.fromtimestamp(kafka_ts.timestamp())

        object_key = (
            f"aqicn/{city}/{current_date.year}/{current_date.month:02d}/"
            f"{current_date.day:02d}/batch_{batch_id}.json"
        )

        batch_data = [row['json_data'] for row in rows]
        batch_json = json.dumps(batch_data, indent=2)

        s3_client.put_object(
            Bucket=settings.aws_s3_bucket,
            Key=object_key,
            Body=batch_json.encode('utf-8'),
            ContentType='application/json'
        )

        logger.info(f"Archived batch {batch_id} with {len(rows)} records to s3://{settings.aws_s3_bucket}/{object_key}")

    except Exception as e:
        logger.error(f"Error archiving batch {batch_id}: {e}")
        raise


def write_to_s3(df: DataFrame, city: str):
    def write_batch(batch_df: DataFrame, batch_id: int):
        write_batch_to_s3(batch_df, batch_id, city)

    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", f"{settings.spark_checkpoint_location}/cold_storage_{city}") \
        .trigger(processingTime="30 minutes") \
        .start()

    return query


def main(city: str):
    logger.info(f"Starting S3 archiver for {city.upper()}")

    spark = get_spark_session()
    df_raw = read_from_kafka(spark, city)
    query = write_to_s3(df_raw, city)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info(f"Stopping S3 archiver for {city}")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        city = sys.argv[1]
    else:
        city = "hanoi"  # default
    main(city)
