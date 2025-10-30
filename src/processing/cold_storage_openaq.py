from ..common import settings, logger
from minio import Minio
from kafka import KafkaConsumer
from datetime import datetime
import json
import io


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


def create_kafka_consumer() -> KafkaConsumer:
    consumer = KafkaConsumer(
        settings.openaq_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="cold-archiver",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    return consumer


def push_raw_json_minio(message, client: Minio):
    current_date = datetime.fromtimestamp(message.timestamp / 1000)

    object_name = f"openaq/{current_date.year}/{current_date.month:02d}/{current_date.day:02d}/{message.offset}.json"

    data_bytes = json.dumps(message.value, indent=2).encode('utf-8')
    data_stream = io.BytesIO(data_bytes)

    client.put_object(
        bucket_name=settings.minio_bucket,
        object_name=object_name,
        data=data_stream,
        length=len(data_bytes),
        content_type='application/json'
    )

    logger.info(f"Archived message offset {message.offset} to {object_name}")


def main():
    logger.info("Starting cold storage archiver for OPENAQ")

    client = create_minio_client()
    consumer = create_kafka_consumer()

    try:
        for message in consumer:
            try:
                push_raw_json_minio(message, client)
            except Exception as e:
                logger.error(f"Error archiving message {message.offset}: {e}")
                continue
    except KeyboardInterrupt:
        logger.info("Stopping cold storage archiver")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
