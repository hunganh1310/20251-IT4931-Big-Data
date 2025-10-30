
import argparse
import json
import os
import sys
import time

current_dir = os.getcwd()
sys.path.append(current_dir)

from src.common import logger, settings
from src.ingestion.api_client import AirQualityAPIClient
from src.ingestion.producer import AirQualityProducer

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch AQICN data and emit to Kafka.")
    parser.add_argument("--city", default="hanoi", help="City feed to fetch.")
    parser.add_argument("--topic", default=None, help="Kafka topic name.")
    parser.add_argument(
        "--no-kafka",
        action="store_true",
        help="Fetch and display data without sending to Kafka.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    topic = args.topic or settings.aqicn_topic

    with AirQualityAPIClient(
        base_url="https://api.waqi.info",
        token=settings.aqicn_token
    ) as client:
        endpoint = f"feed/{args.city}/"
        data = client.fetch(endpoint)

        if data.get("status") != "ok":
            logger.error("AQICN API error: %s", data)
            return

        record = {"city": args.city, "payload": data.get("data")}
        logger.info("Fetched data for %s", args.city)
        print(json.dumps(record, indent=2))

        if args.no_kafka:
            return

        with AirQualityProducer(topic=topic) as producer:
            producer.send(record, key=args.city)
            logger.info("Sent record to Kafka topic %s", topic)


if __name__ == "__main__":
    try:
        while True:
            try:
                logger.info("Calling AQICN API")
                main()
                logger.info("Sleeping for 10 seconds")
                time.sleep(10)
            except Exception as e:
                logger.error(f"Error in producer loop: {e}")
                time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
