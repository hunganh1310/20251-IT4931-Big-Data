
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
    parser = argparse.ArgumentParser(description="Fetch OpenAQ data and emit to Kafka.")
    parser.add_argument("--country", default="vn", help="Country code.")
    parser.add_argument("--parameter", default="pm25", help="Parameter to fetch.")
    parser.add_argument("--limit", type=int, default=100, help="Number of locations.")
    parser.add_argument("--topic", default=None, help="Kafka topic name.")
    parser.add_argument(
        "--no-kafka",
        action="store_true",
        help="Fetch and display data without sending to Kafka.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    topic = args.topic or settings.openaq_topic

    parameter_ids = {"pm25": "2", "pm10": "1", "no2": "3", "o3": "5"}
    param_id = parameter_ids.get(args.parameter, "2")

    with AirQualityAPIClient(
        base_url="https://api.openaq.org/v3",
        api_key=settings.openaq_api_key
    ) as client:
        endpoint = "locations"
        params = {
            "countries": args.country.upper(),
            "parameters": param_id,
            "limit": args.limit
        }
        data = client.fetch(endpoint, params=params)

        record = {
            "country": args.country,
            "parameter": args.parameter,
            "data": data
        }
        logger.info("Fetched OpenAQ data for %s", args.country)
        print(json.dumps(record, indent=2))

        if args.no_kafka:
            return

        with AirQualityProducer(topic=topic) as producer:
            producer.send(record, key=args.country)
            logger.info("Sent record to Kafka topic %s", topic)


if __name__ == "__main__":
    try:
        while True:
            try:
                logger.info("Calling OpenAQ API")
                main()
                logger.info("Sleeping for 10 seconds")
                time.sleep(10)
            except Exception as e:
                logger.error(f"Error in producer loop: {e}")
                time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
