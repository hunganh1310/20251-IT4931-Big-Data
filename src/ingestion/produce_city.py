from src.common import logger, settings
from src.ingestion.api_client import AirQualityAPIClient
from src.ingestion.producer import AirQualityProducer


CITY_API_MAPPING = {
    "cantho": "can-tho",
    "nhatrang": "nha-trang",
    "vungtau": "vung-tau",
}


def main(city: str) -> None:
    topic = f"{settings.aqicn_topic}.{city}"
    api_city = CITY_API_MAPPING.get(city, city)

    with AirQualityAPIClient(
        base_url="https://api.waqi.info",
        token=settings.aqicn_token
    ) as client:
        endpoint = f"feed/{api_city}/"

        try_outs = 5

        for i in range(try_outs):
            try:
                data = client.fetch(endpoint)
                break
            except Exception as e:
                logger.error("Error while fetching %s: %s. Retrying (%d/%d)", endpoint, e, i+1, try_outs)

        record = {"city": city, "payload": data.get("data")}
        logger.info("Fetched data for %s", city.upper())

        with AirQualityProducer(topic=topic) as producer:
            producer.send(record, key=city)
            logger.info("Sent record to Kafka topic %s for %s", topic, city)