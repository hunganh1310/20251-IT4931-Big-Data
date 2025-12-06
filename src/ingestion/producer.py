import json
from typing import Any, Dict, Optional

from kafka import KafkaProducer

from src.common import logger, settings


class AirQualityProducer:
    def __init__(self, topic: str, bootstrap_servers: Optional[str] = None) -> None:
        server = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic

        kafka_config = {
            "bootstrap_servers": server,
            "value_serializer": lambda value: json.dumps(value).encode("utf-8"),
            "key_serializer": lambda key: key.encode("utf-8") if key else None,
        }

        if settings.kafka_security_protocol != "PLAINTEXT":
            kafka_config["security_protocol"] = settings.kafka_security_protocol
            kafka_config["sasl_mechanism"] = settings.kafka_sasl_mechanism
            kafka_config["sasl_plain_username"] = settings.kafka_sasl_username
            kafka_config["sasl_plain_password"] = settings.kafka_sasl_password

        self._producer = KafkaProducer(**kafka_config)

    def send(self, payload: Dict[str, Any], key: Optional[str] = None) -> None:
        self._producer.send(self.topic, value=payload, key=key)
        logger.info("Queued message for topic %s", self.topic)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()

    def __enter__(self) -> "AirQualityProducer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
