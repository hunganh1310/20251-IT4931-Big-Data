import logging
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

# Load .env from project root
load_dotenv()


def _build_logger(name: str = "bigdata") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


@dataclass
class Settings:
    aqicn_token: str
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    aqicn_topic: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_db: str
    clickhouse_user: str
    clickhouse_password: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_s3_bucket: str
    aws_region: str
    spark_checkpoint_location: str

    @classmethod
    def from_env(cls) -> "Settings":
        aqicn_token = os.getenv("AQICN_TOKEN")
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        kafka_security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        kafka_sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
        kafka_sasl_username = os.getenv("KAFKA_SASL_USERNAME", "")
        kafka_sasl_password = os.getenv("KAFKA_SASL_PASSWORD", "")
        aqicn_topic = os.getenv("AQICN_TOPIC", "raw.airquality")
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = int(os.getenv("DB_PORT", "5432"))
        db_name = os.getenv("DB_NAME", "airquality")
        db_user = os.getenv("DB_USER", "airquality")
        db_password = os.getenv("DB_PASSWORD", "airquality123")
        clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
        clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        clickhouse_db = os.getenv("CLICKHOUSE_DB", "airquality")
        clickhouse_user = os.getenv("CLICKHOUSE_USER", "airquality")
        clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "airquality123")
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        aws_s3_bucket = os.getenv("AWS_S3_BUCKET", "airquality-archive")
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        spark_checkpoint_location = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/spark_checkpoints")
        return cls(
            aqicn_token=aqicn_token,
            kafka_bootstrap_servers=bootstrap,
            kafka_security_protocol=kafka_security_protocol,
            kafka_sasl_mechanism=kafka_sasl_mechanism,
            kafka_sasl_username=kafka_sasl_username,
            kafka_sasl_password=kafka_sasl_password,
            aqicn_topic=aqicn_topic,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            clickhouse_host=clickhouse_host,
            clickhouse_port=clickhouse_port,
            clickhouse_db=clickhouse_db,
            clickhouse_user=clickhouse_user,
            clickhouse_password=clickhouse_password,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_s3_bucket=aws_s3_bucket,
            aws_region=aws_region,
            spark_checkpoint_location=spark_checkpoint_location
        )

    def require_token(self) -> str:
        if not self.aqicn_token:
            raise ValueError("AQICN_TOKEN is not configured.")
        return self.aqicn_token


_settings: Optional[Settings] = None
_logger: Optional[logging.Logger] = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
    return _settings


def get_logger() -> logging.Logger:
    global _logger
    if _logger is None:
        _logger = _build_logger()
    return _logger
