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
    aqicn_topic: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    minio_endpoint: str
    minio_user: str
    minio_password: str
    minio_bucket: str
    spark_checkpoint_location: str

    @classmethod
    def from_env(cls) -> "Settings":
        aqicn_token = os.getenv("AQICN_TOKEN")
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        aqicn_topic = os.getenv("AQICN_TOPIC", "raw.airquality")
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = int(os.getenv("DB_PORT", "5432"))
        db_name = os.getenv("DB_NAME", "airquality")
        db_user = os.getenv("DB_USER", "airquality")
        db_password = os.getenv("DB_PASSWORD", "airquality123")
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
        minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        minio_bucket = os.getenv("MINIO_BUCKET", "airquality")
        spark_checkpoint_location = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/spark_checkpoints")
        return cls(
            aqicn_token=aqicn_token,
            kafka_bootstrap_servers=bootstrap,
            aqicn_topic=aqicn_topic,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            minio_endpoint=minio_endpoint,
            minio_user=minio_user,
            minio_password=minio_password,
            minio_bucket=minio_bucket,
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
