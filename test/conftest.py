"""Pytest configuration and shared fixtures."""
import os
import pytest
from unittest.mock import Mock, MagicMock
from kafka import KafkaProducer
import requests


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing."""
    env_vars = {
        "AQICN_TOKEN": "test_aqicn_token",
        "OPENAQ_API_KEY": "test_openaq_key",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092",
        "AQICN_TOPIC": "test.aqicn",
        "OPENAQ_TOPIC": "test.openaq",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "test_db",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_password",
        "MINIO_ENDPOINT": "localhost:9000",
        "MINIO_ROOT_USER": "minioadmin",
        "MINIO_ROOT_PASSWORD": "minioadmin",
        "MINIO_BUCKET": "test-bucket",
        "SPARK_CHECKPOINT_LOCATION": "/tmp/test_checkpoints"
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


@pytest.fixture
def mock_kafka_producer(monkeypatch):
    """Mock Kafka producer."""
    mock_producer = MagicMock(spec=KafkaProducer)
    mock_producer.send.return_value = Mock()
    mock_producer.flush.return_value = None
    mock_producer.close.return_value = None
    
    def mock_init(*args, **kwargs):
        return mock_producer
    
    monkeypatch.setattr("kafka.KafkaProducer", lambda *args, **kwargs: mock_producer)
    return mock_producer


@pytest.fixture
def mock_requests_session(monkeypatch):
    """Mock requests.Session for API calls."""
    mock_session = MagicMock(spec=requests.Session)
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "ok", "data": {"aqi": 50}}
    mock_session.get.return_value = mock_response
    
    monkeypatch.setattr("requests.Session", lambda: mock_session)
    return mock_session


@pytest.fixture
def sample_aqicn_response():
    """Sample AQICN API response."""
    return {
        "status": "ok",
        "data": {
            "aqi": 55,
            "idx": 1437,
            "attributions": [],
            "city": {
                "geo": [21.0278, 105.8342],
                "name": "Hanoi",
                "url": "https://aqicn.org/city/hanoi"
            },
            "dominentpol": "pm25",
            "iaqi": {
                "pm25": {"v": 55},
                "pm10": {"v": 45},
                "t": {"v": 25.5}
            },
            "time": {
                "s": "2025-11-07 10:00:00",
                "tz": "+07:00"
            }
        }
    }


@pytest.fixture
def sample_openaq_response():
    """Sample OpenAQ API response."""
    return {
        "meta": {
            "found": 100,
            "limit": 100,
            "page": 1
        },
        "results": [
            {
                "id": 12345,
                "name": "Hanoi Station",
                "locality": "Hanoi",
                "country": "VN",
                "coordinates": {
                    "latitude": 21.0278,
                    "longitude": 105.8342
                },
                "parameters": [
                    {
                        "id": 2,
                        "name": "pm25",
                        "displayName": "PM2.5",
                        "unit": "µg/m³",
                        "lastValue": 55.5,
                        "lastUpdated": "2025-11-07T10:00:00Z"
                    }
                ]
            }
        ]
    }
