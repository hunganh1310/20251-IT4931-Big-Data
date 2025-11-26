"""
Pytest configuration and fixtures for air quality monitoring tests.
"""
import json
import os
import pytest
from unittest.mock import MagicMock, patch


# Sample API response data for AQICN
@pytest.fixture
def sample_aqicn_response():
    """Sample successful AQICN API response."""
    return {
        "status": "ok",
        "data": {
            "aqi": 85,
            "idx": 5773,
            "dominentpol": "pm25",
            "iaqi": {
                "pm25": {"v": 85.0},
                "pm10": {"v": 45.0},
                "o3": {"v": 12.0},
                "no2": {"v": 8.0},
                "t": {"v": 28.5},
                "h": {"v": 75.0}
            },
            "time": {
                "s": "2025-11-26 10:00:00",
                "tz": "+07:00",
                "v": 1732608000
            },
            "city": {
                "name": "Hanoi, Vietnam",
                "geo": [21.0285, 105.8542]
            }
        }
    }


@pytest.fixture
def sample_aqicn_error_response():
    """Sample error AQICN API response."""
    return {
        "status": "error",
        "data": "Invalid key"
    }


@pytest.fixture
def sample_kafka_message():
    """Sample Kafka message payload."""
    return {
        "city": "hanoi",
        "payload": {
            "aqi": 85,
            "idx": 5773,
            "dominentpol": "pm25",
            "iaqi": {
                "pm25": {"v": 85.0},
                "pm10": {"v": 45.0},
                "t": {"v": 28.5},
                "h": {"v": 75.0}
            },
            "time": {"s": "2025-11-26 10:00:00"}
        }
    }


@pytest.fixture
def mock_settings():
    """Mock settings for testing."""
    with patch.dict(os.environ, {
        "AQICN_TOKEN": "test_token_12345",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "AQICN_TOPIC": "test.airquality",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "test_airquality",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_password",
        "MINIO_ENDPOINT": "localhost:9000",
        "MINIO_ROOT_USER": "testadmin",
        "MINIO_ROOT_PASSWORD": "testpassword",
        "MINIO_BUCKET": "test-bucket",
        "SPARK_CHECKPOINT_LOCATION": "/tmp/test_checkpoints"
    }):
        yield


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing."""
    mock_producer = MagicMock()
    mock_producer.send.return_value = MagicMock()
    mock_producer.flush.return_value = None
    mock_producer.close.return_value = None
    return mock_producer


@pytest.fixture
def mock_requests_session():
    """Mock requests session for API testing."""
    mock_session = MagicMock()
    return mock_session


# Environment setup for all tests
@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch):
    """Set up test environment variables."""
    monkeypatch.setenv("AQICN_TOKEN", "test_token")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("AQICN_TOPIC", "test.airquality")
