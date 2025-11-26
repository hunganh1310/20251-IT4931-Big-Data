"""
Tests for src/common/config.py
"""
import os
import logging
import pytest
from unittest.mock import patch

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.common.config import Settings, get_settings, get_logger, _build_logger


class TestSettings:
    """Tests for Settings dataclass."""

    def test_settings_from_env_with_all_values(self):
        """Test Settings.from_env() with all environment variables set."""
        with patch.dict(os.environ, {
            "AQICN_TOKEN": "my_test_token",
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "AQICN_TOPIC": "raw.test",
            "DB_HOST": "db.example.com",
            "DB_PORT": "5433",
            "DB_NAME": "testdb",
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpass",
            "MINIO_ENDPOINT": "minio:9000",
            "MINIO_ROOT_USER": "miniouser",
            "MINIO_ROOT_PASSWORD": "miniopass",
            "MINIO_BUCKET": "testbucket",
            "SPARK_CHECKPOINT_LOCATION": "/data/checkpoints"
        }, clear=False):
            settings = Settings.from_env()
            
            assert settings.aqicn_token == "my_test_token"
            assert settings.kafka_bootstrap_servers == "kafka:9092"
            assert settings.aqicn_topic == "raw.test"
            assert settings.db_host == "db.example.com"
            assert settings.db_port == 5433
            assert settings.db_name == "testdb"
            assert settings.db_user == "testuser"
            assert settings.db_password == "testpass"
            assert settings.minio_endpoint == "minio:9000"
            assert settings.minio_user == "miniouser"
            assert settings.minio_password == "miniopass"
            assert settings.minio_bucket == "testbucket"
            assert settings.spark_checkpoint_location == "/data/checkpoints"

    def test_settings_from_env_with_defaults(self):
        """Test Settings.from_env() uses defaults when env vars are missing."""
        with patch.dict(os.environ, {"AQICN_TOKEN": "token123"}, clear=True):
            settings = Settings.from_env()
            
            assert settings.kafka_bootstrap_servers == "localhost:29092"
            assert settings.aqicn_topic == "raw.airquality"
            assert settings.db_host == "localhost"
            assert settings.db_port == 5432
            assert settings.db_name == "airquality"
            assert settings.db_user == "airquality"
            assert settings.db_password == "airquality123"
            assert settings.minio_endpoint == "localhost:9000"
            assert settings.minio_user == "minioadmin"
            assert settings.minio_password == "minioadmin"
            assert settings.minio_bucket == "airquality"
            assert settings.spark_checkpoint_location == "/tmp/spark_checkpoints"

    def test_require_token_with_valid_token(self):
        """Test require_token() returns token when set."""
        settings = Settings(
            aqicn_token="valid_token",
            kafka_bootstrap_servers="localhost:9092",
            aqicn_topic="test",
            db_host="localhost",
            db_port=5432,
            db_name="test",
            db_user="test",
            db_password="test",
            minio_endpoint="localhost:9000",
            minio_user="test",
            minio_password="test",
            minio_bucket="test",
            spark_checkpoint_location="/tmp"
        )
        
        assert settings.require_token() == "valid_token"

    def test_require_token_raises_without_token(self):
        """Test require_token() raises ValueError when token is None."""
        settings = Settings(
            aqicn_token=None,
            kafka_bootstrap_servers="localhost:9092",
            aqicn_topic="test",
            db_host="localhost",
            db_port=5432,
            db_name="test",
            db_user="test",
            db_password="test",
            minio_endpoint="localhost:9000",
            minio_user="test",
            minio_password="test",
            minio_bucket="test",
            spark_checkpoint_location="/tmp"
        )
        
        with pytest.raises(ValueError, match="AQICN_TOKEN is not configured"):
            settings.require_token()

    def test_require_token_raises_with_empty_token(self):
        """Test require_token() raises ValueError when token is empty string."""
        settings = Settings(
            aqicn_token="",
            kafka_bootstrap_servers="localhost:9092",
            aqicn_topic="test",
            db_host="localhost",
            db_port=5432,
            db_name="test",
            db_user="test",
            db_password="test",
            minio_endpoint="localhost:9000",
            minio_user="test",
            minio_password="test",
            minio_bucket="test",
            spark_checkpoint_location="/tmp"
        )
        
        with pytest.raises(ValueError, match="AQICN_TOKEN is not configured"):
            settings.require_token()


class TestLogger:
    """Tests for logger functionality."""

    def test_build_logger_returns_logger(self):
        """Test _build_logger returns a Logger instance."""
        logger = _build_logger("test_logger")
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_logger"
        assert logger.level == logging.INFO

    def test_build_logger_has_handler(self):
        """Test _build_logger adds a StreamHandler."""
        logger = _build_logger("handler_test_logger")
        
        assert len(logger.handlers) >= 1
        assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)

    def test_build_logger_has_formatter(self):
        """Test _build_logger sets proper formatter."""
        logger = _build_logger("formatter_test_logger")
        
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                formatter = handler.formatter
                assert formatter is not None
                # Check format contains expected fields
                format_str = formatter._fmt
                assert "%(asctime)s" in format_str
                assert "%(levelname)s" in format_str
                assert "%(name)s" in format_str
                assert "%(message)s" in format_str

    def test_get_logger_returns_singleton(self):
        """Test get_logger returns the same logger instance."""
        # Note: This test depends on module state
        logger1 = get_logger()
        logger2 = get_logger()
        
        assert logger1 is logger2

    def test_logger_propagate_is_false(self):
        """Test logger does not propagate to parent loggers."""
        logger = _build_logger("propagate_test")
        
        assert logger.propagate is False


class TestGetSettings:
    """Tests for get_settings singleton."""

    def test_get_settings_returns_settings_instance(self):
        """Test get_settings returns Settings instance."""
        with patch.dict(os.environ, {"AQICN_TOKEN": "test"}, clear=False):
            settings = get_settings()
            assert isinstance(settings, Settings)
