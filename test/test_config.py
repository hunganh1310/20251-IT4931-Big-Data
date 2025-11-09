"""Tests for configuration module."""
import pytest
import os
from src.common.config import Settings, get_settings, get_logger


class TestSettings:
    """Test Settings class."""
    
    def test_from_env_with_all_variables(self, mock_env_vars):
        """Test Settings creation with all environment variables."""
        settings = Settings.from_env()
        
        assert settings.aqicn_token == "test_aqicn_token"
        assert settings.openaq_api_key == "test_openaq_key"
        assert settings.kafka_bootstrap_servers == "localhost:29092"
        assert settings.aqicn_topic == "test.aqicn"
        assert settings.openaq_topic == "test.openaq"
        assert settings.db_host == "localhost"
        assert settings.db_port == 5432
        assert settings.db_name == "test_db"
        assert settings.db_user == "test_user"
        assert settings.db_password == "test_password"
        assert settings.minio_endpoint == "localhost:9000"
        assert settings.minio_user == "minioadmin"
        assert settings.minio_password == "minioadmin"
        assert settings.minio_bucket == "test-bucket"
        assert settings.spark_checkpoint_location == "/tmp/test_checkpoints"
    
    def test_from_env_with_defaults(self, monkeypatch):
        """Test Settings creation with default values."""
        # Only set required variables
        monkeypatch.setenv("AQICN_TOKEN", "test_token")
        monkeypatch.setenv("OPENAQ_API_KEY", "test_key")
        
        settings = Settings.from_env()
        
        assert settings.kafka_bootstrap_servers == "localhost:29092"
        assert settings.aqicn_topic == "raw.airquality"
        assert settings.openaq_topic == "raw.openaq"
        assert settings.db_host == "localhost"
        assert settings.db_port == 5432
    
    def test_require_token_success(self, mock_env_vars):
        """Test require_token with valid token."""
        settings = Settings.from_env()
        token = settings.require_token()
        assert token == "test_aqicn_token"
    
    def test_require_token_missing(self, monkeypatch):
        """Test require_token raises error when token is missing."""
        monkeypatch.delenv("AQICN_TOKEN", raising=False)
        monkeypatch.setenv("OPENAQ_API_KEY", "test_key")
        
        settings = Settings.from_env()
        
        with pytest.raises(ValueError, match="AQICN_TOKEN is not configured"):
            settings.require_token()


class TestGetSettings:
    """Test get_settings function."""
    
    def test_get_settings_singleton(self, mock_env_vars):
        """Test that get_settings returns the same instance."""
        settings1 = get_settings()
        settings2 = get_settings()
        
        assert settings1 is settings2


class TestGetLogger:
    """Test get_logger function."""
    
    def test_get_logger_returns_logger(self):
        """Test that get_logger returns a logger instance."""
        logger = get_logger()
        
        assert logger is not None
        assert logger.name == "bigdata"
        assert logger.level == 20  # INFO level
    
    def test_get_logger_singleton(self):
        """Test that get_logger returns the same instance."""
        logger1 = get_logger()
        logger2 = get_logger()
        
        assert logger1 is logger2
