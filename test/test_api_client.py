"""Tests for API client module."""
import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from src.ingestion.api_client import AirQualityAPIClient, APIClientError


class TestAirQualityAPIClient:
    """Test AirQualityAPIClient class."""
    
    def test_init_with_token(self):
        """Test client initialization with token."""
        client = AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token"
        )
        
        assert client.base_url == "https://api.waqi.info"
        assert client.token == "test_token"
        assert client.api_key is None
        assert client._session is not None
    
    def test_init_with_api_key(self):
        """Test client initialization with API key."""
        client = AirQualityAPIClient(
            base_url="https://api.openaq.org/v3",
            api_key="test_api_key"
        )
        
        assert client.base_url == "https://api.openaq.org/v3"
        assert client.api_key == "test_api_key"
        assert client.token is None
        assert "X-API-Key" in client._session.headers
        assert client._session.headers["X-API-Key"] == "test_api_key"
    
    @patch('requests.Session')
    def test_fetch_success_with_token(self, mock_session_class, sample_aqicn_response):
        """Test successful fetch with token."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_aqicn_response
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token"
        )
        
        result = client.fetch("feed/hanoi/")
        
        assert result == sample_aqicn_response
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert "token" in call_args[1]["params"]
        assert call_args[1]["params"]["token"] == "test_token"
    
    @patch('requests.Session')
    def test_fetch_success_with_api_key(self, mock_session_class, sample_openaq_response):
        """Test successful fetch with API key."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_openaq_response
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = AirQualityAPIClient(
            base_url="https://api.openaq.org/v3",
            api_key="test_api_key"
        )
        
        result = client.fetch("locations", params={"country": "VN"})
        
        assert result == sample_openaq_response
        mock_session.get.assert_called_once()
    
    @patch('requests.Session')
    def test_fetch_with_params(self, mock_session_class):
        """Test fetch with additional parameters."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = AirQualityAPIClient(
            base_url="https://api.test.com",
            token="test_token"
        )
        
        params = {"country": "VN", "limit": 100}
        result = client.fetch("endpoint", params=params)
        
        call_args = mock_session.get.call_args
        assert call_args[1]["params"]["country"] == "VN"
        assert call_args[1]["params"]["limit"] == 100
        assert call_args[1]["params"]["token"] == "test_token"
    
    @patch('requests.Session')
    def test_fetch_api_error(self, mock_session_class):
        """Test fetch with API error response."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.status_code = 404
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = AirQualityAPIClient(
            base_url="https://api.test.com",
            token="test_token"
        )
        
        with pytest.raises(APIClientError, match="API returned status 404"):
            client.fetch("invalid_endpoint")
    
    @patch('requests.Session')
    def test_fetch_timeout(self, mock_session_class):
        """Test fetch with custom timeout."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = AirQualityAPIClient(
            base_url="https://api.test.com",
            token="test_token",
            timeout=30
        )
        
        client.fetch("endpoint")
        
        call_args = mock_session.get.call_args
        assert call_args[1]["timeout"] == 30
    
    @patch('requests.Session')
    def test_context_manager(self, mock_session_class):
        """Test using client as context manager."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        with AirQualityAPIClient(
            base_url="https://api.test.com",
            token="test_token"
        ) as client:
            assert client is not None
        
        mock_session.close.assert_called_once()
    
    @patch('requests.Session')
    def test_close(self, mock_session_class):
        """Test client close method."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        client = AirQualityAPIClient(
            base_url="https://api.test.com",
            token="test_token"
        )
        client.close()
        
        mock_session.close.assert_called_once()
