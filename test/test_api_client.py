"""
Tests for src/ingestion/api_client.py
"""
import os
import sys
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.ingestion.api_client import AirQualityAPIClient, APIClientError


class TestAirQualityAPIClient:
    """Tests for AirQualityAPIClient class."""

    def test_init_creates_session(self):
        """Test __init__ creates a requests session."""
        client = AirQualityAPIClient(
            base_url="https://api.example.com",
            token="test_token"
        )
        
        assert client.base_url == "https://api.example.com"
        assert client.token == "test_token"
        assert client._session is not None
        client.close()

    def test_init_with_api_key_sets_header(self):
        """Test __init__ with api_key sets X-API-Key header."""
        client = AirQualityAPIClient(
            base_url="https://api.example.com",
            api_key="my_api_key"
        )
        
        assert client._session.headers.get("X-API-Key") == "my_api_key"
        client.close()

    def test_init_with_custom_timeout(self):
        """Test __init__ with custom timeout."""
        client = AirQualityAPIClient(
            base_url="https://api.example.com",
            timeout=30
        )
        
        assert client.timeout == 30
        client.close()

    def test_init_with_existing_session(self):
        """Test __init__ uses provided session."""
        mock_session = MagicMock()
        
        client = AirQualityAPIClient(
            base_url="https://api.example.com",
            session=mock_session
        )
        
        assert client._session is mock_session

    def test_fetch_success(self, sample_aqicn_response):
        """Test fetch() returns data on successful request."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_aqicn_response
        mock_session.get.return_value = mock_response
        
        client = AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token",
            session=mock_session
        )
        
        result = client.fetch("feed/hanoi/")
        
        assert result == sample_aqicn_response
        mock_session.get.assert_called_once()
        
        # Verify URL and params
        call_args = mock_session.get.call_args
        assert "https://api.waqi.info/feed/hanoi/" in call_args[0][0]
        assert call_args[1]["params"]["token"] == "test_token"

    def test_fetch_with_params(self, sample_aqicn_response):
        """Test fetch() passes additional params."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_aqicn_response
        mock_session.get.return_value = mock_response
        
        client = AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token",
            session=mock_session
        )
        
        result = client.fetch("feed/hanoi/", params={"extra": "param"})
        
        call_args = mock_session.get.call_args
        assert call_args[1]["params"]["extra"] == "param"
        assert call_args[1]["params"]["token"] == "test_token"

    def test_fetch_raises_on_non_200(self):
        """Test fetch() raises APIClientError on non-200 status."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_session.get.return_value = mock_response
        
        client = AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token",
            session=mock_session
        )
        
        with pytest.raises(APIClientError, match="API returned status 500"):
            client.fetch("feed/hanoi/")

    def test_fetch_raises_on_401(self):
        """Test fetch() raises APIClientError on 401 Unauthorized."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_session.get.return_value = mock_response
        
        client = AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="invalid_token",
            session=mock_session
        )
        
        with pytest.raises(APIClientError, match="API returned status 401"):
            client.fetch("feed/hanoi/")

    def test_fetch_raises_on_404(self):
        """Test fetch() raises APIClientError on 404 Not Found."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_session.get.return_value = mock_response
        
        client = AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token",
            session=mock_session
        )
        
        with pytest.raises(APIClientError, match="API returned status 404"):
            client.fetch("feed/unknown_city/")

    def test_fetch_uses_timeout(self, sample_aqicn_response):
        """Test fetch() uses configured timeout."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_aqicn_response
        mock_session.get.return_value = mock_response
        
        client = AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token",
            session=mock_session,
            timeout=15
        )
        
        client.fetch("feed/hanoi/")
        
        call_args = mock_session.get.call_args
        assert call_args[1]["timeout"] == 15

    def test_close_closes_session(self):
        """Test close() closes the session when client created it."""
        client = AirQualityAPIClient(base_url="https://api.example.com")
        
        with patch.object(client._session, 'close') as mock_close:
            client.close()
            mock_close.assert_called_once()

    def test_close_does_not_close_provided_session(self):
        """Test close() does not close externally provided session."""
        mock_session = MagicMock()
        
        client = AirQualityAPIClient(
            base_url="https://api.example.com",
            session=mock_session
        )
        
        client.close()
        mock_session.close.assert_not_called()

    def test_context_manager_enter(self):
        """Test __enter__ returns client instance."""
        client = AirQualityAPIClient(base_url="https://api.example.com")
        
        with client as c:
            assert c is client

    def test_context_manager_exit_closes(self):
        """Test __exit__ calls close()."""
        client = AirQualityAPIClient(base_url="https://api.example.com")
        
        with patch.object(client, 'close') as mock_close:
            with client:
                pass
            mock_close.assert_called_once()


class TestAPIClientIntegration:
    """Integration-style tests for API client."""

    def test_full_workflow_with_context_manager(self, sample_aqicn_response):
        """Test full workflow using context manager."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_aqicn_response
        mock_session.get.return_value = mock_response
        
        with AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token",
            session=mock_session
        ) as client:
            result = client.fetch("feed/hanoi/")
            
            assert result["status"] == "ok"
            assert result["data"]["aqi"] == 85
            assert result["data"]["city"]["name"] == "Hanoi, Vietnam"

    def test_fetch_multiple_cities(self, sample_aqicn_response):
        """Test fetching data for multiple cities."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_aqicn_response
        mock_session.get.return_value = mock_response
        
        cities = ["hanoi", "saigon", "danang"]
        
        with AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token",
            session=mock_session
        ) as client:
            results = []
            for city in cities:
                result = client.fetch(f"feed/{city}/")
                results.append(result)
        
        assert len(results) == 3
        assert mock_session.get.call_count == 3
