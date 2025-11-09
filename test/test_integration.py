"""Integration tests for the complete workflow."""
import pytest
from unittest.mock import patch, MagicMock, Mock
from src.ingestion.api_client import AirQualityAPIClient
from src.ingestion.producer import AirQualityProducer


class TestAQICNIntegration:
    """Integration tests for AQICN workflow."""
    
    @patch('src.ingestion.producer.KafkaProducer')
    @patch('requests.Session')
    def test_fetch_and_send_aqicn_data(
        self,
        mock_session_class,
        mock_producer_class,
        sample_aqicn_response,
        mock_env_vars
    ):
        """Test complete workflow of fetching AQICN data and sending to Kafka."""
        # Setup mocks
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_aqicn_response
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        # Execute workflow
        with AirQualityAPIClient(
            base_url="https://api.waqi.info",
            token="test_token"
        ) as client:
            data = client.fetch("feed/hanoi/")
            
            assert data["status"] == "ok"
            assert "data" in data
            
            with AirQualityProducer(topic="test.aqicn") as producer:
                record = {"city": "hanoi", "payload": data.get("data")}
                producer.send(record, key="hanoi")
        
        # Verify
        mock_session.get.assert_called_once()
        mock_producer.send.assert_called_once()
        mock_producer.flush.assert_called()
        mock_producer.close.assert_called_once()


class TestOpenAQIntegration:
    """Integration tests for OpenAQ workflow."""
    
    @patch('src.ingestion.producer.KafkaProducer')
    @patch('requests.Session')
    def test_fetch_and_send_openaq_data(
        self,
        mock_session_class,
        mock_producer_class,
        sample_openaq_response,
        mock_env_vars
    ):
        """Test complete workflow of fetching OpenAQ data and sending to Kafka."""
        # Setup mocks
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_openaq_response
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        # Execute workflow
        with AirQualityAPIClient(
            base_url="https://api.openaq.org/v3",
            api_key="test_api_key"
        ) as client:
            params = {
                "countries": "VN",
                "parameters": "2",
                "limit": 100
            }
            data = client.fetch("locations", params=params)
            
            assert "results" in data
            assert len(data["results"]) > 0
            
            with AirQualityProducer(topic="test.openaq") as producer:
                record = {
                    "country": "vn",
                    "parameter": "pm25",
                    "payload": data.get("results")
                }
                producer.send(record, key="vn")
        
        # Verify
        mock_session.get.assert_called_once()
        mock_producer.send.assert_called_once()
        mock_producer.flush.assert_called()
        mock_producer.close.assert_called_once()


class TestErrorHandling:
    """Test error handling across components."""
    
    @patch('requests.Session')
    def test_api_error_handling(self, mock_session_class):
        """Test handling of API errors."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.status_code = 500
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        with pytest.raises(Exception):
            with AirQualityAPIClient(
                base_url="https://api.test.com",
                token="test_token"
            ) as client:
                client.fetch("endpoint")
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_producer_connection_error(self, mock_producer_class):
        """Test handling of Kafka connection errors."""
        mock_producer_class.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            producer = AirQualityProducer(topic="test.topic")
