"""
Tests for src/ingestion/produce_city.py
"""
import os
import sys
import pytest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.ingestion.produce_city import CITY_API_MAPPING, main


class TestCityAPIMapping:
    """Tests for CITY_API_MAPPING constant."""

    def test_cantho_mapping(self):
        """Test Can Tho city mapping."""
        assert CITY_API_MAPPING["cantho"] == "can-tho"

    def test_nhatrang_mapping(self):
        """Test Nha Trang city mapping."""
        assert CITY_API_MAPPING["nhatrang"] == "nha-trang"

    def test_vungtau_mapping(self):
        """Test Vung Tau city mapping."""
        assert CITY_API_MAPPING["vungtau"] == "vung-tau"

    def test_unmapped_cities_not_in_mapping(self):
        """Test that regular cities are not in mapping."""
        assert "hanoi" not in CITY_API_MAPPING
        assert "saigon" not in CITY_API_MAPPING
        assert "danang" not in CITY_API_MAPPING


class TestMain:
    """Tests for main() function."""

    @patch('src.ingestion.produce_city.AirQualityProducer')
    @patch('src.ingestion.produce_city.AirQualityAPIClient')
    @patch('src.ingestion.produce_city.settings')
    def test_main_fetches_and_sends_data(
        self, mock_settings, mock_client_class, mock_producer_class, sample_aqicn_response
    ):
        """Test main() fetches data and sends to Kafka."""
        mock_settings.aqicn_topic = "raw.airquality"
        mock_settings.aqicn_token = "test_token"
        
        # Setup mock client
        mock_client = MagicMock()
        mock_client.fetch.return_value = sample_aqicn_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client
        
        # Setup mock producer
        mock_producer = MagicMock()
        mock_producer.__enter__ = MagicMock(return_value=mock_producer)
        mock_producer.__exit__ = MagicMock(return_value=False)
        mock_producer_class.return_value = mock_producer
        
        main("hanoi")
        
        # Verify client was created with correct params
        mock_client_class.assert_called_once_with(
            base_url="https://api.waqi.info",
            token="test_token"
        )
        
        # Verify fetch was called
        mock_client.fetch.assert_called_with("feed/hanoi/")
        
        # Verify producer was created with correct topic
        mock_producer_class.assert_called_once_with(topic="raw.airquality.hanoi")
        
        # Verify send was called with correct data
        expected_record = {"city": "hanoi", "payload": sample_aqicn_response["data"]}
        mock_producer.send.assert_called_once_with(expected_record, key="hanoi")

    @patch('src.ingestion.produce_city.AirQualityProducer')
    @patch('src.ingestion.produce_city.AirQualityAPIClient')
    @patch('src.ingestion.produce_city.settings')
    def test_main_uses_city_mapping_for_cantho(
        self, mock_settings, mock_client_class, mock_producer_class, sample_aqicn_response
    ):
        """Test main() uses API mapping for Can Tho."""
        mock_settings.aqicn_topic = "raw.airquality"
        mock_settings.aqicn_token = "test_token"
        
        mock_client = MagicMock()
        mock_client.fetch.return_value = sample_aqicn_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client
        
        mock_producer = MagicMock()
        mock_producer.__enter__ = MagicMock(return_value=mock_producer)
        mock_producer.__exit__ = MagicMock(return_value=False)
        mock_producer_class.return_value = mock_producer
        
        main("cantho")
        
        # Verify fetch was called with mapped city name
        mock_client.fetch.assert_called_with("feed/can-tho/")
        
        # Verify producer topic uses original city name
        mock_producer_class.assert_called_once_with(topic="raw.airquality.cantho")
        
        # Verify record uses original city name
        call_args = mock_producer.send.call_args
        assert call_args[0][0]["city"] == "cantho"

    @patch('src.ingestion.produce_city.AirQualityProducer')
    @patch('src.ingestion.produce_city.AirQualityAPIClient')
    @patch('src.ingestion.produce_city.settings')
    def test_main_uses_city_mapping_for_nhatrang(
        self, mock_settings, mock_client_class, mock_producer_class, sample_aqicn_response
    ):
        """Test main() uses API mapping for Nha Trang."""
        mock_settings.aqicn_topic = "raw.airquality"
        mock_settings.aqicn_token = "test_token"
        
        mock_client = MagicMock()
        mock_client.fetch.return_value = sample_aqicn_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client
        
        mock_producer = MagicMock()
        mock_producer.__enter__ = MagicMock(return_value=mock_producer)
        mock_producer.__exit__ = MagicMock(return_value=False)
        mock_producer_class.return_value = mock_producer
        
        main("nhatrang")
        
        mock_client.fetch.assert_called_with("feed/nha-trang/")

    @patch('src.ingestion.produce_city.AirQualityProducer')
    @patch('src.ingestion.produce_city.AirQualityAPIClient')
    @patch('src.ingestion.produce_city.settings')
    def test_main_uses_city_mapping_for_vungtau(
        self, mock_settings, mock_client_class, mock_producer_class, sample_aqicn_response
    ):
        """Test main() uses API mapping for Vung Tau."""
        mock_settings.aqicn_topic = "raw.airquality"
        mock_settings.aqicn_token = "test_token"
        
        mock_client = MagicMock()
        mock_client.fetch.return_value = sample_aqicn_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client
        
        mock_producer = MagicMock()
        mock_producer.__enter__ = MagicMock(return_value=mock_producer)
        mock_producer.__exit__ = MagicMock(return_value=False)
        mock_producer_class.return_value = mock_producer
        
        main("vungtau")
        
        mock_client.fetch.assert_called_with("feed/vung-tau/")

    @patch('src.ingestion.produce_city.AirQualityProducer')
    @patch('src.ingestion.produce_city.AirQualityAPIClient')
    @patch('src.ingestion.produce_city.settings')
    @patch('src.ingestion.produce_city.logger')
    def test_main_retries_on_error(
        self, mock_logger, mock_settings, mock_client_class, mock_producer_class, sample_aqicn_response
    ):
        """Test main() retries on fetch errors."""
        mock_settings.aqicn_topic = "raw.airquality"
        mock_settings.aqicn_token = "test_token"
        
        mock_client = MagicMock()
        # First 2 calls fail, 3rd succeeds
        mock_client.fetch.side_effect = [
            Exception("Connection error"),
            Exception("Timeout"),
            sample_aqicn_response
        ]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client
        
        mock_producer = MagicMock()
        mock_producer.__enter__ = MagicMock(return_value=mock_producer)
        mock_producer.__exit__ = MagicMock(return_value=False)
        mock_producer_class.return_value = mock_producer
        
        main("hanoi")
        
        # Verify fetch was called 3 times
        assert mock_client.fetch.call_count == 3
        
        # Verify error was logged
        assert mock_logger.error.call_count == 2

    @patch('src.ingestion.produce_city.AirQualityProducer')
    @patch('src.ingestion.produce_city.AirQualityAPIClient')
    @patch('src.ingestion.produce_city.settings')
    def test_main_creates_correct_topic_name(
        self, mock_settings, mock_client_class, mock_producer_class, sample_aqicn_response
    ):
        """Test main() creates correct Kafka topic name."""
        mock_settings.aqicn_topic = "custom.topic"
        mock_settings.aqicn_token = "test_token"
        
        mock_client = MagicMock()
        mock_client.fetch.return_value = sample_aqicn_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client
        
        mock_producer = MagicMock()
        mock_producer.__enter__ = MagicMock(return_value=mock_producer)
        mock_producer.__exit__ = MagicMock(return_value=False)
        mock_producer_class.return_value = mock_producer
        
        main("saigon")
        
        mock_producer_class.assert_called_once_with(topic="custom.topic.saigon")


class TestMainAllCities:
    """Test main() with all supported cities."""

    @pytest.mark.parametrize("city,expected_api_city", [
        ("hanoi", "hanoi"),
        ("saigon", "saigon"),
        ("danang", "danang"),
        ("haiphong", "haiphong"),
        ("cantho", "can-tho"),
        ("nhatrang", "nha-trang"),
        ("vungtau", "vung-tau"),
    ])
    @patch('src.ingestion.produce_city.AirQualityProducer')
    @patch('src.ingestion.produce_city.AirQualityAPIClient')
    @patch('src.ingestion.produce_city.settings')
    def test_main_all_cities(
        self, mock_settings, mock_client_class, mock_producer_class,
        city, expected_api_city, sample_aqicn_response
    ):
        """Test main() works for all supported cities."""
        mock_settings.aqicn_topic = "raw.airquality"
        mock_settings.aqicn_token = "test_token"
        
        mock_client = MagicMock()
        mock_client.fetch.return_value = sample_aqicn_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client
        
        mock_producer = MagicMock()
        mock_producer.__enter__ = MagicMock(return_value=mock_producer)
        mock_producer.__exit__ = MagicMock(return_value=False)
        mock_producer_class.return_value = mock_producer
        
        main(city)
        
        mock_client.fetch.assert_called_with(f"feed/{expected_api_city}/")
        mock_producer_class.assert_called_with(topic=f"raw.airquality.{city}")
