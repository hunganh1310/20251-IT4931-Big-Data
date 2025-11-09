"""Tests for Kafka producer module."""
import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from src.ingestion.producer import AirQualityProducer


class TestAirQualityProducer:
    """Test AirQualityProducer class."""
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_init_with_default_bootstrap_servers(self, mock_producer_class, mock_env_vars):
        """Test producer initialization with default bootstrap servers."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        producer = AirQualityProducer(topic="test.topic")
        
        assert producer.topic == "test.topic"
        mock_producer_class.assert_called_once()
        call_kwargs = mock_producer_class.call_args[1]
        assert call_kwargs["bootstrap_servers"] == "localhost:29092"
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_init_with_custom_bootstrap_servers(self, mock_producer_class):
        """Test producer initialization with custom bootstrap servers."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="custom:9092"
        )
        
        call_kwargs = mock_producer_class.call_args[1]
        assert call_kwargs["bootstrap_servers"] == "custom:9092"
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_send_message_without_key(self, mock_producer_class):
        """Test sending message without key."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        producer = AirQualityProducer(topic="test.topic")
        payload = {"city": "hanoi", "aqi": 55}
        
        producer.send(payload)
        
        mock_producer.send.assert_called_once_with(
            "test.topic",
            value=payload,
            key=None
        )
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_send_message_with_key(self, mock_producer_class):
        """Test sending message with key."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        producer = AirQualityProducer(topic="test.topic")
        payload = {"city": "hanoi", "aqi": 55}
        
        producer.send(payload, key="hanoi")
        
        mock_producer.send.assert_called_once_with(
            "test.topic",
            value=payload,
            key="hanoi"
        )
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_value_serializer(self, mock_producer_class):
        """Test that value serializer is properly configured."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        producer = AirQualityProducer(topic="test.topic")
        
        call_kwargs = mock_producer_class.call_args[1]
        value_serializer = call_kwargs["value_serializer"]
        
        # Test serializer
        test_data = {"test": "data"}
        result = value_serializer(test_data)
        expected = json.dumps(test_data).encode("utf-8")
        
        assert result == expected
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_key_serializer(self, mock_producer_class):
        """Test that key serializer is properly configured."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        producer = AirQualityProducer(topic="test.topic")
        
        call_kwargs = mock_producer_class.call_args[1]
        key_serializer = call_kwargs["key_serializer"]
        
        # Test serializer with string
        result = key_serializer("test_key")
        assert result == b"test_key"
        
        # Test serializer with None
        result = key_serializer(None)
        assert result is None
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_flush(self, mock_producer_class):
        """Test flush method."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        producer = AirQualityProducer(topic="test.topic")
        producer.flush()
        
        mock_producer.flush.assert_called_once()
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_close(self, mock_producer_class):
        """Test close method."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        producer = AirQualityProducer(topic="test.topic")
        producer.close()
        
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_context_manager(self, mock_producer_class):
        """Test using producer as context manager."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        with AirQualityProducer(topic="test.topic") as producer:
            assert producer is not None
            payload = {"test": "data"}
            producer.send(payload)
        
        mock_producer.flush.assert_called()
        mock_producer.close.assert_called_once()
    
    @patch('src.ingestion.producer.KafkaProducer')
    def test_multiple_sends(self, mock_producer_class):
        """Test sending multiple messages."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        producer = AirQualityProducer(topic="test.topic")
        
        messages = [
            {"city": "hanoi", "aqi": 55},
            {"city": "hcm", "aqi": 65},
            {"city": "danang", "aqi": 45}
        ]
        
        for msg in messages:
            producer.send(msg, key=msg["city"])
        
        assert mock_producer.send.call_count == 3
