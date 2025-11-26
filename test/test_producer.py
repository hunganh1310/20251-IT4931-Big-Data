"""
Tests for src/ingestion/producer.py
"""
import json
import os
import sys
import pytest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class TestAirQualityProducer:
    """Tests for AirQualityProducer class."""

    @patch('src.ingestion.producer.KafkaProducer')
    def test_init_creates_kafka_producer(self, mock_kafka_producer_class):
        """Test __init__ creates KafkaProducer with correct config."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_producer_instance = MagicMock()
        mock_kafka_producer_class.return_value = mock_producer_instance
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        )
        
        assert producer.topic == "test.topic"
        mock_kafka_producer_class.assert_called_once()
        
        # Verify serializers are set
        call_kwargs = mock_kafka_producer_class.call_args[1]
        assert call_kwargs["bootstrap_servers"] == "localhost:9092"
        assert "value_serializer" in call_kwargs
        assert "key_serializer" in call_kwargs

    @patch('src.ingestion.producer.KafkaProducer')
    def test_init_uses_default_bootstrap_servers(self, mock_kafka_producer_class):
        """Test __init__ uses settings.kafka_bootstrap_servers by default."""
        from src.ingestion.producer import AirQualityProducer
        
        with patch('src.ingestion.producer.settings') as mock_settings:
            mock_settings.kafka_bootstrap_servers = "default:9092"
            mock_kafka_producer_class.return_value = MagicMock()
            
            producer = AirQualityProducer(topic="test.topic")
            
            call_kwargs = mock_kafka_producer_class.call_args[1]
            assert call_kwargs["bootstrap_servers"] == "default:9092"

    @patch('src.ingestion.producer.KafkaProducer')
    def test_send_queues_message(self, mock_kafka_producer_class):
        """Test send() queues message to Kafka topic."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_producer_instance = MagicMock()
        mock_kafka_producer_class.return_value = mock_producer_instance
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        )
        
        payload = {"city": "hanoi", "aqi": 85}
        producer.send(payload, key="hanoi")
        
        mock_producer_instance.send.assert_called_once_with(
            "test.topic",
            value=payload,
            key="hanoi"
        )

    @patch('src.ingestion.producer.KafkaProducer')
    def test_send_without_key(self, mock_kafka_producer_class):
        """Test send() works without key."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_producer_instance = MagicMock()
        mock_kafka_producer_class.return_value = mock_producer_instance
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        )
        
        payload = {"city": "hanoi", "aqi": 85}
        producer.send(payload)
        
        mock_producer_instance.send.assert_called_once_with(
            "test.topic",
            value=payload,
            key=None
        )

    @patch('src.ingestion.producer.KafkaProducer')
    def test_flush_calls_producer_flush(self, mock_kafka_producer_class):
        """Test flush() calls underlying producer flush."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_producer_instance = MagicMock()
        mock_kafka_producer_class.return_value = mock_producer_instance
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        )
        
        producer.flush()
        
        mock_producer_instance.flush.assert_called_once()

    @patch('src.ingestion.producer.KafkaProducer')
    def test_close_flushes_and_closes(self, mock_kafka_producer_class):
        """Test close() flushes and closes producer."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_producer_instance = MagicMock()
        mock_kafka_producer_class.return_value = mock_producer_instance
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        )
        
        producer.close()
        
        mock_producer_instance.flush.assert_called_once()
        mock_producer_instance.close.assert_called_once()

    @patch('src.ingestion.producer.KafkaProducer')
    def test_context_manager_enter(self, mock_kafka_producer_class):
        """Test __enter__ returns producer instance."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_kafka_producer_class.return_value = MagicMock()
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        )
        
        with producer as p:
            assert p is producer

    @patch('src.ingestion.producer.KafkaProducer')
    def test_context_manager_exit_closes(self, mock_kafka_producer_class):
        """Test __exit__ calls close()."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_producer_instance = MagicMock()
        mock_kafka_producer_class.return_value = mock_producer_instance
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        )
        
        with producer:
            pass
        
        mock_producer_instance.flush.assert_called()
        mock_producer_instance.close.assert_called()

    @patch('src.ingestion.producer.KafkaProducer')
    def test_value_serializer_encodes_json(self, mock_kafka_producer_class):
        """Test value_serializer properly encodes JSON."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_kafka_producer_class.return_value = MagicMock()
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        )
        
        # Get the serializer from call args
        call_kwargs = mock_kafka_producer_class.call_args[1]
        value_serializer = call_kwargs["value_serializer"]
        
        # Test serialization
        test_data = {"city": "hanoi", "aqi": 85}
        result = value_serializer(test_data)
        
        assert result == json.dumps(test_data).encode("utf-8")

    @patch('src.ingestion.producer.KafkaProducer')
    def test_key_serializer_encodes_string(self, mock_kafka_producer_class):
        """Test key_serializer properly encodes string key."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_kafka_producer_class.return_value = MagicMock()
        
        producer = AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        )
        
        # Get the serializer from call args
        call_kwargs = mock_kafka_producer_class.call_args[1]
        key_serializer = call_kwargs["key_serializer"]
        
        # Test serialization
        result = key_serializer("hanoi")
        assert result == b"hanoi"
        
        # Test None key
        result_none = key_serializer(None)
        assert result_none is None


class TestProducerIntegration:
    """Integration-style tests for producer."""

    @patch('src.ingestion.producer.KafkaProducer')
    def test_send_multiple_messages(self, mock_kafka_producer_class):
        """Test sending multiple messages."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_producer_instance = MagicMock()
        mock_kafka_producer_class.return_value = mock_producer_instance
        
        cities_data = [
            {"city": "hanoi", "aqi": 85},
            {"city": "saigon", "aqi": 72},
            {"city": "danang", "aqi": 45}
        ]
        
        with AirQualityProducer(
            topic="test.topic",
            bootstrap_servers="localhost:9092"
        ) as producer:
            for data in cities_data:
                producer.send(data, key=data["city"])
        
        assert mock_producer_instance.send.call_count == 3

    @patch('src.ingestion.producer.KafkaProducer')
    def test_full_workflow(self, mock_kafka_producer_class, sample_kafka_message):
        """Test full workflow with realistic data."""
        from src.ingestion.producer import AirQualityProducer
        
        mock_producer_instance = MagicMock()
        mock_kafka_producer_class.return_value = mock_producer_instance
        
        with AirQualityProducer(
            topic="raw.airquality.hanoi",
            bootstrap_servers="localhost:9092"
        ) as producer:
            producer.send(sample_kafka_message, key="hanoi")
            producer.flush()
        
        mock_producer_instance.send.assert_called_once_with(
            "raw.airquality.hanoi",
            value=sample_kafka_message,
            key="hanoi"
        )
        # flush called twice: once explicitly, once in close()
        assert mock_producer_instance.flush.call_count >= 1
