# Test Suite

This directory contains the test suite for the Air Quality Data Pipeline project.

## Structure

```
test/
├── __init__.py              # Test package initialization
├── conftest.py              # Pytest fixtures and configuration
├── test_config.py           # Tests for configuration module
├── test_api_client.py       # Tests for API client
├── test_producer.py         # Tests for Kafka producer
├── test_integration.py      # Integration tests
└── README.md               # This file
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run specific test file
```bash
pytest test/test_config.py
pytest test/test_api_client.py
pytest test/test_producer.py
```

### Run specific test class
```bash
pytest test/test_config.py::TestSettings
```

### Run specific test function
```bash
pytest test/test_api_client.py::TestAirQualityAPIClient::test_fetch_success_with_token
```

### Run with verbose output
```bash
pytest -v
```

### Run with coverage report
```bash
pytest --cov=src --cov-report=html
pytest --cov=src --cov-report=term-missing
```

### Run only unit tests
```bash
pytest -m unit
```

### Run only integration tests
```bash
pytest -m integration
```

## Test Categories

### Unit Tests
- `test_config.py`: Configuration and settings management
- `test_api_client.py`: API client functionality
- `test_producer.py`: Kafka producer functionality

### Integration Tests
- `test_integration.py`: End-to-end workflow tests

## Fixtures

Common fixtures are defined in `conftest.py`:

- `mock_env_vars`: Mock environment variables
- `mock_kafka_producer`: Mock Kafka producer
- `mock_requests_session`: Mock HTTP requests
- `sample_aqicn_response`: Sample AQICN API response
- `sample_openaq_response`: Sample OpenAQ API response

## Writing New Tests

When adding new tests:

1. Create a new file with prefix `test_`
2. Create test classes with prefix `Test`
3. Create test functions with prefix `test_`
4. Use fixtures from `conftest.py`
5. Add markers for test categorization
6. Follow existing test patterns

Example:
```python
import pytest

class TestMyFeature:
    def test_my_function(self, mock_env_vars):
        # Arrange
        # Act
        # Assert
        pass
```

## Continuous Integration

Tests are automatically run on:
- Every pull request
- Every push to main branch
- Manual workflow dispatch

## Dependencies

Test dependencies are in `requirements.txt`:
- pytest
- pytest-mock
- pytest-cov
- black (for code formatting)
- flake8 (for linting)
