"""
Schemas package for air quality data processing
Provides centralized schema definitions for PySpark streaming
"""

from .air_quality_schemas import aqicn_schema, openaq_schema

__all__ = ['aqicn_schema', 'openaq_schema']