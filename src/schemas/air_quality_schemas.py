"""
Common schemas for air quality data processing
Contains PySpark schemas for AQICN and OpenAQ API responses
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    ArrayType, BooleanType
)


def aqicn_schema() -> StructType:
    """
    Schema for AQICN API response matching produce_aqicn.py output
    Structure: {city: str, payload: {...}}
    """
    return StructType([
        StructField("city", StringType(), True),
        StructField("payload", StructType([
            StructField("aqi", IntegerType(), True),
            StructField("idx", IntegerType(), True),
            StructField("dominentpol", StringType(), True),
            StructField("iaqi", StructType([
                StructField("pm25", StructType([
                    StructField("v", DoubleType(), True)
                ]), True),
                StructField("pm10", StructType([
                    StructField("v", DoubleType(), True)
                ]), True),
                StructField("o3", StructType([
                    StructField("v", DoubleType(), True)
                ]), True),
                StructField("no2", StructType([
                    StructField("v", DoubleType(), True)
                ]), True),
                StructField("t", StructType([
                    StructField("v", DoubleType(), True)
                ]), True),
                StructField("h", StructType([
                    StructField("v", DoubleType(), True)
                ]), True)
            ]), True),
            StructField("time", StructType([
                StructField("s", StringType(), True),
                StructField("tz", StringType(), True),
                StructField("v", IntegerType(), True)
            ]), True),
            StructField("city", StructType([
                StructField("name", StringType(), True),
                StructField("geo", StructType([
                    StructField("0", DoubleType(), True),
                    StructField("1", DoubleType(), True)
                ]), True)
            ]), True)
        ]), True)
    ])


def openaq_schema() -> StructType:
    """
    Schema for OpenAQ API response matching produce_openaq.py output
    Structure: {country: str, parameter: str, data: {...}}
    """
    return StructType([
        StructField("country", StringType(), True),
        StructField("parameter", StringType(), True),
        StructField("data", StructType([
            StructField("meta", StructType([
                StructField("name", StringType(), True),
                StructField("page", IntegerType(), True),
                StructField("limit", IntegerType(), True),
                StructField("found", StringType(), True)
            ]), True),
            StructField("results", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("locality", StringType(), True),
                StructField("timezone", StringType(), True),
                StructField("country", StructType([
                    StructField("id", IntegerType(), True),
                    StructField("code", StringType(), True),
                    StructField("name", StringType(), True)
                ]), True),
                StructField("isMobile", BooleanType(), True),
                StructField("isMonitor", BooleanType(), True),
                StructField("sensors", ArrayType(StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("parameter", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                        StructField("units", StringType(), True),
                        StructField("displayName", StringType(), True)
                    ]), True)
                ])), True),
                StructField("coordinates", StructType([
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True)
                ]), True),
                StructField("datetimeFirst", StructType([
                    StructField("utc", StringType(), True),
                    StructField("local", StringType(), True)
                ]), True),
                StructField("datetimeLast", StructType([
                    StructField("utc", StringType(), True),
                    StructField("local", StringType(), True)
                ]), True)
            ])), True)
        ]), True)
    ])