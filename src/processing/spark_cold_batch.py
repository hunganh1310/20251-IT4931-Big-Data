import io
import json
from datetime import datetime

from minio import Minio

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp,
    hour,
    date_format,
    lag,
    avg,
    desc,
)
from pyspark.sql.window import Window

from src.common import logger, settings


def get_spark_session() -> SparkSession:
    """
    Spark session dùng cho batch processing (không phải streaming).
    Không cần kafka package, chỉ dùng core Spark.
    """
    spark = (
        SparkSession.builder.appName("Cold-Batch-Processing")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_schema() -> StructType:
    """
    Schema JSON giống với schema bạn dùng trong city comparison streaming.
    """
    return StructType(
        [
            StructField("city", StringType(), True),
            StructField(
                "payload",
                StructType(
                    [
                        StructField("aqi", IntegerType(), True),
                        StructField("idx", IntegerType(), True),
                        StructField("dominentpol", StringType(), True),
                        StructField(
                            "iaqi",
                            StructType(
                                [
                                    StructField(
                                        "pm25",
                                        StructType(
                                            [StructField("v", DoubleType(), True)]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        "pm10",
                                        StructType(
                                            [StructField("v", DoubleType(), True)]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        "t",
                                        StructType(
                                            [StructField("v", DoubleType(), True)]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        "h",
                                        StructType(
                                            [StructField("v", DoubleType(), True)]
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            "time",
                            StructType(
                                [
                                    StructField("s", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )


def create_minio_client() -> Minio:
    """
    Tạo MinIO client, giống phong cách file cold_storage streaming.
    """
    client = Minio(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_user,
        secret_key=settings.minio_password,
        secure=False,
    )

    bucket_name = settings.minio_bucket
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Created bucket: {bucket_name}")

    return client


def load_raw_json_from_minio(city: str):
    """
    Đọc toàn bộ JSON raw cho 1 city từ MinIO.
    Mỗi object là 1 record JSON (raw value từ Kafka).
    Trả về list các chuỗi JSON.
    """
    client = create_minio_client()
    bucket_name = settings.minio_bucket

    prefix = f"aqicn/{city}/"
    logger.info(f"Listing MinIO objects with prefix='{prefix}' for city={city}")

    json_strings = []

    try:
        objects = client.list_objects(
            bucket_name=bucket_name, prefix=prefix, recursive=True
        )

        for obj in objects:
            try:
                response = client.get_object(bucket_name, obj.object_name)
                data = response.read()
                response.close()
                response.release_conn()

                json_str = data.decode("utf-8").strip()
                if json_str:
                    json_strings.append(json_str)
            except Exception as e:
                logger.error(
                    f"Error reading object {obj.object_name} for city={city}: {e}"
                )
                continue

    except Exception as e:
        logger.error(f"Error listing objects for city={city}: {e}")
        raise

    logger.info(
        f"Loaded {len(json_strings)} raw JSON records from MinIO for city={city}"
    )
    return json_strings


def build_dataframe_from_json(
    spark: SparkSession, json_strings, schema: StructType
) -> DataFrame:
    """
    Tạo Spark DataFrame từ list JSON string, parse theo schema đã cho.
    """
    if not json_strings:
        return spark.createDataFrame([], schema)  # empty DF

    # Tạo DataFrame 1 cột 'json_str'
    raw_df = spark.createDataFrame(json_strings, StringType()).toDF("json_str")

    parsed_df = raw_df.select(
        from_json(col("json_str"), schema).alias("data"),
    )

    # Flatten theo đúng cách bạn làm ở file city comparison
    df_clean = parsed_df.select(
        col("data.city").alias("city"),
        col("data.payload.aqi").alias("aqi"),
        col("data.payload.iaqi.pm25.v").alias("pm25"),
        col("data.payload.iaqi.pm10.v").alias("pm10"),
        col("data.payload.iaqi.t.v").alias("temperature"),
        col("data.payload.iaqi.h.v").alias("humidity"),
        to_timestamp(col("data.payload.time.s"), "yyyy-MM-dd HH:mm:ss").alias(
            "measurement_time"
        ),
    )

    return df_clean


def add_ml_features(df: DataFrame) -> DataFrame:
    """
    Thêm các feature cho ML:
      - hour_of_day
      - day_of_week
      - pm25_lag_1, pm25_lag_3
      - pm25_rolling_3 (trung bình 3 bản ghi gần nhất)
    Bạn có thể mở rộng thêm nếu cần.
    """
    if df.rdd.isEmpty():
        return df

    # Window theo city + time
    w = Window.partitionBy("city").orderBy("measurement_time")
    w_rows_3 = w.rowsBetween(-2, 0)  # 3 bản ghi gần nhất (bao gồm hiện tại)

    df_feat = (
        df.withColumn("hour_of_day", hour(col("measurement_time")))
        .withColumn("day_of_week", date_format(col("measurement_time"), "u").cast("int"))
        .withColumn("pm25_lag_1", lag("pm25", 1).over(w))
        .withColumn("pm25_lag_3", lag("pm25", 3).over(w))
        .withColumn("pm25_rolling_3", avg("pm25").over(w_rows_3))
    )

    # Sắp xếp cho dễ debug/đọc
    df_feat = df_feat.orderBy(col("measurement_time").asc())

    return df_feat


def write_features_to_minio_as_csv(df: DataFrame, city: str):
    """
    Convert DataFrame thành CSV và upload vào MinIO.
    Vì dữ liệu cho 1 city thường không quá lớn, dùng toPandas() là chấp nhận được
    trong bối cảnh bài tập môn học.
    """
    if df.rdd.isEmpty():
        logger.warn(f"No data after feature engineering for city={city}, skipping CSV.")
        return

    # Đưa về driver để ghi CSV
    pdf = df.toPandas()

    csv_bytes = pdf.to_csv(index=False).encode("utf-8")

    client = create_minio_client()
    bucket_name = settings.minio_bucket

    # Đặt tên file theo timestamp chạy
    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    object_name = f"ml/aqicn/{city}/features_{run_ts}.csv"

    data_stream = io.BytesIO(csv_bytes)

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )

    logger.info(
        f"Wrote ML feature CSV for city={city} to MinIO at object '{object_name}' "
        f"with {len(pdf)} rows."
    )


def process_city_batch(city: str):
    """
    Pipeline batch cho 1 city:
      1. Load JSON raw từ MinIO
      2. Parse JSON → DataFrame
      3. Thêm feature cho ML
      4. Ghi CSV feature vào MinIO
    """
    logger.info(f"[BATCH] Starting cold batch processing for city={city}")

    spark = get_spark_session()
    schema = define_schema()

    try:
        json_strings = load_raw_json_from_minio(city)
        if not json_strings:
            logger.warn(f"[BATCH] No raw data found in MinIO for city={city}")
            spark.stop()
            return

        df = build_dataframe_from_json(spark, json_strings, schema)
        df_with_features = add_ml_features(df)
        write_features_to_minio_as_csv(df_with_features, city)

        logger.info(f"[BATCH] Completed cold batch processing for city={city}")
    except Exception as e:
        logger.error(f"[BATCH] Error in cold batch for city={city}: {e}")
        raise
    finally:
        spark.stop()


def main(city: str):
    """
    Entry point để gọi từ script run_cold_batch.py hoặc scheduler.
    """
    process_city_batch(city)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        city_arg = sys.argv[1]
    else:
        city_arg = "hanoi" 

    main(city_arg)
