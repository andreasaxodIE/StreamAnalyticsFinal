"""
spark_session.py — Shared Spark session and Kafka config for all use case scripts.

Provides:
    - create_spark_session(app_name)  → configured SparkSession
    - read_orders_stream(spark)       → streaming DataFrame from orders topic
    - read_couriers_stream(spark)     → streaming DataFrame from couriers topic
    - ORDER_AVRO_SCHEMA               → JSON string for from_avro()
    - COURIER_AVRO_SCHEMA             → JSON string for from_avro()
"""

import os
import sys

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)
SCHEMA_DIR = os.path.join(REPO_ROOT, "schemas")

try:
    from settings.eventhub_config import (
        SPARK_KAFKA_CONFIG,
        ORDERS_TOPIC,
        COURIERS_TOPIC,
    )
except ImportError:
    print("ERROR: settings/eventhub_config.py not found.")
    print("Check that your Event Hub config file exists and is correct.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Load AVRO schemas as JSON strings (required by from_avro)
# ---------------------------------------------------------------------------
def _load_schema_str(filename: str) -> str:
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


ORDER_AVRO_SCHEMA = _load_schema_str("order_lifecycle_event.avsc")
COURIER_AVRO_SCHEMA = _load_schema_str("courier_status_event.avsc")


# ---------------------------------------------------------------------------
# Spark session factory
# ---------------------------------------------------------------------------
def create_spark_session(app_name: str = "FoodDeliveryStreaming"):
    """
    Create a SparkSession configured for Azure Event Hub (Kafka protocol).
    """
    import platform
    from pyspark.sql import SparkSession

    if platform.system() == "Windows":
        hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")
        os.environ["HADOOP_HOME"] = hadoop_home
        winutils_path = os.path.join(hadoop_home, "bin", "winutils.exe")
        if not os.path.exists(winutils_path):
            os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)
            with open(winutils_path, "w", encoding="utf-8") as f:
                f.write("")

    # Fixed connector versions to avoid the Spark 4.0 class mismatch error
    jar_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2",
    "org.apache.spark:spark-avro_2.13:4.0.2",
    ])

    print("Using packages:")
    print(jar_packages)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages", jar_packages)
        .config("spark.sql.shuffle.partitions", "4")
    )

    if platform.system() == "Windows":
        builder = builder.config(
            "spark.sql.warehouse.dir",
            os.path.join(REPO_ROOT, "spark-warehouse")
        )

    spark = builder.getOrCreate()

    if platform.system() == "Windows":
        spark.sparkContext._jsc.hadoopConfiguration().set("io.native.lib.available", "false")

    spark.sparkContext.setLogLevel("WARN")
    print("Spark runtime:", spark.version)
    return spark


# ---------------------------------------------------------------------------
# Stream readers
# ---------------------------------------------------------------------------
def _kafka_read_config(topic: str) -> dict:
    conf = dict(SPARK_KAFKA_CONFIG)
    conf["subscribe"] = topic
    conf["startingOffsets"] = "latest"  
    conf["groupIdPrefix"] = f"spark-group09-{topic}"
    return conf


def read_orders_stream(spark):
    conf = _kafka_read_config(ORDERS_TOPIC)
    return (
        spark.readStream
        .format("kafka")
        .options(**conf)
        .load()
    )


def read_couriers_stream(spark):
    conf = _kafka_read_config(COURIERS_TOPIC)
    return (
        spark.readStream
        .format("kafka")
        .options(**conf)
        .load()
    )


# ---------------------------------------------------------------------------
# AVRO deserialization helpers
# ---------------------------------------------------------------------------
def deserialize_orders(df):
    from pyspark.sql.avro.functions import from_avro
    from pyspark.sql.functions import col

    decoded = df.select(
        from_avro(col("value"), ORDER_AVRO_SCHEMA).alias("order")
    )

    result = decoded.select(
        col("order.event_id").alias("event_id"),
        col("order.order_id").alias("order_id"),
        col("order.customer_id").alias("customer_id"),
        col("order.restaurant_id").alias("restaurant_id"),
        col("order.courier_id").alias("courier_id"),
        col("order.zone_id").alias("zone_id"),
        col("order.order_status").alias("order_status"),
        col("order.previous_status").alias("previous_status"),
        col("order.event_timestamp").alias("event_timestamp"),
        col("order.ingestion_timestamp").alias("ingestion_timestamp"),
        col("order.order_total_cents").alias("order_total_cents"),
        col("order.estimated_prep_time_seconds").alias("estimated_prep_time_seconds"),
        col("order.estimated_delivery_time_seconds").alias("estimated_delivery_time_seconds"),
        col("order.actual_prep_time_seconds").alias("actual_prep_time_seconds"),
        col("order.actual_delivery_time_seconds").alias("actual_delivery_time_seconds"),
        col("order.is_peak_hour").alias("is_peak_hour"),
        col("order.weather_condition").alias("weather_condition"),
        col("order.cancellation_reason").alias("cancellation_reason"),
        col("order.customer_rating").alias("customer_rating"),
        col("order.is_duplicate").alias("is_duplicate"),
        col("order.is_late_arrival").alias("is_late_arrival"),
    )

    return result

def deserialize_couriers(df):
    from pyspark.sql.avro.functions import from_avro
    from pyspark.sql.functions import col

    decoded = df.select(
        from_avro(col("value"), COURIER_AVRO_SCHEMA).alias("courier")
    )

    result = decoded.select(
        col("courier.event_id").alias("event_id"),
        col("courier.courier_id").alias("courier_id"),
        col("courier.order_id").alias("order_id"),
        col("courier.zone_id").alias("zone_id"),
        col("courier.courier_status").alias("courier_status"),
        col("courier.previous_status").alias("previous_status"),
        col("courier.event_timestamp").alias("event_timestamp"),
        col("courier.ingestion_timestamp").alias("ingestion_timestamp"),
        col("courier.latitude").alias("latitude"),
        col("courier.longitude").alias("longitude"),
        col("courier.location_accuracy_meters").alias("location_accuracy_meters"),
        col("courier.heading_degrees").alias("heading_degrees"),
        col("courier.vehicle_type").alias("vehicle_type"),
        col("courier.distance_to_restaurant_meters").alias("distance_to_restaurant_meters"),
        col("courier.distance_to_customer_meters").alias("distance_to_customer_meters"),
        col("courier.session_id").alias("session_id"),
        col("courier.shift_duration_seconds").alias("shift_duration_seconds"),
        col("courier.deliveries_completed_in_session").alias("deliveries_completed_in_session"),
        col("courier.is_duplicate").alias("is_duplicate"),
        col("courier.is_late_arrival").alias("is_late_arrival"),
        col("courier.anomaly_flag").alias("anomaly_flag"),
    )

    return result
