"""
spark_session.py — Shared Spark session and Kafka config for all use case scripts.

Provides:
    - create_spark_session(app_name)  → configured SparkSession
    - read_orders_stream(spark)       → streaming DataFrame from orders topic
    - read_couriers_stream(spark)     → streaming DataFrame from couriers topic
    - ORDER_AVRO_SCHEMA               → JSON string for from_avro()
    - COURIER_AVRO_SCHEMA             → JSON string for from_avro()
"""

import json
import os
import sys

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)
SCHEMA_DIR = os.path.join(REPO_ROOT, "schemas")

try:
    from config.eventhub_config import (
        SPARK_KAFKA_CONFIG,
        ORDERS_TOPIC,
        COURIERS_TOPIC,
    )
except ImportError:
    print("ERROR: config/eventhub_config.py not found.")
    print("  Copy the template:  cp config/eventhub_config_template.py config/eventhub_config.py")
    print("  Then fill in your Azure Event Hub connection string.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Load AVRO schemas as JSON strings (required by from_avro)
# ---------------------------------------------------------------------------
def _load_schema_str(filename: str) -> str:
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path) as f:
        return f.read()


ORDER_AVRO_SCHEMA = _load_schema_str("order_lifecycle_event.avsc")
COURIER_AVRO_SCHEMA = _load_schema_str("courier_status_event.avsc")


# ---------------------------------------------------------------------------
# Spark session factory
# ---------------------------------------------------------------------------
def create_spark_session(app_name: str = "FoodDeliveryStreaming"):
    """
    Create a SparkSession configured for Azure Event Hub (Kafka protocol).

    Prerequisites:
        - Java 17+ installed (JAVA_HOME set)
        - pip install pyspark
    """
    import platform
    from pyspark.sql import SparkSession

    # Fix for Windows: set HADOOP_HOME if not set
    if platform.system() == "Windows":
        hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")
        os.environ["HADOOP_HOME"] = hadoop_home
        # Create dir + dummy winutils if missing
        winutils_path = os.path.join(hadoop_home, "bin", "winutils.exe")
        if not os.path.exists(winutils_path):
            os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)
            # Create a minimal placeholder
            with open(winutils_path, "w") as f:
                f.write("")

    # JARs for Kafka + AVRO support (downloaded automatically by Maven)
    jar_packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
        "org.apache.spark:spark-avro_2.13:4.0.0",
    ])

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages", jar_packages)
        .config("spark.sql.shuffle.partitions", "4")
    )

    # Windows: disable native IO to avoid winutils errors
    if platform.system() == "Windows":
        builder = builder.config("spark.sql.warehouse.dir", os.path.join(REPO_ROOT, "spark-warehouse"))

    spark = builder.getOrCreate()

    # Disable native Hadoop IO on Windows
    if platform.system() == "Windows":
        spark.sparkContext._jsc.hadoopConfiguration().set("io.native.lib.available", "false")

    # Reduce noisy Spark logs
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Stream readers
# ---------------------------------------------------------------------------
def _kafka_read_config(topic: str) -> dict:
    """Build the full Kafka source config for a given topic."""
    conf = dict(SPARK_KAFKA_CONFIG)
    conf["subscribe"] = topic
    conf["startingOffsets"] = "latest"
    # Consumer group prefix — each UC script gets its own group
    conf["groupIdPrefix"] = f"spark-group09-{topic}"
    return conf


def read_orders_stream(spark):
    """
    Read the raw Kafka stream from the orders topic.

    Returns a streaming DataFrame with columns:
        key, value, topic, partition, offset, timestamp, timestampType
    """
    conf = _kafka_read_config(ORDERS_TOPIC)
    return (
        spark.readStream
        .format("kafka")
        .options(**conf)
        .load()
    )


def read_couriers_stream(spark):
    """
    Read the raw Kafka stream from the couriers topic.

    Returns a streaming DataFrame with columns:
        key, value, topic, partition, offset, timestamp, timestampType
    """
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
    """
    Deserialize AVRO-encoded order events and flatten to columns.

    Input:  raw Kafka DataFrame (with 'value' column)
    Output: DataFrame with one column per schema field
    """
    from pyspark.sql.avro.functions import from_avro
    from pyspark.sql.functions import col, from_unixtime

    # Decode AVRO → struct
    decoded = df.select(from_avro(col("value"), ORDER_AVRO_SCHEMA).alias("order"))

    # Flatten struct to individual columns
    flattened = decoded.select(
        col("order.event_id"),
        col("order.order_id"),
        col("order.customer_id"),
        col("order.restaurant_id"),
        col("order.courier_id"),
        col("order.zone_id"),
        col("order.order_status"),
        col("order.previous_status"),
        col("order.event_timestamp").alias("event_timestamp_ms"),
        col("order.ingestion_timestamp").alias("ingestion_timestamp_ms"),
        col("order.order_total_cents"),
        col("order.estimated_prep_time_seconds"),
        col("order.estimated_delivery_time_seconds"),
        col("order.actual_prep_time_seconds"),
        col("order.actual_delivery_time_seconds"),
        col("order.is_peak_hour"),
        col("order.weather_condition"),
        col("order.cancellation_reason"),
        col("order.customer_rating"),
        col("order.is_duplicate"),
        col("order.is_late_arrival"),
    )

    # Convert epoch millis to proper timestamps for windowing
    result = (
        flattened
        .withColumn("event_timestamp",
                    (col("event_timestamp_ms") / 1000).cast("timestamp"))
        .withColumn("ingestion_timestamp",
                    (col("ingestion_timestamp_ms") / 1000).cast("timestamp"))
    )
    return result


def deserialize_couriers(df):
    """
    Deserialize AVRO-encoded courier events and flatten to columns.

    Input:  raw Kafka DataFrame (with 'value' column)
    Output: DataFrame with one column per schema field
    """
    from pyspark.sql.avro.functions import from_avro
    from pyspark.sql.functions import col

    decoded = df.select(from_avro(col("value"), COURIER_AVRO_SCHEMA).alias("courier"))

    flattened = decoded.select(
        col("courier.event_id"),
        col("courier.courier_id"),
        col("courier.order_id"),
        col("courier.zone_id"),
        col("courier.courier_status"),
        col("courier.previous_status"),
        col("courier.event_timestamp").alias("event_timestamp_ms"),
        col("courier.ingestion_timestamp").alias("ingestion_timestamp_ms"),
        col("courier.latitude"),
        col("courier.longitude"),
        col("courier.location_accuracy_meters"),
        col("courier.heading_degrees"),
        col("courier.vehicle_type"),
        col("courier.distance_to_restaurant_meters"),
        col("courier.distance_to_customer_meters"),
        col("courier.session_id"),
        col("courier.shift_duration_seconds"),
        col("courier.deliveries_completed_in_session"),
        col("courier.is_duplicate"),
        col("courier.is_late_arrival"),
        col("courier.anomaly_flag"),
    )

    result = (
        flattened
        .withColumn("event_timestamp",
                    (col("event_timestamp_ms") / 1000).cast("timestamp"))
        .withColumn("ingestion_timestamp",
                    (col("ingestion_timestamp_ms") / 1000).cast("timestamp"))
    )
    return result
