import os
import platform
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def create_spark_session(app_name: str = "FoodDeliveryStreaming"):
    jar_packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.spark:spark-avro_2.12:3.5.1",
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
    spark.sparkContext.setLogLevel("WARN")

    print("Spark runtime:", spark.version)

    return spark


orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("courier_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

couriers_schema = StructType([
    StructField("courier_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("location_lat", DoubleType(), True),
    StructField("location_lng", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True),
])

restaurants_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("cuisine", StringType(), True),
    StructField("city", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("prep_time_min", IntegerType(), True),
    StructField("updated_at", StringType(), True),
])


def read_orders_stream(spark):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), orders_schema).alias("data"))
        .select("data.*")
    )

    return parsed_df


def read_couriers_stream(spark):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "couriers")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), couriers_schema).alias("data"))
        .select("data.*")
    )

    return parsed_df


def read_restaurants_stream(spark):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "restaurants")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), restaurants_schema).alias("data"))
        .select("data.*")
    )

    return parsed_df
