import os
import platform
import pyspark
from pyspark.sql import SparkSession

# Optional: adjust if you already define REPO_ROOT elsewhere
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def create_spark_session(app_name: str = "FoodDeliveryStreaming"):
    """
    Creates a Spark session with dynamically matched Kafka + Avro dependencies.
    This avoids version mismatch errors between Spark runtime and connectors.
    """

    # Get installed PySpark version (e.g., 3.5.1 or 4.0.0)
    spark_version = ".".join(pyspark.__version__.split(".")[:3])

    # Choose correct Scala version
    # Spark 4.x → Scala 2.13
    # Spark 3.x → Scala 2.12
    scala_suffix = "2.13" if spark_version.startswith("4.") else "2.12"

    # Build correct package strings
    jar_packages = ",".join([
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_suffix}:{spark_version}",
        f"org.apache.spark:spark-avro_{scala_suffix}:{spark_version}",
    ])

    print("========================================")
    print(f" Spark Version Detected: {spark_version}")
    print(f" Scala Version Used: {scala_suffix}")
    print(f" Packages: {jar_packages}")
    print("========================================")

    # Build Spark session
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages", jar_packages)
        .config("spark.sql.shuffle.partitions", "4")
    )

    # Fix for Windows file system issues
    if platform.system() == "Windows":
        builder = builder.config(
            "spark.sql.warehouse.dir",
            os.path.join(REPO_ROOT, "spark-warehouse")
        )

    spark = builder.getOrCreate()

    # Reduce log noise
    spark.sparkContext.setLogLevel("WARN")

    return spark


# Example stream reader (keep yours if already defined)
def read_orders_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .load()
    )
