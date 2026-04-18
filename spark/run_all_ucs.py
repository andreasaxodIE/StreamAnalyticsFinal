#!/usr/bin/env python3
"""
run_all_ucs.py — Runs all 7 use cases in a single Spark session.

Writes aggregated results as PARQUET using Spark's native sink. Output can be
either local disk or Azure Blob (ADLS Gen2), controlled by the OUTPUT_BASE
env var:

    # Local (default)
    python spark/run_all_ucs.py

    # Azure Blob / ADLS Gen2
    OUTPUT_BASE=abfss://<container>@<account>.dfs.core.windows.net/streaming \\
        python spark/run_all_ucs.py

Each UC writes to its own subdirectory (uc1/, uc3/, ...), partitioned by
window_date so old data ages out cleanly and the dashboard can read only
the latest partition.

Requires:
    - HADOOP_HOME set (Windows: C:\\hadoop with winutils.exe)
    - Producer running to send events to Event Hub
    - For Azure output: AZURE_STORAGE_ACCOUNT_KEY env var set
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session,
    read_orders_stream, read_couriers_stream,
    deserialize_orders, deserialize_couriers,
    AZURE_OUTPUT_PATH,
)
from pyspark.sql.functions import (
    col, window, count, avg, min as _min, max as _max,
    sum as _sum, round as _round, when, expr, percentile_approx,
    approx_count_distinct, to_date,
)

# ---------------------------------------------------------------------------
# Output configuration
# ---------------------------------------------------------------------------
# Default: the hard-coded Azure path from spark_session.py.
# Override with OUTPUT_BASE=./output_parquet to run locally.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_BASE = os.environ.get("OUTPUT_BASE", AZURE_OUTPUT_PATH)
CHECKPOINT_BASE = os.environ.get(
    "CHECKPOINT_BASE", os.path.join(REPO_ROOT, "checkpoints")
)

# Ensure local dirs exist when using local paths (no-op for abfss://)
if not OUTPUT_BASE.startswith(("abfss://", "wasbs://", "s3://", "gs://")):
    os.makedirs(OUTPUT_BASE, exist_ok=True)
if not CHECKPOINT_BASE.startswith(("abfss://", "wasbs://", "s3://", "gs://")):
    os.makedirs(CHECKPOINT_BASE, exist_ok=True)


def parquet_sink(df, name, partition_cols=("window_date",)):
    """Native Spark Parquet sink for a streaming DataFrame.

    Writes to {OUTPUT_BASE}/{name}/ with checkpoint at {CHECKPOINT_BASE}/{name}/.
    Appends new rows (required for Parquet sink) — rows are emitted once their
    window closes per the watermark.
    """
    output_path = f"{OUTPUT_BASE.rstrip('/')}/{name}"
    checkpoint_path = f"{CHECKPOINT_BASE.rstrip('/')}/{name}"
    writer = (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .queryName(name)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    return writer.start()


def main():
    spark = create_spark_session("FoodDelivery_AllUCs")

    print(f"\n{'='*60}")
    print(f"  Spark Streaming — All Use Cases (Parquet sink)")
    print(f"  Output:     {OUTPUT_BASE}")
    print(f"  Checkpoint: {CHECKPOINT_BASE}")
    print(f"{'='*60}\n")

    # ===================================================================
    # READ STREAMS (one per topic, shared across UCs)
    # For stream-stream joins we need separate reads
    # ===================================================================
    raw_orders_1 = read_orders_stream(spark)
    raw_orders_2 = read_orders_stream(spark)  # for UC10 self-join (left)
    raw_orders_3 = read_orders_stream(spark)  # for UC10 self-join (right)
    raw_orders_4 = read_orders_stream(spark)  # for UC9 demand side
    raw_couriers_1 = read_couriers_stream(spark)
    raw_couriers_2 = read_couriers_stream(spark)  # for UC9 supply side

    orders = deserialize_orders(raw_orders_1)
    couriers = deserialize_couriers(raw_couriers_1)

    # ===================================================================
    # UC1 — Order volume and cancellation rate by zone
    # ===================================================================
    # Counts unique orders by exploiting the fact that certain statuses
    # fire exactly once per order (PLACED at start, CANCELLED terminal).
    # We can't use countDistinct() because streaming aggregations don't
    # support it — but summing a status-matching indicator works and is
    # exact (not approximate).
    print("Starting UC1: Order volume & cancellation rate...")
    uc1 = (
        orders
        .filter(col("is_duplicate") == False)
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(window(col("event_timestamp"), "1 minute"), col("zone_id"))
        .agg(
            _sum(when(col("order_status") == "PLACED", 1).otherwise(0)).alias("total_orders"),
            _sum(when(col("order_status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_orders"),
        )
        .withColumn("cancellation_rate",
                    when(col("total_orders") == 0, 0.0)
                    .otherwise(_round(col("cancelled_orders") / col("total_orders") * 100, 2)))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            col("zone_id"), col("total_orders"),
            col("cancelled_orders"), col("cancellation_rate"),
        )
    )
    q1 = parquet_sink(uc1, "uc1_order_volume")

    # ===================================================================
    # UC3 — Peak-hour prep time SLA breaches
    # ===================================================================
    print("Starting UC3: Prep SLA breaches...")
    uc3 = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "READY_FOR_PICKUP")
        .filter(col("actual_prep_time_seconds").isNotNull())
        .filter(col("actual_prep_time_seconds") > 1800)
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(
            window(col("event_timestamp"), "2 minutes", "1 minute"),
            col("zone_id"), col("restaurant_id"), col("is_peak_hour"),
        )
        .agg(
            count("*").alias("breach_count"),
            _round(avg("actual_prep_time_seconds"), 0).alias("avg_prep_time_sec"),
            _max("actual_prep_time_seconds").alias("max_prep_time_sec"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            col("zone_id"), col("restaurant_id"), col("is_peak_hour"),
            col("breach_count"), col("avg_prep_time_sec"), col("max_prep_time_sec"),
        )
    )
    q3 = parquet_sink(uc3, "uc3_prep_sla")

    # ===================================================================
    # UC4 — Weather impact on delivery times
    # ===================================================================
    print("Starting UC4: Weather impact...")
    uc4 = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "DELIVERED")
        .filter(col("actual_delivery_time_seconds").isNotNull())
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(window(col("event_timestamp"), "2 minutes"), col("weather_condition"))
        .agg(
            count("*").alias("order_count"),
            _round(avg("actual_delivery_time_seconds"), 0).alias("avg_delivery_sec"),
            _min("actual_delivery_time_seconds").alias("min_delivery_sec"),
            _max("actual_delivery_time_seconds").alias("max_delivery_sec"),
            _round(percentile_approx("actual_delivery_time_seconds", 0.95), 0).alias("p95_delivery_sec"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            col("weather_condition"), col("order_count"),
            col("avg_delivery_sec"), col("min_delivery_sec"),
            col("max_delivery_sec"), col("p95_delivery_sec"),
        )
    )
    q4 = parquet_sink(uc4, "uc4_weather")

    # ===================================================================
    # UC7 — Anomaly detection
    # ===================================================================
    print("Starting UC7: Anomaly detection...")
    uc7 = (
        couriers
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(
            window(col("event_timestamp"), "2 minutes", "1 minute"),
            col("zone_id"), col("vehicle_type"),
        )
        .agg(
            count("*").alias("total_events"),
            _sum(when(col("anomaly_flag") == "IMPOSSIBLE_SPEED", 1).otherwise(0)).alias("impossible_speed"),
            _sum(when(col("anomaly_flag") == "LOCATION_JUMP", 1).otherwise(0)).alias("location_jump"),
            _sum(when(col("anomaly_flag") == "OFFLINE_MID_DELIVERY", 1).otherwise(0)).alias("offline_mid_delivery"),
            _sum(when(col("anomaly_flag").isNotNull(), 1).otherwise(0)).alias("total_anomalies"),
        )
        .withColumn("anomaly_rate",
                    when(col("total_events") == 0, 0.0)
                    .otherwise(_round(col("total_anomalies") / col("total_events") * 100, 2)))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            col("zone_id"), col("vehicle_type"),
            col("total_events"), col("total_anomalies"),
            col("anomaly_rate"), col("impossible_speed"),
            col("location_jump"), col("offline_mid_delivery"),
        )
    )
    q7 = parquet_sink(uc7, "uc7_anomalies")

    # ===================================================================
    # UC9 — Supply vs demand (stream-stream join)
    # ===================================================================
    print("Starting UC9: Supply vs demand...")
    demand = (
        deserialize_orders(raw_orders_4)
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PLACED")
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(window(col("event_timestamp"), "1 minute"), col("zone_id"))
        .agg(count("*").alias("demand_orders"))
    )
    supply = (
        deserialize_couriers(raw_couriers_2)
        .filter(col("is_duplicate") == False)
        .filter(col("courier_status") == "ONLINE_IDLE")
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(window(col("event_timestamp"), "1 minute"), col("zone_id"))
        .agg(approx_count_distinct("courier_id").alias("supply_couriers"))
    )
    uc9 = (
        demand.join(supply, on=["window", "zone_id"], how="inner")
        .withColumn("demand_supply_ratio",
            _round(col("demand_orders") / when(col("supply_couriers") == 0, 1).otherwise(col("supply_couriers")), 2))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            col("zone_id"), col("demand_orders"),
            col("supply_couriers"), col("demand_supply_ratio"),
        )
    )
    q9 = parquet_sink(uc9, "uc9_supply_demand")

    # ===================================================================
    # UC10 — Avg processing time (PLACED → PICKED_UP self-join)
    # ===================================================================
    print("Starting UC10: Avg processing time...")
    placed = (
        deserialize_orders(raw_orders_2)
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PLACED")
        .select(col("order_id"), col("zone_id"), col("event_timestamp").alias("placed_ts"))
        .withWatermark("placed_ts", "30 seconds")
    )
    picked_up = (
        deserialize_orders(raw_orders_3)
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PICKED_UP")
        .select(col("order_id").alias("pu_order_id"), col("event_timestamp").alias("picked_up_ts"))
        .withWatermark("picked_up_ts", "30 seconds")
    )
    uc10 = (
        placed.join(picked_up,
            expr("order_id = pu_order_id AND picked_up_ts >= placed_ts AND picked_up_ts <= placed_ts + interval 2 hours"),
            how="inner")
        .withColumn("processing_time_sec", col("picked_up_ts").cast("long") - col("placed_ts").cast("long"))
        .groupBy(window(col("placed_ts"), "1 minute"), col("zone_id"))
        .agg(
            count("*").alias("order_count"),
            _round(avg("processing_time_sec"), 0).alias("avg_processing_sec"),
            _min("processing_time_sec").alias("min_processing_sec"),
            _max("processing_time_sec").alias("max_processing_sec"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            col("zone_id"), col("order_count"),
            col("avg_processing_sec"), col("min_processing_sec"),
            col("max_processing_sec"),
        )
    )
    q10 = parquet_sink(uc10, "uc10_processing_time")

    # ===================================================================
    # UC11 — Avg order value per zone
    # ===================================================================
    print("Starting UC11: Avg order value...")
    uc11 = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PLACED")
        .filter(col("order_total_cents").isNotNull())
        .withColumn("order_total_eur", _round(col("order_total_cents") / 100, 2))
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(window(col("event_timestamp"), "1 minute"), col("zone_id"))
        .agg(
            count("*").alias("order_count"),
            _round(avg("order_total_eur"), 2).alias("avg_order_eur"),
            _round(_min("order_total_eur"), 2).alias("min_order_eur"),
            _round(_max("order_total_eur"), 2).alias("max_order_eur"),
            _round(_sum("order_total_eur"), 2).alias("total_revenue_eur"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            col("zone_id"), col("order_count"),
            col("avg_order_eur"), col("min_order_eur"),
            col("max_order_eur"), col("total_revenue_eur"),
        )
    )
    q11 = parquet_sink(uc11, "uc11_order_value")

    # ===================================================================
    # Wait for all queries
    # ===================================================================
    queries = [q1, q3, q4, q7, q9, q10, q11]
    print(f"\n✓ All {len(queries)} streaming queries started!")
    print(f"  Parquet outputs → {OUTPUT_BASE}")
    print(f"  Press Ctrl+C to stop.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping all queries...")
        for q in queries:
            q.stop()
        spark.stop()
        print("Done.")


if __name__ == "__main__":
    main()
