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
    # Breach threshold: 20 min (was 30 min). The simulated restaurants have
    # avg prep times of 10-30 min, so a 30-min threshold only caught outliers
    # — which rarely show up in a short demo. 20 min still represents a real
    # SLA breach but produces enough rows to populate the dashboard.
    uc3 = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "READY_FOR_PICKUP")
        .filter(col("actual_prep_time_seconds").isNotNull())
        .filter(col("actual_prep_time_seconds") > 1200)
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
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
        .groupBy(window(col("event_timestamp"), "1 minute"), col("weather_condition"))
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
            window(col("event_timestamp"), "1 minute"),
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
    # UC10 — Avg prep time per zone
    # ===================================================================
    # Originally a PLACED→PICKED_UP stream-stream join, but append-mode
    # joins hold state until the watermark crosses the join interval,
    # making the first output land many minutes late. For a live demo we
    # pivot to a single-stream aggregation over `actual_prep_time_seconds`
    # on READY_FOR_PICKUP events. Same "processing time" intuition, no join.
    print("Starting UC10: Avg prep time per zone...")
    uc10 = (
        deserialize_orders(raw_orders_2)
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "READY_FOR_PICKUP")
        .filter(col("actual_prep_time_seconds").isNotNull())
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(window(col("event_timestamp"), "1 minute"), col("zone_id"))
        .agg(
            count("*").alias("order_count"),
            _round(avg("actual_prep_time_seconds"), 0).alias("avg_processing_sec"),
            _min("actual_prep_time_seconds").alias("min_processing_sec"),
            _max("actual_prep_time_seconds").alias("max_processing_sec"),
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
    # UC12 — Estimated ETA by zone × weather (the platform's promise)
    # ===================================================================
    # Business question: "What delivery time does our ETA model promise
    # customers, broken down by zone and weather?"
    #
    # Originally attempted as an actual-vs-estimated stream-stream join,
    # but `estimated_delivery_time_seconds` is only populated on the PLACED
    # event (per the generator contract), so join with DELIVERED would need
    # ~10-min state retention — too slow for the demo's append-mode sink.
    #
    # Pivot: aggregate the estimate at PLACED time directly. The dashboard
    # can compare this side-by-side with UC6 (actual delivery by weather)
    # to spot systematic over/underestimation — same business insight via
    # two single-stream queries instead of one expensive join.
    print("Starting UC12: Estimated ETA by zone × weather...")
    uc12 = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PLACED")
        .filter(col("estimated_delivery_time_seconds").isNotNull())
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("zone_id"), col("weather_condition"),
        )
        .agg(
            count("*").alias("order_count"),
            _round(avg("estimated_delivery_time_seconds"), 0).alias("avg_eta_sec"),
            _min("estimated_delivery_time_seconds").alias("min_eta_sec"),
            _max("estimated_delivery_time_seconds").alias("max_eta_sec"),
            _round(percentile_approx("estimated_delivery_time_seconds", 0.9), 0).alias("p90_eta_sec"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            col("zone_id"), col("weather_condition"),
            col("order_count"), col("avg_eta_sec"),
            col("min_eta_sec"), col("max_eta_sec"), col("p90_eta_sec"),
        )
    )
    q12 = parquet_sink(uc12, "uc12_eta_accuracy")

    # ===================================================================
    # UC13 — Courier productivity by vehicle type × zone
    # ===================================================================
    # Business question: "Which vehicle type is most productive in each
    # zone, and what's the distribution of deliveries per session?"
    #
    # The courier stream emits status events with `shift_duration_seconds`
    # (shift length so far) and `deliveries_completed_in_session` (running
    # count). We take the MAX of both per session — representing the final
    # tally at session end — then aggregate across sessions within each
    # (zone, vehicle_type) bucket.
    #
    # Emits:
    #   - session_count        : distinct sessions observed
    #   - avg_deliveries       : mean deliveries per session
    #   - p50_deliveries       : median (robust to outliers)
    #   - p90_deliveries       : top decile
    #   - avg_shift_minutes
    #   - deliveries_per_hour  : overall productivity metric
    print("Starting UC13: Courier productivity by vehicle × zone...")
    session_finals = (
        couriers
        .filter(col("is_duplicate") == False)
        .filter(col("shift_duration_seconds").isNotNull())
        .filter(col("deliveries_completed_in_session").isNotNull())
        .withWatermark("event_timestamp", "30 seconds")
        # Aggregate per-session: max values represent the session's final state
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("zone_id"), col("vehicle_type"), col("session_id"),
        )
        .agg(
            _max("deliveries_completed_in_session").alias("session_deliveries"),
            _max("shift_duration_seconds").alias("session_shift_sec"),
        )
    )
    uc13 = (
        session_finals
        # Roll up across sessions within each (zone, vehicle_type) bucket
        .groupBy(col("window"), col("zone_id"), col("vehicle_type"))
        .agg(
            count("*").alias("session_count"),
            _round(avg("session_deliveries"), 2).alias("avg_deliveries"),
            percentile_approx("session_deliveries", 0.5).alias("p50_deliveries"),
            percentile_approx("session_deliveries", 0.9).alias("p90_deliveries"),
            _round(avg("session_shift_sec") / 60, 1).alias("avg_shift_minutes"),
            _sum("session_deliveries").alias("total_deliveries"),
            _sum("session_shift_sec").alias("total_shift_sec"),
        )
        .withColumn(
            "deliveries_per_hour",
            when(col("total_shift_sec") == 0, 0.0)
            .otherwise(_round(col("total_deliveries") * 3600 / col("total_shift_sec"), 2)),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            col("zone_id"), col("vehicle_type"),
            col("session_count"), col("avg_deliveries"),
            col("p50_deliveries"), col("p90_deliveries"),
            col("avg_shift_minutes"), col("deliveries_per_hour"),
        )
    )
    q13 = parquet_sink(uc13, "uc13_courier_productivity")

    # ===================================================================
    # Wait for all queries
    # ===================================================================
    queries = [q1, q3, q4, q7, q9, q10, q11, q12, q13]
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
