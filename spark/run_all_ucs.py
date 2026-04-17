#!/usr/bin/env python3
"""
run_all_ucs.py — Runs all 8 use cases in a single Spark session.

Writes aggregated results to CSV files in output/ for the Streamlit dashboard.

Usage:
    python spark/run_all_ucs.py

Requires:
    - HADOOP_HOME set (Windows: C:\\hadoop with winutils.exe)
    - Producer running to send events to Event Hub
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session,
    read_orders_stream, read_couriers_stream,
    deserialize_orders, deserialize_couriers,
)
from pyspark.sql.functions import (
    col, window, count, countDistinct, avg, min as _min, max as _max,
    sum as _sum, round as _round, when, expr, percentile_approx,
    approx_count_distinct,
)

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(REPO_ROOT, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_to_csv(df, batch_id, filename, key_cols=("window_start", "zone_id")):
    """foreachBatch sink: merge batch rows with existing CSV, deduping by key.

    With outputMode('update'), each batch contains only rows whose aggregate
    value changed in that batch. Simply overwriting the CSV would lose all
    prior history. Instead, read existing rows, concatenate the new batch,
    and keep the last value per (window_start, zone_id) key.
    """
    try:
        n = df.count()
        if n == 0:
            # Quiet heartbeat so we know the query is alive
            if batch_id % 10 == 0:
                print(f"  [{filename}] batch {batch_id}: 0 rows")
            return
        import pandas as pd
        path = os.path.join(OUTPUT_DIR, filename)
        new_rows = df.toPandas()

        keys = [c for c in key_cols if c in new_rows.columns]

        if os.path.exists(path) and os.path.getsize(path) > 0:
            try:
                existing = pd.read_csv(path)
                combined = pd.concat([existing, new_rows], ignore_index=True)
                if keys:
                    combined = combined.drop_duplicates(subset=keys, keep="last")
            except Exception as e:
                print(f"  [{filename}] could not merge existing ({e}); overwriting")
                combined = new_rows
        else:
            combined = new_rows

        combined.to_csv(path, index=False)
        print(f"  [{filename}] batch {batch_id}: +{n} rows → {len(combined)} total")
    except Exception as e:
        # Surface errors loudly — foreachBatch swallows them otherwise
        import traceback
        print(f"  [{filename}] ERROR in batch {batch_id}: {e}")
        traceback.print_exc()
        raise


def main():
    spark = create_spark_session("FoodDelivery_AllUCs")

    print(f"\n{'='*60}")
    print(f"  Spark Streaming — All Use Cases")
    print(f"  Output: {OUTPUT_DIR}")
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
    print("Starting UC1: Order volume & cancellation rate...")
    uc1 = (
        orders
        .filter(col("is_duplicate") == False)
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(window(col("event_timestamp"), "5 minutes"), col("zone_id"))
        .agg(
            count("*").alias("total_orders"),
            _sum(when(col("order_status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_orders"),
        )
        .withColumn("cancellation_rate", _round(col("cancelled_orders") / col("total_orders") * 100, 2))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id", "total_orders", "cancelled_orders", "cancellation_rate",
        )
    )
    q1 = (
        uc1.writeStream.outputMode("update")
        .foreachBatch(lambda df, bid: write_to_csv(df, bid, "uc1_order_volume.csv"))
        .queryName("uc1")
        .start()
    )

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
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            window(col("event_timestamp"), "10 minutes", "5 minutes"),
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
            "zone_id", "restaurant_id", "is_peak_hour",
            "breach_count", "avg_prep_time_sec", "max_prep_time_sec",
        )
    )
    q3 = (
        uc3.writeStream.outputMode("update")
        .foreachBatch(lambda df, bid: write_to_csv(
            df, bid, "uc3_prep_sla.csv",
            key_cols=("window_start", "zone_id", "restaurant_id", "is_peak_hour"),
        ))
        .queryName("uc3")
        .start()
    )

    # ===================================================================
    # UC4 — Weather impact on delivery times
    # ===================================================================
    print("Starting UC4: Weather impact...")
    uc4 = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "DELIVERED")
        .filter(col("actual_delivery_time_seconds").isNotNull())
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(window(col("event_timestamp"), "10 minutes"), col("weather_condition"))
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
            "weather_condition", "order_count",
            "avg_delivery_sec", "min_delivery_sec", "max_delivery_sec", "p95_delivery_sec",
        )
    )
    q4 = (
        uc4.writeStream.outputMode("update")
        .foreachBatch(lambda df, bid: write_to_csv(
            df, bid, "uc4_weather.csv",
            key_cols=("window_start", "weather_condition"),
        ))
        .queryName("uc4")
        .start()
    )

    # ===================================================================
    # UC6 — Available couriers per zone
    # ===================================================================
    # Use countDistinct (exact) instead of approx_count_distinct — HLL is
    # unreliable for the small cardinalities we expect per zone. Widen the
    # window to 2 minutes and the watermark to 3 minutes so we're not
    # closing windows before most couriers have pinged at least once
    # (pings arrive every ~15s).
    print("Starting UC6: Available couriers...")
    uc6 = (
        couriers
        .filter(col("is_duplicate") == False)
        .filter(col("courier_status") == "ONLINE_IDLE")
        .withWatermark("event_timestamp", "3 minutes")
        .groupBy(window(col("event_timestamp"), "2 minutes"), col("zone_id"))
        .agg(countDistinct("courier_id").alias("idle_couriers"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id", "idle_couriers",
        )
    )
    q6 = (
        uc6.writeStream.outputMode("update")
        .foreachBatch(lambda df, bid: write_to_csv(df, bid, "uc6_supply.csv"))
        .queryName("uc6")
        .start()
    )

    # ===================================================================
    # UC7 — Anomaly detection
    # ===================================================================
    print("Starting UC7: Anomaly detection...")
    uc7 = (
        couriers
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            window(col("event_timestamp"), "10 minutes", "5 minutes"),
            col("zone_id"), col("vehicle_type"),
        )
        .agg(
            count("*").alias("total_events"),
            _sum(when(col("anomaly_flag") == "IMPOSSIBLE_SPEED", 1).otherwise(0)).alias("impossible_speed"),
            _sum(when(col("anomaly_flag") == "LOCATION_JUMP", 1).otherwise(0)).alias("location_jump"),
            _sum(when(col("anomaly_flag") == "OFFLINE_MID_DELIVERY", 1).otherwise(0)).alias("offline_mid_delivery"),
            _sum(when(col("anomaly_flag").isNotNull(), 1).otherwise(0)).alias("total_anomalies"),
        )
        .withColumn("anomaly_rate", _round(col("total_anomalies") / col("total_events") * 100, 2))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id", "vehicle_type", "total_events", "total_anomalies",
            "anomaly_rate", "impossible_speed", "location_jump", "offline_mid_delivery",
        )
    )
    q7 = (
        uc7.writeStream.outputMode("update")
        .foreachBatch(lambda df, bid: write_to_csv(
            df, bid, "uc7_anomalies.csv",
            key_cols=("window_start", "zone_id", "vehicle_type"),
        ))
        .queryName("uc7")
        .start()
    )

    # ===================================================================
    # UC9 — Supply vs demand (stream-stream join)
    # ===================================================================
    print("Starting UC9: Supply vs demand...")
    demand = (
        deserialize_orders(raw_orders_4)
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PLACED")
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(window(col("event_timestamp"), "5 minutes"), col("zone_id"))
        .agg(count("*").alias("demand_orders"))
    )
    supply = (
        deserialize_couriers(raw_couriers_2)
        .filter(col("is_duplicate") == False)
        .filter(col("courier_status") == "ONLINE_IDLE")
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(window(col("event_timestamp"), "5 minutes"), col("zone_id"))
        .agg(approx_count_distinct("courier_id").alias("supply_couriers"))
    )
    uc9 = (
        demand.join(supply, on=["window", "zone_id"], how="inner")
        .withColumn("demand_supply_ratio",
            _round(col("demand_orders") / when(col("supply_couriers") == 0, 1).otherwise(col("supply_couriers")), 2))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id", "demand_orders", "supply_couriers", "demand_supply_ratio",
        )
    )
    q9 = (
        uc9.writeStream.outputMode("append")
        .foreachBatch(lambda df, bid: write_to_csv(df, bid, "uc9_supply_demand.csv"))
        .queryName("uc9")
        .start()
    )

    # ===================================================================
    # UC10 — Avg processing time (PLACED → PICKED_UP self-join)
    # ===================================================================
    print("Starting UC10: Avg processing time...")
    placed = (
        deserialize_orders(raw_orders_2)
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PLACED")
        .select(col("order_id"), col("zone_id"), col("event_timestamp").alias("placed_ts"))
        .withWatermark("placed_ts", "2 minutes")
    )
    picked_up = (
        deserialize_orders(raw_orders_3)
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PICKED_UP")
        .select(col("order_id").alias("pu_order_id"), col("event_timestamp").alias("picked_up_ts"))
        .withWatermark("picked_up_ts", "2 minutes")
    )
    uc10 = (
        placed.join(picked_up,
            expr("order_id = pu_order_id AND picked_up_ts >= placed_ts AND picked_up_ts <= placed_ts + interval 2 hours"),
            how="inner")
        .withColumn("processing_time_sec", col("picked_up_ts").cast("long") - col("placed_ts").cast("long"))
        .groupBy(window(col("placed_ts"), "5 minutes"), col("zone_id"))
        .agg(
            count("*").alias("order_count"),
            _round(avg("processing_time_sec"), 0).alias("avg_processing_sec"),
            _min("processing_time_sec").alias("min_processing_sec"),
            _max("processing_time_sec").alias("max_processing_sec"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id", "order_count",
            "avg_processing_sec", "min_processing_sec", "max_processing_sec",
        )
    )
    q10 = (
        uc10.writeStream.outputMode("append")
        .foreachBatch(lambda df, bid: write_to_csv(df, bid, "uc10_processing_time.csv"))
        .queryName("uc10")
        .start()
    )

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
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(window(col("event_timestamp"), "5 minutes"), col("zone_id"))
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
            "zone_id", "order_count",
            "avg_order_eur", "min_order_eur", "max_order_eur", "total_revenue_eur",
        )
    )
    q11 = (
        uc11.writeStream.outputMode("update")
        .foreachBatch(lambda df, bid: write_to_csv(df, bid, "uc11_order_value.csv"))
        .queryName("uc11")
        .start()
    )

    # ===================================================================
    # Wait for all queries
    # ===================================================================
    queries = [q1, q3, q4, q6, q7, q9, q10, q11]
    print(f"\n✓ All {len(queries)} streaming queries started!")
    print(f"  CSV outputs → {OUTPUT_DIR}")
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
