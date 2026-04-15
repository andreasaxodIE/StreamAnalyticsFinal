#!/usr/bin/env python3
"""
UC7 — Anomaly detection pipeline.

Flags courier events with anomaly_flag (IMPOSSIBLE_SPEED, LOCATION_JUMP,
OFFLINE_MID_DELIVERY, CLOCK_SKEW, DUPLICATE_PING). Computes anomaly rate
per zone and vehicle type in sliding windows.

Outputs: window, zone_id, vehicle_type, anomaly_type, anomaly_count,
         total_events, anomaly_rate

Run:  python spark/uc7_anomaly_detection.py
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session, read_couriers_stream,
    deserialize_couriers,
)
from pyspark.sql.functions import (
    col, window, count, when, sum as _sum, round as _round, lit,
)


def main():
    spark = create_spark_session("UC7_AnomalyDetection")

    raw = read_couriers_stream(spark)
    couriers = deserialize_couriers(raw)

    # No duplicate filter here — we want to detect duplicates too
    couriers_wm = couriers.withWatermark("event_timestamp", "2 minutes")

    # Sliding window: 10 min window, 5 min slide, by zone + vehicle type
    result = (
        couriers_wm
        .groupBy(
            window(col("event_timestamp"), "10 minutes", "5 minutes"),
            col("zone_id"),
            col("vehicle_type"),
        )
        .agg(
            count("*").alias("total_events"),
            # Count each anomaly type
            _sum(when(col("anomaly_flag") == "IMPOSSIBLE_SPEED", 1).otherwise(0))
                .alias("impossible_speed"),
            _sum(when(col("anomaly_flag") == "LOCATION_JUMP", 1).otherwise(0))
                .alias("location_jump"),
            _sum(when(col("anomaly_flag") == "OFFLINE_MID_DELIVERY", 1).otherwise(0))
                .alias("offline_mid_delivery"),
            _sum(when(col("anomaly_flag") == "CLOCK_SKEW", 1).otherwise(0))
                .alias("clock_skew"),
            _sum(when(col("anomaly_flag") == "DUPLICATE_PING", 1).otherwise(0))
                .alias("duplicate_ping"),
            # Total anomalies
            _sum(when(col("anomaly_flag").isNotNull(), 1).otherwise(0))
                .alias("total_anomalies"),
        )
        .withColumn(
            "anomaly_rate",
            _round(col("total_anomalies") / col("total_events") * 100, 2),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id",
            "vehicle_type",
            "total_events",
            "total_anomalies",
            "anomaly_rate",
            "impossible_speed",
            "location_jump",
            "offline_mid_delivery",
            "clock_skew",
            "duplicate_ping",
        )
    )

    query = (
        result.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("uc7_anomalies")
        .start()
    )

    print("\n✓ UC7 streaming query started: uc7_anomalies")
    print("  Press Ctrl+C to stop.\n")

    import time
    try:
        while query.isActive:
            time.sleep(10)
            spark.sql("""
                SELECT window_start, zone_id, vehicle_type,
                       total_events, total_anomalies, anomaly_rate,
                       impossible_speed, location_jump, offline_mid_delivery
                FROM uc7_anomalies
                WHERE total_anomalies > 0
                ORDER BY anomaly_rate DESC
            """).show(20, truncate=False)
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
