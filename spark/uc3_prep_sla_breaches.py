#!/usr/bin/env python3
"""
UC3 — Peak-hour prep time SLA breaches.

Filters READY_FOR_PICKUP events where actual_prep_time_seconds exceeds a
threshold (default 1800s = 30 min). Segments by is_peak_hour to measure
how kitchens degrade under load.

Outputs: window, zone_id, restaurant_id, is_peak_hour, breach_count,
         avg_prep_time, max_prep_time

Run:  python spark/uc3_prep_sla_breaches.py
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session, read_orders_stream,
    deserialize_orders,
)
from pyspark.sql.functions import (
    col, window, count, avg, max as _max, round as _round,
)


SLA_THRESHOLD_SECONDS = 1800  # 30 minutes


def main():
    spark = create_spark_session("UC3_PrepSLABreaches")

    raw = read_orders_stream(spark)
    orders = deserialize_orders(raw)

    # Filter: only READY_FOR_PICKUP events with actual prep time exceeding SLA
    breaches = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "READY_FOR_PICKUP")
        .filter(col("actual_prep_time_seconds").isNotNull())
        .filter(col("actual_prep_time_seconds") > SLA_THRESHOLD_SECONDS)
    )

    # Watermark for late data
    breaches_wm = breaches.withWatermark("event_timestamp", "2 minutes")

    # Sliding window: 10-min window, sliding every 5 min, grouped by zone + peak
    result = (
        breaches_wm
        .groupBy(
            window(col("event_timestamp"), "10 minutes", "5 minutes"),
            col("zone_id"),
            col("restaurant_id"),
            col("is_peak_hour"),
        )
        .agg(
            count("*").alias("breach_count"),
            _round(avg("actual_prep_time_seconds"), 0).alias("avg_prep_time_sec"),
            _max("actual_prep_time_seconds").alias("max_prep_time_sec"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id",
            "restaurant_id",
            "is_peak_hour",
            "breach_count",
            "avg_prep_time_sec",
            "max_prep_time_sec",
        )
    )

    query = (
        result.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("uc3_prep_sla")
        .start()
    )

    print(f"\n✓ UC3 streaming query started: uc3_prep_sla  (SLA threshold: {SLA_THRESHOLD_SECONDS}s)")
    print("  Press Ctrl+C to stop.\n")

    import time
    try:
        while query.isActive:
            time.sleep(10)
            spark.sql("""
                SELECT window_start, zone_id, restaurant_id, is_peak_hour,
                       breach_count, avg_prep_time_sec, max_prep_time_sec
                FROM uc3_prep_sla
                ORDER BY breach_count DESC
            """).show(20, truncate=False)
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
