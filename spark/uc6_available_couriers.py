#!/usr/bin/env python3
"""
UC6 — Available couriers per zone (real-time supply signal).

Counts distinct ONLINE_IDLE couriers per zone_id in 1-minute tumbling
windows. Enables surge pricing triggers and courier rebalancing.

Outputs: window, zone_id, idle_couriers

Run:  python spark/uc6_available_couriers.py
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session, read_couriers_stream,
    deserialize_couriers,
)
from pyspark.sql.functions import (
    col, window, approx_count_distinct,
)


def main():
    spark = create_spark_session("UC6_AvailableCouriers")

    raw = read_couriers_stream(spark)
    couriers = deserialize_couriers(raw)

    # Filter: only ONLINE_IDLE status, no duplicates
    idle = (
        couriers
        .filter(col("is_duplicate") == False)
        .filter(col("courier_status") == "ONLINE_IDLE")
    )

    idle_wm = idle.withWatermark("event_timestamp", "1 minute")

    # Tumbling window: 1 minute, count distinct couriers per zone
    result = (
        idle_wm
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("zone_id"),
        )
        .agg(
            approx_count_distinct("courier_id").alias("idle_couriers"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id",
            "idle_couriers",
        )
    )

    query = (
        result.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("uc6_supply")
        .start()
    )

    print("\n✓ UC6 streaming query started: uc6_supply")
    print("  Press Ctrl+C to stop.\n")

    import time
    try:
        while query.isActive:
            time.sleep(10)
            spark.sql("""
                SELECT window_start, zone_id, idle_couriers
                FROM uc6_supply
                ORDER BY window_start DESC, idle_couriers ASC
            """).show(20, truncate=False)
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
