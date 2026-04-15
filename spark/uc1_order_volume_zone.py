#!/usr/bin/env python3
"""
UC1 — Real-time order volume and cancellation rate by zone.

Tumbling window (5 min) counts total orders and cancellations per zone.
Outputs: window, zone_id, total_orders, cancelled_orders, cancellation_rate

Run:  python spark/uc1_order_volume_zone.py
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session, read_orders_stream,
    deserialize_orders,
)
from pyspark.sql.functions import (
    col, window, count, sum as _sum, round as _round, when,
)


def main():
    spark = create_spark_session("UC1_OrderVolumeCancellation")

    # Read and deserialize order events
    raw = read_orders_stream(spark)
    orders = deserialize_orders(raw)

    # Filter out duplicates (generator-labelled)
    orders_clean = orders.filter(col("is_duplicate") == False)

    # Watermark: tolerate up to 2 minutes of late data
    orders_wm = orders_clean.withWatermark("event_timestamp", "2 minutes")

    # Tumbling window: 5 minutes, grouped by zone_id
    result = (
        orders_wm
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("zone_id"),
        )
        .agg(
            count("*").alias("total_orders"),
            _sum(when(col("order_status") == "CANCELLED", 1).otherwise(0))
                .alias("cancelled_orders"),
        )
        .withColumn(
            "cancellation_rate",
            _round(col("cancelled_orders") / col("total_orders") * 100, 2),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id",
            "total_orders",
            "cancelled_orders",
            "cancellation_rate",
        )
    )

    # Write to memory table for interactive querying
    query = (
        result.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("uc1_order_volume")
        .start()
    )

    print("\n✓ UC1 streaming query started: uc1_order_volume")
    print("  Run SQL: spark.sql('SELECT * FROM uc1_order_volume ORDER BY cancellation_rate DESC').show()")
    print("  Press Ctrl+C to stop.\n")

    # Poll and display results periodically
    import time
    try:
        while query.isActive:
            time.sleep(10)
            spark.sql("""
                SELECT window_start, window_end, zone_id,
                       total_orders, cancelled_orders, cancellation_rate
                FROM uc1_order_volume
                ORDER BY window_start DESC, cancellation_rate DESC
            """).show(20, truncate=False)
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
