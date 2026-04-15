#!/usr/bin/env python3
"""
UC11 — Average order value (basket size) per zone.

Aggregates order_total_cents from PLACED events by zone_id in tumbling
windows. Converts cents to euros and computes avg, min, max basket size.

Identifies high-value vs low-value delivery zones for pricing and
promotion strategies.

Outputs: window, zone_id, order_count, avg_order_eur, min_order_eur,
         max_order_eur, total_revenue_eur

Run:  python spark/uc11_avg_order_value.py
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session, read_orders_stream,
    deserialize_orders,
)
from pyspark.sql.functions import (
    col, window, count, avg, min as _min, max as _max,
    sum as _sum, round as _round,
)


def main():
    spark = create_spark_session("UC11_AvgOrderValue")

    raw = read_orders_stream(spark)
    orders = deserialize_orders(raw)

    # Filter: only PLACED events with order_total_cents populated
    placed = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PLACED")
        .filter(col("order_total_cents").isNotNull())
        # Convert cents to euros
        .withColumn("order_total_eur", _round(col("order_total_cents") / 100, 2))
    )

    placed_wm = placed.withWatermark("event_timestamp", "2 minutes")

    # Tumbling window: 5 minutes, grouped by zone_id
    result = (
        placed_wm
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("zone_id"),
        )
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
            "zone_id",
            "order_count",
            "avg_order_eur",
            "min_order_eur",
            "max_order_eur",
            "total_revenue_eur",
        )
    )

    query = (
        result.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("uc11_order_value")
        .start()
    )

    print("\n✓ UC11 streaming query started: uc11_order_value")
    print("  Press Ctrl+C to stop.\n")

    import time
    try:
        while query.isActive:
            time.sleep(10)
            spark.sql("""
                SELECT window_start, zone_id, order_count,
                       avg_order_eur, min_order_eur, max_order_eur,
                       total_revenue_eur
                FROM uc11_order_value
                ORDER BY window_start DESC, avg_order_eur DESC
            """).show(20, truncate=False)
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
