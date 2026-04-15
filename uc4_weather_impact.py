#!/usr/bin/env python3
"""
UC4 — Weather impact on delivery times.

Groups DELIVERED events by weather_condition and computes avg / P95
actual_delivery_time_seconds. Quantifies the delay caused by each
weather condition.

Outputs: window, weather_condition, order_count, avg_delivery_sec,
         min_delivery_sec, max_delivery_sec

Run:  python spark/uc4_weather_impact.py
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session, read_orders_stream,
    deserialize_orders,
)
from pyspark.sql.functions import (
    col, window, count, avg, min as _min, max as _max,
    round as _round, percentile_approx,
)


def main():
    spark = create_spark_session("UC4_WeatherImpact")

    raw = read_orders_stream(spark)
    orders = deserialize_orders(raw)

    # Filter: only DELIVERED events with actual delivery time
    delivered = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "DELIVERED")
        .filter(col("actual_delivery_time_seconds").isNotNull())
    )

    delivered_wm = delivered.withWatermark("event_timestamp", "2 minutes")

    # Tumbling window: 10 minutes, grouped by weather condition
    result = (
        delivered_wm
        .groupBy(
            window(col("event_timestamp"), "10 minutes"),
            col("weather_condition"),
        )
        .agg(
            count("*").alias("order_count"),
            _round(avg("actual_delivery_time_seconds"), 0).alias("avg_delivery_sec"),
            _min("actual_delivery_time_seconds").alias("min_delivery_sec"),
            _max("actual_delivery_time_seconds").alias("max_delivery_sec"),
            _round(
                percentile_approx("actual_delivery_time_seconds", 0.95), 0
            ).alias("p95_delivery_sec"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "weather_condition",
            "order_count",
            "avg_delivery_sec",
            "min_delivery_sec",
            "max_delivery_sec",
            "p95_delivery_sec",
        )
    )

    query = (
        result.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("uc4_weather")
        .start()
    )

    print("\n✓ UC4 streaming query started: uc4_weather")
    print("  Press Ctrl+C to stop.\n")

    import time
    try:
        while query.isActive:
            time.sleep(10)
            spark.sql("""
                SELECT window_start, weather_condition,
                       order_count, avg_delivery_sec, p95_delivery_sec,
                       min_delivery_sec, max_delivery_sec
                FROM uc4_weather
                ORDER BY window_start DESC, avg_delivery_sec DESC
            """).show(20, truncate=False)
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
