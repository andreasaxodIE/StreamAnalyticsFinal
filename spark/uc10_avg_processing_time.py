#!/usr/bin/env python3
"""
UC10 — Average total processing time per zone (PLACED → PICKED_UP).

Self-joins the order feed on order_id, pairing PLACED events (start)
with PICKED_UP events (food collected by courier). The difference is
the total processing time: restaurant accept + cook + courier pickup.

This is a stream-stream SELF-JOIN: two filtered views of the same
Kafka topic joined on order_id within a time bound.

Outputs: window, zone_id, order_count, avg_processing_sec,
         min_processing_sec, max_processing_sec

Run:  python spark/uc10_avg_processing_time.py
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session, read_orders_stream,
    deserialize_orders,
)
from pyspark.sql.functions import (
    col, window, count, avg, min as _min, max as _max,
    round as _round, expr,
)


def main():
    spark = create_spark_session("UC10_AvgProcessingTime")

    # We need TWO independent reads from the same topic for the self-join
    # (Spark requires separate streaming sources for each side of a join)
    raw_placed = read_orders_stream(spark)
    raw_pickup = read_orders_stream(spark)

    # ---------------------------------------------------------------------------
    # LEFT side: PLACED events (start of processing)
    # ---------------------------------------------------------------------------
    placed = (
        deserialize_orders(raw_placed)
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PLACED")
        .select(
            col("order_id"),
            col("zone_id"),
            col("event_timestamp").alias("placed_ts"),
            col("is_peak_hour"),
            col("weather_condition"),
        )
        .withWatermark("placed_ts", "2 minutes")
    )

    # ---------------------------------------------------------------------------
    # RIGHT side: PICKED_UP events (courier collected food)
    # ---------------------------------------------------------------------------
    picked_up = (
        deserialize_orders(raw_pickup)
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PICKED_UP")
        .select(
            col("order_id").alias("pu_order_id"),
            col("event_timestamp").alias("picked_up_ts"),
        )
        .withWatermark("picked_up_ts", "2 minutes")
    )

    # ---------------------------------------------------------------------------
    # STREAM-STREAM SELF-JOIN on order_id
    # Time bound: PICKED_UP must occur within 2 hours after PLACED
    # ---------------------------------------------------------------------------
    joined = (
        placed.join(
            picked_up,
            expr("""
                order_id = pu_order_id
                AND picked_up_ts >= placed_ts
                AND picked_up_ts <= placed_ts + interval 2 hours
            """),
            how="inner",
        )
        .withColumn(
            "processing_time_sec",
            (col("picked_up_ts").cast("long") - col("placed_ts").cast("long")),
        )
    )

    # ---------------------------------------------------------------------------
    # Tumbling window: 5 min, grouped by zone_id
    # ---------------------------------------------------------------------------
    result = (
        joined
        .groupBy(
            window(col("placed_ts"), "5 minutes"),
            col("zone_id"),
        )
        .agg(
            count("*").alias("order_count"),
            _round(avg("processing_time_sec"), 0).alias("avg_processing_sec"),
            _min("processing_time_sec").alias("min_processing_sec"),
            _max("processing_time_sec").alias("max_processing_sec"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id",
            "order_count",
            "avg_processing_sec",
            "min_processing_sec",
            "max_processing_sec",
        )
    )

    query = (
        result.writeStream
        .outputMode("append")
        .format("memory")
        .queryName("uc10_processing_time")
        .start()
    )

    print("\n✓ UC10 streaming query started: uc10_processing_time")
    print("  Measures PLACED → PICKED_UP duration per zone")
    print("  Press Ctrl+C to stop.\n")

    import time
    try:
        while query.isActive:
            time.sleep(15)
            spark.sql("""
                SELECT window_start, zone_id, order_count,
                       avg_processing_sec, min_processing_sec, max_processing_sec
                FROM uc10_processing_time
                ORDER BY window_start DESC, avg_processing_sec DESC
            """).show(20, truncate=False)
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
