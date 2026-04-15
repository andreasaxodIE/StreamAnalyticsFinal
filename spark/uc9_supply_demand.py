#!/usr/bin/env python3
"""
UC9 — Supply vs demand imbalance by zone (stream-stream join).

Joins windowed order counts (demand) with windowed idle courier counts
(supply) per zone_id. Computes demand/supply ratio.

This is a TRUE stream-stream join: two independent Kafka topics joined
in real time on zone_id within aligned tumbling windows.

Outputs: window, zone_id, demand (orders), supply (idle couriers),
         demand_supply_ratio

Run:  python spark/uc9_supply_demand.py
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session,
    read_orders_stream, read_couriers_stream,
    deserialize_orders, deserialize_couriers,
)
from pyspark.sql.functions import (
    col, window, count, countDistinct, round as _round, when,
)


def main():
    spark = create_spark_session("UC9_SupplyDemand")

    # -----------------------------------------------------------------------
    # DEMAND side: order events (count PLACED events per zone per window)
    # -----------------------------------------------------------------------
    raw_orders = read_orders_stream(spark)
    orders = deserialize_orders(raw_orders)

    demand = (
        orders
        .filter(col("is_duplicate") == False)
        .filter(col("order_status") == "PLACED")
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("zone_id"),
        )
        .agg(count("*").alias("demand_orders"))
    )

    # -----------------------------------------------------------------------
    # SUPPLY side: courier events (count ONLINE_IDLE couriers per zone)
    # -----------------------------------------------------------------------
    raw_couriers = read_couriers_stream(spark)
    couriers = deserialize_couriers(raw_couriers)

    supply = (
        couriers
        .filter(col("is_duplicate") == False)
        .filter(col("courier_status") == "ONLINE_IDLE")
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("zone_id"),
        )
        .agg(countDistinct("courier_id").alias("supply_couriers"))
    )

    # -----------------------------------------------------------------------
    # STREAM-STREAM JOIN on zone_id + window
    # -----------------------------------------------------------------------
    joined = (
        demand.join(
            supply,
            on=["window", "zone_id"],
            how="inner",
        )
        .withColumn(
            "demand_supply_ratio",
            _round(
                col("demand_orders") /
                when(col("supply_couriers") == 0, 1).otherwise(col("supply_couriers")),
                2,
            ),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "zone_id",
            "demand_orders",
            "supply_couriers",
            "demand_supply_ratio",
        )
    )

    query = (
        joined.writeStream
        .outputMode("append")
        .format("memory")
        .queryName("uc9_supply_demand")
        .start()
    )

    print("\n✓ UC9 streaming query started: uc9_supply_demand")
    print("  Ratio > 1.0 = demand exceeds supply (needs more couriers)")
    print("  Ratio < 1.0 = surplus supply (couriers underutilised)")
    print("  Press Ctrl+C to stop.\n")

    import time
    try:
        while query.isActive:
            time.sleep(15)
            spark.sql("""
                SELECT window_start, zone_id,
                       demand_orders, supply_couriers, demand_supply_ratio
                FROM uc9_supply_demand
                ORDER BY window_start DESC, demand_supply_ratio DESC
            """).show(20, truncate=False)
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
