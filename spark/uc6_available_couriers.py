#!/usr/bin/env python3
"""
UC6 (fixed) — Available couriers per zone.

Runs independently from run_all_ucs.py. Writes to output/uc6_supply_fixed.csv.

What's different from the original UC6:
  - countDistinct (exact) instead of approx_count_distinct. HyperLogLog is
    unreliable for the small courier cardinalities we have per zone, which
    was producing 1.0 in every zone.
  - 2-minute window + 3-minute watermark so couriers (~15s pings) have time
    to check in before the window closes.
  - Accumulating CSV sink: merges each batch with existing rows instead of
    overwriting, so the running aggregate is persisted across batches.

Run it in a separate terminal from run_all_ucs.py:
    python spark/uc6_available_couriers_fixed.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import (
    create_spark_session,
    read_couriers_stream,
    deserialize_couriers,
)
from pyspark.sql.functions import col, window, countDistinct

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(REPO_ROOT, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "uc6_supply_fixed.csv")


def write_accumulate(df, batch_id):
    """Merge new batch with existing CSV, dedupe by (window_start, zone_id)."""
    if df.count() == 0:
        return
    import pandas as pd
    new_rows = df.toPandas()
    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
        try:
            existing = pd.read_csv(OUTPUT_FILE)
            combined = pd.concat([existing, new_rows], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["window_start", "zone_id"], keep="last"
            )
        except Exception:
            combined = new_rows
    else:
        combined = new_rows
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"  [uc6_fixed] batch {batch_id}: +{len(new_rows)} rows → {len(combined)} total")


def main():
    spark = create_spark_session("UC6_Fixed")

    raw = read_couriers_stream(spark)
    couriers = deserialize_couriers(raw)

    idle = (
        couriers
        .filter(col("is_duplicate") == False)
        .filter(col("courier_status") == "ONLINE_IDLE")
        .withWatermark("event_timestamp", "3 minutes")
    )

    result = (
        idle
        .groupBy(
            window(col("event_timestamp"), "2 minutes"),
            col("zone_id"),
        )
        .agg(countDistinct("courier_id").alias("idle_couriers"))
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
        .foreachBatch(write_accumulate)
        .queryName("uc6_fixed")
        .start()
    )

    print(f"\n✓ UC6 (fixed) streaming query started")
    print(f"  Writing to: {OUTPUT_FILE}")
    print(f"  Press Ctrl+C to stop.\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
