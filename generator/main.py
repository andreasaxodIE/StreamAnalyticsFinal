#!/usr/bin/env python3
"""
main.py — CLI entry point for the food-delivery streaming event generator.

Usage examples:
    # Generate default sample data (100 orders, 80 couriers, 1-hour window)
    python main.py

    # Custom scale
    python main.py --orders 500 --couriers 150 --restaurants 80 --duration 7200

    # Enable demand surge in downtown zone
    python main.py --orders 300 --surge --surge-zone zone_downtown --surge-multiplier 4.0

    # Reproducible run with fixed seed
    python main.py --seed 12345 --orders 200

    # Output directory
    python main.py --output-dir ./my_sample_data
"""

import argparse
import json
import os
import sys
import struct
import io
from datetime import datetime, timezone, timedelta
from typing import List, Dict

# Allow running from any working directory
sys.path.insert(0, os.path.dirname(__file__))

from config import GeneratorConfig
from order_generator import OrderEventGenerator
from courier_generator import CourierFleetGenerator
from avro_writer import AvroWriter


# ---------------------------------------------------------------------------
# Load schemas
# ---------------------------------------------------------------------------
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")

def _load_schema(filename: str) -> Dict:
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def write_json_lines(events: List[Dict], filepath: str):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
    print(f"  ✓ JSON:  {filepath}  ({len(events)} events)")


def write_avro(events: List[Dict], schema: Dict, filepath: str):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with AvroWriter(schema, filepath) as w:
        for ev in events:
            w.write(ev)
    print(f"  ✓ AVRO:  {filepath}  ({len(events)} events)")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Food-delivery streaming event generator (Milestone 1)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Scale settings
    p.add_argument("--orders",       type=int,   default=100,  help="Number of orders to simulate")
    p.add_argument("--couriers",     type=int,   default=80,   help="Number of couriers in the fleet")
    p.add_argument("--restaurants",  type=int,   default=50,   help="Number of restaurants")
    p.add_argument("--customers",    type=int,   default=500,  help="Customer pool size")
    p.add_argument("--duration",     type=int,   default=3600, help="Simulation window in seconds")
    p.add_argument("--seed",         type=int,   default=42,   help="Random seed for reproducibility")
    p.add_argument("--output-dir",   type=str,   default="./sample_data", help="Output directory")

    # Edge case rates
    p.add_argument("--late-rate",        type=float, default=0.05, help="Late arrival event fraction")
    p.add_argument("--duplicate-rate",   type=float, default=0.02, help="Duplicate event fraction")
    p.add_argument("--cancel-rate",      type=float, default=0.12, help="Order cancellation probability")
    p.add_argument("--anomaly-rate",     type=float, default=0.02, help="Impossible-duration fraction")

    # Demand surge
    p.add_argument("--surge",            action="store_true",          help="Enable demand surge")
    p.add_argument("--surge-zone",       type=str, default="zone_downtown")
    p.add_argument("--surge-multiplier", type=float, default=3.0)

    return p.parse_args()


def main():
    # Parse CLI args first so the run is configurable
    args = parse_args()

    # Build a config object that all generators use.
    # Keeps the parameters centralized and easy to pass around.
    cfg = GeneratorConfig(
        num_restaurants=args.restaurants,
        num_couriers=args.couriers,
        num_customers=args.customers,
        simulation_duration_seconds=args.duration,
        late_arrival_rate=args.late_rate,
        duplicate_rate=args.duplicate_rate,
        cancellation_rate=args.cancel_rate,
        impossible_duration_rate=args.anomaly_rate,
        surge_active=args.surge,
        surge_zone=args.surge_zone,
        surge_multiplier=args.surge_multiplier,
        output_dir=args.output_dir,
        random_seed=args.seed,
    )

    # Base timestamp: simulate yesterday's lunch peak (noon)
    now = datetime.now(timezone.utc)
    sim_start = datetime(now.year, now.month, now.day, 11, 0, 0, tzinfo=timezone.utc) - timedelta(days=1)
    start_ts_ms = int(sim_start.timestamp() * 1000)

    print(f"\n{'='*60}")
    print(f"  Food Delivery Event Generator — Milestone 1")
    print(f"{'='*60}")
    print(f"  Simulation start : {sim_start.isoformat()}")
    print(f"  Orders           : {args.orders}")
    print(f"  Couriers         : {args.couriers}")
    print(f"  Restaurants      : {args.restaurants}")
    print(f"  Duration         : {args.duration}s ({args.duration//60} min)")
    print(f"  Surge            : {'ON → ' + args.surge_zone if args.surge else 'OFF'}")
    print(f"  Random seed      : {args.seed}")
    print(f"  Output directory : {args.output_dir}")
    print(f"{'='*60}\n")

    # Load schemas
    order_schema   = _load_schema("order_lifecycle_event.avsc")
    courier_schema = _load_schema("courier_status_event.avsc")

    # Feed 1: Order Lifecycle Events
    print("Generating Feed 1: Order Lifecycle Events ...")
    order_gen = OrderEventGenerator(cfg)
    order_events = list(order_gen.stream(start_ts_ms, n_orders=args.orders))
    print(f"  Generated {len(order_events)} order events from {args.orders} orders")

    out_dir = args.output_dir
    write_json_lines(order_events, os.path.join(out_dir, "order_lifecycle_events.jsonl"))
    write_avro(order_events, order_schema, os.path.join(out_dir, "order_lifecycle_events.avro"))

    # Feed 2: Courier Status Events
    print("\nGenerating Feed 2: Courier Status Events ...")
    courier_gen = CourierFleetGenerator(cfg)
    courier_events = list(courier_gen.stream(start_ts_ms))
    print(f"  Generated {len(courier_events)} courier events from {args.couriers} couriers")

    write_json_lines(courier_events, os.path.join(out_dir, "courier_status_events.jsonl"))
    write_avro(courier_events, courier_schema, os.path.join(out_dir, "courier_status_events.avro"))

    # Summary statistics
    print(f"\n{'='*60}")
    print("  Summary Statistics")
    print(f"{'='*60}")

    order_statuses = {}
    for e in order_events:
        s = e["order_status"]
        order_statuses[s] = order_statuses.get(s, 0) + 1

    print("  Order events by status:")
    for status, count in sorted(order_statuses.items()):
        print(f"    {status:<25} {count:>5}")

    duplicates_orders  = sum(1 for e in order_events   if e["is_duplicate"])
    late_orders        = sum(1 for e in order_events   if e["is_late_arrival"])
    duplicates_courier = sum(1 for e in courier_events if e["is_duplicate"])
    late_courier       = sum(1 for e in courier_events if e["is_late_arrival"])
    anomalies          = sum(1 for e in courier_events if e.get("anomaly_flag"))

    print(f"\n  Edge cases injected:")
    print(f"    Order duplicates    : {duplicates_orders}")
    print(f"    Order late arrivals : {late_orders}")
    print(f"    Courier duplicates  : {duplicates_courier}")
    print(f"    Courier late pings  : {late_courier}")
    print(f"    Courier anomalies   : {anomalies}")
    print(f"\n  Total events: {len(order_events) + len(courier_events)}")
    print(f"{'='*60}\n")
    print("Done! Sample data written to:", os.path.abspath(out_dir))


if __name__ == "__main__":
    main()
