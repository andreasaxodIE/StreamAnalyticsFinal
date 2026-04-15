#!/usr/bin/env python3
"""
eventhub_producer.py — Publishes generated food-delivery events to Azure Event Hub.

Uses the existing generator (Milestone 1) to create realistic events, serializes
them as AVRO binary, and streams them to two Event Hub topics via the Kafka protocol.

Usage:
    # From repo root:
    python producer/eventhub_producer.py

    # Custom scale:
    python producer/eventhub_producer.py --orders 500 --couriers 150 --duration 3600

    # Fast mode (no delay between events):
    python producer/eventhub_producer.py --no-delay
"""

import argparse
import io
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------------------------
# Path setup — allow importing from generator/ and settings/
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from generator.config import GeneratorConfig
from generator.order_generator import OrderEventGenerator
from generator.courier_generator import CourierFleetGenerator

try:
    from settings.eventhub_config import (
        KAFKA_SASL_CONFIG,
        ORDERS_TOPIC,
        COURIERS_TOPIC,
    )
except ImportError:
    print("ERROR: settings/eventhub_config.py not found.")
    print("  Copy the template:  cp settings/eventhub_config_template.py settings/eventhub_config.py")
    print("  Then fill in your Azure Event Hub connection string.")
    sys.exit(1)

import fastavro
from confluent_kafka import Producer


# ---------------------------------------------------------------------------
# AVRO helpers
# ---------------------------------------------------------------------------
SCHEMA_DIR = os.path.join(REPO_ROOT, "schemas")


def load_avro_schema(filename: str) -> dict:
    """Load and parse an AVRO schema from the schemas/ directory."""
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path) as f:
        schema = json.load(f)
    return fastavro.parse_schema(schema)


def serialize_avro(record: dict, parsed_schema) -> bytes:
    """Serialize a single record as AVRO binary (schemaless — no OCF container)."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Kafka / Event Hub producer
# ---------------------------------------------------------------------------
def create_producer() -> Producer:
    """Create a confluent-kafka Producer configured for Azure Event Hub."""
    return Producer(KAFKA_SASL_CONFIG)


def delivery_callback(err, msg):
    """Called once per message to confirm delivery or report errors."""
    if err is not None:
        print(f"  ✗ Delivery failed: {err}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Publish food-delivery events to Azure Event Hub",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--orders",      type=int,   default=100,  help="Number of orders to simulate")
    p.add_argument("--couriers",    type=int,   default=80,   help="Number of couriers")
    p.add_argument("--restaurants", type=int,   default=50,   help="Number of restaurants")
    p.add_argument("--customers",   type=int,   default=500,  help="Customer pool size")
    p.add_argument("--duration",    type=int,   default=3600, help="Simulation window in seconds")
    p.add_argument("--seed",        type=int,   default=42,   help="Random seed")
    p.add_argument("--batch-size",  type=int,   default=50,   help="Events per flush batch")
    p.add_argument("--delay",       type=float, default=0.5,  help="Seconds between batches")
    p.add_argument("--no-delay",    action="store_true",       help="Send all events as fast as possible")
    return p.parse_args()


def main():
    args = parse_args()

    # Build generator config (reuses M1 generator)
    cfg = GeneratorConfig(
        num_restaurants=args.restaurants,
        num_couriers=args.couriers,
        num_customers=args.customers,
        simulation_duration_seconds=args.duration,
        random_seed=args.seed,
    )

    # Simulation starts at yesterday noon (same as M1)
    now = datetime.now(timezone.utc)
    sim_start = datetime(now.year, now.month, now.day, 11, 0, 0, tzinfo=timezone.utc) - timedelta(days=1)
    start_ts_ms = int(sim_start.timestamp() * 1000)

    # Load AVRO schemas
    order_schema = load_avro_schema("order_lifecycle_event.avsc")
    courier_schema = load_avro_schema("courier_status_event.avsc")

    # Generate events
    print(f"\n{'='*60}")
    print(f"  Event Hub Producer — Group 09")
    print(f"{'='*60}")
    print(f"  Orders: {args.orders}  |  Couriers: {args.couriers}")
    print(f"  Topics: {ORDERS_TOPIC}, {COURIERS_TOPIC}")
    print(f"{'='*60}\n")

    print("Generating order events...")
    order_gen = OrderEventGenerator(cfg)
    order_events = list(order_gen.stream(start_ts_ms, n_orders=args.orders))
    print(f"  {len(order_events)} order events generated")

    print("Generating courier events...")
    courier_gen = CourierFleetGenerator(cfg)
    courier_events = list(courier_gen.stream(start_ts_ms))
    print(f"  {len(courier_events)} courier events generated\n")

    # Create producer
    producer = create_producer()

    # ---------------------------------------------------------------------------
    # Publish order events
    # ---------------------------------------------------------------------------
    print(f"Publishing {len(order_events)} order events to [{ORDERS_TOPIC}]...")
    order_sent = 0
    for i, event in enumerate(order_events):
        payload = serialize_avro(event, order_schema)
        # Partition by zone_id for locality
        producer.produce(
            topic=ORDERS_TOPIC,
            value=payload,
            key=event["zone_id"].encode("utf-8"),
            callback=delivery_callback,
        )
        order_sent += 1

        # Flush in batches
        if (i + 1) % args.batch_size == 0:
            producer.flush()
            print(f"  → {order_sent}/{len(order_events)} order events sent")
            if not args.no_delay:
                time.sleep(args.delay)

    producer.flush()
    print(f"  ✓ All {order_sent} order events published\n")

    # ---------------------------------------------------------------------------
    # Publish courier events
    # ---------------------------------------------------------------------------
    print(f"Publishing {len(courier_events)} courier events to [{COURIERS_TOPIC}]...")
    courier_sent = 0
    for i, event in enumerate(courier_events):
        payload = serialize_avro(event, courier_schema)
        # Partition by zone_id
        producer.produce(
            topic=COURIERS_TOPIC,
            value=payload,
            key=event["zone_id"].encode("utf-8"),
            callback=delivery_callback,
        )
        courier_sent += 1

        if (i + 1) % args.batch_size == 0:
            producer.flush()
            print(f"  → {courier_sent}/{len(courier_events)} courier events sent")
            if not args.no_delay:
                time.sleep(args.delay)

    producer.flush()
    print(f"  ✓ All {courier_sent} courier events published\n")

    # Summary
    print(f"{'='*60}")
    print(f"  Done! Total events sent: {order_sent + courier_sent}")
    print(f"    Orders:   {order_sent} → {ORDERS_TOPIC}")
    print(f"    Couriers: {courier_sent} → {COURIERS_TOPIC}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
