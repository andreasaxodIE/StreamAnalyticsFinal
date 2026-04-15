# Real-Time Food Delivery Analytics Pipeline

> **Milestone 1 — Streaming Data Feed Design & Generation**  
> Course: Big Data & Streaming Systems

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Team Structure](#team-structure)
3. [Repository Layout](#repository-layout)
4. [Feed Design — Rationale & Analytics Intent](#feed-design)
5. [Schema Overview](#schema-overview)
6. [Edge Cases & Streaming Correctness](#edge-cases)
7. [Quick Start](#quick-start)
8. [Planned Analytics (Milestones 2–4)](#planned-analytics)

---

## Project Overview

In this project, our group has designed a **real-time analytics pipeline** for a food-delivery platform inspired popular ordering systems such as Uber Eats, Glovo, and Deliveroo.

We have implemented this platform as a **three-sided marketplace**:

```
Customer ──places order──▶ Platform ──dispatches──▶ Courier ──delivers──▶ Customer
                               │
                               ▼
                          Restaurant
                      (accepts & prepares)
```

Every interaction produces **streaming events**. Our repository implements:

- **Two streaming data feeds** covering the order lifecycle and courier fleet
- **AVRO schemas** with full type safety and schema versioning support
- **A configurable Python generator** that produces realistic, time-stamped events with peak-hour and weather effects
- **Sample datasets** in both JSON Lines and AVRO binary format

---

## Team Structure

| Name | Role |
|------|------|
| Andrea Saxod | Feed Architect — schema design, AVRO versioning | 
| Matias Arevalo | Generator Engineer — Python generator, edge-case injection | 
| Cloe Chapotot | Data Quality Lead — watermark testing, anomaly labelling | 
| Joao Paulo Tobar Prado | Pipeline Engineer — Spark Structured Streaming (M2+) | 
| Vittorio Fialdini | Pipeline Engineer — Spark Structured Streaming (M2+) |
| Clementine, Francesca Mathieu | Dashboard Engineer — visualisation layer (M4) | 

---

## Repository Layout

```
food-delivery-streaming/
│
├── README.md                        ← This file
│
├── schemas/
│   ├── order_lifecycle_event.avsc   ← AVRO schema: Feed 1
│   └── courier_status_event.avsc    ← AVRO schema: Feed 2
│
├── generator/
│   ├── main.py                      ← CLI entry point
│   ├── config.py                    ← All tuneable parameters & reference data
│   ├── order_generator.py           ← Feed 1 generator
│   ├── courier_generator.py         ← Feed 2 generator
│   ├── avro_writer.py               ← Pure-stdlib AVRO OCF writer
│   └── README.md                    ← Generator usage guide
│
└── sample_data/
    ├── order_lifecycle_events.jsonl  ← Feed 1 sample (JSON Lines)
    ├── order_lifecycle_events.avro   ← Feed 1 sample (AVRO binary)
    ├── courier_status_events.jsonl   ← Feed 2 sample (JSON Lines)
    └── courier_status_events.avro    ← Feed 2 sample (AVRO binary)
```

---

## Feed Design

### Two Feeds

When incorporating a food delivery platform, the core operational challenge comes down to two questions:
> **"What is happening to each order right now?"**  
> **"Where is each courier and are they available?"**

These questions are answered by different data sources with different schemas, cardinalities, and emission frequencies:

| Dimension | Feed 1: Order Lifecycle | Feed 2: Courier Status |
|-----------|------------------------|----------------------|
| **Emission trigger** | State transition (sparse) | GPS ping + state change (dense) |
| **Frequency** | ~6–10 events per order | Every ~15 seconds per active courier |
| **Primary key** | `order_id` | `courier_id` |
| **Join surface** | `restaurant_id`, `courier_id`, `zone_id` | `order_id`, `zone_id` |
| **Primary analytics** | Fulfilment rates, cancellation, ETA accuracy | Courier utilisation, delivery speed, anomaly detection |

Together they enable **stream-stream joins** and **supply-demand analysis**.

---

### Feed 1: Order Lifecycle Events

**What it models:**  
Each event represents a **single state transition** in the order finite state machine. One event is emitted per status change; downstream processors reconstruct the full order history using `order_id` as the join key.

**State machine:**

```
PLACED → ACCEPTED → PREPARING → READY_FOR_PICKUP → PICKED_UP → EN_ROUTE → DELIVERED
                                                                    ↘ CANCELLED / FAILED
```

**Key fields:**

| Field | Type | Purpose |
|-------|------|---------|
| `event_timestamp` | long (timestamp-millis) | Watermark anchor — when the transition actually occurred |
| `ingestion_timestamp` | long (timestamp-millis) | Broker arrival time; delta reveals late data |
| `order_id` | string | Primary join key |
| `restaurant_id` / `zone_id` | string | Zone and restaurant-level aggregations |
| `courier_id` | string (nullable) | Null until ACCEPTED; join key to courier feed |
| `is_peak_hour` | boolean | True during lunch (12–14h) and dinner (19–21h) rush |
| `weather_condition` | enum | CLEAR / RAIN / HEAVY_RAIN / SNOW — affects delivery ETA |
| `estimated_prep_time_seconds` | int (nullable) | Inflated during peak hours to model kitchen load |
| `estimated_delivery_time_seconds` | int (nullable) | Inflated under bad weather to model traffic delays |
| `actual_prep_time_seconds` | int (nullable) | Populated on READY_FOR_PICKUP for ETA accuracy analytics |
| `actual_delivery_time_seconds` | int (nullable) | Populated on DELIVERED for end-to-end latency analytics |
| `is_late_arrival` | boolean | Pre-labelled late data for watermark validation |
| `is_duplicate` | boolean | Pre-labelled duplicates for idempotency testing |

**Analytics enabled:**

- **End-to-end order latency** — tumbling window on `event_timestamp`, comparing PLACED → DELIVERED timestamps
- **Cancellation rate by zone and hour** — filter on CANCELLED, group by `zone_id` and window
- **ETA accuracy** — compare `estimated_delivery_time_seconds` vs `actual_delivery_time_seconds`
- **Peak-hour prep time SLA breaches** — flag orders where `actual_prep_time_seconds` exceeds threshold, segmented by `is_peak_hour`
- **Weather impact on delivery times** — aggregate `actual_delivery_time_seconds` grouped by `weather_condition`

---

### Feed 2: Courier Status Events

**What it models:**  
High-frequency GPS pings and status-change events for each courier. A single active courier emits events every ~15 seconds, making this feed **20–50× denser** than the order feed.

**Courier state machine:**

```
OFFLINE → ONLINE_IDLE → HEADING_TO_RESTAURANT → WAITING_AT_RESTAURANT
                                                        ↓
                                                   PICKED_UP → EN_ROUTE_TO_CUSTOMER → DELIVERED
                                                        ↘ OFFLINE (mid-delivery anomaly)
```

**Key fields:**

| Field | Type | Purpose |
|-------|------|---------|
| `event_timestamp` | long (timestamp-millis) | GPS event time; primary watermark field |
| `ingestion_timestamp` | long (timestamp-millis) | Network/device delay; clock skew detection |
| `courier_id` | string | Primary key; join with order feed |
| `order_id` | string (nullable) | Null when idle; links active delivery to order feed |
| `zone_id` | string | Supply-side zone signal |
| `latitude` / `longitude` | double | Courier position for distance and ETA computation |
| `location_accuracy_meters` | float (nullable) | GPS signal quality; high values indicate unreliable pings |
| `heading_degrees` | float (nullable) | Direction of travel; null when stationary |
| `vehicle_type` | enum | BICYCLE / SCOOTER / MOTORCYCLE / CAR / FOOT |
| `distance_to_restaurant_meters` | int (nullable) | Populated while HEADING_TO_RESTAURANT |
| `distance_to_customer_meters` | int (nullable) | Populated while EN_ROUTE_TO_CUSTOMER |
| `session_id` | string | Groups all events within one courier shift |
| `deliveries_completed_in_session` | int | Cumulative deliveries per shift |
| `anomaly_flag` | enum (nullable) | Pre-labelled anomalies for detection pipeline testing |
| `is_late_arrival` | boolean | Pre-labelled late GPS pings |
| `is_duplicate` | boolean | Pre-labelled duplicate pings |

**Analytics enabled:**

- **Courier utilisation rate** — time spent in active delivery states vs. total online time per session
- **Delivery speed by vehicle type** — distance travelled vs. time between PICKED_UP and DELIVERED, grouped by `vehicle_type`
- **Available couriers per zone** — count `ONLINE_IDLE` couriers per `zone_id` in tumbling windows
- **Anomaly detection** — flag IMPOSSIBLE_SPEED, LOCATION_JUMP, OFFLINE_MID_DELIVERY events
- **Wait time at restaurants** — duration in WAITING_AT_RESTAURANT state per order, correlated with `is_peak_hour` from the order feed

---

## Schema Overview

Both schemas are defined in [Apache AVRO](https://avro.apache.org/) format (`.avsc`), stored in `schemas/`. Current version: **1.1.0**.

### Design Principles

**`schema_version` field** — Every record carries a string version (`"1.1.0"`). This allows a Schema Registry to detect breaking vs. additive changes before a consumer is affected.

**Union types for nullable fields** — All optional fields use `["null", <type>]` unions with `"default": null`, enabling backward-compatible additions without breaking existing consumers.

**Enum types for controlled vocabularies** — `order_status`, `courier_status`, `vehicle_type`, `weather_condition`, `cancellation_reason`, and `anomaly_flag` are all enums. This prevents invalid string values and enables efficient integer encoding in AVRO binary.

**`timestamp-millis` logical type** — Both `event_timestamp` and `ingestion_timestamp` are stored as `long` with the `timestamp-millis` logical type annotation, making them directly compatible with Spark's `timestamp` type and Flink's watermark APIs.

**`metadata` map field** — An extensible `map<string>` field on both schemas absorbs future key-value pairs without requiring a schema migration.

---

## Edge Cases & Streaming Correctness

The generator injects the following edge cases to stress-test downstream watermark and deduplication logic:

| Edge Case | How It's Injected | Streaming Challenge |
|-----------|------------------|---------------------|
| **Late arrivals** | `event_timestamp` set in the past; `ingestion_timestamp` = now | Watermark must tolerate configurable late-data threshold |
| **Duplicates** | Same logical event re-emitted with a new `event_id` and slightly later `ingestion_timestamp` | Exactly-once semantics; deduplicate on `event_id` |
| **Missing lifecycle steps** | One intermediate status removed (e.g. PLACED → ACCEPTED → EN_ROUTE, skipping PREPARING) | State machines must not assume contiguous transitions |
| **Impossible durations** | Prep or delivery time set to 1–5 seconds or 5+ hours | Anomaly detection; outlier filtering in aggregations |
| **Courier offline mid-delivery** | OFFLINE event injected after PICKED_UP | Order status stuck in terminal state — timeout detection required |
| **Location jumps** | GPS coordinates jump >5 km in a single ping | Spatial outlier detection |
| **Out-of-order events** | Customer rating events arrive with `event_timestamp` minutes/hours after DELIVERED | Late-arriving enrichment must not be dropped by tight watermarks |

All edge-case events carry boolean flags (`is_duplicate`, `is_late_arrival`) or an enum (`anomaly_flag`) so downstream tests can measure recall and precision of detection logic.

### Realism Features

**Peak-hour demand and prep inflation** — Orders placed between 12–14h (lunch) and 19–21h (dinner) have `is_peak_hour = true`. During these windows, `estimated_prep_time_seconds` is inflated by a ×1.4 multiplier to model restaurants handling higher order volumes simultaneously.

**Weather-driven delivery delays** — Each simulation window is assigned a city-wide `weather_condition` (CLEAR 60%, RAIN 25%, HEAVY_RAIN 10%, SNOW 5%). RAIN inflates `estimated_delivery_time_seconds` by ×1.2; HEAVY_RAIN by ×1.45; SNOW by ×1.6, modelling real traffic congestion effects.

**Zone-level demand skew** — Orders and couriers are distributed across 6 geographic zones with different demand weights, producing the downtown-heavy skew typical of real delivery platforms.

**Weekend uplift** — Order volume is boosted by a ×1.25 multiplier on weekends.

---

## Quick Start

### Requirements

- Python 3.8+
- No third-party packages required (pure stdlib)

### Generate sample data

```bash
# Default: 100 orders, 80 couriers, 1-hour window
python generator/main.py

# Larger dataset
python generator/main.py --orders 1000 --couriers 200 --restaurants 80 --duration 7200

# With a demand surge in downtown
python generator/main.py --orders 500 --surge --surge-zone zone_downtown --surge-multiplier 3.5

# Reproducible run
python generator/main.py --seed 99 --orders 250
```

### Output files

| File | Format | Description |
|------|--------|-------------|
| `order_lifecycle_events.jsonl` | JSON Lines | One event per line; inspect with `head -1 file.jsonl \| python3 -m json.tool` |
| `order_lifecycle_events.avro` | AVRO OCF | Binary; load with Spark, Flink, or `avro-tools` |
| `courier_status_events.jsonl` | JSON Lines | One GPS/status event per line |
| `courier_status_events.avro` | AVRO OCF | Binary format for pipeline testing |

### Inspect sample data

```bash
# Pretty-print the first order event
head -1 sample_data/order_lifecycle_events.jsonl | python3 -m json.tool

# Count events by order status
cat sample_data/order_lifecycle_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(l)['order_status'] for l in sys.stdin)
[print(f'{s:<25} {n}') for s,n in sorted(c.items())]
"

# Check weather distribution
cat sample_data/order_lifecycle_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(l)['weather_condition'] for l in sys.stdin)
[print(f'{s:<15} {n}') for s,n in sorted(c.items())]
"

# Count anomaly types in courier feed
cat sample_data/courier_status_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(l).get('anomaly_flag') for l in sys.stdin)
[print(f'{str(s):<30} {n}') for s,n in sorted(c.items())]
"
```

## Planned Analytics (Milestones 2)

| Milestone | Technology | Key Deliverable |
|-----------|-----------|-----------------|
| **M2** | Kafka / Redpanda | Publish both feeds to topics; partition by `zone_id` |
| **M2** | Spark Structured Streaming | Windowed aggregations, watermarks, stream-stream joins, anomaly detection |
| **M2** | Grafana / Streamlit | Live dashboard: order volume, ETA accuracy, courier utilisation, weather impact |
