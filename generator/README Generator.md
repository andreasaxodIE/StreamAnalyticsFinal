# How to Use the Generator

This directory contains the Python generator to simulate the food-delivery streaming pipeline. It creates two synthetic event sreams, one for orders and one for couriers, and saves them in JSON Lines and AVRO formats. It runs using only Python’s standard library, so no additional packages need to be installed.


## Files

| File | Description |
|------|-------------|
| `main.py` | Command-line entry point - run this to generate data |
| `config.py` | Contains all the configurable parameters such as zones, weather, demand curves, and simulation logic |
| `order_generator.py` | `OrderEventGenerator` - Generates Feed 1 (order lifecycle events) |
| `courier_generator.py` | `CourierFleetGenerator` + `CourierSimulator` - Generates Feed 2 (courier movement and status events) |
| `avro_writer.py` | Lightweight AVRO Object Container File writer (standard library) |

---

## How to Run the Generator

### Requirements
- Python 3.8+
- No pip installs needed

### Basic usage

```bash
# From the main project folder:
python generator/main.py
```

This runs a 1-hour simulation with 100 orders and 80 couriers. It then saves the 4 output files (JSONL and AVRO versions of each feed) to `./sample_data/`.


## Avaliable Paramters

You can modify the simulation using command-line options:

```
python generator/main.py [options]

Size of simulation:
  --orders N: Number of orders to simulate (default: 100)
  --couriers N: Courier fleet size (default: 80)
  --restaurants N: Restaurant pool size (default: 50)
  --customers N: Customer pool size(default: 500)
  --duration SECONDS:Simulation time window in seconds (default: 3600)
  --seed N:Random seed for reproducibility (default: 42)
  --output-dir PATH: Where to write output files (default: ./sample_data)

Edge case rates (adding realistic streaming problems):
  --late-rate FLOAT: Fraction of events with late arrival (default: 0.05)
  --duplicate-rate FLOAT: Fraction of events duplicated (default: 0.02)
  --cancel-rate FLOAT: Order cancellation probability (default: 0.12)
  --anomaly-rate FLOAT: Fraction with impossible durations (default: 0.02)

Demand surge:
  --surge: Enable zone-level demand surge
  --surge-zone ZONE_ID: Zone to surge (default: zone_downtown)
  --surge-multiplier N: Demand multiplier during surge (default: 3.0)
```

### Examples

```bash
# Generate a Larger dataset
python generator/main.py --orders 1000 --couriers 200 --restaurants 80 --duration 7200

# Generate data with late and duplicate events
python generator/main.py --orders 500 --late-rate 0.20 --duplicate-rate 0.10

# Simulate a increase in demand downtown (for example at the end of astadium game)
python generator/main.py --orders 300 --surge --surge-zone zone_downtown --surge-multiplier 4.0

# Reproducible run with fixed seed
python generator/main.py --seed 12345 --orders 250
```

## Architecture

```
main.py
  ├── loads schemas from ../schemas/
  ├── OrderEventGenerator (order_generator.py)
  │     ├── builds restaurant & customer reference pools
  │     ├── for each order: generate_order_lifecycle()
  │     │     ├── determine context: is_peak_hour? weather_condition?
  │     │     ├── sample order items and total
  │     │     ├── decide: cancel? skip step? anomalous duration?
  │     │     ├── emit one event per status transition
  │     │     │     ├── inflate prep time if is_peak_hour
  │     │     │     └── inflate delivery ETA if weather is RAIN/SNOW
  │     │     └── inject duplicates and late arrivals
  │     └── stream(): sort all events by ingestion_timestamp
  │
  ├── CourierFleetGenerator (courier_generator.py)
  │     ├── one CourierSimulator per courier_id
  │     ├── CourierSimulator.generate_shift()
  │     │     ├── GPS pings every ~15s when active
  │     │     ├── full IDLE → HEADING → WAITING → PICKED_UP → EN_ROUTE → DELIVERED cycle
  │     │     └── inject: offline-mid-delivery, location jumps, impossible speeds
  │     └── merge all courier event lists, sort by ingestion_timestamp
  │
  └── writes JSON Lines + AVRO OCF for each feed
```



## Realistic Model

The generator has several realistic modelling assumptions:

### Geographic Zones

Orders and couriers are distributed across 6 zones with weighted probabilities, meaning downtown receives the highest share of orders, like in real food delivery apps:


| Zone ID | Label | Demand Weight | Courier Density |
|---------|-------|:---:|:---:|
| `zone_downtown` | Downtown | 3.5 | 3.0 |
| `zone_midtown` | Midtown | 2.5 | 2.0 |
| `zone_west` | West Side | 2.0 | 1.8 |
| `zone_east` | East Side | 1.8 | 1.5 |
| `zone_uptown` | Uptown | 1.5 | 1.2 |
| `zone_suburbs` | Suburbs | 0.8 | 0.5 |

### Time-of-Day Demand

The generator uses an hourly demand curve so that order volume peaks at lunch and dinner:

| Period | Hours | Demand Level |
|--------|-------|-------------|
| Night | 00–06 | Very low |
| Breakfast | 07–09 | Low–medium |
| Lunch peak | 12–14 | **High** (`is_peak_hour = true`) |
| Afternoon | 15–17 | Medium |
| Dinner peak | 19–21 | **Highest** (`is_peak_hour = true`) |
| Late night | 22–23 | Low |

During peak hours, preperation time (`estimated_prep_time_seconds`) is increased by **×1.4** to reflect restaurants handling higher order volumes simultaneously.

Weekend demand is slightly higher (**×1.25**) compared to weekdays.

### Weather

At the start of each run, one weather condition is chosen and applied for the entire simulation:

| Condition | Probability | Delivery time multiplier |
|-----------|:-----------:|:------------------------:|
| `CLEAR` | 60% | ×1.00 (no change) |
| `RAIN` | 25% | ×1.20 |
| `HEAVY_RAIN` | 10% | ×1.45 |
| `SNOW` | 5% | ×1.60 |

The multiplier is applied to `estimated_delivery_time_seconds` at the moment the order is placed.

### Courier Movement

Courier GPS positions move realistically towards their target using the Haversine formula, at speeds depending on the courier's vehicle:

| Vehicle | Speed range (km/h) |
|---------|--------------------|
| Bicycle | 8 – 25 |
| Scooter | 15 – 45 |
| Motorcycle | 20 – 80 |
| Car | 10 – 90 |
| Foot | 3 – 8 |


## Edge Cases Injected

| Edge Case | Flag/Field | Controlled by |
|-----------|-----------|---------------|
| Late-arriving events | `is_late_arrival = true` | `--late-rate` |
| Duplicate events | `is_duplicate = true` | `--duplicate-rate` |
| Missing lifecycle step | No flag (gap in status sequence) | `--anomaly-rate` (missing_step_rate) |
| Impossible prep/delivery duration | No flag (outlier value) | `--anomaly-rate` |
| Courier offline mid-delivery | `anomaly_flag = OFFLINE_MID_DELIVERY` | internal rate |
| GPS location jump | `anomaly_flag = LOCATION_JUMP` | internal rate (3%) |
| Impossible courier speed | `anomaly_flag = IMPOSSIBLE_SPEED` | `--anomaly-rate` |
| Out-of-order rating events | `is_late_arrival = true` | automatic on DELIVERED |


## Output Files

Running `python generator/main.py` produces:

```
sample_data/
├── order_lifecycle_events.jsonl   # Feed 1 - one JSON object per line
├── order_lifecycle_events.avro    # Feed 1 - AVRO Object Container File
├── courier_status_events.jsonl    # Feed 2 - one JSON object per line
└── courier_status_events.avro     # Feed 2 - AVRO Object Container File
```

### Quick inspection commands

```bash
# Pretty-print the first order event
head -1 sample_data/order_lifecycle_events.jsonl | python3 -m json.tool

# Count order events by status
cat sample_data/order_lifecycle_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(l)['order_status'] for l in sys.stdin)
[print(f'{s:<25} {n}') for s,n in sorted(c.items())]
"

# Check weather distribution across orders
cat sample_data/order_lifecycle_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(l)['weather_condition'] for l in sys.stdin)
[print(f'{s:<15} {n}') for s,n in sorted(c.items())]
"

# Count courier anomaly types
cat sample_data/courier_status_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(l).get('anomaly_flag') for l in sys.stdin)
[print(f'{str(s):<30} {n}') for s,n in sorted(c.items())]
"
```

## Extending the Generator

### Add a new field to a feed

1. Add the field to the AVRO schema in `../schemas/` with a `"default"` value for backward compatibility
2. Populate the field in the relevant `_make_event()` method in the generator
3. Update the schema version

### Add a new geographic zone

In `config.py`, add an entry to the `ZONES` dict:

```python
"zone_airport": {
    "lat_range": (40.6350, 40.6450),
    "lon_range": (-73.7850, -73.7750),
    "demand_weight": 1.2,
    "courier_density": 0.8,
    "label": "Airport",
},
```

### Change peak hour windows

In `config.py`, edit the `is_peak_hour()` function:

```python
def is_peak_hour(hour: int) -> bool:
    return hour in (12, 13, 14, 19, 20, 21)  # adjust hours here
```

