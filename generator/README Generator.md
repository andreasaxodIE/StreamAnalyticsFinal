# Generator guide

This folder contains the data generator we used for the project. It creates fake food delivery events for two streams:

- order lifecycle events
- courier status events

The script writes both JSONL and AVRO files. We kept this part standard-library only, so you can run it without installing extra Python packages.

## Files in this folder

| File | What it does |
| --- | --- |
| `main.py` | Command-line entry point |
| `config.py` | Shared configuration, zones, demand weights, weather, and helper settings |
| `order_generator.py` | Builds the order lifecycle feed |
| `courier_generator.py` | Builds the courier feed and movement simulation |
| `avro_writer.py` | Small AVRO writer used for the output files |

## Quick start

From the project root:

```bash
python generator/main.py
```

Or from inside this folder:

```bash
python main.py
```

The default run simulates:

- 100 orders
- 80 couriers
- 50 restaurants
- 500 customers
- 1 hour of activity

The output is written to `sample_data/` by default.

## Useful command-line options

### Size of the simulation

| Option | Meaning | Default |
| --- | --- | --- |
| `--orders` | Number of orders to create | `100` |
| `--couriers` | Size of the courier fleet | `80` |
| `--restaurants` | Number of restaurants | `50` |
| `--customers` | Number of customers in the reference pool | `500` |
| `--duration` | Length of the simulation in seconds | `3600` |
| `--seed` | Random seed for reproducible runs | `42` |
| `--output-dir` | Folder where files are written | `./sample_data` |

### Data quality and edge-case options

| Option | Meaning | Default |
| --- | --- | --- |
| `--late-rate` | Fraction of events marked as late arrivals | `0.05` |
| `--duplicate-rate` | Fraction of duplicate events | `0.02` |
| `--cancel-rate` | Order cancellation probability | `0.12` |
| `--anomaly-rate` | Rate for impossible or broken behaviours | `0.02` |

### Demand surge options

| Option | Meaning | Default |
| --- | --- | --- |
| `--surge` | Turns surge mode on | off |
| `--surge-zone` | Zone where demand is boosted | `zone_downtown` |
| `--surge-multiplier` | Multiplier applied in surge mode | `3.0` |

## Example commands

Create a larger sample:

```bash
python generator/main.py --orders 1000 --couriers 200 --restaurants 80 --duration 7200
```

Create noisier data with more late and duplicate events:

```bash
python generator/main.py --orders 500 --late-rate 0.20 --duplicate-rate 0.10
```

Simulate a surge in one zone:

```bash
python generator/main.py --orders 300 --surge --surge-zone zone_downtown --surge-multiplier 4.0
```

Run with a fixed seed:

```bash
python generator/main.py --seed 12345 --orders 250
```

## What the generator is trying to model

The generator is simple, but it still tries to look realistic enough for streaming analytics work.

### Geographic zones

Orders and couriers are spread across six zones with different weights. Downtown gets the most demand and also has the highest courier density.

| Zone ID | Label | Demand weight | Courier density |
| --- | --- | --- | --- |
| `zone_downtown` | Downtown | 3.5 | 3.0 |
| `zone_midtown` | Midtown | 2.5 | 2.0 |
| `zone_west` | West Side | 2.0 | 1.8 |
| `zone_east` | East Side | 1.8 | 1.5 |
| `zone_uptown` | Uptown | 1.5 | 1.2 |
| `zone_suburbs` | Suburbs | 0.8 | 0.5 |

### Time-of-day demand

Demand changes by hour, with lunch and dinner treated as peak periods. During peak hours, the generator also increases the estimated prep time.

- Lunch peak: 12:00 to 14:59
- Dinner peak: 19:00 to 21:59
- Weekend demand is slightly higher than weekday demand

### Weather

Each run picks one weather condition and applies it to the whole simulation. That weather then changes the estimated delivery time.

| Condition | Probability | Delivery time multiplier |
| --- | --- | --- |
| `CLEAR` | 60% | 1.00 |
| `RAIN` | 25% | 1.20 |
| `HEAVY_RAIN` | 10% | 1.45 |
| `SNOW` | 5% | 1.60 |

### Courier movement

Courier position updates depend on vehicle type. The movement logic uses distance calculations to move the courier toward the restaurant or customer.

| Vehicle | Speed range in km/h |
| --- | --- |
| Bicycle | 8 to 25 |
| Scooter | 15 to 45 |
| Motorcycle | 20 to 80 |
| Car | 10 to 90 |
| Foot | 3 to 8 |

## Injected edge cases

We intentionally inject some bad or messy cases because they are useful for testing streaming logic.

| Edge case | Related field or effect |
| --- | --- |
| Late-arriving events | `is_late_arrival = true` |
| Duplicate events | `is_duplicate = true` |
| Order cancellation | terminal cancelled path |
| Missing lifecycle step | gap in the order status sequence |
| Impossible duration | unrealistic prep or delivery time |
| Courier offline mid-delivery | `anomaly_flag = OFFLINE_MID_DELIVERY` |
| GPS jump | `anomaly_flag = LOCATION_JUMP` |
| Impossible speed | `anomaly_flag = IMPOSSIBLE_SPEED` |

## Output files

A run produces four files:

```text
sample_data/
|-- order_lifecycle_events.jsonl
|-- order_lifecycle_events.avro
|-- courier_status_events.jsonl
`-- courier_status_events.avro
```

A couple of quick checks you can run after generation:

```bash
head -1 sample_data/order_lifecycle_events.jsonl | python3 -m json.tool
```

```bash
cat sample_data/order_lifecycle_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(line)['order_status'] for line in sys.stdin)
for status, count in sorted(c.items()):
    print(f'{status:<25} {count}')
"
```

```bash
cat sample_data/courier_status_events.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter(json.loads(line).get('anomaly_flag') for line in sys.stdin)
for flag, count in sorted(c.items()):
    print(f'{str(flag):<30} {count}')
"
```

## If you want to extend it

A normal workflow for adding a new field is:

1. Add the field to the AVRO schema in `../schemas/`.
2. Give it a sensible default if you want backward compatibility.
3. Populate the field in the relevant generator code.
4. Bump the schema version if needed.

If you want to add a new zone or change peak-hour windows, `config.py` is the main file to edit.

## Final note

This generator was made for a class project, so the goal was clarity and usefulness, not a perfect simulation of a real delivery company. It is still good enough to create streams with joins, windowing, duplicates, late arrivals, and anomaly cases.
