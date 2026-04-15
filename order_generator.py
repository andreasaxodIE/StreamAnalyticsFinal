"""
order_generator.py — Creates OrderLifecycleEvent streams.
"""

from __future__ import annotations
import random
import uuid
import copy
from datetime import datetime, timezone
from typing import List, Optional, Dict, Tuple, Iterator

from config import (
    GeneratorConfig, ZONES, MENU_ITEMS_BY_CUISINE, RESTAURANT_CUISINES,
    PEAK_HOUR_PREP_MULTIPLIER, WEATHER_DELIVERY_MULTIPLIER,
    weighted_choice, zone_weighted_choice, is_peak_hour, sample_weather,
)



# Reference entities ---------------------------------------------------------------------------

def build_restaurants(n: int) -> List[Dict]:
    restaurants = []
    for i in range(n):
        zone_id = zone_weighted_choice()
        cuisine = random.choice(RESTAURANT_CUISINES)
        restaurants.append({
            "restaurant_id": f"rest_{i:04d}",
            "name": f"The {cuisine} Place #{i}",
            "cuisine": cuisine,
            "zone_id": zone_id,
            "avg_prep_time_seconds": random.randint(600, 1800),
            "prep_time_std_seconds": random.randint(60, 300),
            "rating": round(random.uniform(3.2, 5.0), 1),
        })
    return restaurants


def build_customers(n: int) -> List[Dict]:
    return [
        {
            "customer_id": f"cust_{i:05d}",
            "zone_id": zone_weighted_choice(),
        }
        for i in range(n)
    ]



# Order lifecycle builder ---------------------------------------------------------------------------
# Orders normally transition through these states sequentially, this sequence may later be truncated (cancellation)or modified (missing steps or anomalies).

FULL_LIFECYCLE = [
    "PLACED",
    "ACCEPTED",
    "PREPARING",
    "READY_FOR_PICKUP",
    "PICKED_UP",
    "EN_ROUTE",
    "DELIVERED",
]

CANCELLATION_REASONS = [
    "CUSTOMER_REQUEST",
    "RESTAURANT_CLOSED",
    "RESTAURANT_REJECTED",
    "NO_COURIER_AVAILABLE",
    "PAYMENT_FAILED",
    "ITEM_UNAVAILABLE",
    "FRAUD_DETECTED",
    "SYSTEM_ERROR",
]
CANCELLATION_REASON_WEIGHTS = [0.30, 0.15, 0.15, 0.20, 0.08, 0.07, 0.03, 0.02]


def _sample_order_items(cuisine: str) -> Tuple[List[Dict], int]:
    menu = MENU_ITEMS_BY_CUISINE.get(cuisine, MENU_ITEMS_BY_CUISINE["American"])
    n_items = random.choices([1, 2, 3, 4, 5], weights=[0.15, 0.30, 0.30, 0.15, 0.10])[0]
    items = []
    total = 0
    for _ in range(n_items):
        name, price = random.choice(menu)
        qty = random.choices([1, 2, 3], weights=[0.70, 0.22, 0.08])[0]
        items.append({
            "item_id": f"item_{uuid.uuid4().hex[:8]}",
            "item_name": name,
            "quantity": qty,
            "unit_price_cents": price,
        })
        total += price * qty
    return items, total


def _inter_event_delay(from_status: str, to_status: str, anomalous: bool,
                       peak: bool, weather: str) -> int:
    """ Computes the time difference between two lifecycle states"""
    delays = {
        ("PLACED",           "ACCEPTED"):         (30,  120),
        ("ACCEPTED",         "PREPARING"):        (10,  60),
        ("PREPARING",        "READY_FOR_PICKUP"):  (480, 1500),
        ("READY_FOR_PICKUP", "PICKED_UP"):         (60,  600),
        ("PICKED_UP",        "EN_ROUTE"):          (5,   30),
        ("EN_ROUTE",         "DELIVERED"):         (300, 1800),
    }
    key = (from_status, to_status)
    lo, hi = delays.get(key, (30, 120))

    if anomalous:
        return random.randint(1, 5) if random.random() < 0.5 else random.randint(7200, 18000)

    base = random.randint(lo, hi)

    # Peak hour: prep takes longer
    if peak and to_status in ("READY_FOR_PICKUP",):
        base = int(base * PEAK_HOUR_PREP_MULTIPLIER)

    # Bad weather: delivery takes longer
    weather_mult = WEATHER_DELIVERY_MULTIPLIER.get(weather, 1.0)
    if to_status == "DELIVERED" and weather_mult > 1.0:
        base = int(base * weather_mult)

    return base


class OrderEventGenerator:
    def __init__(self, config: GeneratorConfig):
        self.cfg = config
        random.seed(config.random_seed)
        self.restaurants = build_restaurants(config.num_restaurants)
        self.customers = build_customers(config.num_customers)
        self.courier_ids = [f"courier_{i:04d}" for i in range(config.num_couriers)]

    def _make_base_event(
        self,
        order_id, customer, restaurant, courier_id,
        status, previous_status, event_ts_ms, ingestion_ts_ms,
        items, total_cents, estimated_prep, estimated_delivery,
        actual_prep, actual_delivery, cancellation_reason,
        is_duplicate, is_late, peak_hour, weather,
    ) -> Dict:
        return {
            "schema_version": "1.1.0",
            "event_id": str(uuid.uuid4()),
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "restaurant_id": restaurant["restaurant_id"],
            "courier_id": courier_id,
            "zone_id": restaurant["zone_id"],
            "order_status": status,
            "previous_status": previous_status,
            "event_timestamp": event_ts_ms,
            "ingestion_timestamp": ingestion_ts_ms,
            "order_items": items,
            "order_total_cents": total_cents,
            "estimated_prep_time_seconds": estimated_prep,
            "estimated_delivery_time_seconds": estimated_delivery,
            "actual_prep_time_seconds": actual_prep,
            "actual_delivery_time_seconds": actual_delivery,
            "is_peak_hour": peak_hour,
            "weather_condition": weather,
            "cancellation_reason": cancellation_reason,
            "customer_rating": None,
            "is_duplicate": is_duplicate,
            "is_late_arrival": is_late,
            "metadata": None,
        }

    def generate_order_lifecycle(self, base_ts_ms: int) -> List[Dict]:
        cfg = self.cfg
        restaurant = random.choice(self.restaurants)
        customer = random.choice(self.customers)
        order_id = f"ord_{uuid.uuid4().hex[:12]}"
        items, total = _sample_order_items(restaurant["cuisine"])

        # Determine context at order placement time
        placed_hour = datetime.fromtimestamp(base_ts_ms / 1000, tz=timezone.utc).hour
        peak = is_peak_hour(placed_hour)
        weather = sample_weather()
        weather_mult = WEATHER_DELIVERY_MULTIPLIER.get(weather, 1.0)

        will_cancel = random.random() < cfg.cancellation_rate
        skip_step = random.random() < cfg.missing_step_rate
        anomalous = random.random() < cfg.impossible_duration_rate
        courier_vanish = random.random() < cfg.courier_offline_mid_delivery_rate

        lifecycle = list(FULL_LIFECYCLE)
        if will_cancel:
            cancel_at = random.randint(1, 4)
            lifecycle = lifecycle[:cancel_at] + ["CANCELLED"]
        elif skip_step:
            removable = [2, 3, 4]
            remove_idx = random.choice(removable)
            if remove_idx < len(lifecycle) - 1:
                lifecycle.pop(remove_idx)

        events: List[Dict] = []
        current_ts = base_ts_ms
        courier_id = None
        actual_prep = None
        actual_delivery = None
        prep_start_ts = None
        placed_ts = base_ts_ms

        for i, status in enumerate(lifecycle):
            prev = lifecycle[i - 1] if i > 0 else None

            if i > 0:
                delay = _inter_event_delay(prev, status, anomalous and i == len(lifecycle) - 1,
                                           peak, weather)
                current_ts += delay * 1000

            if status == "ACCEPTED":
                courier_id = random.choice(self.courier_ids)
                prep_start_ts = current_ts

            if status == "READY_FOR_PICKUP" and prep_start_ts:
                actual_prep = (current_ts - prep_start_ts) // 1000

            if status == "DELIVERED":
                actual_delivery = (current_ts - placed_ts) // 1000

            is_late = random.random() < cfg.late_arrival_rate
            if is_late:
                ingestion_ts = current_ts + random.randint(30_000, cfg.max_late_arrival_seconds * 1000)
            else:
                ingestion_ts = current_ts + random.randint(50, 2000)

            cancel_reason = None
            if status == "CANCELLED":
                cancel_reason = weighted_choice(CANCELLATION_REASONS, CANCELLATION_REASON_WEIGHTS)

            ev_items  = items if status == "PLACED" else None
            ev_total  = total if status == "PLACED" else None

            base_prep = restaurant["avg_prep_time_seconds"]
            if peak:
                base_prep = int(base_prep * PEAK_HOUR_PREP_MULTIPLIER)
            est_prep     = base_prep if status in ("PLACED", "ACCEPTED") else None
            est_delivery = int((base_prep + 900) * weather_mult) if status == "PLACED" else None

            event = self._make_base_event(
                order_id=order_id,
                customer=customer,
                restaurant=restaurant,
                courier_id=courier_id,
                status=status,
                previous_status=prev,
                event_ts_ms=current_ts,
                ingestion_ts_ms=ingestion_ts,
                items=ev_items,
                total_cents=ev_total,
                estimated_prep=est_prep,
                estimated_delivery=est_delivery,
                actual_prep=actual_prep,
                actual_delivery=actual_delivery,
                cancellation_reason=cancel_reason,
                is_duplicate=False,
                is_late=is_late,
                peak_hour=peak,
                weather=weather,
            )
            events.append(event)

            # Courier vanishes mid-delivery
            if courier_vanish and status == "PICKED_UP":
                oo_event = copy.deepcopy(event)
                oo_event["event_id"] = str(uuid.uuid4())
                oo_event["order_status"] = "CANCELLED"
                oo_event["previous_status"] = "PICKED_UP"
                oo_event["cancellation_reason"] = "NO_COURIER_AVAILABLE"
                oo_event["event_timestamp"] = current_ts + random.randint(300_000, 900_000)
                oo_event["ingestion_timestamp"] = oo_event["event_timestamp"] + 1000
                events.append(oo_event)
                break

        if lifecycle[-1] == "DELIVERED" and random.random() < 0.65:
            rating_event = copy.deepcopy(events[-1])
            rating_event["event_id"] = str(uuid.uuid4())
            rating_event["customer_rating"] = round(max(1.0, min(5.0, random.gauss(4.1, 0.8))), 1)
            rating_event["event_timestamp"] += random.randint(60_000, 3_600_000)
            rating_event["is_late_arrival"] = True
            events.append(rating_event)

        duplicated = []
        for ev in events:
            duplicated.append(ev)
            if random.random() < cfg.duplicate_rate:
                dup = copy.deepcopy(ev)
                dup["ingestion_timestamp"] += random.randint(100, 5000)
                dup["is_duplicate"] = True
                duplicated.append(dup)

        return duplicated

    def stream(self, start_ts_ms: int, n_orders: int) -> Iterator[Dict]:
        all_events = []
        for _ in range(n_orders):
            jitter = random.randint(0, self.cfg.simulation_duration_seconds * 1000)
            order_events = self.generate_order_lifecycle(start_ts_ms + jitter)
            all_events.extend(order_events)
        all_events.sort(key=lambda e: e["ingestion_timestamp"])
        yield from all_events
