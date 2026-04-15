"""
This file generates synthetic CourierStatusEvent data streams.
The goal is to simulate how couriers move through a shift, how their status changes over time,
and how some realistic irregular cases can also appear in the data.
"""

from __future__ import annotations
import random
import uuid
import math
import copy
from typing import List, Dict, Optional, Iterator

from config import (
    GeneratorConfig, ZONES, weighted_choice,
    zone_courier_weighted_choice, random_coords_in_zone,
)

# This dictionary stores rough speed ranges for each type of vehicle.
# The values are not meant to be perfectly realistic in every situation,
# but they give the simulation a believable range for how fast a courier
# might move depending on what they are using.
VEHICLE_SPEED: Dict[str, Dict] = {
    "BICYCLE":    {"min": 8,  "max": 25,  "avg": 15},
    "SCOOTER":    {"min": 15, "max": 45,  "avg": 28},
    "MOTORCYCLE": {"min": 20, "max": 80,  "avg": 40},
    "CAR":        {"min": 10, "max": 90,  "avg": 35},
    "FOOT":       {"min": 3,  "max": 8,   "avg": 5},
}

# These are the available vehicle types and the probability weights used
# when assigning one to a courier.
# For example, bicycles and scooters are more common than cars or walking couriers.
VEHICLE_TYPES = list(VEHICLE_SPEED.keys())
VEHICLE_WEIGHTS = [0.35, 0.30, 0.15, 0.12, 0.08]

# This controls how often the courier sends a location update while moving.
# In this simulation, a ping is created every 15 seconds.
PING_INTERVAL_SECONDS = 15


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    """
    Calculate the approximate distance between two points on Earth in kilometres.

    This uses the Haversine formula, which is a common way to estimate
    geographic distance from latitude and longitude coordinates.
    """
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _move_towards(lat, lon, target_lat, target_lon, speed_kmh, seconds):
    """
    Move a point a small step closer to a target location.

    The function does not try to model real roads or traffic.
    It simply moves in a straight line from the current point toward the target,
    based on the chosen speed and time interval.
    """
    dist_km = _haversine_km(lat, lon, target_lat, target_lon)

    if dist_km < 0.01:
        return target_lat, target_lon

    travel_km = (speed_kmh * seconds) / 3600.0
    frac = min(travel_km / dist_km, 1.0)

    return (
        round(lat + frac * (target_lat - lat), 6),
        round(lon + frac * (target_lon - lon), 6),
    )


class CourierSimulator:
    """
    This class simulates the behaviour of one courier during a single shift.

    It handles the courier's movement, status changes, delivery progression,
    and some unusual events such as duplicates, late arrivals, and offline interruptions.
    """

    def __init__(self, courier_id: str, config: GeneratorConfig):
        self.courier_id = courier_id
        self.cfg = config
        self.vehicle_type = weighted_choice(VEHICLE_TYPES, VEHICLE_WEIGHTS)
        self.zone_id = zone_courier_weighted_choice()
        self.lat, self.lon = random_coords_in_zone(self.zone_id)
        self.status = "ONLINE_IDLE"
        self.prev_status = None
        self.session_id = str(uuid.uuid4())
        self.deliveries_completed = 0
        self.shift_start_ts = 0
        self.order_id: Optional[str] = None

    def _speed(self) -> float:
        """
        Sample a speed for the courier based on their vehicle type.

        A fresh value is chosen each time so the simulation feels less rigid
        and more like real movement, where speed can vary from moment to moment.
        """
        s = VEHICLE_SPEED[self.vehicle_type]
        return random.uniform(s["min"], s["max"])

    def _make_event(
        self,
        ts_ms: int,
        status: str,
        anomaly_flag=None,
        is_duplicate=False,
        is_late=False,
        distance_to_restaurant=None,
        distance_to_customer=None,
    ) -> Dict:
        """
        Build a single courier status event.

        This function collects the current courier state and packages it into
        one event dictionary, including timing, location, movement details,
        and any anomaly markers.
        """

        moving = status not in ("ONLINE_IDLE", "OFFLINE", "BREAK", "WAITING_AT_RESTAURANT")
        heading = round(random.uniform(0, 360), 1) if moving else None

        ingestion_lag = random.randint(50, 2000)
        if is_late:
            ingestion_lag += random.randint(30_000, self.cfg.max_late_arrival_seconds * 1000)

        shift_dur = (ts_ms - self.shift_start_ts) // 1000 if self.shift_start_ts else 0

        return {
            "schema_version": "1.1.0",
            "event_id": str(uuid.uuid4()),
            "courier_id": self.courier_id,
            "order_id": self.order_id,
            "zone_id": self.zone_id,
            "courier_status": status,
            "previous_status": self.prev_status,
            "event_timestamp": ts_ms,
            "ingestion_timestamp": ts_ms + ingestion_lag,
            "latitude": self.lat,
            "longitude": self.lon,
            "location_accuracy_meters": round(random.uniform(3.0, 25.0), 1),
            "heading_degrees": heading,
            "vehicle_type": self.vehicle_type,
            "distance_to_restaurant_meters": distance_to_restaurant,
            "distance_to_customer_meters": distance_to_customer,
            "session_id": self.session_id,
            "shift_duration_seconds": shift_dur,
            "deliveries_completed_in_session": self.deliveries_completed,
            "is_duplicate": is_duplicate,
            "is_late_arrival": is_late,
            "anomaly_flag": anomaly_flag,
            "metadata": None,
        }

    def generate_shift(self, start_ts_ms: int, duration_seconds: int) -> List[Dict]:
        """
        Generate all courier events for one shift.

        The shift begins with the courier online and idle.
        After that, the courier repeatedly waits for work, heads to restaurants,
        waits there, picks orders up, travels to customers, and completes deliveries,
        until the shift ends.
        """
        self.shift_start_ts = start_ts_ms
        ts = start_ts_ms
        end_ts = start_ts_ms + duration_seconds * 1000
        events: List[Dict] = []
        cfg = self.cfg

        self.status, self.prev_status = "ONLINE_IDLE", None
        events.append(self._make_event(ts, "ONLINE_IDLE"))

        while ts < end_ts:
            # This simulates the time the courier spends idle before receiving
            # the next order assignment.
            idle_wait = random.randint(30, 300)
            ts += idle_wait * 1000

            if ts >= end_ts:
                break

            # A new order is created for this delivery cycle.
            self.order_id = f"ord_{uuid.uuid4().hex[:12]}"

            # A restaurant zone is selected, and both restaurant and customer
            # coordinates are generated inside that zone.
            # This keeps the trip local and reasonably believable.
            restaurant_zone = zone_courier_weighted_choice()
            rest_lat, rest_lon = random_coords_in_zone(restaurant_zone)
            cust_lat, cust_lon = random_coords_in_zone(restaurant_zone)

            # The courier is now travelling toward the restaurant.
            self.prev_status, self.status = self.status, "HEADING_TO_RESTAURANT"
            dist_to_rest_m = int(_haversine_km(self.lat, self.lon, rest_lat, rest_lon) * 1000)
            events.append(
                self._make_event(
                    ts,
                    "HEADING_TO_RESTAURANT",
                    distance_to_restaurant=dist_to_rest_m,
                )
            )

            # Travel time is estimated from distance and sampled speed.
            # A minimum of 60 seconds is enforced so trips do not become unrealistically short.
            travel_time = max(60, int(dist_to_rest_m / (self._speed() * 1000 / 3600)))
            ping_ts = ts

            while ping_ts < ts + travel_time * 1000:
                ping_ts += PING_INTERVAL_SECONDS * 1000

                self.lat, self.lon = _move_towards(
                    self.lat,
                    self.lon,
                    rest_lat,
                    rest_lon,
                    self._speed(),
                    PING_INTERVAL_SECONDS,
                )

                remaining = int(_haversine_km(self.lat, self.lon, rest_lat, rest_lon) * 1000)
                is_late = random.random() < cfg.late_arrival_rate

                ping_ev = self._make_event(
                    ping_ts,
                    "HEADING_TO_RESTAURANT",
                    distance_to_restaurant=remaining,
                    is_late=is_late,
                )
                events.append(ping_ev)

                # Sometimes a duplicate event is deliberately added.
                # This helps simulate messy streaming data that downstream systems
                # may need to detect and handle properly.
                if random.random() < cfg.duplicate_rate:
                    dup = copy.deepcopy(ping_ev)
                    dup["event_id"] = str(uuid.uuid4())
                    dup["ingestion_timestamp"] += random.randint(100, 3000)
                    dup["is_duplicate"] = True
                    events.append(dup)

            ts += travel_time * 1000
            self.lat, self.lon = rest_lat, rest_lon

            # Once the courier reaches the restaurant, they wait for the order
            # to be prepared and handed over.
            self.prev_status, self.status = self.status, "WAITING_AT_RESTAURANT"
            events.append(self._make_event(ts, "WAITING_AT_RESTAURANT"))
            wait_time = random.randint(120, 900)
            ts += wait_time * 1000

            # In some cases, the courier goes offline in the middle of the delivery flow.
            # This creates an interruption and starts a new session later on.
            if random.random() < cfg.courier_offline_mid_delivery_rate:
                self.prev_status, self.status = self.status, "OFFLINE"
                events.append(
                    self._make_event(
                        ts,
                        "OFFLINE",
                        anomaly_flag="OFFLINE_MID_DELIVERY",
                    )
                )
                self.order_id = None
                ts += random.randint(300, 1800) * 1000
                self.prev_status, self.status = "OFFLINE", "ONLINE_IDLE"
                self.session_id = str(uuid.uuid4())
                self.shift_start_ts = ts
                events.append(self._make_event(ts, "ONLINE_IDLE"))
                continue

            # The courier has picked up the order and is now associated with
            # the distance to the customer instead of the restaurant.
            self.prev_status, self.status = self.status, "PICKED_UP"
            dist_to_cust_m = int(_haversine_km(self.lat, self.lon, cust_lat, cust_lon) * 1000)
            events.append(self._make_event(ts, "PICKED_UP", distance_to_customer=dist_to_cust_m))

            # The courier is now on the way to the customer.
            self.prev_status, self.status = self.status, "EN_ROUTE_TO_CUSTOMER"
            events.append(self._make_event(ts, "EN_ROUTE_TO_CUSTOMER", distance_to_customer=dist_to_cust_m))

            delivery_time = max(60, int(dist_to_cust_m / (self._speed() * 1000 / 3600)))
            ping_ts = ts

            # This anomaly creates a sudden location jump that would look suspicious
            # in a real tracking system.
            if random.random() < 0.03:
                jump_ev = self._make_event(
                    ping_ts + 5000,
                    "EN_ROUTE_TO_CUSTOMER",
                    anomaly_flag="LOCATION_JUMP",
                )
                jump_ev["latitude"] = round(self.lat + random.uniform(-0.05, 0.05), 6)
                jump_ev["longitude"] = round(self.lon + random.uniform(-0.05, 0.05), 6)
                events.append(jump_ev)

            # This anomaly marks a case where the timing would suggest an
            # unrealistically high speed.
            if random.random() < cfg.impossible_duration_rate:
                fast_ev = self._make_event(
                    ping_ts + 8000,
                    "EN_ROUTE_TO_CUSTOMER",
                    anomaly_flag="IMPOSSIBLE_SPEED",
                )
                events.append(fast_ev)

            while ping_ts < ts + delivery_time * 1000:
                ping_ts += PING_INTERVAL_SECONDS * 1000

                self.lat, self.lon = _move_towards(
                    self.lat,
                    self.lon,
                    cust_lat,
                    cust_lon,
                    self._speed(),
                    PING_INTERVAL_SECONDS,
                )

                remaining = int(_haversine_km(self.lat, self.lon, cust_lat, cust_lon) * 1000)
                is_late = random.random() < cfg.late_arrival_rate

                events.append(
                    self._make_event(
                        ping_ts,
                        "EN_ROUTE_TO_CUSTOMER",
                        distance_to_customer=remaining,
                        is_late=is_late,
                    )
                )

            ts += delivery_time * 1000
            self.lat, self.lon = cust_lat, cust_lon

            # The order is now complete.
            self.prev_status, self.status = self.status, "DELIVERED"
            self.deliveries_completed += 1
            events.append(self._make_event(ts, "DELIVERED"))

            # After delivery, the courier returns to an idle online state
            # and becomes ready for the next possible order.
            self.order_id = None
            self.prev_status, self.status = "DELIVERED", "ONLINE_IDLE"
            events.append(self._make_event(ts + 5000, "ONLINE_IDLE"))
            ts += 5000

        # The shift ends with the courier going offline.
        self.prev_status, self.status = self.status, "OFFLINE"
        events.append(self._make_event(ts, "OFFLINE"))

        events.sort(key=lambda e: e["event_timestamp"])
        return events


class CourierFleetGenerator:
    """
    This class manages a whole fleet of couriers rather than just one.

    It creates many courier simulators, gives them random shift start times
    and durations, and combines all their generated events into one dataset.
    """

    def __init__(self, config: GeneratorConfig):
        self.cfg = config
        random.seed(config.random_seed + 1)

        # Courier IDs are created in a simple padded format so they are neat,
        # consistent, and easy to read during testing.
        self.courier_ids = [f"courier_{i:04d}" for i in range(config.num_couriers)]

    def generate(self, start_ts_ms: int) -> List[Dict]:
        """
        Generate courier event data for the full fleet.

        Each courier gets their own simulated shift, and then all events are
        merged together and sorted by ingestion time.
        """
        all_events: List[Dict] = []

        for cid in self.courier_ids:
            sim = CourierSimulator(cid, self.cfg)
            shift_start = start_ts_ms + random.randint(0, 3600_000)
            shift_dur = random.randint(3600, self.cfg.simulation_duration_seconds)
            all_events.extend(sim.generate_shift(shift_start, shift_dur))

        all_events.sort(key=lambda e: e["ingestion_timestamp"])
        return all_events

    def stream(self, start_ts_ms: int) -> Iterator[Dict]:
        """
        Yield the generated events one by one.

        This is useful when you want to treat the output more like a stream
        instead of consuming the full list at once.
        """
        yield from self.generate(start_ts_ms)
