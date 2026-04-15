"""
config.py

This file keeps the main configuration values and shared reference data
for the food delivery event generator in one place.
"""

from __future__ import annotations
import random
from dataclasses import dataclass, field
from typing import List, Dict, Tuple


# This section defines the city zones.
# Each zone has a latitude range and a longitude range so the generator
# can place restaurants, customers, and couriers inside realistic-looking
# areas of the city.
#
# The city itself is fictional, but the coordinates are designed to resemble
# a medium-sized urban setting with busy central areas and quieter outer areas.
#
# demand_weight shows how likely a zone is to generate customer orders.
# courier_density shows how likely couriers are to be placed in that zone.
ZONES: Dict[str, Dict] = {
    "zone_downtown": {
        "lat_range": (40.7050, 40.7250),
        "lon_range": (-74.0100, -73.9900),
        "demand_weight": 3.5,
        "courier_density": 3.0,
        "label": "Downtown",
    },
    "zone_midtown": {
        "lat_range": (40.7250, 40.7550),
        "lon_range": (-74.0050, -73.9750),
        "demand_weight": 2.5,
        "courier_density": 2.0,
        "label": "Midtown",
    },
    "zone_west": {
        "lat_range": (40.6500, 40.7000),
        "lon_range": (-74.0100, -73.9600),
        "demand_weight": 2.0,
        "courier_density": 1.8,
        "label": "West Side",
    },
    "zone_east": {
        "lat_range": (40.7100, 40.7450),
        "lon_range": (-73.9700, -73.9400),
        "demand_weight": 1.8,
        "courier_density": 1.5,
        "label": "East Side",
    },
    "zone_uptown": {
        "lat_range": (40.7550, 40.7900),
        "lon_range": (-73.9900, -73.9500),
        "demand_weight": 1.5,
        "courier_density": 1.2,
        "label": "Uptown",
    },
    "zone_suburbs": {
        "lat_range": (40.6000, 40.6500),
        "lon_range": (-74.0500, -73.9000),
        "demand_weight": 0.8,
        "courier_density": 0.5,
        "label": "Suburbs",
    },
}

# This dictionary controls how demand changes across the day.
# The key is the hour of the day from 0 to 23, and the value is a relative
# multiplier that tells the generator how busy that hour should be.
#
# Bigger values mean more orders are expected at that time.
# Lower values mean demand is quieter.
#
# The main busy periods are around lunch and dinner, which is common in
# food delivery platforms.
HOURLY_DEMAND_WEIGHTS: Dict[int, float] = {
    0: 0.1, 1: 0.05, 2: 0.03, 3: 0.02, 4: 0.02, 5: 0.05,
    6: 0.15, 7: 0.35, 8: 0.50,
    9: 0.40, 10: 0.35, 11: 0.70,
    12: 1.80, 13: 2.00, 14: 1.20,
    15: 0.80, 16: 0.70, 17: 1.00,
    18: 1.80, 19: 2.20, 20: 2.00, 21: 1.50,
    22: 0.90, 23: 0.40,
}

# During busy hours, restaurants usually take longer to prepare orders.
# This multiplier lets the simulation reflect that by increasing prep times
# during lunch and dinner rush periods.
PEAK_HOUR_PREP_MULTIPLIER: float = 1.4

# These values slightly adjust demand depending on whether the day is a
# weekday or weekend. Weekends often lead to more food delivery activity.
WEEKEND_MULTIPLIER: float = 1.25
WEEKDAY_MULTIPLIER: float = 1.00

# This section defines possible weather conditions in the city and how likely
# each one is to happen.
#
# The weather is sampled for the whole city rather than by zone.
# The weights do not need to add up to 1 exactly, because the weighted
# selection function works with relative values.
WEATHER_CONDITIONS = ["CLEAR", "RAIN", "HEAVY_RAIN", "SNOW"]
WEATHER_WEIGHTS = [0.60, 0.25, 0.10, 0.05]

# Bad weather tends to slow deliveries down.
# These multipliers are used to increase the estimated delivery time
# depending on the weather condition that was sampled.
WEATHER_DELIVERY_MULTIPLIER: Dict[str, float] = {
    "CLEAR": 1.00,
    "RAIN": 1.20,
    "HEAVY_RAIN": 1.45,
    "SNOW": 1.60,
}

# This list contains the cuisine categories used when generating restaurants.
# It gives some variety to the dataset so that restaurants do not all look alike.
RESTAURANT_CUISINES = [
    "Italian", "Japanese", "Mexican", "Indian", "American",
    "Chinese", "Thai", "Mediterranean", "Korean", "French",
]

# This dictionary links each cuisine to a small menu.
# Each menu item is stored as a tuple:
# item name, price in cents
#
# Using cents instead of decimal currency values helps avoid floating point issues
# and is a common practice in programming.
MENU_ITEMS_BY_CUISINE: Dict[str, List[Tuple[str, int]]] = {
    "Italian": [
        ("Margherita Pizza", 1299),
        ("Carbonara Pasta", 1599),
        ("Tiramisu", 699),
        ("Caesar Salad", 999),
    ],
    "Japanese": [
        ("Salmon Sushi Roll", 1499),
        ("Ramen Bowl", 1399),
        ("Gyoza (6pc)", 799),
        ("Miso Soup", 399),
    ],
    "Mexican": [
        ("Beef Burrito", 1099),
        ("Tacos Al Pastor (3pc)", 999),
        ("Guacamole & Chips", 699),
        ("Churros", 599),
    ],
    "Indian": [
        ("Butter Chicken", 1349),
        ("Garlic Naan", 349),
        ("Biryani Rice", 1199),
        ("Mango Lassi", 449),
    ],
    "American": [
        ("Smash Burger", 1299),
        ("Mac & Cheese", 999),
        ("Buffalo Wings", 1099),
        ("Milkshake", 649),
    ],
    "Chinese": [
        ("Kung Pao Chicken", 1199),
        ("Dim Sum (4pc)", 899),
        ("Fried Rice", 849),
        ("Spring Rolls", 699),
    ],
    "Thai": [
        ("Pad Thai", 1249),
        ("Green Curry", 1299),
        ("Mango Sticky Rice", 749),
        ("Tom Yum Soup", 849),
    ],
    "Mediterranean": [
        ("Falafel Wrap", 1099),
        ("Hummus Plate", 799),
        ("Shakshuka", 1199),
        ("Baklava", 549),
    ],
    "Korean": [
        ("Bibimbap", 1349),
        ("Korean BBQ Platter", 1999),
        ("Tteokbokki", 999),
        ("Kimchi Pancake", 849),
    ],
    "French": [
        ("Croque Monsieur", 1149),
        ("French Onion Soup", 999),
        ("Crêpes Suzette", 849),
        ("Quiche Lorraine", 1099),
    ],
}


# This dataclass stores the main settings for the generator.
# It makes it easier to change how large the simulation is, how long it runs,
# and how many unusual or imperfect cases should be injected into the data.
#
# This is useful because realistic datasets often include messy situations,
# not just perfect order flows.
@dataclass
class GeneratorConfig:
    # These values control the size of the generated world.
    # More restaurants, couriers, and customers will usually produce a richer dataset.
    num_restaurants: int = 50
    num_couriers: int = 80
    num_customers: int = 500

    # These values control how long the simulation runs and roughly how many
    # events it tries to generate each second.
    simulation_duration_seconds: int = 3600
    events_per_second_target: float = 10.0

    # These rates introduce imperfect or edge-case behaviour into the data.
    # This is useful for testing analytics pipelines and checking whether
    # downstream systems can handle messy real-world situations.
    late_arrival_rate: float = 0.05
    max_late_arrival_seconds: int = 300
    duplicate_rate: float = 0.02
    cancellation_rate: float = 0.12
    missing_step_rate: float = 0.03
    impossible_duration_rate: float = 0.02
    courier_offline_mid_delivery_rate: float = 0.015

    # These values control whether a surge is active.
    # A surge means one specific zone experiences much higher demand than usual.
    surge_active: bool = False
    surge_zone: str = "zone_downtown"
    surge_multiplier: float = 3.0

    # These settings control where output files are saved and how many example
    # JSON records should be produced for inspection.
    output_dir: str = "./sample_data"
    json_sample_orders: int = 100
    json_sample_courier_events: int = 200
    random_seed: int = 42


# This helper chooses one item from a list using custom weights.
# Items with larger weights are more likely to be selected.
def weighted_choice(population: List, weights: List[float]):
    total = sum(weights)
    r = random.uniform(0, total)
    cumulative = 0.0

    for item, w in zip(population, weights):
        cumulative += w
        if r <= cumulative:
            return item

    # This final return acts as a safety fallback in case of rounding behaviour.
    return population[-1]


# This function picks a zone based on demand weights.
# In other words, zones where more orders are expected are more likely to be chosen.
def zone_weighted_choice() -> str:
    zones = list(ZONES.keys())
    weights = [ZONES[z]["demand_weight"] for z in zones]
    return weighted_choice(zones, weights)


# This function picks a zone based on courier density.
# Zones with more courier presence are more likely to be selected.
def zone_courier_weighted_choice() -> str:
    zones = list(ZONES.keys())
    weights = [ZONES[z]["courier_density"] for z in zones]
    return weighted_choice(zones, weights)


# This function generates a random latitude and longitude inside a chosen zone.
# The coordinates are rounded to 6 decimal places so they look neat and realistic.
def random_coords_in_zone(zone_id: str) -> Tuple[float, float]:
    z = ZONES[zone_id]
    lat = random.uniform(*z["lat_range"])
    lon = random.uniform(*z["lon_range"])
    return round(lat, 6), round(lon, 6)


# This function returns the demand multiplier for a given hour.
# It also adjusts the value depending on whether the date falls on a weekend.
def hourly_demand_weight(hour: int, is_weekend: bool) -> float:
    base = HOURLY_DEMAND_WEIGHTS.get(hour, 0.5)
    return base * (WEEKEND_MULTIPLIER if is_weekend else WEEKDAY_MULTIPLIER)


# This function checks whether the given hour falls inside one of the main rush periods.
# We treat lunch and dinner as peak hours because those are usually the busiest
# times in delivery platforms.
def is_peak_hour(hour: int) -> bool:
    """Return True when the hour is in the lunch or dinner rush window."""
    return hour in (12, 13, 14, 19, 20, 21)


# This function samples one weather condition using the predefined weights.
# Since the choice is weighted, clear weather will happen more often than snow.
def sample_weather() -> str:
    return weighted_choice(WEATHER_CONDITIONS, WEATHER_WEIGHTS)
