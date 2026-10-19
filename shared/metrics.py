from __future__ import annotations

from prometheus_client import Counter

# Events observed by the web board consumer (one series per topic).
EVENTS_TOTAL = Counter(
    "chaotic_kitchen_events_total",
    "Kafka events observed by the web board",
    ["topic"],
)

# Orders placed via the web UI (POST /api/orders).
ORDERS_PLACED_TOTAL = Counter(
    "chaotic_kitchen_orders_placed_total",
    "Orders placed through the web board API",
    ["item", "station"],
)
