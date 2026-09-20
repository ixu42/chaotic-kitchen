from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_INCOMING = "orders.incoming"
TOPIC_GRILL = "station.grill"
TOPIC_DRINKS = "station.drinks"
TOPIC_DESSERT = "station.dessert"
TOPIC_BURNT = "orders.burnt"
TOPIC_READY = "orders.ready"

STATION_TOPICS = {
    "grill": TOPIC_GRILL,
    "drinks": TOPIC_DRINKS,
    "dessert": TOPIC_DESSERT,
}

ITEM_STATION = {
    "burger": "grill",
    "steak": "grill",
    "fries": "grill",
    "coffee": "drinks",
    "soda": "drinks",
    "smoothie": "drinks",
    "cake": "dessert",
    "ice-cream": "dessert",
    "pie": "dessert",
}


@dataclass
class Order:
    order_id: str
    item: str
    station: str
    created_at: str
    attempt: int = 1

    @classmethod
    def new(cls, order_id: str, item: str) -> "Order":
        station = ITEM_STATION.get(item)
        if station is None:
            raise ValueError(f"Unknown item: {item}")
        return cls(
            order_id=order_id,
            item=item,
            station=station,
            created_at=datetime.now(timezone.utc).isoformat(),
            attempt=1,
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Order":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data: dict[str, Any] = json.loads(raw)
        return cls(**data)
