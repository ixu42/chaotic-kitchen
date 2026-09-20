from __future__ import annotations

import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.kafka_client import make_consumer
from shared.models import (
    TOPIC_BURNT,
    TOPIC_DESSERT,
    TOPIC_DRINKS,
    TOPIC_GRILL,
    TOPIC_INCOMING,
    TOPIC_READY,
    Order,
)

TOPICS = [
    TOPIC_INCOMING,
    TOPIC_GRILL,
    TOPIC_DRINKS,
    TOPIC_DESSERT,
    TOPIC_READY,
    TOPIC_BURNT,
]


def render(counts: Counter[str], last: list[str]) -> None:
    # Clear-ish screen for a live board feel
    print("\033[2J\033[H", end="")
    print("╔══════════════════════════════════════════╗")
    print("║         CHAOTIC KITCHEN — LIVE           ║")
    print("╚══════════════════════════════════════════╝")
    print()
    print(f"  Incoming seen : {counts[TOPIC_INCOMING]:4d}")
    print(f"  Grill tickets : {counts[TOPIC_GRILL]:4d}")
    print(f"  Drinks tickets: {counts[TOPIC_DRINKS]:4d}")
    print(f"  Dessert tickets:{counts[TOPIC_DESSERT]:4d}")
    print(f"  Ready         : {counts[TOPIC_READY]:4d}")
    print(f"  Burnt (DLQ)   : {counts[TOPIC_BURNT]:4d}")
    print()
    print("Recent:")
    for line in last[-8:]:
        print(f"  {line}")
    print()
    print("Ctrl+C to stop.")


def main() -> None:
    consumer = make_consumer("kitchen-board", TOPICS)
    counts: Counter[str] = Counter()
    last: list[str] = []
    print("Board starting…")

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                render(counts, last)
                continue
            if msg.error():
                continue

            topic = msg.topic()
            counts[topic] += 1
            try:
                order = Order.from_json(msg.value())
                last.append(f"{topic:18} {order.order_id} {order.item}")
            except Exception:
                last.append(f"{topic:18} <unreadable>")
            render(counts, last)
            time.sleep(0.05)
    except KeyboardInterrupt:
        print("\nBoard closed.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
