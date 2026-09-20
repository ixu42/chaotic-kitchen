from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.kafka_client import delivery_report, make_consumer, make_producer
from shared.models import STATION_TOPICS, TOPIC_BURNT, TOPIC_READY, Order

# Simulate station speed + flakiness (fun chaos knobs)
STATION_CONFIG = {
    "grill": {"cook_secs": (1.5, 3.5), "burn_chance": 0.12},
    "drinks": {"cook_secs": (0.4, 1.0), "burn_chance": 0.05},
    "dessert": {"cook_secs": (0.8, 2.0), "burn_chance": 0.08},
}


def run_station(station: str) -> None:
    topic = STATION_TOPICS[station]
    cfg = STATION_CONFIG[station]
    consumer = make_consumer(f"station-{station}", [topic])
    producer = make_producer()
    print(f"🔥 {station.upper()} station on {topic}")
    print("Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            order = Order.from_json(msg.value())
            cook_for = random.uniform(*cfg["cook_secs"])
            print(f"  cooking {order.order_id} ({order.item}) ~{cook_for:.1f}s…")
            time.sleep(cook_for)

            if random.random() < cfg["burn_chance"]:
                order.attempt += 1
                producer.produce(
                    TOPIC_BURNT,
                    key=order.order_id.encode("utf-8"),
                    value=order.to_json().encode("utf-8"),
                    callback=delivery_report,
                )
                print(f"  💀 BURNT {order.order_id} ({order.item}) → {TOPIC_BURNT}")
            else:
                producer.produce(
                    TOPIC_READY,
                    key=order.order_id.encode("utf-8"),
                    value=order.to_json().encode("utf-8"),
                    callback=delivery_report,
                )
                print(f"  ✅ READY  {order.order_id} ({order.item})")
            producer.poll(0)
    except KeyboardInterrupt:
        print(f"\n{station} station closed.")
    finally:
        producer.flush()
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Chaotic Kitchen station worker")
    parser.add_argument(
        "station",
        choices=sorted(STATION_TOPICS),
        help="Which kitchen station to run",
    )
    args = parser.parse_args()
    run_station(args.station)


if __name__ == "__main__":
    main()
