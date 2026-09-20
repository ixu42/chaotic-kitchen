from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.kafka_client import delivery_report, make_consumer, make_producer
from shared.models import STATION_TOPICS, TOPIC_BURNT, TOPIC_INCOMING, Order


def main() -> None:
    consumer = make_consumer("kitchen-router", [TOPIC_INCOMING])
    producer = make_producer()
    print(f"Router listening on {TOPIC_INCOMING}")
    print("Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                order = Order.from_json(msg.value())
                dest = STATION_TOPICS[order.station]
                producer.produce(
                    dest,
                    key=order.order_id.encode("utf-8"),
                    value=order.to_json().encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)
                print(f"  → route {order.order_id}  {order.item} → {dest}")
            except Exception as exc:  # noqa: BLE001 — demo: burn bad payloads
                raw = msg.value() or b""
                producer.produce(
                    TOPIC_BURNT,
                    key=msg.key(),
                    value=raw,
                    callback=delivery_report,
                )
                producer.poll(0)
                print(f"  ✗ burnt bad message: {exc}")
    except KeyboardInterrupt:
        print("\nShutting down router…")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
