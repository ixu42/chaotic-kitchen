from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.kafka_client import delivery_report, make_consumer, make_producer
from shared.models import (
    MAX_ATTEMPTS,
    RETRY_BACKOFF_SECS,
    STATION_TOPICS,
    TOPIC_BURNT,
    TOPIC_DISCARDED,
    Order,
)


def main() -> None:
    consumer = make_consumer("kitchen-retry", [TOPIC_BURNT])
    producer = make_producer()
    print(f"Retryer listening on {TOPIC_BURNT}")
    print(f"Max attempts={MAX_ATTEMPTS}, backoff={RETRY_BACKOFF_SECS}s")
    print("Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            raw = msg.value() or b""
            try:
                order = Order.from_json(raw)
            except Exception as exc:  # noqa: BLE001 — poison payload → discarded
                producer.produce(
                    TOPIC_DISCARDED,
                    key=msg.key(),
                    value=raw,
                    callback=delivery_report,
                )
                producer.poll(0)
                print(f"  ✗ discarded unreadable burnt message: {exc}")
                continue

            if order.attempt > MAX_ATTEMPTS:
                producer.produce(
                    TOPIC_DISCARDED,
                    key=order.order_id.encode("utf-8"),
                    value=order.to_json().encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)
                print(
                    f"  🗑️  discard {order.order_id} ({order.item}) — "
                    f"failed {MAX_ATTEMPTS} cook attempts, giving up"
                )
                continue

            dest = STATION_TOPICS[order.station]
            print(
                f"  ⏳ backoff {RETRY_BACKOFF_SECS}s then retry "
                f"{order.order_id} ({order.item}) attempt={order.attempt} → {dest}"
            )
            time.sleep(RETRY_BACKOFF_SECS)
            producer.produce(
                dest,
                key=order.order_id.encode("utf-8"),
                value=order.to_json().encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            print(f"  🔁 requeued {order.order_id} → {dest}")
    except KeyboardInterrupt:
        print("\nRetryer closed.")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
