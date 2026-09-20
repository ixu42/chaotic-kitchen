from __future__ import annotations

import random
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.kafka_client import delivery_report, make_producer
from shared.models import ITEM_STATION, TOPIC_INCOMING, Order

ITEMS = list(ITEM_STATION.keys())


def main() -> None:
    rate = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    producer = make_producer()
    print(f"Chaotic Kitchen producer — ~{rate} order(s)/sec → {TOPIC_INCOMING}")
    print("Ctrl+C to stop.\n")

    try:
        while True:
            item = random.choice(ITEMS)
            order = Order.new(order_id=f"ord-{uuid.uuid4().hex[:8]}", item=item)
            producer.produce(
                TOPIC_INCOMING,
                key=order.order_id.encode("utf-8"),
                value=order.to_json().encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            print(f"  + {order.order_id}  {order.item:10} → {order.station}")
            time.sleep(max(0.05, 1.0 / rate))
    except KeyboardInterrupt:
        print("\nFlushing…")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
