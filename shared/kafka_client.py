from __future__ import annotations

from confluent_kafka import Consumer, Producer

from shared.models import BOOTSTRAP


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP})


def make_consumer(group_id: str, topics: list[str]) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(topics)
    return consumer


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")
