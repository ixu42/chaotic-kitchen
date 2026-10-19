from __future__ import annotations

import asyncio
import json
import random
import sys
import threading
import uuid
from collections import Counter
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.kafka_client import delivery_report, make_consumer, make_producer
from shared.metrics import EVENTS_TOTAL, ORDERS_PLACED_TOTAL
from shared.models import (
    ITEM_STATION,
    MAX_ATTEMPTS,
    TOPIC_BURNT,
    TOPIC_DESSERT,
    TOPIC_DISCARDED,
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
    TOPIC_DISCARDED,
]

STATIC = Path(__file__).resolve().parent / "static"

# Own consumer group so ASCII board and web board can run together.
GROUP_ID = "kitchen-web-board"


def format_recent_line(topic: str, raw: bytes | None) -> str:
    """Human-readable recent-event row, including attempt / discard reason."""
    try:
        order = Order.from_json(raw or b"")
    except Exception:
        if topic == TOPIC_DISCARDED:
            return f"{topic:18} <unreadable>  poison payload"
        return f"{topic:18} <unreadable>"

    base = f"{topic:18} {order.order_id} {order.item}  attempt={order.attempt}"
    if topic == TOPIC_DISCARDED:
        if order.attempt > MAX_ATTEMPTS:
            return f"{base}  max attempts"
        return f"{base}  discarded"
    if topic == TOPIC_BURNT:
        return f"{base}  burnt"
    if topic == TOPIC_READY:
        return f"{base}  ready"
    return base


class BoardHub:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.recent: list[str] = []
        self._lock = threading.Lock()
        self._clients: set[WebSocket] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "counts": {t: self.counts[t] for t in TOPICS},
                "recent": list(self.recent[-20:]),
            }

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._stop.clear()
        self._thread = threading.Thread(target=self._consume_loop, name="web-board-kafka", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3.0)

    async def register(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.add(ws)
        await ws.send_text(json.dumps(self.snapshot()))

    def unregister(self, ws: WebSocket) -> None:
        self._clients.discard(ws)

    def _consume_loop(self) -> None:
        consumer = make_consumer(GROUP_ID, TOPICS)
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    continue

                topic = msg.topic() or ""
                with self._lock:
                    self.counts[topic] += 1
                    self.recent.append(format_recent_line(topic, msg.value()))
                    if len(self.recent) > 100:
                        self.recent = self.recent[-100:]
                    payload = {
                        "counts": {t: self.counts[t] for t in TOPICS},
                        "recent": list(self.recent[-20:]),
                    }
                EVENTS_TOTAL.labels(topic=topic).inc()

                if self._loop is not None:
                    asyncio.run_coroutine_threadsafe(self._broadcast(payload), self._loop)
        finally:
            consumer.close()

    async def _broadcast(self, payload: dict) -> None:
        raw = json.dumps(payload)
        dead: list[WebSocket] = []
        for ws in list(self._clients):
            try:
                await ws.send_text(raw)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._clients.discard(ws)


hub = BoardHub()
_producer = None
MENU = sorted(ITEM_STATION.keys())


class PlaceOrderBody(BaseModel):
    item: str | None = Field(
        default=None,
        description="Menu item; omit or null to pick a random one",
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _producer
    _producer = make_producer()
    hub.start(asyncio.get_running_loop())
    print("Web board on http://127.0.0.1:8000  (Ctrl+C to stop)")
    yield
    hub.stop()
    if _producer is not None:
        _producer.flush(2)
        _producer = None


app = FastAPI(title="Chaotic Kitchen", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/favicon.ico")
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api/items")
async def list_items() -> dict:
    return {"items": MENU}


@app.post("/api/orders")
async def place_order(body: PlaceOrderBody | None = None) -> dict:
    if _producer is None:
        raise HTTPException(status_code=503, detail="Producer not ready")

    item = (body.item if body else None) or random.choice(MENU)
    if item not in ITEM_STATION:
        raise HTTPException(status_code=400, detail=f"Unknown item: {item}")

    order = Order.new(order_id=f"ord-{uuid.uuid4().hex[:8]}", item=item)
    _producer.produce(
        TOPIC_INCOMING,
        key=order.order_id.encode("utf-8"),
        value=order.to_json().encode("utf-8"),
        callback=delivery_report,
    )
    _producer.poll(0)
    _producer.flush(1.0)
    ORDERS_PLACED_TOTAL.labels(item=order.item, station=order.station).inc()
    return {
        "order_id": order.order_id,
        "item": order.item,
        "station": order.station,
    }


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.websocket("/ws")
async def ws_board(ws: WebSocket) -> None:
    await hub.register(ws)
    try:
        while True:
            # Keep the socket open; client does not need to send.
            await ws.receive_text()
    except WebSocketDisconnect:
        hub.unregister(ws)
    except Exception:
        hub.unregister(ws)


def main() -> None:
    import uvicorn

    uvicorn.run(
        "web.main:app",
        # Listen beyond loopback so Prometheus in Docker can scrape /metrics.
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
