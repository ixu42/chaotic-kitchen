from __future__ import annotations

import asyncio
import json
import sys
import threading
from collections import Counter
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.kafka_client import make_consumer
from shared.models import (
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
                    try:
                        order = Order.from_json(msg.value())
                        line = f"{topic:18} {order.order_id} {order.item}"
                    except Exception:
                        line = f"{topic:18} <unreadable>"
                    self.recent.append(line)
                    if len(self.recent) > 100:
                        self.recent = self.recent[-100:]
                    payload = {
                        "counts": {t: self.counts[t] for t in TOPICS},
                        "recent": list(self.recent[-20:]),
                    }

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


@asynccontextmanager
async def lifespan(_app: FastAPI):
    hub.start(asyncio.get_running_loop())
    print("Web board on http://127.0.0.1:8000  (Ctrl+C to stop)")
    yield
    hub.stop()


app = FastAPI(title="Chaotic Kitchen", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/favicon.ico")
async def favicon() -> Response:
    return Response(status_code=204)


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
        host="127.0.0.1",
        port=8000,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
