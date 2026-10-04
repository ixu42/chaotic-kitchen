# Chaotic Kitchen

A Kafka hobby project: a chaotic restaurant kitchen.

Orders stream in on Kafka. A router sends them to grill / drinks / dessert stations.
Stations sometimes burn food → **dead-letter topic**. A retryer requeues with backoff
(or discards after max attempts). A live board shows the madness.

Useful for learning: topics, producers, consumers, consumer groups, keys, DLQ.

## Tech stack

- Apache Kafka 4.3.1 (Docker, KRaft)
- Python 3.12
- `confluent-kafka` 2.15.1

## Architecture

```mermaid
flowchart LR
    A[Order producer] --> B[Incoming orders]
    B --> C[Router]
    C --> D[Kitchen stations]
    D -->|Success| E[Ready orders]
    D -->|Failure| F[Burnt orders]
    F -->|Retry after backoff| D
    F -->|Max attempts| H[Discarded]
    B -.-> G[Live board]
    D -.-> G
    E -.-> G
    F -.-> G
    H -.-> G
```

Solid arrows = order path. Dotted arrows = live board observing. Stations = grill / drinks / dessert.

## Topics

| Topic | Role |
|--------|------|
| `orders.incoming` | New orders |
| `station.grill` | Hot food |
| `station.drinks` | Drinks |
| `station.dessert` | Desserts |
| `orders.ready` | Completed |
| `orders.burnt` | Dead-letter (failures, retried) |
| `orders.discarded` | Gave up after max attempts / poison |

## Setup

```bash
cd /path/to/chaotic-kitchen
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d
```

Wait until Kafka is healthy and `init-topics` finishes (`docker compose ps`).

If Kafka was already running from an older checkout, recreate topics:

```bash
docker compose up init-topics
```

## Run (separate terminals)

```bash
source .venv/bin/activate

# 1) Live board
python board/main.py

# 2) Router
python router/main.py

# 3) Stations
python workers/station.py grill
python workers/station.py drinks
python workers/station.py dessert

# 4) Retry burnt orders (backoff, then station or discard)
python workers/retry.py

# 5) Flood the kitchen (orders per second)
python producer/main.py 2
```

Turn up the producer rate (`5`, `10`, …) and watch grill lag / burns climb.

## Optional: inspect topics

```bash
# list topics
docker exec -it chaotic-kitchen-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

# consume from the beginning (Ctrl+C to stop)
docker exec -it chaotic-kitchen-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.incoming \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
```

Swap `orders.incoming` for `station.grill`, `orders.ready`, `orders.burnt`, `orders.discarded`, etc.

## Stop

```bash
docker compose down -v
```

## Concepts covered

- Event-driven pipeline with multiple consumer groups
- Partition key = `order_id` (ordering per order)
- Dead-letter topic with retry and a final discard topic
- Backoff before retry — without a pause, burnt orders can loop `burnt → station → burnt` and flood the system
- Backpressure / lag visible when grill is slower than intake
