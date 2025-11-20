# Kafka Avro Orders — Producer & Consumer

A compact Python-based Kafka demo showcasing Avro serialization (with `fastavro`), the Confluent Kafka client, message failure handling with retries and a Dead Letter Queue (DLQ), and simple aggregation of order prices.

This repository contains a small producer that generates randomized orders and a consumer that deserializes Avro messages, applies failure rules, retries when appropriate, and funnels permanently failing messages to an `orders-dlq` topic with helpful metadata.

## Screenshots

Place your screenshots in the `Documents/` folder. Example filenames used in this README (replace with your actual screenshots):

- `Documents/producer-send.png` — Producer run / delivery reports
- `Documents/consumer-run.png` — Consumer console output showing processing
- `Documents/dlq-messages.png` — Inspection of `orders-dlq` topic showing headers

To embed them in Markdown (once you add them), use:

```markdown
![Producer run](Documents/producer-send.png)
![Consumer run](Documents/consumer-run.png)
![DLQ messages](Documents/dlq-messages.png)
```

## Quick summary

- Producer: `producer.py` — generates 15 random orders, serializes each using Avro (`order.avsc`) via `fastavro`, and sends them to the `orders` Kafka topic.
- Consumer: `consumer.py` — subscribes to `orders`, deserializes Avro messages, enforces failure rules, retries transient failures, and routes permanent failures to `orders-dlq` with headers explaining the reason.
- Schema: `order.avsc` — Avro schema describing an `Order` with `orderId`, `product`, and `price`.

## Technologies used

- Python 3 (tested with Python 3.12 in this workspace)
- Apache Kafka (local broker used for testing)
- Avro serialization via `fastavro`
- Confluent Kafka Python client: `confluent-kafka`
- Faker (for random product names)
- Standard libs: `json`, `uuid`, `random`, `io`, `time`

## Required pip packages

Install the runtime dependencies (recommended inside a virtual environment):

```bash
# if you prefer a fresh venv
python3 -m venv venv
source venv/bin/activate

# or use the provided virtualenv in the repo (if present):
# source bigdata/bin/activate

pip install --upgrade pip
pip install confluent-kafka fastavro faker
```

Note: On some platforms `confluent-kafka` may require system libraries (librdkafka). On Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y librdkafka-dev
```

If you prefer a `requirements.txt`, create one from the installed packages:

```bash
pip freeze > requirements.txt
```

## Files in this repo

- `producer.py` — Avro-based Kafka producer that generates and sends sample orders.
- `consumer.py` — Avro-based Kafka consumer that enforces failure rules, retries, aggregates results, and sends permanent failures to DLQ.
- `order.avsc` — Avro schema used by producer and consumer.
- `Documents/` — Suggested place for screenshots used in this README.

## How it works — theory and implementation notes

1. Avro serialization
   - The producer reads `order.avsc` and uses `fastavro.parse_schema` + `fastavro.writer` to serialize Python dicts into Avro bytes. This provides compact, schema-driven serialization and ensures both producer and consumer agree on the record layout.

2. Confluent Kafka client
   - `confluent-kafka` provides high-performance producers and consumers backed by librdkafka. The producer uses `Producer.produce()` and polls for delivery reports. The consumer uses `Consumer.poll()` and manual offset handling to support retry semantics.

3. Consumer behavior, retry rules, and DLQ
   - The consumer disables auto commits (`enable.auto.commit: False`) and performs manual commit logic to allow messages to be retried or sent to a DLQ when needed.
   - Failure rules implemented in `consumer.py`:
     - Transient failure: `5.0 <= price <= 50.0` — these are considered transient and will be retried (not committed) so they can be reprocessed.
     - Permanent failure: `price > 1000.0` — these are permanently failed and are forwarded to the DLQ topic `orders-dlq` with metadata headers.
   - DLQ headers include useful metadata fields: `error_reason`, `original_topic`, `original_partition`, `original_offset`, and `timestamp`. The raw failing message bytes are used as DLQ message `value` so the original payload is preserved.
   - Retry logic: consumer implements a retry loop (`max_retries` configurable) — if processing consistently fails beyond this threshold the message is sent to DLQ.

4. Aggregation and stats
   - The consumer keeps running aggregations (total price, order count, average) and prints summaries when interrupted. This demonstrates how stateful processing/metrics can be collected alongside per-message failure handling.

## Setup & running steps

1) Start Kafka (local broker)

- Ensure Kafka is running and reachable at `localhost:9092`. If you're using a packaged Kafka (confluent or Apache), start ZooKeeper (if required) and Kafka broker first.

2) Create the required topics (optional if auto-create is enabled)

```bash
# Create topics using your Kafka distribution's kafka-topics tool (adjust path if needed)
# Example (Apache Kafka):
kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

3) Activate environment and install dependencies

```bash
# Use the provided venv (if present):
source bigdata/bin/activate

# Or create/activate your own venv:
# python3 -m venv venv
# source venv/bin/activate

pip install confluent-kafka fastavro faker
```

4) Run the consumer (recommended before producing so it can process immediately)

```bash
python consumer.py
```

5) Produce sample messages

```bash
python producer.py
```

The `producer.py` will generate 15 orders with random prices between roughly 5.0 and 1500.0. Because of the configured failure rules a few messages may trigger transient failures or be sent to the DLQ.

6) Inspect DLQ messages

To read messages from the DLQ (raw bytes—if using `kafka-console-consumer`, you will see serialized bytes unless decoded):

```bash
kafka-console-consumer --topic orders-dlq --bootstrap-server localhost:9092 --from-beginning --property print.headers=true
```

If you're using this consumer script's logic, the DLQ messages include headers that explain the failure reason and original offsets.

## Expected behavior and notes

- Transient failures are simulated for prices between 5.0 and 50.0: the consumer will not commit offsets for transient failures so the message will be retried according to your consumer group's behavior and the script's retry logic.
- Permanent failures (price > 1000.0) are forwarded to `orders-dlq` with headers and are not reprocessed.
- The producer sets message keys to `orderId` to help partitioning/ordering.
- Both producer and consumer use Avro with the same `order.avsc` schema — ensure both sides use the same schema file.

## Troubleshooting

- "Unable to import confluent_kafka": ensure `confluent-kafka` is installed in your active Python environment. If you see compilation or wheel issues, install system dependencies like `librdkafka-dev` before reinstalling.
- Kafka connectivity errors: confirm the broker address and that Kafka is running. If using Docker, ensure correct port mappings.
- Avro deserialize errors: check that `order.avsc` matches the record structure produced by `producer.py`.

## Next steps (suggestions)

- Add a `requirements.txt` and/or a lightweight `docker-compose` to stand up Kafka + Zookeeper for reproducible testing.
- Add automated tests for the producer and consumer (unit tests for serialize/deserialize and integration tests against a test Kafka broker).
- Add exponential backoff for retries and more granular failure categories.
- Add schema registry integration for centralized Avro schema management.

## License

This repository is provided as-is for learning and demo purposes.


---

If you'd like, I can also:

- Generate a `requirements.txt` with pinned versions.
- Add example screenshot image files to `Documents/` (placeholders) so the README images render immediately.
- Create a `docker-compose.yml` to run Kafka locally for testing.

Tell me which next step you want and I'll implement it.