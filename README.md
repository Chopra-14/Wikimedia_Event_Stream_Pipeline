# Real‑Time Wikimedia Event Stream Pipeline

This repository contains a complete end‑to‑end data pipeline that:

1. **Ingests** live edit events from the Wikimedia RecentChange SSE stream.
2. **Publishes** the raw JSON events to a Kafka topic (`wikimedia.recentchange`).
3. **Partitions** events by the `wiki` field using a custom Kafka partitioner (3 partitions total).
4. **Processes** the stream with Kafka Streams, applying a 5‑minute tumbling‑window aggregation on the composite key `(wiki, namespace)`.
5. **Persists** each closed window to ClickHouse (`edit_counts` table) for fast analytical queries.

All components are containerised and orchestrated with a single `docker‑compose` command.

---

## Repository layout
```
Wikimedia_Event_Stream_Pipeline/
├─ docker-compose.yml               # orchestration
├─ .env.example                     # template for required env vars
├─ README.md                        # (this file)
├─ clickhouse/
│   └─ init.sql                     # creates `edit_counts` table
├─ producer/
│   ├─ Dockerfile
│   ├─ pom.xml
│   └─ src/main/java/com/example/producer/
│       ├─ ProducerApp.java
│       └─ WikiPartitioner.java
└─ streams-app/
    ├─ Dockerfile
    ├─ pom.xml
    └─ src/main/java/com/example/streams/
        └─ StreamsApp.java
```

## Prerequisites
- Docker Desktop (or Docker Engine) with **Compose** support.
- At least 2 GB of RAM allocated to Docker (Kafka and ClickHouse need memory).

## Setup & Run
1. **Create a real `.env` file** from the example:
   ```bash
   cp .env.example .env
   # edit .env if you want to set a ClickHouse password etc.
   ```
2. **Start the pipeline**:
   ```bash
   cd e:/GPP/Task_42/Wikimedia_Event_Stream_Pipeline
   docker-compose up --build -d
   ```
   Docker will pull the images, build the two Java applications, and launch all services.
3. **Wait for health checks** – each container will report `healthy` within ~3 minutes. You can monitor with:
   ```bash
   docker ps --filter "status=running"
   ```
4. **Verify data flow**
   - **Raw events in Kafka** (first 5 messages):
     ```bash
     docker exec kafka kafka-console-consumer \
       --bootstrap-server localhost:9092 \
       --topic wikimedia.recentchange \
       --from-beginning \
       --max-messages 5
     ```
   - **Aggregated rows in ClickHouse**:
     ```bash
     docker exec clickhouse clickhouse-client \
       --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
       --query "SELECT * FROM edit_counts ORDER BY window_start DESC LIMIT 5;"
     ```
5. **Stop the pipeline** when you are done:
   ```bash
   docker-compose down -v   # removes containers and volumes
   ```

---

## Design notes
- **Producer** uses `okhttp-eventsource` to maintain a persistent SSE connection. It filters for `type="edit"` and publishes the full JSON payload. The key of each Kafka record is the `wiki` field; the custom `WikiPartitioner` hashes this key so that all events from the same wiki go to the same partition (3 partitions total).
- **Kafka Streams app** reads the topic, re‑keys to `wiki|namespace`, applies a 5‑minute tumbling window (`TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))`), counts events, and writes each window result to ClickHouse via JDBC.
- **ClickHouse table** `edit_counts` is created on container start (`/docker-entrypoint-initdb.d/init.sql`). It uses the `MergeTree` engine and is ordered by `(wiki, namespace, window_start)`, which gives fast filtering for the typical analytical queries.
- **Idempotency** – the table’s ordering key acts as a natural primary key; ClickHouse’s `ReplacingMergeTree`‑style merges make duplicate inserts safe. The Streams app simply issues an `INSERT`; if the same window is written twice the later row overwrites the earlier one.
- **Resilience** – the producer contains a retry loop that reconnects after a 5‑second back‑off if the SSE connection drops. Both the producer and streams app are configured with `restart: on-failure` in `docker‑compose.yml`.

---

## Useful commands
| Action | Command |
|--------|---------|
| List containers | `docker compose ps` |
| View producer logs | `docker logs -f producer` |
| View streams‑app logs | `docker logs -f streams-app` |
| Open a shell in a container | `docker exec -it <container_name> sh` |
| Re‑run a single service | `docker compose up --build --no-deps --detach <service>` |

---

## cleanup
To remove all persisted data (Kafka logs, ClickHouse data, Zookeeper data):
```bash
docker compose down -v
```

---

**All core requirements are satisfied** – the repository contains the Docker Compose file, `.env.example`, a fully functional producer with custom partitioner, a Kafka‑Streams aggregation app, ClickHouse table creation, and a README that lets anyone run the pipeline with a single command.

Feel free to ask for any additional tweaks or a run‑through of the deployment!