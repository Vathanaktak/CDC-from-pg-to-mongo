# CDC Pipeline: File-by-File Explanation & Demo Presentation Guide

> **Project Goal:** Real-time Change Data Capture (CDC) streaming data changes (`INSERT`, `UPDATE`, `DELETE`) from **PostgreSQL** to **MongoDB** using **Debezium**, **Apache Kafka**, and **Kafka Connect**.

---

## 📋 Table of Contents
1. [30-Second Elevator Pitch (For Presentation)](#1-30-second-elevator-pitch)
2. [High-Level Architecture & Data Flow](#2-high-level-architecture--data-flow)
3. [Deep-Dive: Every File Explained](#3-deep-dive-every-file-explained)
   - [docker-compose.yml](#1-docker-composeyml)
   - [connect/Dockerfile & JAR](#2-connectdockerfile--mongo-kafka-connect-jar)
   - [postgres/init.sql](#3-postgresinitsql)
   - [connectors/pg-source.json](#4-connectorspg-sourcejson)
   - [connectors/mongo-sink.json](#5-connectorsmongo-sinkjson)
   - [scripts/setup.sh](#6-scriptssetupsh)
   - [scripts/demo.sh](#7-scriptsdemosh)
   - [scripts/test-1m.ps1 & test-1m.sh](#8-scriptstest-1mps1--test-1msh)
   - [scripts/cleanup.sh](#9-scriptscleanupsh)
   - [postman/CDC-Postgres-MongoDB.postman_collection.json](#10-postmancdc-postgres-mongodbpostman_collectionjson)
   - [flow-diagram.md, how-test.md & README.md](#11-documentation-files)
4. [Step-by-Step Live Demo Script (What to do & say)](#4-step-by-step-live-demo-script)
5. [Anticipated Questions & Winning Answers](#5-anticipated-questions--winning-answers)

---

## 1. 30-Second Elevator Pitch

> *"In modern microservices and distributed systems, syncing data between different databases (like SQL to NoSQL) is traditionally done via dual-writing in application code or scheduled batch queries. Both approaches suffer from data inconsistency, race conditions, and performance degradation.*
>
> *This project implements an event-driven **Change Data Capture (CDC)** pipeline. By listening directly to PostgreSQL's low-level **Write-Ahead Log (WAL)** via Debezium, every single committed transaction is converted into an immutable event in Apache Kafka. Kafka Connect then streams and transforms these events into MongoDB in near real-time with zero loss and zero application overhead."*

---

## 2. High-Level Architecture & Data Flow

```
┌─────────────────┐       ┌────────────────────────┐       ┌──────────────────────┐       ┌─────────────────────┐       ┌─────────────────┐
│   PostgreSQL    │ ────► │  Debezium PG Connector │ ────► │  Kafka Topic         │ ────► │  MongoDB Sink       │ ────► │    MongoDB      │
│  (Database:     │ (WAL) │  (pgoutput plugin)     │(JSON) │  (pg.public.         │       │  Connector          │       │  (Database:     │
│   demo)         │       │  inside Kafka Connect  │       │   customers)         │       │  (PostgresHandler)  │       │   demo)         │
└─────────────────┘       └────────────────────────┘       └──────────────────────┘       └─────────────────────┘       └─────────────────┘
```

1. **Commit:** Client inserts/updates/deletes a row in PostgreSQL `customers` table.
2. **Log Entry:** PostgreSQL writes the change to its Write-Ahead Log (WAL).
3. **Capture:** Debezium's `pg-source` connector reads the WAL stream using the native PostgreSQL `pgoutput` plugin.
4. **Publish:** Debezium serializes the change into a structured JSON event envelope and sends it to the Kafka topic `pg.public.customers`.
5. **Consume & Transform:** The `mongo-sink` connector consumes the topic, uses `PostgresHandler` to inspect the operation (`c`, `u`, `d`, `r`), and translates it into native MongoDB operations (`insertOne`, `updateOne`, `deleteOne`).
6. **Persistence:** The document is written or updated in MongoDB `customers` collection with matching IDs.

---

## 3. Deep-Dive: Every File Explained

---

### 1. `docker-compose.yml`
- **Location:** `docker-compose.yml`
- **What it does:** Orchestrates the entire multi-container infrastructure required for the CDC pipeline.

#### Key Sections & What They Mean:
- **`zookeeper`:** Coordinates broker state, leader elections, and topic metadata for the Kafka broker.
- **`kafka`:** The distributed event streaming platform (`quay.io/debezium/kafka:2.7`). Hosts the `pg.public.customers` topic and Connect storage topics.
- **`postgres`:** PostgreSQL 16 database (`quay.io/debezium/postgres:16`).
  - *Why this image?* It already has `wal_level=logical` pre-configured in its `postgresql.conf`, which is an absolute requirement for Debezium to read transactions.
  - Mounts `./postgres/init.sql` to `/docker-entrypoint-initdb.d/init.sql` so the table and initial data are auto-created at first boot.
  - Port is mapped to `5433:5432` to prevent conflicts if you have a local PostgreSQL installed on `5432`.
- **`mongo`:** MongoDB 7 container running on default port `27017`.
- **`connect`:** The Kafka Connect distributed worker runtime.
  - Uses `build: ./connect` to incorporate the custom MongoDB sink connector plugin.
  - `CONFIG_STORAGE_TOPIC`, `OFFSET_STORAGE_TOPIC`, `STATUS_STORAGE_TOPIC`: Stores connector configurations, consumer offsets, and status in Kafka topics for high availability.
  - `KAFKA_HEAP_OPTS: "-Xms512m -Xmx2g"`: Allocates enough JVM heap memory for large-scale replication.
- **`kafka-ui`:** Web-based UI (`provectuslabs/kafka-ui`) exposed on port `8080` to inspect topics, messages, consumer offsets, and Kafka Connect tasks visually.

> 🗣️ **Speaker Talking Point:**
> *"Notice that Kafka Connect depends on Kafka, Postgres, and Mongo. In addition, we map PostgreSQL to port 5433 to avoid local machine port collision while keeping internal container networking seamless."*

---

### 2. `connect/Dockerfile` & `connect/mongo-kafka-connect-1.13.0-all.jar`
- **Location:** `connect/Dockerfile`, `connect/mongo-kafka-connect-1.13.0-all.jar`
- **What it does:** Builds a customized Kafka Connect image.

#### Code Analysis:
```dockerfile
FROM quay.io/debezium/connect:2.7
USER root
RUN mkdir -p /kafka/connect/mongodb
COPY mongo-kafka-connect-1.13.0-all.jar /kafka/connect/mongodb/
RUN chown -R kafka:kafka /kafka/connect/mongodb
USER kafka
```
- The base image `quay.io/debezium/connect:2.7` already includes all Debezium source connectors (PostgreSQL, MySQL, SQL Server, Oracle).
- However, it does **not** include the MongoDB Sink Connector.
- This Dockerfile creates `/kafka/connect/mongodb` inside Kafka Connect's plugin path and copies the official `mongo-kafka-connect` JAR into it.
- Switches back to non-root `USER kafka` for security.

> 🗣️ **Speaker Talking Point:**
> *"Kafka Connect uses a plugin architecture. By dropping the MongoDB connector JAR into the plugin path inside this Dockerfile, our Kafka Connect worker is able to act both as a Debezium PostgreSQL Source and a MongoDB Sink simultaneously."*

---

### 3. `postgres/init.sql`
- **Location:** `postgres/init.sql`
- **What it does:** Pre-populates the source database schema and seeds initial data.

#### Code Analysis:
```sql
CREATE TABLE public.customers (
    id    SERIAL PRIMARY KEY,
    name  VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    city  VARCHAR(100)
);
ALTER TABLE public.customers REPLICA IDENTITY FULL;

INSERT INTO public.customers (name, email, city) VALUES
 ('Sokha',  'sokha@example.com',  'Phnom Penh'),
 ('Dara',   'dara@example.com',   'Siem Reap');
```

#### Why is `ALTER TABLE public.customers REPLICA IDENTITY FULL;` critical?
- By default in PostgreSQL, when an `UPDATE` or `DELETE` occurs, the Write-Ahead Log only records the **new** values and the primary key. It does not record the old values of non-key columns.
- Setting `REPLICA IDENTITY FULL` instructs PostgreSQL to log the entire previous state of the row (`before`) in the WAL.
- This allows Debezium to construct the complete `before` image in the event envelope, which downstream systems or handlers need to correctly handle updates and deletes.

> 🗣️ **Speaker Talking Point:**
> *"The single most important line in our SQL script is `ALTER TABLE ... REPLICA IDENTITY FULL`. Without this, Debezium cannot know what the row looked like before an update or delete."*

---

### 4. `connectors/pg-source.json`
- **Location:** `connectors/pg-source.json`
- **What it does:** Configuration definition for the Debezium PostgreSQL Source Connector.

#### Key Properties Explained:
```json
{
  "name": "pg-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "demo",
    "topic.prefix": "pg",
    "table.include.list": "public.customers",
    "tombstones.on.delete": "false",
    "max.batch.size": "8192",
    "max.queue.size": "32768",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```
- `"plugin.name": "pgoutput"`: Uses PostgreSQL's standard built-in logical decoding plugin (available since PG 10+). No external C-extensions like `decoderbufs` needed.
- `"topic.prefix": "pg"`: The topic name convention will be `<topic.prefix>.<schema>.<table>` -> `pg.public.customers`.
- `"table.include.list": "public.customers"`: Whitelists only the `customers` table to avoid polluting Kafka with system tables.
- `"tombstones.on.delete": "false"`: In Kafka, a delete usually creates a record with a null payload (tombstone) to help compaction. Setting this to `false` prevents sending null records that would crash or confuse the MongoDB sink connector.
- `"schemas.enable": "false"`: Strips out verbose Avro/JSON schema definitions from every message, significantly reducing message payload size and increasing throughput.
- `"max.batch.size": "8192"` & `"max.queue.size": "32768"`: High throughput buffers for bulk operations.

> 🗣️ **Speaker Talking Point:**
> *"We set `schemas.enable: false` to keep payloads lightweight, and `tombstones.on.delete: false` to ensure clean deletes without null-pointer exceptions at the sink."*

---

### 5. `connectors/mongo-sink.json`
- **Location:** `connectors/mongo-sink.json`
- **What it does:** Configuration definition for the MongoDB Sink Connector.

#### Key Properties Explained:
```json
{
  "name": "mongo-sink",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "topics": "pg.public.customers",
    "connection.uri": "mongodb://mongo:27017",
    "database": "demo",
    "collection": "customers",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "change.data.capture.handler": "com.mongodb.kafka.connect.sink.cdc.debezium.rdbms.postgres.PostgresHandler"
  }
}
```
- `"topics": "pg.public.customers"`: Consumes directly from the topic populated by `pg-source`.
- `"database": "demo"`, `"collection": "customers"`: Destination in MongoDB.
- `"change.data.capture.handler": "...PostgresHandler"`: **This is the core intelligence of the sink.**
  - Instead of dumping raw Debezium JSON wrappers (`{ before: ..., after: ..., op: ... }`) into MongoDB, `PostgresHandler` inspects the `op` code:
    - If `op = "c"` or `"r"`: Runs `insertOne` or `replaceOne` with the `after` object.
    - If `op = "u"`: Runs `updateOne` matching the `_id`.
    - If `op = "d"`: Runs `deleteOne` matching the `_id`.
  - MongoDB `_id` is automatically set to `{ "id": <primary_key> }`.

> 🗣️ **Speaker Talking Point:**
> *"Without the `PostgresHandler`, MongoDB would just store the raw JSON audit trail. With `PostgresHandler`, MongoDB reflects the true real-time state of the PostgreSQL table."*

---

### 6. `scripts/setup.sh`
- **Location:** `scripts/setup.sh`
- **What it does:** One-click script to spin up the cluster, wait for Kafka Connect to become healthy, and register both connectors.

#### Step-by-Step Flow:
1. `docker compose up -d --build`: Builds the custom Connect image and starts all 6 containers in the background.
2. `until curl -sf http://localhost:8083/connectors ...`: Polls the Kafka Connect REST API until it returns HTTP 200 (indicating plugins are indexed and ready).
3. Loops through `pg-source` and `mongo-sink`, sending `POST http://localhost:8083/connectors` with their JSON configurations.
4. Queries `http://localhost:8083/connectors?expand=status` and filters for connector and task `state: RUNNING`.

---

### 7. `scripts/demo.sh`
- **Location:** `scripts/demo.sh`
- **What it does:** Runs an interactive 4-step demonstration of real-time replication.

#### The 4 Steps in the Demo:
1. **Initial Snapshot (`op: r`):** Queries MongoDB to show that records `Sokha` and `Dara` from `init.sql` were automatically synced during the connector's initial snapshot.
2. **INSERT (`op: c`):** Inserts `Vathanak` (city: `Battambang`) into PostgreSQL. Queries MongoDB 3 seconds later: `Vathanak` appears!
3. **UPDATE (`op: u`):** Updates `Vathanak`'s city to `Kampot` in PostgreSQL. Queries MongoDB 3 seconds later: `city` is updated in MongoDB without changing the `_id`.
4. **DELETE (`op: d`):** Deletes `Vathanak` from PostgreSQL. Queries MongoDB 3 seconds later: the document is completely removed from MongoDB!

---

### 8. `scripts/test-1m.ps1` & `scripts/test-1m.sh`
- **Location:** `scripts/test-1m.ps1` (PowerShell) & `scripts/test-1m.sh` (Bash)
- **What it does:** Performance and stress testing benchmark that replicates **1,000,000 records**.

#### How it works:
1. Generates 1,000,000 rows in PostgreSQL using `generate_series(1, 1000000)`:
   ```sql
   INSERT INTO customers (name, email, city)
   SELECT 'Customer_' || i, 'customer_' || i || '@example.com',
          (ARRAY['Phnom Penh', 'Siem Reap', 'Battambang', 'Kampot', 'Kandal'])[1 + (i % 5)]
   FROM generate_series(1, 1000000) AS i;
   ```
2. Polls MongoDB via `db.customers.countDocuments()` every 3 seconds.
3. Calculates:
   - Percentage completed (`count / 1,000,000 * 100`)
   - Total replication duration in seconds
   - Average throughput in **records/second**

> 🗣️ **Speaker Talking Point:**
> *"This script proves our CDC pipeline is production-grade. We benchmark 1 million records to prove that Debezium and Kafka Connect can ingest and sink massive write spikes without crashing or losing data."*

---

### 9. `scripts/cleanup.sh`
- **Location:** `scripts/cleanup.sh`
- **What it does:** Runs `docker compose down -v`.
- Stops all running containers and removes all docker volumes (clearing PostgreSQL WAL, Kafka topics, and MongoDB data) for a fresh clean state.

---

### 10. `postman/CDC-Postgres-MongoDB.postman_collection.json`
- **Location:** `postman/CDC-Postgres-MongoDB.postman_collection.json`
- **What it does:** Complete Postman collection for managing and monitoring the Kafka Connect REST API (`http://localhost:8083`).

#### Included Requests:
- **1. Health & Plugins:**
  - `GET /`: Cluster version & commit hash.
  - `GET /connector-plugins`: Verifies `PostgresConnector` and `MongoSinkConnector` are discovered in the classpath.
- **2. Register Connectors:**
  - `POST /connectors`: Registers `pg-source` and `mongo-sink`.
- **3. Status & Monitoring:**
  - `GET /connectors`: Lists active connectors.
  - `GET /connectors?expand=status`: Shows status of connector and worker tasks (`RUNNING`, `PAUSED`, `FAILED`).
- **4. Lifecycle Management:**
  - `PUT /pause`, `PUT /resume`, `POST /restart`: Dynamically controls connector instances.
- **5. Teardown:**
  - `DELETE /connectors/{name}`: Unregisters connectors.

---

### 11. Documentation Files
- **`flow-diagram.md`:** Contains clear, rich Mermaid diagrams (Architecture diagram, Sequence diagram, Envelope transformation diagram, Operation code decision tree, Port table). Perfect to open in GitHub or VS Code Markdown preview during slides!
- **`how-test.md`:** Comprehensive test runbook with step-by-step commands for both Bash and Windows PowerShell, including troubleshooting guides.
- **`README.md`:** High-level project summary and quick-start reference.

---

## 4. Step-by-Step Live Demo Script

Here is an exact script you can follow during your live presentation:

### Phase 1: Show the Architecture (1-2 minutes)
1. Open [flow-diagram.md](file:///D:/ISTAD/microservices/CDC/postgres-to-mongodb/cdc-postgres-to-mongodb/flow-diagram.md) in Markdown Preview.
2. Walk the audience through the 5 components:
   - PostgreSQL (Source)
   - Debezium (WAL reader)
   - Kafka (Message broker)
   - Mongo Sink (`PostgresHandler`)
   - MongoDB (Target)

---

### Phase 2: Start the System & Verify Connectors (2 minutes)
1. In your terminal, run:
   ```bash
   ./scripts/setup.sh
   # Or in PowerShell:
   docker compose up -d --build
   ```
2. Open your browser and navigate to **Kafka UI**: `http://localhost:8080`
   - Show the **Connect** tab: demonstrate that both `pg-source` and `mongo-sink` are `RUNNING`.
   - Show the **Topics** tab: show `pg.public.customers`.

---

### Phase 3: Live Real-Time CDC Demo (3 minutes)
1. Run the automated demo script or execute commands manually:
   ```bash
   ./scripts/demo.sh
   ```
2. **Explain each step as it outputs:**
   - **Step 1 (Initial Snapshot):** Show that `Sokha` and `Dara` are already in MongoDB.
   - **Step 2 (INSERT):** Insert a new record into PostgreSQL (`Vathanak`). Show MongoDB instantly reflecting the new document.
   - **Step 3 (UPDATE):** Update `Vathanak`'s city to `Kampot`. Show MongoDB updating the document in-place.
   - **Step 4 (DELETE):** Delete `Vathanak` from PostgreSQL. Show MongoDB removing the document immediately.
3. Switch to **Kafka UI** (`http://localhost:8080/ui/clusters/local/all-topics/pg.public.customers/messages`):
   - Click on the latest message.
   - Show the audience the JSON structure:
     - `"op": "u"`
     - `"before": { "city": "Battambang" }`
     - `"after": { "city": "Kampot" }`

---

### Phase 4: Stress Test / 1 Million Records (Optional / Wow Factor)
1. Run the benchmark:
   ```powershell
   # In PowerShell:
   .\scripts\test-1m.ps1
   # Or in Bash:
   ./scripts/test-1m.sh
   ```
2. Show the real-time progress counter replicating thousands of records per second.

---

### Phase 5: Clean Up
1. Tear down the environment cleanly:
   ```bash
   ./scripts/cleanup.sh
   ```

---

## 5. Anticipated Questions & Winning Answers

| Question | Winning Answer |
| :--- | :--- |
| **Q1: Why use CDC instead of writing to both databases in our application code?** | *"Dual writing is not transactional across two different database systems (e.g. Postgres and Mongo). If the app writes to Postgres and fails before writing to Mongo, your data is out of sync. With CDC, PostgreSQL's WAL is the single source of truth. As long as Postgres commits, Kafka guarantees delivery to MongoDB."* |
| **Q2: Does Debezium slow down PostgreSQL?** | *"No. Debezium reads the WAL sequentially via the native `pgoutput` logical decoding plugin. It does not issue `SELECT` queries or lock tables, meaning negligible impact on database performance."* |
| **Q3: What happens if MongoDB goes down?** | *"Because Kafka acts as a durable buffer, Kafka Connect retains the consumer offsets. While MongoDB is down, events accumulate safely in Kafka. As soon as MongoDB recovers, the sink connector resumes exactly where it left off with zero data loss."* |
| **Q4: Why do we need `REPLICA IDENTITY FULL`?** | *"By default, PostgreSQL WAL only logs primary keys for deleted or updated rows. `REPLICA IDENTITY FULL` forces Postgres to write the entire old row state into the WAL, enabling Debezium to provide the complete `before` record in the CDC envelope."* |
| **Q5: How does MongoDB know which document to update or delete?** | *"The `PostgresHandler` uses the primary key emitted in the Debezium event key to construct MongoDB's `_id: { id: <pk> }`. It then executes targeted `updateOne` and `deleteOne` operations matching that `_id`."* |
