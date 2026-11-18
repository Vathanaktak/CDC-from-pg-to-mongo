# Architecture & Data Flow Diagrams

This document visually explains how data flows in real-time from **PostgreSQL** to **MongoDB** using **Debezium**, **Apache Kafka**, and **Kafka Connect**.

---

## 1. High-Level Architecture Diagram

```mermaid
flowchart LR
    subgraph SourceSystem["Source Database"]
        PG[("PostgreSQL 16\n(Database: demo)")]
        WAL["Write-Ahead Log\n(wal_level=logical)"]
        PG -->|"Writes changes"| WAL
    end

    subgraph KafkaConnectCluster["Kafka Connect Runtime (:8083)"]
        DebeziumSrc["Debezium PG Source Connector\n(pg-source)"]
        MongoSink["MongoDB Sink Connector\n(mongo-sink / PostgresHandler)"]
    end

    subgraph KafkaBroker["Kafka Cluster"]
        ZK["ZooKeeper (:2181)"]
        KTopic{{"Kafka Topic:\npg.public.customers (:9092)"}}
        ZK --- KTopic
    end

    subgraph TargetSystem["Target Database"]
        Mongo[("MongoDB 7\n(Database: demo\nCollection: customers)")]
    end

    WAL -->|"1. Reads WAL stream via pgoutput"| DebeziumSrc
    DebeziumSrc -->|"2. Publishes JSON event"| KTopic
    KTopic -->|"3. Consumes event stream"| MongoSink
    MongoSink -->|"4. Executes insert / update / delete"| Mongo
```

---

## 2. End-to-End Sequence Diagram

This sequence diagram illustrates what happens when an application or user performs an `INSERT`, `UPDATE`, or `DELETE` in PostgreSQL:

```mermaid
sequenceDiagram
    autonumber
    actor Client as User / App
    participant PG as PostgreSQL
    participant WAL as PG Write-Ahead Log
    participant Connect as Debezium Source Connector
    participant Kafka as Kafka Topic (pg.public.customers)
    participant Sink as MongoDB Sink Connector
    participant Mongo as MongoDB

    Client->>PG: INSERT / UPDATE / DELETE row
    PG->>WAL: Commit transaction to WAL
    PG-->>Client: Success response (Row updated)

    Note over WAL,Connect: Asynchronous Change Data Capture
    Connect->>WAL: Read new WAL log entry (pgoutput plugin)
    Connect->>Connect: Wrap row into Debezium CDC Envelope (JSON)
    Connect->>Kafka: Publish event message to topic

    Note over Kafka,Sink: Event Streaming
    Kafka->>Sink: Consume event message
    Sink->>Sink: Parse 'op' code (c = insert, u = update, d = delete)
    Sink->>Mongo: Execute MongoDB command (insertOne / updateOne / deleteOne)
    Mongo-->>Sink: Operation confirmed
```

---

## 3. Data Transformation at Each Stage

This diagram shows how a row changes structure as it travels from PostgreSQL to MongoDB:

```mermaid
flowchart TD
    subgraph Step1["1. PostgreSQL Row"]
        R1["Table: public.customers\n----------------------------\nid: 3\nname: Vathanak\nemail: vathanak@example.com\ncity: Kampot"]
    end

    subgraph Step2["2. Debezium CDC JSON Envelope in Kafka"]
        R2["Topic: pg.public.customers\n----------------------------\n{\n  'before': { 'id': 3, 'city': 'Battambang', ... },\n  'after':  { 'id': 3, 'name': 'Vathanak', 'city': 'Kampot', ... },\n  'op': 'u',\n  'source': { 'connector': 'postgresql', 'db': 'demo', ... }\n}"]
    end

    subgraph Step3["3. MongoDB Document"]
        R3["Collection: demo.customers\n----------------------------\n{\n  '_id': { 'id': 3 },\n  'name': 'Vathanak',\n  'email': 'vathanak@example.com',\n  'city': 'Kampot'\n}"]
    end

    Step1 -->|"Debezium encodes row changes"| Step2
    Step2 -->|"PostgresHandler translates envelope"| Step3
```

---

## 4. Operation Code (`op`) Decision Tree

When the **MongoDB Sink Connector** receives an event from Kafka, the built-in `PostgresHandler` decides what to do based on the `op` field:

```mermaid
flowchart TD
    Event["Debezium Event from Kafka"] --> CheckOp{"What is 'op'?"}

    CheckOp -->|"op = 'r' (Initial Snapshot)"| OpR["Create Document\n(db.customers.insertOne / replaceOne)"]
    CheckOp -->|"op = 'c' (SQL INSERT)"| OpC["Insert Document\n(db.customers.insertOne)"]
    CheckOp -->|"op = 'u' (SQL UPDATE)"| OpU["Update Document\n(db.customers.updateOne matching _id)"]
    CheckOp -->|"op = 'd' (SQL DELETE)"| OpD["Remove Document\n(db.customers.deleteOne matching _id)"]

    OpR --> MongoTarget[("MongoDB customers Collection")]
    OpC --> MongoTarget
    OpU --> MongoTarget
    OpD --> MongoTarget
```

---

## 5. Summary of Components & Ports

| Component | Container Name | Port | Description |
| :--- | :--- | :--- | :--- |
| **PostgreSQL** | `cdc-postgres-to-mongodb-postgres-1` | `5432` | Source database holding the `customers` table with `REPLICA IDENTITY FULL`. |
| **ZooKeeper** | `cdc-postgres-to-mongodb-zookeeper-1` | `2181` | Coordinates the Kafka broker cluster state. |
| **Kafka** | `cdc-postgres-to-mongodb-kafka-1` | `9092` | Durable distributed event log hosting the `pg.public.customers` topic. |
| **Kafka Connect** | `cdc-postgres-to-mongodb-connect-1` | `8083` | JVM runtime running Debezium PG Source & MongoDB Sink plugins. |
| **Kafka UI** | `cdc-postgres-to-mongodb-kafka-ui-1` | `8080` | Web UI for inspecting Kafka topics, CDC messages, and Kafka Connect tasks. |
| **MongoDB** | `cdc-postgres-to-mongodb-mongo-1` | `27017` | Target document database storing the replicated `customers` documents. |
