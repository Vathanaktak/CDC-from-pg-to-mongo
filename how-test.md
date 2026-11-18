# Testing Guide: PostgreSQL to MongoDB CDC Pipeline

This guide explains how to test the Change Data Capture (CDC) pipeline end-to-end, both via automated scripts and manual step-by-step commands (supports Bash and Windows PowerShell).

---

## 1. Prerequisites

Before testing, verify the following:
- **Docker & Docker Compose** are installed and running.
- The following host ports are free:
  - `5432` (PostgreSQL)
  - `27017` (MongoDB)
  - `9092` (Kafka)
  - `2181` (ZooKeeper)
  - `8083` (Kafka Connect)
  - `8080` (Kafka UI)

---

## Method A: Automated Testing (Git Bash / WSL / Linux / macOS)

If you are using **Git Bash**, **WSL**, or **Linux/macOS**, open your terminal in this repository folder:
```bash
# In Git Bash on Windows:
cd /d/ISTAD/microservices/CDC/postgres-to-mongodb/cdc-postgres-to-mongodb
```

> [!TIP]
> **Git Bash on Windows Tips:**
> - When running interactive shells (like `psql` or `mongosh`), prepend `winpty` (e.g. `winpty docker compose exec postgres psql -U postgres -d demo`).
> - For commands with absolute Linux paths (like `/kafka/bin/...`), prepend `MSYS_NO_PATHCONV=1` so Git Bash doesn't alter the path into Windows format.
> - When passing commands to `docker compose exec`, adding the `-T` flag avoids TTY errors in Git Bash (e.g. `docker compose exec -T postgres ...`).

### Step 1: Start the cluster & register connectors
```bash
./scripts/setup.sh
```
This builds the custom Kafka Connect image, boots all containers, waits for port 8083 to be ready, and registers both `pg-source` and `mongo-sink`.

### Step 2: Run the test sequence (INSERT, UPDATE, DELETE)
```bash
./scripts/demo.sh
```
This script executes:
1. Shows initial snapshot in MongoDB (records inserted by `init.sql`).
2. Inserts a new customer (`Vathanak`) into PostgreSQL and queries MongoDB.
3. Updates `city` to `Kampot` in PostgreSQL and queries MongoDB.
4. Deletes `Vathanak` from PostgreSQL and verifies removal in MongoDB.

### Step 3: Tear down after testing
```bash
./scripts/cleanup.sh
```

---

## Method B: Manual Testing Step-by-Step (PowerShell or Bash)

If you are using **Windows PowerShell** or prefer manual verification, follow these steps.

### Step 1: Start the Containers

```powershell
docker compose up -d --build
```

Wait ~15–30 seconds for Kafka Connect to finish initializing. You can verify it is ready with:

```powershell
# In PowerShell:
Invoke-RestMethod http://localhost:8083/connectors

# In Bash / curl:
curl -s http://localhost:8083/connectors
```
When it returns `[]` or a list of connectors, Kafka Connect is ready.

---

### Step 2: Register the Connectors

#### 1. Register PostgreSQL Source Connector:
```powershell
# PowerShell:
Invoke-RestMethod -Uri http://localhost:8083/connectors -Method Post -InFile ./connectors/pg-source.json -ContentType "application/json"

# Bash / curl:
curl -i -X POST -H "Content-Type: application/json" --data @connectors/pg-source.json http://localhost:8083/connectors
```

#### 2. Register MongoDB Sink Connector:
```powershell
# PowerShell:
Invoke-RestMethod -Uri http://localhost:8083/connectors -Method Post -InFile ./connectors/mongo-sink.json -ContentType "application/json"

# Bash / curl:
curl -i -X POST -H "Content-Type: application/json" --data @connectors/mongo-sink.json http://localhost:8083/connectors
```

#### 3. Check Connector Status:
Verify that both connectors and their tasks are in `RUNNING` state:

```powershell
# PowerShell:
(Invoke-RestMethod http://localhost:8083/connectors/pg-source/status).connector.state
(Invoke-RestMethod http://localhost:8083/connectors/mongo-sink/status).connector.state

# Bash / curl:
curl -s http://localhost:8083/connectors?expand=status
```

---

### Step 3: Verify Initial Snapshot in MongoDB

PostgreSQL was initialized with 2 rows (`Sokha` and `Dara`) in [init.sql](file:///D:/ISTAD/microservices/CDC/postgres-to-mongodb/cdc-postgres-to-mongodb/postgres/init.sql). Check if MongoDB has already synced them:

```bash
docker compose exec mongo mongosh demo --eval "db.customers.find()"
```

**Expected output:**
```json
[
  { "_id": { "id": 1 }, "name": "Sokha", "email": "sokha@example.com", "city": "Phnom Penh" },
  { "_id": { "id": 2 }, "name": "Dara", "email": "dara@example.com", "city": "Siem Reap" }
]
```

---

### Step 4: Test INSERT Operation

Insert a new customer in PostgreSQL:

```bash
docker compose exec postgres psql -U postgres -d demo -c "INSERT INTO customers (name, email, city) VALUES ('Vathanak', 'vathanak@example.com', 'Battambang');"
```

Verify in MongoDB (wait 1–2 seconds):

```bash
docker compose exec mongo mongosh demo --eval "db.customers.find({ name: 'Vathanak' })"
```

**Expected output:**
```json
[
  {
    "_id": { "id": 3 },
    "name": "Vathanak",
    "email": "vathanak@example.com",
    "city": "Battambang"
  }
]
```

---

### Step 5: Test UPDATE Operation

Update the city of the newly created customer in PostgreSQL:

```bash
docker compose exec postgres psql -U postgres -d demo -c "UPDATE customers SET city = 'Kampot' WHERE name = 'Vathanak';"
```

Verify in MongoDB:

```bash
docker compose exec mongo mongosh demo --eval "db.customers.find({ name: 'Vathanak' })"
```

**Expected output:**
The city field should now be updated to `"Kampot"`.

---

### Step 6: Test DELETE Operation

Delete the customer in PostgreSQL:

```bash
docker compose exec postgres psql -U postgres -d demo -c "DELETE FROM customers WHERE name = 'Vathanak';"
```

Verify in MongoDB:

```bash
docker compose exec mongo mongosh demo --eval "db.customers.find({ name: 'Vathanak' })"
```

**Expected output:**
No records returned (the document is deleted from MongoDB).

---

## Method C: Testing via Postman (Kafka Connect REST API)

In this project, the HTTP API that is tested with Postman is the **Kafka Connect REST API** on port `8083`. A ready-to-import Postman collection is included in [postman/CDC-Postgres-MongoDB.postman_collection.json](file:///D:/ISTAD/microservices/CDC/postgres-to-mongodb/cdc-postgres-to-mongodb/postman/CDC-Postgres-MongoDB.postman_collection.json).

### Step 1: Import the Collection
1. Open **Postman**.
2. Click **Import** (top-left).
3. Select or drag-and-drop [CDC-Postgres-MongoDB.postman_collection.json](file:///D:/ISTAD/microservices/CDC/postgres-to-mongodb/cdc-postgres-to-mongodb/postman/CDC-Postgres-MongoDB.postman_collection.json).

### Step 2: Test Endpoints in Order
1. **Check Plugins**: `GET http://localhost:8083/connector-plugins`
   - Verifies that `io.debezium.connector.postgresql.PostgresConnector` and `com.mongodb.kafka.connect.MongoSinkConnector` are available.
2. **Register pg-source**: `POST http://localhost:8083/connectors`
   - Body: Raw JSON from `connectors/pg-source.json`.
   - Expected status: `201 Created`.
3. **Register mongo-sink**: `POST http://localhost:8083/connectors`
   - Body: Raw JSON from `connectors/mongo-sink.json`.
   - Expected status: `201 Created`.
4. **Verify Health**:
   - `GET http://localhost:8083/connectors/pg-source/status` (Check state is `RUNNING` for connector and task `0`).
   - `GET http://localhost:8083/connectors/mongo-sink/status` (Check state is `RUNNING` for connector and task `0`).

---

## 3. Deep-Dive: Inspect Kafka CDC Messages

You can inspect the raw Debezium CDC messages passing through Kafka in two ways:

### Option 1: Via Kafka UI (Browser)
Open **http://localhost:8080** in your web browser:
1. Navigate to **Topics** -> **`pg.public.customers`** -> **Messages**.
2. Navigate to **Kafka Connect** -> **`connect`** to see both `pg-source` and `mongo-sink` connectors and their task health.

### Option 2: Via Terminal CLI
```bash
# In Git Bash:
MSYS_NO_PATHCONV=1 docker compose exec -T kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic pg.public.customers \
  --from-beginning

# In PowerShell / Linux Bash:
docker compose exec kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic pg.public.customers \
  --from-beginning
```

Each event message in Kafka will look like this:
```json
{
  "before": null,
  "after": { "id": 3, "name": "Vathanak", "email": "vathanak@example.com", "city": "Battambang" },
  "source": { "version": "2.7.0.Final", "connector": "postgresql", "db": "demo", "table": "customers", ... },
  "op": "c",
  "ts_ms": 1700000000000
}
```
- `op: "r"` = Snapshot Read
- `op: "c"` = Create / Insert
- `op: "u"` = Update
- `op: "d"` = Delete

---

## 4. Benchmark: Testing 1 Million Records (1,000,000 Rows)

You can run a high-volume performance test to replicate 1,000,000 records end-to-end from PostgreSQL to MongoDB:

### Run via Benchmark Scripts:
* **In Windows PowerShell:**
  ```powershell
  .\scripts\test-1m.ps1
  ```
* **In Git Bash / Linux:**
  ```bash
  ./scripts/test-1m.sh
  ```

### Or Run Manually via SQL:
1. Insert 1,000,000 rows in PostgreSQL using `generate_series`:
   ```sql
   INSERT INTO customers (name, email, city)
   SELECT 
       'Customer_' || i,
       'customer_' || i || '@example.com',
       (ARRAY['Phnom Penh', 'Siem Reap', 'Battambang', 'Kampot', 'Kandal'])[1 + (i % 5)]
   FROM generate_series(1, 1000000) AS i;
   ```
2. Monitor MongoDB document count:
   ```bash
   docker compose exec mongo mongosh demo --eval "db.customers.countDocuments()"
   ```
3. Monitor consumer group lag in **Kafka UI**:
   Open **http://localhost:8080** -> **Consumers** -> `connect-mongo-sink` to observe real-time message consumption throughput.

---

## 5. Troubleshooting & Helpful Commands

| Task | Command |
| :--- | :--- |
| **View Connect logs** | `docker compose logs -f connect` |
| **View Postgres logs** | `docker compose logs -f postgres` |
| **Check registered connectors** | `curl -s localhost:8083/connectors` |
| **Delete connector `pg-source`** | `curl -X DELETE localhost:8083/connectors/pg-source` |
| **Delete connector `mongo-sink`** | `curl -X DELETE localhost:8083/connectors/mongo-sink` |
| **Restart a failed connector task** | `curl -X POST localhost:8083/connectors/pg-source/tasks/0/restart` |
| **Interactive PostgreSQL shell (Git Bash)** | `winpty docker compose exec postgres psql -U postgres -d demo` |
| **Interactive MongoDB shell (Git Bash)** | `winpty docker compose exec mongo mongosh demo` |

---

## 6. Cleanup

When done testing, destroy the containers and remove attached data volumes:

```bash
docker compose down -v
```
