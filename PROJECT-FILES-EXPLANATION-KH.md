# ការណែនាំពន្យល់រាល់ File ទាំងអស់ និងវិធី Demo គម្រោង CDC (ភាសាខ្មែរ)

> **គោលបំណងនៃគម្រោង:** បង្កើតប្រព័ន្ធ **Change Data Capture (CDC)** ដើម្បីបញ្ជូនទិន្នន័យដែលប្រែប្រួល (`INSERT`, `UPDATE`, `DELETE`) ពី **PostgreSQL** ទៅកាន់ **MongoDB** ភ្លាមៗក្នុងពេលជាក់ស្តែង (Real-time) ដោយប្រើប្រាស់ **Debezium**, **Apache Kafka**, និង **Kafka Connect**។

---

## 📋 មាតិកា (Table of Contents)
1. [សេចក្តីសង្ខេបសម្រាប់ធ្វើបទបង្ហាញ (30-Second Elevator Pitch)](#1-សេចក្តីសង្ខេបសម្រាប់ធ្វើបទបង្ហាញ-30-second-elevator-pitch)
2. [ស្ថាបត្យកម្មប្រព័ន្ធ និងដំណើរការទិន្នន័យ (Architecture & Data Flow)](#2-ស្ថាបត្យកម្មប្រព័ន្ធ-និងដំណើរការទិន្នន័យ)
3. [ការពន្យល់លម្អិតលើគ្រប់ File ទាំងអស់ (Deep-Dive Every File)](#3-ការពន្យល់លម្អិតលើគ្រប់-file-ទាំងអស់)
   - [docker-compose.yml](#1-docker-composeyml)
   - [connect/Dockerfile & JAR Plugin](#2-connectdockerfile--mongo-kafka-connect-jar)
   - [postgres/init.sql](#3-postgresinitsql)
   - [connectors/pg-source.json](#4-connectorspg-sourcejson)
   - [connectors/mongo-sink.json](#5-connectorsmongo-sinkjson)
   - [scripts/setup.sh](#6-scriptssetupsh)
   - [scripts/demo.sh](#7-scriptsdemosh)
   - [scripts/test-1m.ps1 & test-1m.sh](#8-scriptstest-1mps1--test-1msh-benchmark-1-លានទិន្នន័យ)
   - [scripts/cleanup.sh](#9-scriptscleanupsh)
   - [postman/CDC-Postgres-MongoDB.postman_collection.json](#10-postman-collection)
   - [flow-diagram.md, how-test.md & README.md](#11-ឯកសារជំនួយ-documentation)
4. [លំដាប់លំដោយនៃការធ្វើ Demo ផ្ទាល់ (Live Demo Script)](#4-លំដាប់លំដោយនៃការធ្វើ-demo-ផ្ទាល់-live-demo-script)
5. [សំណួរដែលអាចនឹងត្រូវបានសួរ និងចម្លើយគន្លឹះ (Q&A)](#5-សំណួរដែលអាចនឹងត្រូវបានសួរ-និងចម្លើយគន្លឹះ-qa)

---

## 1. សេចក្តីសង្ខេបសម្រាប់ធ្វើបទបង្ហាញ (30-Second Elevator Pitch)

> *"នៅក្នុងប្រព័ន្ធ Microservices ឬ Distributed Systems បច្ចុប្បន្ន ការធ្វើសមកាលកម្មទិន្នន័យ (Sync Data) រវាង SQL និង NoSQL ច្រើនតែធ្វើឡើងដោយការសរសេរកូដ Dual-writing (សរសេរចូល Postgres ផង Mongo ផងនៅក្នុង App) ឬប្រើ Cron job ទាញទិន្នន័យជាកញ្ចប់ (Batch polling)។ វិធីទាំងនេះងាយនឹងជួបបញ្ហាទិន្នន័យមិនត្រូវគ្នា (Data Inconsistency), បាត់បង់ទិន្នន័យពេល Network ដាច់, និងធ្វើឱ្យ Database ដើរយឺត។*
>
> *គម្រោងរបស់យើងនេះប្រើប្រាស់វិធីសាស្ត្រ **Change Data Capture (CDC)**។ យើងចាប់យកប្រតិបត្តិការដោយផ្ទាល់ចេញពី **Write-Ahead Log (WAL)** របស់ PostgreSQL តាមរយៈ **Debezium** ហើយបញ្ជូនជា Event ចូលទៅកាន់ **Apache Kafka**។ បន្ទាប់មក **Kafka Connect** នឹងបម្លែង និងសរសេរទិន្នន័យចូលទៅ **MongoDB** ភ្លាមៗ (Real-time) ដោយមិនប៉ះពាល់ដល់ល្បឿនរបស់ Database និងមិនចាំបាច់សរសេរកូដ Sync ក្នុង Application ឡើយ។"*

---

## 2. ស្ថាបត្យកម្មប្រព័ន្ធ និងដំណើរការទិន្នន័យ

```
┌──────────────────┐       ┌────────────────────────┐       ┌──────────────────────┐       ┌─────────────────────┐       ┌──────────────────┐
│   PostgreSQL     │ ────► │  Debezium PG Connector │ ────► │  Kafka Topic         │ ────► │  MongoDB Sink       │ ────► │    MongoDB       │
│  (Database:      │ (WAL) │  (pgoutput plugin)     │(JSON) │  (pg.public.         │       │  Connector          │       │  (Database:      │
│   demo)          │       │  នៅក្នុង Kafka Connect │       │   customers)         │       │  (PostgresHandler)  │       │   demo)          │
└──────────────────┘       └────────────────────────┘       └──────────────────────┘       └─────────────────────┘       └──────────────────┘
```

1. **Transaction Commit:** កម្មវិធី ឬ User ធ្វើការ `INSERT`, `UPDATE`, ឬ `DELETE` លើតារាង `customers` ក្នុង PostgreSQL។
2. **Log Entry:** PostgreSQL កត់ត្រាប្រតិបត្តិការនោះចូលទៅក្នុង Write-Ahead Log (WAL)។
3. **Capture:** Debezium Source Connector អាន WAL នោះតាមរយៈ PostgreSQL `pgoutput` plugin។
4. **Publish Event:** Debezium រៀបចំទិន្នន័យជាទម្រង់ JSON Envelope (មាន `before`, `after`, `op`) រួចបាញ់ទៅកាន់ Kafka Topic ឈ្មោះ `pg.public.customers`។
5. **Consume & Transform:** MongoDB Sink Connector ទាញយក Event ពី Kafka ហើយប្រើ `PostgresHandler` ដើម្បីពិនិត្យមើលកូដ `op` (`c`=បង្កើត, `u`=កែប្រែ, `d`=លុប) រួចបម្លែងទៅជាបញ្ជា MongoDB (`insertOne`, `updateOne`, `deleteOne`)។
6. **Persist:** ទិន្នន័យត្រូវបង្កើត កែប្រែ ឬលុបក្នុង Collection `customers` របស់ MongoDB ភ្លាមៗដោយមាន ID ដូចគ្នា។

---

## 3. ការពន្យល់លម្អិតលើគ្រប់ File ទាំងអស់

---

### 1. `docker-compose.yml`
- **ទីតាំង:** `docker-compose.yml`
- **តួនាទី:** កំណត់រចនាសម្ព័ន្ធ និងដំណើរការ Containers ទាំង ៦ នៃប្រព័ន្ធទាំងមូលដោយស្វ័យប្រវត្តិតាមរយៈ Docker Compose។

#### ការពន្យល់លើ Services នីមួយៗ:
- **`zookeeper`:** គ្រប់គ្រងស្ថានភាព Cluster, ការបោះឆ្នោត Leader, និង Metadata របស់ Kafka Broker (Port `2181`)។
- **`kafka`:** Message Broker កណ្តាលសម្រាប់ផ្ទុក Topic `pg.public.customers` និង Topic របស់ Kafka Connect (Port `9092`)។
- **`postgres`:** Database មេ (PostgreSQL 16) ប្រើ Image `quay.io/debezium/postgres:16`។
  - *មូលហេតុដែលប្រើ Image នេះ:* វាបានកំណត់ `wal_level=logical` រួចជាស្រេច ដែលជាលក្ខខណ្ឌចាំបាច់បំផុតសម្រាប់ Debezium អាន WAL។
  - ភ្ជាប់ File `./postgres/init.sql` ចូលទៅ `/docker-entrypoint-initdb.d/init.sql` ដើម្បីបង្កើត Table និងទិន្នន័យគំរូពេលចាប់ផ្តើមដំបូង។
  - ប្រើ Port `5433:5432` លើ Host ដើម្បីកុំឱ្យជាន់គ្នាជាមួយ Postgres ដែលអាចមានស្រាប់លើកុំព្យូទ័រ (Port 5432)។
- **`mongo`:** Database គោលដៅ (MongoDB 7) ដំណើរការលើ Port `27017`។
- **`connect`:** ម៉ាស៊ីនដំណើរការ Kafka Connect (JVM Runtime)។
  - ប្រើ `build: ./connect` ដើម្បី Build រួមបញ្ចូល MongoDB Sink Plugin JAR។
  - កំណត់ Topic សម្រាប់រក្សាទុក Config, Offsets, និង Status របស់ Connectors ក្នុង Kafka (`connect_configs`, `connect_offsets`, `connect_statuses`)។
  - កំណត់ទំហំ Memory `KAFKA_HEAP_OPTS: "-Xms512m -Xmx2g"` ដើម្បីឱ្យវាអាចទ្រទ្រង់ការ Sync ទិន្នន័យរាប់លាន Record ដោយមិនអស់ Memory។
- **`kafka-ui`:** Web Dashboard (Port `8080`) សម្រាប់មើល Topics, Messages, Consumer Groups, និង Connector Status លើ Browser យ៉ាងងាយស្រួល។

> 🗣️ **ចំណុចត្រូវនិយាយពេល Demo:**
> *"នៅក្នុង docker-compose.yml យើងរៀបចំ Service ទាំង ៦ ឱ្យមានទំនាក់ទំនងគ្នាយ៉ាងរលូន។ យើងប្រើ Debezium Postgres image ដែលមាន wal_level=logical រួចជាស្រេច និង Kafka Connect ដែលផ្ទុក Plugin ទាំងពីរ។"*

---

### 2. `connect/Dockerfile` & `connect/mongo-kafka-connect-1.13.0-all.jar`
- **ទីតាំង:** `connect/Dockerfile`, `connect/mongo-kafka-connect-1.13.0-all.jar`
- **តួនាទី:** បង្កើត Custom Kafka Connect Image ដោយបន្ថែម MongoDB Connector Plugin។

#### ការបកស្រាយកូដ:
```dockerfile
FROM quay.io/debezium/connect:2.7
USER root
RUN mkdir -p /kafka/connect/mongodb
COPY mongo-kafka-connect-1.13.0-all.jar /kafka/connect/mongodb/
RUN chown -R kafka:kafka /kafka/connect/mongodb
USER kafka
```
- Image ដើម `quay.io/debezium/connect:2.7` មានស្រាប់តែ Source Connectors (ដូចជា Postgres, MySQL, Oracle) តែ**គ្មាន** MongoDB Sink Connector ឡើយ។
- Dockerfile នេះបង្កើត Folder `/kafka/connect/mongodb` នៅក្នុង Plugin Path របស់ Kafka Connect រួចចម្លង File JAR `mongo-kafka-connect-1.13.0-all.jar` ចូល។
- បន្ទាប់មកប្តូរមកប្រើ `USER kafka` ធម្មតាដើម្បីធានាសុវត្ថិភាព។

> 🗣️ **ចំណុចត្រូវនិយាយពេល Demo:**
> *"Kafka Connect ដំណើរការតាមបែប Plugin Architecture។ ដោយគ្រាន់តែដាក់ File JAR របស់ MongoDB ចូលទៅក្នុង Plugin Directory នោះ Kafka Connect របស់យើងអាចដើរតួជាទាំង Source ទាញចេញពី Postgres ផង និងជា Sink បញ្ចូលទៅ Mongo ផងក្នុង Container តែមួយ។"*

---

### 3. `postgres/init.sql`
- **ទីតាំង:** `postgres/init.sql`
- **តួនាទី:** បង្កើតតារាង `customers`, កំណត់ Replica Identity, និងបញ្ចូលទិន្នន័យដំបូង។

#### ការបកស្រាយកូដ:
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

#### ហេតុអ្វីត្រូវមាន `REPLICA IDENTITY FULL`?
- តាមលំនាំដើមរបស់ PostgreSQL ពេលមានការ `UPDATE` ឬ `DELETE` វាកត់ត្រាចូល WAL តែតម្លៃ Primary Key ប៉ុណ្ណោះ វាមិនកត់ត្រាតម្លៃចាស់របស់ Column ផ្សេងទៀតទេ។
- ពាក្យបញ្ជា `REPLICA IDENTITY FULL` បង្ខំឱ្យ PostgreSQL កត់ត្រា**តម្លៃចាស់ទាំងស្រុង** (`before` image) ចូលទៅក្នុង WAL។
- ការធ្វើបែបនេះទើប Debezium អាចដឹងថា Record មុនពេល Update ឬ Delete មានទិន្នន័យអ្វីខ្លះ ហើយអាចផ្ញើ Envelope ពេញលេញទៅកាន់ Sink បានត្រឹមត្រូវ។

> 🗣️ **ចំណុចត្រូវនិយាយពេល Demo:**
> *"បន្ទាត់កូដដ៏សំខាន់បំផុតក្នុង SQL នេះគឺ `ALTER TABLE ... REPLICA IDENTITY FULL`។ បើគ្មានបន្ទាត់នេះទេ Debezium មិនអាចដឹងពីទិន្នន័យចាស់ (before state) ពេលយើងធ្វើ UPDATE ឬ DELETE ឡើយ។"*

---

### 4. `connectors/pg-source.json`
- **ទីតាំង:** `connectors/pg-source.json`
- **តួនាទី:** កំណត់ Configuration សម្រាប់ចុះឈ្មោះ Debezium PostgreSQL Source Connector ជាមួយ Kafka Connect REST API។

#### ការពន្យល់ Config សំខាន់ៗ:
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
- `"plugin.name": "pgoutput"`: ប្រើ Logical Decoding Plugin ផ្លូវការដែលមានស្រាប់នៅក្នុង PostgreSQL 10+ ដោយមិនចាំបាច់ដំឡើង C-extension បន្ថែម។
- `"topic.prefix": "pg"`: បង្កើតឈ្មោះ Topic តាមទម្រង់ `<topic.prefix>.<schema>.<table>` ពោលគឺ `pg.public.customers`។
- `"table.include.list": "public.customers"`: ចាប់យកតែតារាង `customers` ជៀសវាងការចាប់តារាង System ផ្សេងៗ។
- `"tombstones.on.delete": "false"`: ក្នុង Kafka ពេលមានការ Delete វាតែងតែបង្កើត Message ទទេ (null payload tombstone)។ ការដាក់ `false` ជួយការពារកុំឱ្យផ្ញើសារ Null ដែលនាំឱ្យ MongoDB Sink គាំង។
- `"schemas.enable": "false"`: កាត់បន្ថយ Schema Metadata វែងអន្លាយចេញពី Message ដើម្បីឱ្យ Payload តូច និងបញ្ជូនបានលឿនបំផុត។
- `"max.batch.size": "8192"` & `"max.queue.size": "32768"`: បង្កើនទំហំ Buffer ដើម្បីទ្រទ្រង់ការ Sync ទិន្នន័យកម្រិតខ្ពស់។

---

### 5. `connectors/mongo-sink.json`
- **ទីតាំង:** `connectors/mongo-sink.json`
- **តួនាទី:** កំណត់ Configuration សម្រាប់ចុះឈ្មោះ MongoDB Sink Connector។

#### ការពន្យល់ Config សំខាន់ៗ:
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
- `"topics": "pg.public.customers"`: ចង្អុលឱ្យទាញទិន្នន័យពី Topic ដែល Debezium បាញ់ចូល។
- `"database": "demo"`, `"collection": "customers"`: ទីតាំង Document ដែលត្រូវរក្សាទុកក្នុង MongoDB។
- `"change.data.capture.handler": "...PostgresHandler"`: **នេះជាបេះដូងរបស់ Sink!**
  - ប្រសិនបើគ្មាន Handler នេះទេ MongoDB នឹងគ្រាន់តែយក JSON របស់ Debezium ទាំងមូល (មាន `before`, `after`, `source`, `op`) ទៅ Save ជា Document ធម្មតា។
  - ប៉ុន្តែដោយសារមាន `PostgresHandler` វានឹងអានកូដ `op`:
    - បើ `op = "c"` ឬ `"r"`: វានឹងរត់ `insertOne` ឬ `replaceOne` នូវទិន្នន័យក្នុង `after`។
    - បើ `op = "u"`: វានឹងរត់ `updateOne` ទៅលើ Document ដែលមាន `_id` ត្រូវគ្នា។
    - បើ `op = "d"`: វានឹងរត់ `deleteOne` លើ Document នោះចេញពី MongoDB ភ្លាម។
  - ហើយ `_id` ក្នុង MongoDB ត្រូវបាន map ចេញពី Primary Key របស់ Postgres ដោយស្វ័យប្រវត្តិ (ឧ. `{ "_id": { "id": 1 } }`)។

---

### 6. `scripts/setup.sh`
- **ទីតាំង:** `scripts/setup.sh`
- **តួនាទី:** ស្គ្រីបសម្រាប់ Run តែមួយបន្ទាត់ ដើម្បីបញ្ឆេះប្រព័ន្ធទាំងមូល និងចុះឈ្មោះ Connectors។

#### ដំណើរការរបស់វា:
1. បញ្ជា `docker compose up -d --build` ដើម្បី Build Image និងបើក Containers ទាំងអស់។
2. ធ្វើរង្វិលជុំ (Loop) រង់ចាំរហូតដល់ Kafka Connect REST API លើ Port 8083 ឆ្លើយតបមកវិញ (HTTP 200)។
3. ផ្ញើ HTTP POST Request ទៅកាន់ `http://localhost:8083/connectors` ដើម្បីចុះឈ្មោះ `pg-source` និង `mongo-sink`។
4. បង្ហាញ Status ថា Connector ទាំងពីរ និង Tasks កំពុងដំណើរការ (`RUNNING`)។

---

### 7. `scripts/demo.sh`
- **ទីតាំង:** `scripts/demo.sh`
- **តួនាទី:** ស្គ្រីបស្វ័យប្រវត្តិសម្រាប់បង្ហាញការធ្វើសមកាលកម្មទិន្នន័យជាក់ស្តែង ៤ ជំហាន។

#### ជំហានទាំង ៤ ក្នុងពេល Demo:
1. **Initial Snapshot (`op: r`):** បង្ហាញថាទិន្នន័យដើម (`Sokha` និង `Dara`) ត្រូវបាន Sync ចូល MongoDB រួចរាល់។
2. **INSERT (`op: c`):** បន្ថែមអតិថិជនថ្មីឈ្មោះ `Vathanak` (ទីក្រុង `Battambang`) ចូល PostgreSQL រួច Query មើលក្នុង MongoDB ឃើញភ្លាមៗ!
3. **UPDATE (`op: u`):** កែប្រែទីក្រុងរបស់ `Vathanak` ទៅជា `Kampot` ក្នុង PostgreSQL រួច Query មើលក្នុង MongoDB ឃើញទិន្នន័យប្រែប្រួលភ្លាមៗ ដោយរក្សា `_id` ដដែល។
4. **DELETE (`op: d`):** លុប `Vathanak` ចេញពី PostgreSQL រួច Query មើលក្នុង MongoDB ឃើញទិន្នន័យនោះបាត់ភ្លាមៗ!

---

### 8. `scripts/test-1m.ps1` & `test-1m.sh` (Benchmark ១ លានទិន្នន័យ)
- **ទីតាំង:** `scripts/test-1m.ps1` (សម្រាប់ Windows PowerShell) និង `scripts/test-1m.sh` (សម្រាប់ Bash)
- **តួនាទី:** ធ្វើតេស្តសាកល្បងសមត្ថភាពខ្ពស់ (Stress Test) ដោយបញ្ចូលទិន្នន័យចំនួន **១,០០០,០០០ (១ លាន) Records** ចូល PostgreSQL។

#### របៀបដំណើរការ:
1. ប្រើ `generate_series(1, 1000000)` បញ្ចូល ១ លាន Rows ចូល PostgreSQL តែមួយប៉ព្រិចភ្នែក។
2. ស្គ្រីបនឹង Query ទៅ MongoDB រៀងរាល់ ៣ វិនាទីម្តង (`db.customers.countDocuments()`) ដើម្បីតាមដានចំនួន Record ដែលកំពុងហូរចូល។
3. បង្ហាញភាគរយ (%) រហូតដល់គ្រប់ ១០០% (១ លាន Records)។
4. គណនារយៈពេលសរុប (Seconds) និងល្បឿនជាមធ្យម (**Records ក្នុងមួយវិនាទី** - Throughput)។

> 🗣️ **ចំណុចត្រូវនិយាយពេល Demo:**
> *"ស្គ្រីប Benchmark ១ លាន Records នេះ បង្ហាញយ៉ាងច្បាស់ថា ស្ថាបត្យកម្ម CDC របស់យើងមិនមែនសម្រាប់តែ Demo តូចតាចទេ គឺវាមានសមត្ថភាពទ្រទ្រង់ទិន្នន័យកម្រិត Enterprise High-Throughput បានយ៉ាងរលូន។"*

---

### 9. `scripts/cleanup.sh`
- **ទីតាំង:** `scripts/cleanup.sh`
- **តួនាទី:** សម្អាតប្រព័ន្ធទាំងមូលដោយរត់ `docker compose down -v`។
- បិទ Containers ទាំងអស់ និងលុប Volumes ចោលទាំងអស់ (សម្អាតទាំង WAL, Kafka Messages, និង MongoDB Data) ដើម្បីឱ្យប្រព័ន្ធស្អាតដូចដើមវិញ។

---

### 10. Postman Collection
- **ទីតាំង:** `postman/CDC-Postgres-MongoDB.postman_collection.json`
- **តួនាទី:** រៀបចំ API Requests ស្រេចៗសម្រាប់គ្រប់គ្រង និងត្រួតពិនិត្យ Kafka Connect REST API (`http://localhost:8083`)។
- **មាន Request ដូចជា:**
  - ពិនិត្យមើល Cluster Health និង Plugins ដែលបានដំឡើង។
  - ចុះឈ្មោះ Connector ថ្មី។
  - មើល Status របស់ Connector និង Task នីមួយៗ។
  - ផ្អាក (Pause), បើកដំណើរការឡើងវិញ (Resume), ឬ Restart Connector។
  - លុប (Delete) Connector។

---

### 11. ឯកសារជំនួយ (Documentation)
- **`flow-diagram.md`:** ផ្ទុក Diagram បែប Mermaid យ៉ាងច្បាស់លាស់ (Architecture Diagram, Sequence Diagram, Data Transformation Flow, Operation Decision Tree)។ អាចបើកបង្ហាញជាស្លាយពេលធ្វើបទបង្ហាញបានយ៉ាងស្អាត។
- **`how-test.md`:** ការណែនាំលម្អិតមួយជំហានម្តងៗទាំងលើ Bash និង Windows PowerShell ព្រមទាំងវិធីដោះស្រាយបញ្ហា (Troubleshooting)។
- **`README.md`:** សេចក្តីសង្ខេបគម្រោង និងពាក្យបញ្ជារហ័ស។

---

## 4. លំដាប់លំដោយនៃការធ្វើ Demo ផ្ទាល់ (Live Demo Script)

នៅពេលឡើងធ្វើបទបង្ហាញ សូមអនុវត្តតាមលំដាប់ ៥ វគ្គដូចខាងក្រោម៖

### វគ្គទី ១: បង្ហាញ Architecture (រយៈពេល ១ ទៅ ២ នាទី)
1. បើក File [flow-diagram.md](file:///D:/ISTAD/microservices/CDC/postgres-to-mongodb/cdc-postgres-to-mongodb/flow-diagram.md) ជា Markdown Preview។
2. ពន្យល់សមាសភាគទាំង ៥:
   - **Postgres:** កន្លែងផ្ទុកទិន្នន័យដើម និងកត់ត្រា WAL។
   - **Debezium Source:** អ្នកចាប់យក WAL។
   - **Kafka:** បំពង់បង្ហូរ និងផ្ទុក Event កណ្តាល។
   - **Mongo Sink:** អ្នកបម្លែង Event តាមរយៈ `PostgresHandler`។
   - **MongoDB:** កន្លែងទទួលទិន្នន័យចុងក្រោយ។

---

### វគ្គទី ២: ចាប់ផ្តើមប្រព័ន្ធ និងពិនិត្យមើល Status (រយៈពេល ២ នាទី)
1. បើក Terminal ហើយដំណើរការ:
   ```bash
   ./scripts/setup.sh
   # ឬក្នុង PowerShell:
   docker compose up -d --build
   ```
2. បើក Browser ចូលទៅកាន់ **Kafka UI**: `http://localhost:8080`
   - ចុចលើ Menu **Kafka Connect**: បង្ហាញទស្សនិកជនថា `pg-source` និង `mongo-sink` កំពុងដំណើរការ (`RUNNING`)។
   - ចុចលើ Menu **Topics**: បង្ហាញ Topic ឈ្មោះ `pg.public.customers`។

---

### វគ្គទី ៣: បង្ហាញការ Sync ជាក់ស្តែង (CRUD Demo) (រយៈពេល ៣ នាទី)
1. ដំណើរការស្គ្រីប Demo:
   ```bash
   ./scripts/demo.sh
   ```
2. **ពន្យល់ទស្សនិកជនតាមលំដាប់លទ្ធផលដែលបង្ហាញលើអេក្រង់:**
   - **ជំហានទី ១ (Initial Snapshot):** ទិន្នន័យ `Sokha` និង `Dara` មានស្រាប់ក្នុង MongoDB។
   - **ជំហានទី ២ (INSERT):** បន្ថែម `Vathanak` ចូល Postgres ➔ MongoDB បង្ហាញឈ្មោះ `Vathanak` ភ្លាមៗ!
   - **ជំហានទី ៣ (UPDATE):** កែប្រែទីក្រុងរបស់ `Vathanak` ទៅជា `Kampot` ➔ MongoDB Update ភ្លាមៗដោយរក្សា `_id` ដដែល!
   - **ជំហានទី ៤ (DELETE):** លុប `Vathanak` ចេញពី Postgres ➔ MongoDB លុប Document នោះចោលភ្លាមៗ!
3. ត្រឡប់មក **Kafka UI** វិញ (`http://localhost:8080`):
   - ចូលទៅកាន់ Topic `pg.public.customers` ហើយចុចមើល Message ចុងក្រោយ។
   - បង្ហាញទម្រង់ JSON Envelope របស់ Debezium ឱ្យទស្សនិកជនឃើញ៖
     - `"op": "u"` (Update)
     - `"before": { "city": "Battambang" }`
     - `"after": { "city": "Kampot" }`

---

### វគ្គទី ៤: បង្ហាញ Benchmark ១ លាន Records (បង្កើតភាពអស្ចារ្យ - Wow Factor)
1. ដំណើរការស្គ្រីបតេស្តកម្លាំងម៉ាស៊ីន:
   ```powershell
   # លើ PowerShell:
   .\scripts\test-1m.ps1
   # ឬលើ Bash:
   ./scripts/test-1m.sh
   ```
2. បង្ហាញភាគរយនៃការរត់ និង Throughput រាប់ពាន់ Records ក្នុងមួយវិនាទី។

---

### វគ្គទី ៥: សម្អាតប្រព័ន្ធ (Cleanup)
1. បញ្ចប់ការ Demo ដោយរត់ស្គ្រីបសម្អាត:
   ```bash
   ./scripts/cleanup.sh
   ```

---

## 5. សំណួរដែលអាចនឹងត្រូវបានសួរ និងចម្លើយគន្លឹះ (Q&A)

| សំណួរ (Questions) | ចម្លើយគន្លឹះសម្រាប់ឆ្លើយ (Winning Answers) |
| :--- | :--- |
| **សំណួរទី ១: ហេតុអ្វីមិនសរសេរកូដ Sync ទាំង Postgres និង Mongo ក្នុង Application Code តែម្តង (Dual-writing)?** | *"ការសរសេរ Dual-writing ក្នុង Code មិនមានលក្ខណៈ Transactional ឆ្លង Database ពីរផ្សេងគ្នាទេ។ បើសិនជា App សរសេរចូល Postgres ជោគជ័យ តែពេលកំពុងសរសេរចូល Mongo ស្រាប់តែដាច់ភ្លើង ឬដាច់ Network នោះទិន្នន័យនឹងលែងត្រូវគ្នា (Data Inconsistency)។ ចំណែកឯ CDC យក WAL របស់ Postgres ជា Single Source of Truth ដរាបណា Postgres បាន Commit នោះ Kafka ធានាថានឹងបញ្ជូនទៅដល់ Mongo ដោយមិនបាត់បង់ទិន្នន័យឡើយ។"* |
| **សំណួរទី ២: តើ Debezium ធ្វើឱ្យ PostgreSQL ដើរយឺតដែរឬទេ?** | *"អត់ទេ ពីព្រោះ Debezium អានទិន្នន័យចេញពី Write-Ahead Log (WAL) ដោយផ្ទាល់តាមរយៈ `pgoutput` plugin។ វាមិនបានធ្វើការ Run `SELECT` queries ឬ Lock តារាងនៅក្នុង Database នោះទេ ដូច្នេះផលប៉ះពាល់លើ Performance គឺទាបបំផុតស្ទើរតែស្មើនឹងសូន្យ។"* |
| **សំណួរទី ៣: ចុះបើ MongoDB គាំង ឬដាច់ Network តើទិន្នន័យបាត់បង់ដែរឬទេ?** | *"មិនបាត់បង់ទេ! ពីព្រោះ Apache Kafka ដើរតួជាកន្លែងស្តុកសារ (Durable Buffer)។ នៅពេល MongoDB ដាច់ សារទាំងអស់នឹងត្រូវរក្សាទុកក្នុង Kafka ដោយសុវត្ថិភាព។ នៅពេល MongoDB ដំណើរការឡើងវិញ Kafka Connect នឹងទាញយកទិន្នន័យបន្តពីចំណុច Offset ចុងក្រោយដោយមិនបាត់សូម្បីតែមួយ Record។"* |
| **សំណួរទី ៤: ហេតុអ្វីបានជាចាំបាច់ត្រូវកំណត់ `REPLICA IDENTITY FULL` លើ PostgreSQL?** | *"តាមធម្មតា PostgreSQL WAL កត់ត្រាតែ Primary Key ប៉ុណ្ណោះសម្រាប់ប្រតិបត្តិការ UPDATE និង DELETE។ ការដាក់ `REPLICA IDENTITY FULL` គឺដើម្បីបង្ខំឱ្យ Postgres កត់ត្រាតម្លៃចាស់ទាំងអស់នៃ Row ចូលទៅក្នុង WAL ដែលធ្វើឱ្យ Debezium អាចបង្កើតរូបភាព `before` ពេញលេញសម្រាប់ Sink យកទៅប្រើប្រាស់។"* |
| **សំណួរទី ៥: តើ MongoDB ដឹងដោយរបៀបណាថាត្រូវ Update ឬ Delete លើ Document មួយណា?** | *"តាមរយៈ `PostgresHandler` របស់ MongoDB Sink Connector។ Handler នេះទាញយក Primary Key របស់ Postgres ចេញពី Message Key របស់ Kafka ដើម្បីបង្កើតជា `_id: { id: <pk> }` ក្នុង MongoDB។ ដូច្នេះពេលមាន Update ឬ Delete វានឹងរត់ `updateOne` ឬ `deleteOne` ដោយផ្អែកលើ `_id` នោះ។"* |
