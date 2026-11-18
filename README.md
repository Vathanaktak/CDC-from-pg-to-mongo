# CDC Demo: PostgreSQL → MongoDB (Debezium)

Pipeline: PostgreSQL → Debezium source → Kafka → MongoDB sink → MongoDB

## Requirements
Docker + Docker Compose, curl, python3 (Git Bash/WSL on Windows).

## Run
```bash
./scripts/setup.sh     # start stack + register both connectors
./scripts/demo.sh      # INSERT / UPDATE / DELETE and show MongoDB each time
./scripts/test-1m.sh   # Benchmark: 1,000,000 records replication test (or test-1m.ps1)
./scripts/cleanup.sh   # tear down
```

## Manual demo commands
```bash
docker compose exec postgres psql -U postgres -d demo
  INSERT INTO customers(name,email,city) VALUES ('Vathanak','v@example.com','Kampot');
  UPDATE customers SET city='Siem Reap' WHERE name='Vathanak';
  DELETE FROM customers WHERE name='Vathanak';

docker compose exec mongo mongosh demo --eval "db.customers.find()"
```

## Useful checks
- Kafka UI: http://localhost:8080 (inspect topics, messages, and connectors)
- Postman Collection: `postman/CDC-Postgres-MongoDB.postman_collection.json` (test Kafka Connect REST API)
- Connector status: `curl localhost:8083/connectors/pg-source/status`
- Kafka topic: `docker compose exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic pg.public.customers --from-beginning`
- Logs: `docker compose logs -f connect`

## Notes
- `wal_level=logical` is preset in the Debezium Postgres image.
- `REPLICA IDENTITY FULL` and a primary key make updates/deletes map correctly.
- `tombstones.on.delete=false` avoids null tombstone messages hitting the sink.
- MongoDB `_id` is built from the PostgreSQL primary key (e.g. `{ "id": 3 }`).
