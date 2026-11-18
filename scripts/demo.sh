#!/usr/bin/env bash
cd "$(dirname "$0")/.."
PSQL="docker compose exec -T postgres psql -U postgres -d demo -c"
SHOW='docker compose exec -T mongo mongosh --quiet demo --eval "printjson(db.customers.find().toArray())"'
step() { echo; echo "=== $1 ==="; }

step "1. Initial snapshot in MongoDB";  sleep 2; eval $SHOW
step "2. INSERT in PostgreSQL";        $PSQL "INSERT INTO customers(name,email,city) VALUES ('Vathanak','vathanak@example.com','Battambang');"; sleep 3; eval $SHOW
step "3. UPDATE in PostgreSQL";        $PSQL "UPDATE customers SET city='Kampot' WHERE name='Vathanak';"; sleep 3; eval $SHOW
step "4. DELETE in PostgreSQL";        $PSQL "DELETE FROM customers WHERE name='Vathanak';"; sleep 3; eval $SHOW
