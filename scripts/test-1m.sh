#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

echo "=========================================================="
echo "    CDC Benchmark: Testing 1,000,000 Records Replication   "
echo "=========================================================="

echo "Step 1: Inserting 1,000,000 rows into PostgreSQL..."
START_TIME=$(date +%s)

docker compose exec -T postgres psql -U postgres -d demo -c "
INSERT INTO customers (name, email, city)
SELECT 
    'Customer_' || i,
    'customer_' || i || '@example.com',
    (ARRAY['Phnom Penh', 'Siem Reap', 'Battambang', 'Kampot', 'Kandal'])[1 + (i % 5)]
FROM generate_series(1, 1000000) AS i;
"

PG_TIME=$(date +%s)
echo "-> PostgreSQL insert done in $((PG_TIME - START_TIME)) seconds."
echo
echo "Step 2: Tracking replication progress into MongoDB..."

while true; do
  COUNT=$(docker compose exec -T mongo mongosh --quiet demo --eval "db.customers.countDocuments()" 2>/dev/null | tr -d '\r\n ')
  if [[ "$COUNT" =~ ^[0-9]+$ ]]; then
    PERCENT=$((COUNT * 100 / 1000000))
    echo "[$(date +'%T')] Replicated: $COUNT / 1,000,000 ($PERCENT%)"
    if [ "$COUNT" -ge 1000000 ]; then
      END_TIME=$(date +%s)
      TOTAL=$((END_TIME - START_TIME))
      echo "=========================================================="
      echo " SUCCESS: 1,000,000 records synced in $TOTAL seconds!"
      echo " Average throughput: $((1000000 / TOTAL)) records/sec"
      echo "=========================================================="
      break
    fi
  fi
  sleep 3
done
