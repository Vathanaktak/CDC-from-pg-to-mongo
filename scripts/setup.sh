#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
docker compose up -d --build
echo "Waiting for Kafka Connect..."
until curl -sf http://localhost:8083/connectors >/dev/null; do sleep 3; done
for c in pg-source mongo-sink; do
  curl -s -X POST -H "Content-Type: application/json" \
       --data @connectors/$c.json http://localhost:8083/connectors >/dev/null
  echo "Registered $c"
done
sleep 5
echo; curl -s http://localhost:8083/connectors?expand=status | (python -m json.tool 2>/dev/null || python3 -m json.tool 2>/dev/null || cat) | grep -E '"(name|state)"' || true
