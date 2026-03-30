#!/bin/bash
# Запуск всех dotazy через Docker
# Использование: bash Dotazy/run-queries.sh

MONGO_USER="${MONGO_ADMIN_USER:-admin}"
MONGO_PASS="${MONGO_ADMIN_PASSWORD:-admin}"

mongos_exec() {
  docker exec mongos mongosh --port 27017 \
    -u "$MONGO_USER" -p "$MONGO_PASS" \
    --authenticationDatabase admin --quiet --eval "$1"
}

shard_exec() {
  local container="$1"; shift
  docker exec "$container" mongosh --port 27018 \
    -u "$MONGO_USER" -p "$MONGO_PASS" \
    --authenticationDatabase admin --quiet --eval "$1"
}

echo "===== Kopírování dotazy.js do kontejneru ====="
docker cp Dotazy/dotazy.js mongos:/tmp/dotazy.js

echo ""
echo "===== KATEGORIE 1-2 a 4-5: spuštění přes mongos ====="
docker exec mongos mongosh --port 27017 \
  -u "$MONGO_USER" -p "$MONGO_PASS" \
  --authenticationDatabase admin \
  /tmp/dotazy.js

echo ""
echo "===== KATEGORIE 3: Konfigurace (vyžaduje přímé připojení) ====="

echo "--- sh.status() (cluster overview) ---"
mongos_exec "sh.status()"

echo "--- rs.status() na shard1RS (s1a) ---"
shard_exec s1a "rs.status().members.forEach(m => print(m.name, '->', m.stateStr, '| health:', m.health))"

echo "--- rs.conf() na shard1RS (s1a) ---"
shard_exec s1a "const c = rs.conf(); c.members.forEach(m => print(m.host, 'priority:', m.priority))"

echo "--- rs.status() na cfgRS (cfg1) ---"
docker exec cfg1 mongosh --port 27019 \
  -u "$MONGO_USER" -p "$MONGO_PASS" \
  --authenticationDatabase admin --quiet \
  --eval "rs.status().members.forEach(m => print(m.name, '->', m.stateStr))"

echo "--- Replication lag shard1RS ---"
shard_exec s1a "rs.printSecondaryReplicationInfo()"

echo "--- Chunky na shardech ---"
mongos_exec "
db.getSiblingDB('config').chunks.aggregate([
  { \$group: { _id: '\$shard', count: { \$sum: 1 } } },
  { \$sort: { _id: 1 } }
]).forEach(d => print(d._id, ':', d.count, 'chunks'))
"

echo "--- dbStats gamesdb ---"
mongos_exec "printjson(db.getSiblingDB('gamesdb').runCommand({ dbStats: 1, scale: 1048576 }))"

echo ""
echo "===== Hotovo! ====="
