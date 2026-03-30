#!/bin/bash
set -e

MONGO_ADMIN_USER="${MONGO_ADMIN_USER:-admin}"

if [ -z "$MONGO_ADMIN_PASSWORD" ]; then
  echo "ERROR: MONGO_ADMIN_PASSWORD is not set"
  exit 1
fi

mongosh_auth() {
  mongosh \
    --username "$MONGO_ADMIN_USER" \
    --password "$MONGO_ADMIN_PASSWORD" \
    --authenticationDatabase admin \
    "$@"
}

wait_ping() {
  local host="$1"
  local port="$2"
  echo "Waiting for $host:$port ..."
  until mongosh --host "$host" --port "$port" --quiet \
    --eval 'db.adminCommand({ ping: 1 }).ok' 2>/dev/null | grep -q 1; do
    sleep 2
  done
}

wait_mongo_auth() {
  local host="$1"
  local port="$2"
  echo "Waiting for auth on $host:$port ..."
  until mongosh_auth --host "$host" --port "$port" --quiet \
    --eval 'db.adminCommand({ ping: 1 }).ok' 2>/dev/null | grep -q 1; do
    sleep 2
  done
  echo "Auth ready on $host:$port"
}

wait_mongos_ready() {
  echo "Waiting for mongos ..."
  until mongosh --host mongos --port 27017 --quiet \
    --eval 'db.adminCommand({ ping: 1 }).ok' 2>/dev/null | grep -q 1; do
    sleep 2
  done
}

wait_mongos_metadata_ready() {
  echo "Waiting for mongos metadata readiness ..."
  until mongosh_auth --host mongos --port 27017 --quiet --eval '
    try {
      db.adminCommand({ ping: 1 });
      const cfg = db.getSiblingDB("config");
      cfg.shards.find().toArray();
      print(true);
    } catch(e) {
      print(false);
    }' 2>/dev/null | grep -q true; do
    sleep 3
  done
}

ensure_shards_added() {
  echo "Ensuring shards are added to mongos ..."
  mongosh_auth --host mongos --port 27017 --quiet <<'EOF'
const cfg = db.getSiblingDB("config");
const existing = cfg.shards.find().toArray().map(s => s._id);

if (!existing.includes("shard1RS")) {
  sh.addShard("shard1RS/s1a:27018,s1b:27018,s1c:27018");
  print("Added shard1RS");
} else {
  print("shard1RS already exists");
}

if (!existing.includes("shard2RS")) {
  sh.addShard("shard2RS/s2a:27018,s2b:27018,s2c:27018");
  print("Added shard2RS");
} else {
  print("shard2RS already exists");
}

if (!existing.includes("shard3RS")) {
  sh.addShard("shard3RS/s3a:27018,s3b:27018,s3c:27018");
  print("Added shard3RS");
} else {
  print("shard3RS already exists");
}
EOF
}

wait_non_draining_shards() {
  echo "Waiting for non-draining shards ..."
  until mongosh_auth --host mongos --port 27017 --quiet --eval '
    try {
      const cfg = db.getSiblingDB("config");
      const total = cfg.shards.countDocuments({});
      const draining = cfg.shards.countDocuments({ draining: true });
      print(total >= 3 && draining === 0);
    } catch(e) {
      print(false);
    }' 2>/dev/null | grep -q true; do
    sleep 3
  done
}

echo "=== Waiting for config servers ==="
wait_ping cfg1 27019
wait_ping cfg2 27019
wait_ping cfg3 27019

echo "=== Waiting for shard servers ==="
wait_ping s1a 27018
wait_ping s1b 27018
wait_ping s1c 27018
wait_ping s2a 27018
wait_ping s2b 27018
wait_ping s2c 27018
wait_ping s3a 27018
wait_ping s3b 27018
wait_ping s3c 27018

echo "=== Waiting for primary auth (RS init + user creation handled by init-primary.sh) ==="
wait_mongo_auth cfg1 27019
wait_mongo_auth s1a 27018
wait_mongo_auth s2a 27018
wait_mongo_auth s3a 27018

echo "=== Waiting for mongos ==="
wait_mongos_ready
wait_mongos_metadata_ready

echo "=== Ensuring shards are registered ==="
ensure_shards_added
wait_non_draining_shards

echo "=== Initializing games database, schema, sharding and indexes ==="
mongosh_auth --host mongos --port 27017 /mongo-init/01-init-db.js

echo "=== Splitting and distributing chunks ==="
mongosh_auth --host mongos --port 27017 /mongo-init/02-post-sharding.js

echo "=== Importing unified dataset if collection is empty ==="
DOC_COUNT=$(mongosh_auth --host mongos --port 27017 --quiet \
  --eval 'db.getSiblingDB("gamesdb").games_unified_validated.countDocuments()' | tail -n 1 | tr -d '\r')
echo "DOC_COUNT=$DOC_COUNT"

if [ "$DOC_COUNT" = "0" ]; then
  echo "Collection is empty, starting mongoimport..."
  mongoimport \
    --host mongos \
    --port 27017 \
    --username "$MONGO_ADMIN_USER" \
    --password "$MONGO_ADMIN_PASSWORD" \
    --authenticationDatabase admin \
    --db gamesdb \
    --collection games_unified_validated \
    --file /import/games_unified.json \
    --verbose

  echo "Documents after import:"
  mongosh_auth --host mongos --port 27017 --quiet \
    --eval 'print(db.getSiblingDB("gamesdb").games_unified_validated.countDocuments())'
else
  echo "Collection already contains data ($DOC_COUNT documents), skipping import"
fi

echo "=== Final status ==="
mongosh_auth --host mongos --port 27017 /mongo-init/03-final-check.js

echo "=== Cluster setup finished ==="
