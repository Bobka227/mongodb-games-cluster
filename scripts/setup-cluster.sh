#!/bin/bash
set -e

MONGO_ADMIN_USER="${MONGO_ADMIN_USER:-admin}"

if [ -z "$MONGO_ADMIN_PASSWORD" ]; then
  echo "ERROR: MONGO_ADMIN_PASSWORD is not set"
  exit 1
fi

# Wrapper: adds admin auth to every mongosh call
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
  until mongosh --host "$host" --port "$port" --quiet --eval 'db.adminCommand({ ping: 1 }).ok' | grep -q 1; do
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

rs_exists() {
  local host="$1"
  local port="$2"
  mongosh_auth --host "$host" --port "$port" --quiet \
    --eval 'try { rs.status().ok } catch(e) { print(0) }' 2>/dev/null | tail -n 1 | grep -q 1
}

wait_rs_primary() {
  local host="$1"
  local port="$2"
  local name="$3"
  echo "Waiting for PRIMARY in $name ..."
  until mongosh_auth --host "$host" --port "$port" --quiet \
    --eval 'try { rs.status().members.some(m => m.stateStr === "PRIMARY") } catch(e) { false }' \
    2>/dev/null | grep -q true; do
    sleep 2
  done
}

ensure_cfg_rs() {
  echo "Checking cfgRS ..."
  if rs_exists cfg1 27019; then
    echo "cfgRS already initialized."
  else
    echo "Initializing cfgRS ..."
    mongosh_auth --host cfg1 --port 27019 --quiet <<'EOF'
rs.initiate({
  _id: "cfgRS",
  configsvr: true,
  members: [
    { _id: 0, host: "cfg1:27019" },
    { _id: 1, host: "cfg2:27019" },
    { _id: 2, host: "cfg3:27019" }
  ]
})
EOF
  fi
}

ensure_shard_rs() {
  local host="$1"
  local rsname="$2"
  shift 2
  local members=("$@")

  echo "Checking $rsname ..."
  if rs_exists "$host" 27018; then
    echo "$rsname already initialized."
    return
  fi

  echo "Initializing $rsname ..."
  local js="rs.initiate({ _id: '$rsname', members: ["
  local i=0
  for member in "${members[@]}"; do
    if [ $i -gt 0 ]; then
      js+=", "
    fi
    js+="{ _id: $i, host: '$member' }"
    i=$((i+1))
  done
  js+="] })"

  mongosh_auth --host "$host" --port 27018 --quiet --eval "$js"
}

wait_mongos_ready() {
  echo "Waiting for mongos ..."
  until mongosh --host mongos --port 27017 --quiet --eval 'db.adminCommand({ ping: 1 }).ok' | grep -q 1; do
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

echo "=== Waiting for config server auth ==="
wait_mongo_auth cfg1 27019

echo "=== Ensuring config replica set ==="
ensure_cfg_rs
wait_rs_primary cfg1 27019 cfgRS

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

echo "=== Waiting for shard primary auth ==="
wait_mongo_auth s1a 27018
wait_mongo_auth s2a 27018
wait_mongo_auth s3a 27018

echo "=== Ensuring shard replica sets ==="
ensure_shard_rs s1a shard1RS s1a:27018 s1b:27018 s1c:27018
ensure_shard_rs s2a shard2RS s2a:27018 s2b:27018 s2c:27018
ensure_shard_rs s3a shard3RS s3a:27018 s3b:27018 s3c:27018

wait_rs_primary s1a 27018 shard1RS
wait_rs_primary s2a 27018 shard2RS
wait_rs_primary s3a 27018 shard3RS

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
