#!/bin/bash
set -e

echo "=== Waiting for config servers ==="
until mongosh --host cfg1 --port 27019 --quiet --eval "db.adminCommand({ ping: 1 }).ok" >/dev/null 2>&1; do sleep 2; done
until mongosh --host cfg2 --port 27019 --quiet --eval "db.adminCommand({ ping: 1 }).ok" >/dev/null 2>&1; do sleep 2; done
until mongosh --host cfg3 --port 27019 --quiet --eval "db.adminCommand({ ping: 1 }).ok" >/dev/null 2>&1; do sleep 2; done

echo "=== Waiting for shard servers ==="
for host in s1a s1b s1c s2a s2b s2c s3a s3b s3c; do
  until mongosh --host "$host" --port 27018 --quiet --eval "db.adminCommand({ ping: 1 }).ok" >/dev/null 2>&1; do
    sleep 2
  done
done

echo "=== Initiating config replica set ==="
mongosh --host cfg1 --port 27019 --quiet <<'EOF'
try {
  rs.status();
  print("cfgRS already initialized.");
} catch (e) {
  rs.initiate({
    _id: "cfgRS",
    configsvr: true,
    members: [
      { _id: 0, host: "cfg1:27019" },
      { _id: 1, host: "cfg2:27019" },
      { _id: 2, host: "cfg3:27019" }
    ]
  });
  print("cfgRS initiated.");
}
EOF

echo "=== Waiting for cfgRS election ==="
sleep 15

echo "=== Initiating shard1 replica set ==="
mongosh --host s1a --port 27018 --quiet <<'EOF'
try {
  rs.status();
  print("shard1RS already initialized.");
} catch (e) {
  rs.initiate({
    _id: "shard1RS",
    members: [
      { _id: 0, host: "s1a:27018" },
      { _id: 1, host: "s1b:27018" },
      { _id: 2, host: "s1c:27018" }
    ]
  });
  print("shard1RS initiated.");
}
EOF

echo "=== Initiating shard2 replica set ==="
mongosh --host s2a --port 27018 --quiet <<'EOF'
try {
  rs.status();
  print("shard2RS already initialized.");
} catch (e) {
  rs.initiate({
    _id: "shard2RS",
    members: [
      { _id: 0, host: "s2a:27018" },
      { _id: 1, host: "s2b:27018" },
      { _id: 2, host: "s2c:27018" }
    ]
  });
  print("shard2RS initiated.");
}
EOF

echo "=== Initiating shard3 replica set ==="
mongosh --host s3a --port 27018 --quiet <<'EOF'
try {
  rs.status();
  print("shard3RS already initialized.");
} catch (e) {
  rs.initiate({
    _id: "shard3RS",
    members: [
      { _id: 0, host: "s3a:27018" },
      { _id: 1, host: "s3b:27018" },
      { _id: 2, host: "s3c:27018" }
    ]
  });
  print("shard3RS initiated.");
}
EOF

echo "=== Waiting for shard primaries ==="
sleep 20

echo "=== Waiting for mongos ==="
until mongosh --host mongos --port 27017 --quiet --eval "db.adminCommand({ ping: 1 }).ok" >/dev/null 2>&1; do
  sleep 2
done

echo "=== Adding shards to mongos ==="
mongosh --host mongos --port 27017 --quiet <<'EOF'
const existing = db.getSiblingDB("config").shards.find().toArray().map(s => s._id);

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

echo "=== Initializing games database, schema, sharding and indexes ==="
mongosh --host mongos --port 27017 /mongo-init/01-init-db.js

echo "=== Importing unified dataset if collection is empty ==="
DOC_COUNT=$(mongosh --host mongos --port 27017 --quiet --eval 'db.getSiblingDB("gamesdb").games_unified_validated.countDocuments()' | tail -n 1 | tr -d '\r')
echo "DOC_COUNT=$DOC_COUNT"

if [ "$DOC_COUNT" = "0" ]; then
  echo "Collection is empty, starting mongoimport..."

  mongoimport \
    --host mongos \
    --port 27017 \
    --db gamesdb \
    --collection games_unified_validated \
    --file /import/games_unified.json \
    --verbose

  echo "Documents after import:"
  mongosh --host mongos --port 27017 --quiet --eval 'print(db.getSiblingDB("gamesdb").games_unified_validated.countDocuments())'
else
  echo "Collection already contains data ($DOC_COUNT documents), skipping import"
fi

echo "=== Splitting and distributing chunks ==="
mongosh --host mongos --port 27017 /mongo-init/02-post-sharding.js

echo "=== Final status ==="
mongosh --host mongos --port 27017 /mongo-init/03-final-check.js

echo "=== Cluster setup finished ==="