#!/bin/bash
set -e

echo "Waiting for config servers..."
until mongosh --host cfg1 --port 27019 --eval "db.adminCommand({ ping: 1 })" >/dev/null 2>&1; do sleep 2; done
until mongosh --host cfg2 --port 27019 --eval "db.adminCommand({ ping: 1 })" >/dev/null 2>&1; do sleep 2; done
until mongosh --host cfg3 --port 27019 --eval "db.adminCommand({ ping: 1 })" >/dev/null 2>&1; do sleep 2; done

echo "Waiting for shard servers..."
for host in s1a s1b s1c s2a s2b s2c s3a s3b s3c; do
  until mongosh --host "$host" --port 27018 --eval "db.adminCommand({ ping: 1 })" >/dev/null 2>&1; do
    sleep 2
  done
done

echo "Initiating config replica set..."
mongosh --host cfg1 --port 27019 <<'EOF'
try {
  rs.initiate({
    _id: "cfgRS",
    configsvr: true,
    members: [
      { _id: 0, host: "cfg1:27019" },
      { _id: 1, host: "cfg2:27019" },
      { _id: 2, host: "cfg3:27019" }
    ]
  })
} catch (e) {
  print("cfgRS already initialized or init failed: " + e)
}
EOF

echo "Waiting for cfgRS primary..."
sleep 15

echo "Initiating shard1 replica set..."
mongosh --host s1a --port 27018 <<'EOF'
try {
  rs.initiate({
    _id: "shard1RS",
    members: [
      { _id: 0, host: "s1a:27018" },
      { _id: 1, host: "s1b:27018" },
      { _id: 2, host: "s1c:27018" }
    ]
  })
} catch (e) {
  print("shard1RS already initialized or init failed: " + e)
}
EOF

echo "Initiating shard2 replica set..."
mongosh --host s2a --port 27018 <<'EOF'
try {
  rs.initiate({
    _id: "shard2RS",
    members: [
      { _id: 0, host: "s2a:27018" },
      { _id: 1, host: "s2b:27018" },
      { _id: 2, host: "s2c:27018" }
    ]
  })
} catch (e) {
  print("shard2RS already initialized or init failed: " + e)
}
EOF

echo "Initiating shard3 replica set..."
mongosh --host s3a --port 27018 <<'EOF'
try {
  rs.initiate({
    _id: "shard3RS",
    members: [
      { _id: 0, host: "s3a:27018" },
      { _id: 1, host: "s3b:27018" },
      { _id: 2, host: "s3c:27018" }
    ]
  })
} catch (e) {
  print("shard3RS already initialized or init failed: " + e)
}
EOF

echo "Waiting for shard primaries..."
sleep 20

echo "Waiting for mongos..."
until mongosh --host mongos --port 27017 --eval "db.adminCommand({ ping: 1 })" >/dev/null 2>&1; do
  sleep 2
done

echo "Adding shards to mongos..."
mongosh --host mongos --port 27017 <<'EOF'
try { sh.addShard("shard1RS/s1a:27018,s1b:27018,s1c:27018") } catch(e) { print(e) }
try { sh.addShard("shard2RS/s2a:27018,s2b:27018,s2c:27018") } catch(e) { print(e) }
try { sh.addShard("shard3RS/s3a:27018,s3b:27018,s3c:27018") } catch(e) { print(e) }
sh.status()
EOF

echo "Cluster setup finished."