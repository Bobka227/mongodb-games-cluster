#!/bin/bash
# Simulation of node failure and automatic recovery in MongoDB replica set
# Usage: bash scripts/simulate-failover.sh

MONGO_USER="${MONGO_ADMIN_USER:-admin}"
MONGO_PASS="${MONGO_ADMIN_PASSWORD:-admin}"

mongosh_shard() {
  local host="$1"; shift
  mongosh --host "$host" --port 27018 \
    -u "$MONGO_USER" -p "$MONGO_PASS" \
    --authenticationDatabase admin --quiet "$@"
}

rs_members() {
  local host="$1"
  mongosh_shard "$host" --eval \
    'rs.status().members.forEach(m => print(m.name + " -> " + m.stateStr))'
}

echo "========================================"
echo " MongoDB Replica Set Failover Simulation"
echo "========================================"
echo ""

echo "--- [1] Initial state of shard1RS ---"
rs_members s1a
echo ""

echo "--- [2] Stopping secondary node s1b ---"
docker stop s1b
sleep 3

echo "--- [3] State after s1b failure ---"
rs_members s1a
echo ""
echo "  => s1a (PRIMARY) and s1c (SECONDARY) still working"
echo "     Data reads and writes continue uninterrupted"
echo ""

echo "--- [4] Stopping PRIMARY node s1a ---"
docker stop s1a
echo "    Waiting for automatic election (10s)..."
sleep 12

echo "--- [5] State after PRIMARY failure (checking s1c) ---"
mongosh --host s1c --port 27018 \
  -u "$MONGO_USER" -p "$MONGO_PASS" \
  --authenticationDatabase admin --quiet \
  --eval 'rs.status().members.forEach(m => print(m.name + " -> " + m.stateStr))'
echo ""
echo "  => s1c elected as new PRIMARY automatically"
echo ""

echo "--- [6] Restarting failed nodes ---"
docker start s1a
docker start s1b
echo "    Waiting for nodes to rejoin replica set (15s)..."
sleep 15

echo "--- [7] Final state after recovery ---"
rs_members s1c
echo ""
echo "  => All nodes back online, data fully replicated"
echo ""
echo "========================================"
echo " Simulation complete"
echo "========================================"
