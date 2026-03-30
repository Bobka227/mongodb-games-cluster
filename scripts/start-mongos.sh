#!/bin/bash
set -e

# Copy keyfile with correct permissions
mkdir -p /etc/mongo
cp /run/secrets/keyfile /etc/mongo/keyfile
chmod 400 /etc/mongo/keyfile

echo "Waiting for cfg1 ping before starting mongos..."
until mongosh --host cfg1 --port 27019 --quiet --eval 'db.adminCommand({ ping: 1 }).ok' | grep -q 1; do
  sleep 2
done

echo "cfg1 is reachable. Starting mongos..."
exec mongos \
  --configdb cfgRS/cfg1:27019,cfg2:27019,cfg3:27019 \
  --bind_ip_all \
  --port 27017 \
  --keyFile /etc/mongo/keyfile
