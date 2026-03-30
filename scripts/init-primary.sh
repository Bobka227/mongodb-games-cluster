#!/bin/bash
# Entrypoint for PRIMARY nodes (cfg1, s1a, s2a, s3a).
# Starts mongod, waits for it, creates admin user via localhost exception.
set -e

PORT="${MONGOD_PORT:-27017}"

# Copy keyfile and set correct permissions (mongod requires owner-only read)
mkdir -p /etc/mongo
cp /run/secrets/keyfile /etc/mongo/keyfile
chmod 400 /etc/mongo/keyfile

echo "[$HOSTNAME] Starting mongod on port $PORT..."
"$@" &
MONGOD_PID=$!

echo "[$HOSTNAME] Waiting for local mongod..."
until mongosh "mongodb://127.0.0.1:$PORT" \
  --eval 'try { print(db.adminCommand({ping:1}).ok) } catch(e) { print(0) }' \
  2>/dev/null | grep -q "^1"; do
  if ! kill -0 $MONGOD_PID 2>/dev/null; then
    echo "[$HOSTNAME] ERROR: mongod exited unexpectedly"
    exit 1
  fi
  sleep 1
done

echo "[$HOSTNAME] Creating admin user via localhost exception..."
mongosh --host 127.0.0.1 --port "$PORT" --eval "
  try {
    const admin = db.getSiblingDB('admin');
    admin.createUser({
      user: '${MONGO_ADMIN_USER}',
      pwd: '${MONGO_ADMIN_PASSWORD}',
      roles: [{ role: 'root', db: 'admin' }]
    });
    print('[${HOSTNAME}] Admin user created');
  } catch(e) {
    if (e.code === 51003 || String(e).includes('already exists')) {
      print('[${HOSTNAME}] Admin user already exists, skipping');
    } else {
      print('[${HOSTNAME}] createUser error (code=' + e.code + '): ' + e);
    }
  }
" 2>&1 || true

echo "[$HOSTNAME] Init complete, mongod running."
wait $MONGOD_PID
