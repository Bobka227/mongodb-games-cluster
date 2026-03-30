#!/bin/bash
# Entrypoint for PRIMARY nodes (cfg1, s1a, s2a, s3a).
# Starts mongod, initiates replica set from localhost, waits for PRIMARY, creates admin user.
set -e

PORT="${MONGOD_PORT:-27017}"

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
echo "[$HOSTNAME] Mongod is ready"

echo "[$HOSTNAME] Initiating replica set $RS_NAME ..."
MEMBERS_JS=""
IFS=',' read -ra MEMBER_ARRAY <<< "$RS_MEMBERS"
for i in "${!MEMBER_ARRAY[@]}"; do
  [ -n "$MEMBERS_JS" ] && MEMBERS_JS+=", "
  MEMBERS_JS+="{ _id: $i, host: '${MEMBER_ARRAY[$i]}' }"
done

CONFIGSVR_LINE=""
[ "${IS_CONFIGSVR:-false}" = "true" ] && CONFIGSVR_LINE="configsvr: true,"

mongosh "mongodb://127.0.0.1:$PORT" --eval "
  try {
    var r = rs.initiate({
      _id: '${RS_NAME}',
      ${CONFIGSVR_LINE}
      members: [${MEMBERS_JS}]
    });
    print('[${HOSTNAME}] rs.initiate: ' + JSON.stringify(r));
  } catch(e) {
    if (e.code === 23 || String(e).includes('already initialized')) {
      print('[${HOSTNAME}] RS already initialized');
    } else {
      print('[${HOSTNAME}] rs.initiate error (code=' + e.code + '): ' + e);
    }
  }
" 2>&1 || true

echo "[$HOSTNAME] Waiting to become PRIMARY..."
until mongosh "mongodb://127.0.0.1:$PORT" \
  --eval 'try { print(rs.status().myState) } catch(e) { print(0) }' \
  2>/dev/null | grep -q "^1$"; do
  if ! kill -0 $MONGOD_PID 2>/dev/null; then
    echo "[$HOSTNAME] ERROR: mongod exited unexpectedly"
    exit 1
  fi
  sleep 2
done
echo "[$HOSTNAME] Node is PRIMARY"

echo "[$HOSTNAME] Checking/creating admin user..."
if mongosh "mongodb://127.0.0.1:$PORT" \
  --username "${MONGO_ADMIN_USER}" \
  --password "${MONGO_ADMIN_PASSWORD}" \
  --authenticationDatabase admin \
  --eval 'db.adminCommand({ping:1}).ok' \
  2>/dev/null | grep -q "^1$"; then
  echo "[$HOSTNAME] Admin user already exists"
else
  mongosh "mongodb://127.0.0.1:$PORT" --eval "
    try {
      db.getSiblingDB('admin').createUser({
        user: '${MONGO_ADMIN_USER}',
        pwd: '${MONGO_ADMIN_PASSWORD}',
        roles: [{ role: 'root', db: 'admin' }]
      });
      print('[${HOSTNAME}] Admin user created');
    } catch(e) {
      if (e.code === 51003 || String(e).includes('already exists')) {
        print('[${HOSTNAME}] Admin user already exists');
      } else {
        print('[${HOSTNAME}] createUser error (code=' + e.code + '): ' + e);
      }
    }
  " 2>&1 || true
fi

echo "[$HOSTNAME] Init complete, mongod running."
wait $MONGOD_PID
