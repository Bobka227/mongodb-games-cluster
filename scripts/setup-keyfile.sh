#!/bin/bash
# Entrypoint for SECONDARY nodes.
# Copies keyfile with correct permissions, then exec's mongod.
set -e

mkdir -p /etc/mongo
cp /run/secrets/keyfile /etc/mongo/keyfile
chmod 400 /etc/mongo/keyfile

exec "$@"
