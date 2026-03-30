#!/bin/bash
# Run this script ONCE on the server before "docker compose up"
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_DIR="$SCRIPT_DIR/../secrets"

mkdir -p "$SECRETS_DIR"
openssl rand -base64 756 > "$SECRETS_DIR/keyfile"
chmod 400 "$SECRETS_DIR/keyfile"
echo "Keyfile generated at $SECRETS_DIR/keyfile"
