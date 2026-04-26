#!/usr/bin/env bash
# Quick smoke test for the ingestion API
set -euo pipefail

BASE="http://localhost:8080"

echo "==> Create source account"
SOURCE=$(curl -sf -X POST "$BASE/v1/accounts" \
  -H "Content-Type: application/json" \
  -d '{"external_id":"ACC-001","owner_id":"user-123","currency":"USD"}' | jq -r '.id')
echo "Source: $SOURCE"

echo "==> Create dest account"
DEST=$(curl -sf -X POST "$BASE/v1/accounts" \
  -H "Content-Type: application/json" \
  -d '{"external_id":"ACC-002","owner_id":"user-456","currency":"USD"}' | jq -r '.id')
echo "Dest: $DEST"

echo "==> Submit transfer (first time)"
IDEM_KEY="txn-$(uuidgen)"
curl -sf -X POST "$BASE/v1/transactions" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -d "{
    \"transaction_type\": \"Transfer\",
    \"amount\": \"250.00\",
    \"currency\": \"USD\",
    \"source_account_id\": \"$SOURCE\",
    \"dest_account_id\": \"$DEST\",
    \"description\": \"Test transfer\"
  }" | jq .

echo "==> Replay same request (must return was_duplicate=true)"
curl -sf -X POST "$BASE/v1/transactions" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -d "{
    \"transaction_type\": \"Transfer\",
    \"amount\": \"250.00\",
    \"currency\": \"USD\",
    \"source_account_id\": \"$SOURCE\",
    \"dest_account_id\": \"$DEST\",
    \"description\": \"Test transfer\"
  }" | jq .

echo "==> Done"
