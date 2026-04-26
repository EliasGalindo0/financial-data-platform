#!/usr/bin/env bash
# ==============================================================================
# Seed via API
# Cria contas e submete transações de exemplo chamando o serviço de ingestion.
# Requer que os serviços estejam rodando (make dev ou make docker-up).
#
# Uso:
#   bash scripts/seed-api.sh
#   make seed-api
# ==============================================================================

set -euo pipefail

BASE="${API_BASE:-http://localhost:8080}"
BOLD='\033[1m'
GREEN='\033[32m'
CYAN='\033[36m'
YELLOW='\033[33m'
RED='\033[31m'
RESET='\033[0m'

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

log()     { echo -e "${CYAN}$*${RESET}"; }
success() { echo -e "${GREEN}✓ $*${RESET}"; }
warn()    { echo -e "${YELLOW}⚠ $*${RESET}"; }
fail()    { echo -e "${RED}✗ $*${RESET}"; exit 1; }

# Chama a API e mostra o resultado formatado
api() {
    local method="$1"
    local path="$2"
    local description="$3"
    local data="${4:-}"
    local idem_key="${5:-}"

    log "  $description"

    local args=(-s -f -X "$method" "$BASE$path" -H "Content-Type: application/json")
    [ -n "$data" ]     && args+=(-d "$data")
    [ -n "$idem_key" ] && args+=(-H "Idempotency-Key: $idem_key")

    local response
    response=$(curl "${args[@]}") || fail "Requisição falhou: $method $path"

    echo "$response" | jq -r '.'
    echo "$response"
}

# Verifica se a API está acessível
check_api() {
    log "Verificando API em $BASE..."
    if ! curl -sf "$BASE/health" > /dev/null; then
        fail "API não está acessível em $BASE. Execute 'make dev' ou 'make docker-up' primeiro."
    fi
    success "API disponível"
}

# ------------------------------------------------------------------------------
# Início
# ------------------------------------------------------------------------------

echo ""
echo -e "${BOLD}==============================${RESET}"
echo -e "${BOLD}  Seed via API — Fin Platform ${RESET}"
echo -e "${BOLD}==============================${RESET}"
echo ""

check_api

# ------------------------------------------------------------------------------
# 1. CRIAR CONTAS
# ------------------------------------------------------------------------------

echo -e "\n${BOLD}1. Criando contas${RESET}"

log "  Conta Alice (USD)"
ALICE=$(curl -sf -X POST "$BASE/v1/accounts" \
    -H "Content-Type: application/json" \
    -d '{"external_id":"ACC-ALICE-USD","owner_id":"user-alice","currency":"Usd"}' \
    | jq -r '.id')
success "Alice: $ALICE"

log "  Conta Bob (USD)"
BOB=$(curl -sf -X POST "$BASE/v1/accounts" \
    -H "Content-Type: application/json" \
    -d '{"external_id":"ACC-BOB-USD","owner_id":"user-bob","currency":"Usd"}' \
    | jq -r '.id')
success "Bob: $BOB"

log "  Conta Corp (USD)"
CORP=$(curl -sf -X POST "$BASE/v1/accounts" \
    -H "Content-Type: application/json" \
    -d '{"external_id":"ACC-CORP-USD","owner_id":"org-acme","currency":"Usd"}' \
    | jq -r '.id')
success "Corp: $CORP"

log "  Conta Merchant (USD)"
MERCHANT=$(curl -sf -X POST "$BASE/v1/accounts" \
    -H "Content-Type: application/json" \
    -d '{"external_id":"ACC-MERCHANT-USD","owner_id":"org-shop","currency":"Usd"}' \
    | jq -r '.id')
success "Merchant: $MERCHANT"

# Salva os IDs em arquivo para referência futura
cat > /tmp/seed-ids.env <<EOF
ALICE=$ALICE
BOB=$BOB
CORP=$CORP
MERCHANT=$MERCHANT
EOF
log "  IDs salvos em /tmp/seed-ids.env"

# ------------------------------------------------------------------------------
# 2. DEPÓSITOS INICIAIS (Credit)
# ------------------------------------------------------------------------------

echo -e "\n${BOLD}2. Depósitos iniciais${RESET}"

log "  Depositar \$50.000 em Alice"
curl -sf -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-deposit-alice" \
    -d "{
        \"transaction_type\": \"Credit\",
        \"amount\": \"50000.00\",
        \"currency\": \"Usd\",
        \"dest_account_id\": \"$ALICE\",
        \"description\": \"Depósito inicial — Alice\"
    }" | jq -r '"  -> status: \(.status), id: \(.transaction_id)"'
success "Depósito Alice enviado"

log "  Depositar \$1.000 em Bob"
curl -sf -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-deposit-bob" \
    -d "{
        \"transaction_type\": \"Credit\",
        \"amount\": \"1000.00\",
        \"currency\": \"Usd\",
        \"dest_account_id\": \"$BOB\",
        \"description\": \"Depósito inicial — Bob\"
    }" | jq -r '"  -> status: \(.status), id: \(.transaction_id)"'
success "Depósito Bob enviado"

log "  Depositar \$100.000 em Corp"
curl -sf -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-deposit-corp" \
    -d "{
        \"transaction_type\": \"Credit\",
        \"amount\": \"100000.00\",
        \"currency\": \"Usd\",
        \"dest_account_id\": \"$CORP\",
        \"description\": \"Capital inicial — Corp\"
    }" | jq -r '"  -> status: \(.status), id: \(.transaction_id)"'
success "Depósito Corp enviado"

# ------------------------------------------------------------------------------
# 3. TRANSFERÊNCIAS ENTRE CONTAS
# ------------------------------------------------------------------------------

echo -e "\n${BOLD}3. Transferências${RESET}"

log "  Alice -> Corp: \$1.500 (pagamento de fatura)"
T1=$(curl -sf -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-transfer-001" \
    -d "{
        \"transaction_type\": \"Transfer\",
        \"amount\": \"1500.00\",
        \"currency\": \"Usd\",
        \"source_account_id\": \"$ALICE\",
        \"dest_account_id\": \"$CORP\",
        \"description\": \"Pagamento fatura #INV-2024-001\",
        \"metadata\": {\"invoice_id\": \"INV-2024-001\"}
    }" | jq -r '.transaction_id')
success "Transfer #1: $T1"

log "  Alice -> Merchant: \$299.99 (compra online)"
T2=$(curl -sf -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-transfer-002" \
    -d "{
        \"transaction_type\": \"Transfer\",
        \"amount\": \"299.99\",
        \"currency\": \"Usd\",
        \"source_account_id\": \"$ALICE\",
        \"dest_account_id\": \"$MERCHANT\",
        \"description\": \"Compra online — Pedido #ORD-9921\",
        \"metadata\": {\"order_id\": \"ORD-9921\"}
    }" | jq -r '.transaction_id')
success "Transfer #2: $T2"

log "  Alice -> Corp: \$15.000 (alto valor — aparece no CTR)"
T3=$(curl -sf -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-transfer-003" \
    -d "{
        \"transaction_type\": \"Transfer\",
        \"amount\": \"15000.00\",
        \"currency\": \"Usd\",
        \"source_account_id\": \"$ALICE\",
        \"dest_account_id\": \"$CORP\",
        \"description\": \"Pagamento de contrato — alto valor\"
    }" | jq -r '.transaction_id')
success "Transfer #3 (alto valor): $T3"

log "  Corp -> Bob: \$200 (pagamento de serviço)"
T4=$(curl -sf -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-transfer-004" \
    -d "{
        \"transaction_type\": \"Transfer\",
        \"amount\": \"200.00\",
        \"currency\": \"Usd\",
        \"source_account_id\": \"$CORP\",
        \"dest_account_id\": \"$BOB\",
        \"description\": \"Pagamento de serviço — freelancer\"
    }" | jq -r '.transaction_id')
success "Transfer #4: $T4"

# ------------------------------------------------------------------------------
# 4. TAXA (Fee)
# ------------------------------------------------------------------------------

echo -e "\n${BOLD}4. Taxas${RESET}"

log "  Fee mensal de Alice: \$9.90"
TF=$(curl -sf -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-fee-001" \
    -d "{
        \"transaction_type\": \"Fee\",
        \"amount\": \"9.90\",
        \"currency\": \"Usd\",
        \"source_account_id\": \"$ALICE\",
        \"description\": \"Taxa de manutenção — Janeiro 2024\"
    }" | jq -r '.transaction_id')
success "Fee: $TF"

# ------------------------------------------------------------------------------
# 5. CENÁRIOS DE ERRO para testar validações
# ------------------------------------------------------------------------------

echo -e "\n${BOLD}5. Cenários de erro (espera-se falha)${RESET}"

log "  Tentativa de transfer sem saldo (Bob -> Corp: \$9.999)"
HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
    -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-fail-001" \
    -d "{
        \"transaction_type\": \"Transfer\",
        \"amount\": \"9999.00\",
        \"currency\": \"Usd\",
        \"source_account_id\": \"$BOB\",
        \"dest_account_id\": \"$CORP\",
        \"description\": \"Deve falhar por saldo insuficiente\"
    }" 2>&1 || echo "")
# Status 409 é esperado aqui; o worker recusará com InsufficientFunds
success "Transação submetida (será rejeitada pelo worker com 409 no processamento)"

log "  Replay da mesma transferência (idempotência) — Alice -> Corp \$1.500"
REPLAY=$(curl -sf -X POST "$BASE/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: seed-api-transfer-001" \
    -d "{
        \"transaction_type\": \"Transfer\",
        \"amount\": \"1500.00\",
        \"currency\": \"Usd\",
        \"source_account_id\": \"$ALICE\",
        \"dest_account_id\": \"$CORP\",
        \"description\": \"Pagamento fatura #INV-2024-001\",
        \"metadata\": {\"invoice_id\": \"INV-2024-001\"}
    }")
WAS_DUP=$(echo "$REPLAY" | jq -r '.was_duplicate')
if [ "$WAS_DUP" = "true" ]; then
    success "Idempotência OK: was_duplicate=true"
else
    warn "Esperado was_duplicate=true, recebi: $REPLAY"
fi

# ------------------------------------------------------------------------------
# RESUMO
# ------------------------------------------------------------------------------

echo ""
echo -e "${BOLD}==============================${RESET}"
echo -e "${BOLD}  Seed concluído!${RESET}"
echo -e "${BOLD}==============================${RESET}"
echo ""
echo "  IDs criados:"
echo "    Alice    : $ALICE"
echo "    Bob      : $BOB"
echo "    Corp     : $CORP"
echo "    Merchant : $MERCHANT"
echo ""
echo "  Transações submetidas: 8"
echo "  As transações serão processadas pelo worker em segundos."
echo ""
echo -e "${CYAN}Verifique o estado com:${RESET}"
echo "  make stats          — resumo do banco"
echo "  make balances       — saldos das contas"
echo "  make reconcile      — consistência ledger vs saldo"
echo "  make audit-log      — últimas entradas do audit log"
echo ""
echo -e "${CYAN}Relatórios:${RESET}"
echo "  curl http://localhost:8082/v1/reports/daily"
echo "  curl http://localhost:8082/v1/reports/high-value"
echo "  curl http://localhost:8082/v1/reports/ctr"
echo ""
