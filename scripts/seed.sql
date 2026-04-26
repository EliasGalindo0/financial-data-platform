-- ==============================================================================
-- Seed de dados iniciais
-- Popula o banco com contas e transações de exemplo para testes manuais.
--
-- Uso:
--   psql $DATABASE_URL -f scripts/seed.sql
--   ou:
--   make seed
--
-- O seed é idempotente: usa ON CONFLICT DO NOTHING.
-- Pode ser executado múltiplas vezes sem duplicar dados.
-- ==============================================================================

BEGIN;

-- ==============================================================================
-- 1. CONTAS DE TESTE
-- ==============================================================================
-- Usamos UUIDs fixos para facilitar referência nos testes manuais.
-- Copie esses IDs nas requisições de teste ou use make seed-api.

INSERT INTO accounts (id, external_id, owner_id, currency, balance, is_active)
VALUES
    -- Conta principal — Alice (USD, saldo alto para testar transfers)
    ('00000000-0000-0000-0000-000000000001',
     'ACC-ALICE-USD', 'user-alice', 'USD', 50000.0000, true),

    -- Conta secundária — Alice em EUR
    ('00000000-0000-0000-0000-000000000002',
     'ACC-ALICE-EUR', 'user-alice', 'EUR', 10000.0000, true),

    -- Conta — Bob (USD, saldo baixo para testar insufficient funds)
    ('00000000-0000-0000-0000-000000000003',
     'ACC-BOB-USD', 'user-bob', 'USD', 500.0000, true),

    -- Conta — Corp (conta corporativa para receber pagamentos)
    ('00000000-0000-0000-0000-000000000004',
     'ACC-CORP-USD', 'org-acme-corp', 'USD', 100000.0000, true),

    -- Conta — Merchant (loja online)
    ('00000000-0000-0000-0000-000000000005',
     'ACC-MERCHANT-USD', 'org-shop', 'USD', 25000.0000, true),

    -- Conta inativa — para testar rejeição de transações
    ('00000000-0000-0000-0000-000000000006',
     'ACC-INACTIVE', 'user-charlie', 'USD', 0.0000, false),

    -- Conta — Dave (GBP)
    ('00000000-0000-0000-0000-000000000007',
     'ACC-DAVE-GBP', 'user-dave', 'GBP', 8000.0000, true),

    -- Conta — Eve (USD, para testes de fraude — sem saldo)
    ('00000000-0000-0000-0000-000000000008',
     'ACC-EVE-USD', 'user-eve', 'USD', 0.0000, true)

ON CONFLICT (external_id) DO NOTHING;


-- ==============================================================================
-- 2. TRANSAÇÕES JÁ PROCESSADAS (status = SETTLED)
-- Representa histórico de transações passadas com ledger já postado.
-- ==============================================================================

-- Depósito inicial na conta de Alice
INSERT INTO transactions
    (id, idempotency_key, transaction_type, status, amount, currency,
     dest_account_id, description, request_hash, submitted_at, processed_at, settled_at,
     version, compliance_checked, risk_score)
VALUES
    ('10000000-0000-0000-0000-000000000001',
     'seed-txn-001', 'CREDIT', 'SETTLED', 50000.0000, 'USD',
     '00000000-0000-0000-0000-000000000001',
     'Depósito inicial — Alice USD',
     'seed-hash-001',
     NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days',
     2, true, 0.0100)
ON CONFLICT (idempotency_key) DO NOTHING;

-- Depósito inicial — Bob
INSERT INTO transactions
    (id, idempotency_key, transaction_type, status, amount, currency,
     dest_account_id, description, request_hash, submitted_at, processed_at, settled_at,
     version, compliance_checked, risk_score)
VALUES
    ('10000000-0000-0000-0000-000000000002',
     'seed-txn-002', 'CREDIT', 'SETTLED', 500.0000, 'USD',
     '00000000-0000-0000-0000-000000000003',
     'Depósito inicial — Bob USD',
     'seed-hash-002',
     NOW() - INTERVAL '25 days', NOW() - INTERVAL '25 days', NOW() - INTERVAL '25 days',
     2, true, 0.0100)
ON CONFLICT (idempotency_key) DO NOTHING;

-- Transfer: Alice -> Corp (pagamento de fatura)
INSERT INTO transactions
    (id, idempotency_key, transaction_type, status, amount, currency,
     source_account_id, dest_account_id, description, request_hash,
     submitted_at, processed_at, settled_at, version, compliance_checked, risk_score)
VALUES
    ('10000000-0000-0000-0000-000000000003',
     'seed-txn-003', 'TRANSFER', 'SETTLED', 1500.0000, 'USD',
     '00000000-0000-0000-0000-000000000001',
     '00000000-0000-0000-0000-000000000004',
     'Pagamento fatura #INV-2024-001',
     'seed-hash-003',
     NOW() - INTERVAL '20 days', NOW() - INTERVAL '20 days', NOW() - INTERVAL '20 days',
     2, true, 0.1000)
ON CONFLICT (idempotency_key) DO NOTHING;

-- Transfer: Alice -> Merchant (compra online)
INSERT INTO transactions
    (id, idempotency_key, transaction_type, status, amount, currency,
     source_account_id, dest_account_id, description, request_hash,
     submitted_at, processed_at, settled_at, version, compliance_checked, risk_score)
VALUES
    ('10000000-0000-0000-0000-000000000004',
     'seed-txn-004', 'TRANSFER', 'SETTLED', 299.99, 'USD',
     '00000000-0000-0000-0000-000000000001',
     '00000000-0000-0000-0000-000000000005',
     'Compra online — Pedido #ORD-9921',
     'seed-hash-004',
     NOW() - INTERVAL '15 days', NOW() - INTERVAL '15 days', NOW() - INTERVAL '15 days',
     2, true, 0.0500)
ON CONFLICT (idempotency_key) DO NOTHING;

-- Fee cobrada de Alice
INSERT INTO transactions
    (id, idempotency_key, transaction_type, status, amount, currency,
     source_account_id, description, request_hash,
     submitted_at, processed_at, settled_at, version, compliance_checked, risk_score)
VALUES
    ('10000000-0000-0000-0000-000000000005',
     'seed-txn-005', 'FEE', 'SETTLED', 9.90, 'USD',
     '00000000-0000-0000-0000-000000000001',
     'Taxa de manutenção mensal',
     'seed-hash-005',
     NOW() - INTERVAL '10 days', NOW() - INTERVAL '10 days', NOW() - INTERVAL '10 days',
     2, true, 0.0100)
ON CONFLICT (idempotency_key) DO NOTHING;

-- Transação FAILED (saldo insuficiente simulado)
INSERT INTO transactions
    (id, idempotency_key, transaction_type, status, amount, currency,
     source_account_id, dest_account_id, description, request_hash,
     submitted_at, processed_at, version, retry_count, last_error,
     compliance_checked, risk_score)
VALUES
    ('10000000-0000-0000-0000-000000000006',
     'seed-txn-006', 'TRANSFER', 'FAILED', 9999.00, 'USD',
     '00000000-0000-0000-0000-000000000003',  -- Bob só tem 500
     '00000000-0000-0000-0000-000000000004',
     'Tentativa de transferência com saldo insuficiente',
     'seed-hash-006',
     NOW() - INTERVAL '7 days', NOW() - INTERVAL '7 days',
     2, 0, 'insufficient_funds: required=9999.00, available=500.00',
     true, 0.3000)
ON CONFLICT (idempotency_key) DO NOTHING;

-- Transação de alto valor (será reportada no CTR)
INSERT INTO transactions
    (id, idempotency_key, transaction_type, status, amount, currency,
     source_account_id, dest_account_id, description, request_hash,
     submitted_at, processed_at, settled_at, version, compliance_checked, risk_score)
VALUES
    ('10000000-0000-0000-0000-000000000007',
     'seed-txn-007', 'TRANSFER', 'SETTLED', 15000.0000, 'USD',
     '00000000-0000-0000-0000-000000000001',
     '00000000-0000-0000-0000-000000000004',
     'Pagamento de contrato — alto valor',
     'seed-hash-007',
     NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days',
     2, true, 0.4000)
ON CONFLICT (idempotency_key) DO NOTHING;

-- Transação pendente (ainda não processada pelo worker)
INSERT INTO transactions
    (id, idempotency_key, transaction_type, status, amount, currency,
     source_account_id, dest_account_id, description, request_hash,
     submitted_at, version, compliance_checked)
VALUES
    ('10000000-0000-0000-0000-000000000008',
     'seed-txn-008', 'TRANSFER', 'PENDING', 200.0000, 'USD',
     '00000000-0000-0000-0000-000000000001',
     '00000000-0000-0000-0000-000000000003',
     'Transfer pendente — aguardando processamento',
     'seed-hash-008',
     NOW(), 1, false)
ON CONFLICT (idempotency_key) DO NOTHING;


-- ==============================================================================
-- 3. LEDGER ENTRIES para as transações SETTLED
-- (em produção são escritas pelo worker; aqui inserimos direto para consistência)
-- ==============================================================================

-- CREDIT: Depósito Alice (txn-001) — só crédito, sem contrapartida externa
INSERT INTO ledger_entries
    (id, transaction_id, account_id, entry_type, amount, currency, running_balance, posted_at, sequence_num)
VALUES
    ('20000000-0000-0000-0000-000000000001',
     '10000000-0000-0000-0000-000000000001',
     '00000000-0000-0000-0000-000000000001',
     'CREDIT', 50000.0000, 'USD', 50000.0000,
     NOW() - INTERVAL '30 days', 1)
ON CONFLICT DO NOTHING;

-- CREDIT: Depósito Bob (txn-002)
INSERT INTO ledger_entries
    (id, transaction_id, account_id, entry_type, amount, currency, running_balance, posted_at, sequence_num)
VALUES
    ('20000000-0000-0000-0000-000000000002',
     '10000000-0000-0000-0000-000000000002',
     '00000000-0000-0000-0000-000000000003',
     'CREDIT', 500.0000, 'USD', 500.0000,
     NOW() - INTERVAL '25 days', 1)
ON CONFLICT DO NOTHING;

-- TRANSFER Alice -> Corp (txn-003): DEBIT Alice, CREDIT Corp
INSERT INTO ledger_entries
    (id, transaction_id, account_id, entry_type, amount, currency, running_balance, posted_at, sequence_num)
VALUES
    ('20000000-0000-0000-0000-000000000003',
     '10000000-0000-0000-0000-000000000003',
     '00000000-0000-0000-0000-000000000001',
     'DEBIT', 1500.0000, 'USD', 48500.0000,
     NOW() - INTERVAL '20 days', 2),

    ('20000000-0000-0000-0000-000000000004',
     '10000000-0000-0000-0000-000000000003',
     '00000000-0000-0000-0000-000000000004',
     'CREDIT', 1500.0000, 'USD', 1500.0000,
     NOW() - INTERVAL '20 days', 1)
ON CONFLICT DO NOTHING;

-- TRANSFER Alice -> Merchant (txn-004)
INSERT INTO ledger_entries
    (id, transaction_id, account_id, entry_type, amount, currency, running_balance, posted_at, sequence_num)
VALUES
    ('20000000-0000-0000-0000-000000000005',
     '10000000-0000-0000-0000-000000000004',
     '00000000-0000-0000-0000-000000000001',
     'DEBIT', 299.9900, 'USD', 48200.0100,
     NOW() - INTERVAL '15 days', 3),

    ('20000000-0000-0000-0000-000000000006',
     '10000000-0000-0000-0000-000000000004',
     '00000000-0000-0000-0000-000000000005',
     'CREDIT', 299.9900, 'USD', 299.9900,
     NOW() - INTERVAL '15 days', 1)
ON CONFLICT DO NOTHING;

-- FEE de Alice (txn-005)
INSERT INTO ledger_entries
    (id, transaction_id, account_id, entry_type, amount, currency, running_balance, posted_at, sequence_num)
VALUES
    ('20000000-0000-0000-0000-000000000007',
     '10000000-0000-0000-0000-000000000005',
     '00000000-0000-0000-0000-000000000001',
     'DEBIT', 9.9000, 'USD', 48190.1100,
     NOW() - INTERVAL '10 days', 4)
ON CONFLICT DO NOTHING;

-- TRANSFER alto valor Alice -> Corp (txn-007)
INSERT INTO ledger_entries
    (id, transaction_id, account_id, entry_type, amount, currency, running_balance, posted_at, sequence_num)
VALUES
    ('20000000-0000-0000-0000-000000000008',
     '10000000-0000-0000-0000-000000000007',
     '00000000-0000-0000-0000-000000000001',
     'DEBIT', 15000.0000, 'USD', 33190.1100,
     NOW() - INTERVAL '2 days', 5),

    ('20000000-0000-0000-0000-000000000009',
     '10000000-0000-0000-0000-000000000007',
     '00000000-0000-0000-0000-000000000004',
     'CREDIT', 15000.0000, 'USD', 16500.0000,
     NOW() - INTERVAL '2 days', 2)
ON CONFLICT DO NOTHING;


-- ==============================================================================
-- 4. ATUALIZA SALDOS para refletir as ledger entries
-- ==============================================================================

UPDATE accounts SET balance = 33190.1100, version = version + 1
WHERE id = '00000000-0000-0000-0000-000000000001';  -- Alice

UPDATE accounts SET balance = 16500.0000, version = version + 1
WHERE id = '00000000-0000-0000-0000-000000000004';  -- Corp

UPDATE accounts SET balance = 25299.9900, version = version + 1
WHERE id = '00000000-0000-0000-0000-000000000005';  -- Merchant


-- ==============================================================================
-- 5. AUDIT LOG (registro das operações do seed)
-- ==============================================================================

INSERT INTO audit_log
    (entity_type, entity_id, action, old_state, new_state, actor_id, actor_type, correlation_id)
VALUES
    ('Account', '00000000-0000-0000-0000-000000000001',
     'SEED_CREATED', NULL,
     '{"external_id":"ACC-ALICE-USD","balance":33190.11}',
     'seed-script', 'SYSTEM', gen_random_uuid()),

    ('Account', '00000000-0000-0000-0000-000000000003',
     'SEED_CREATED', NULL,
     '{"external_id":"ACC-BOB-USD","balance":500.00}',
     'seed-script', 'SYSTEM', gen_random_uuid()),

    ('Account', '00000000-0000-0000-0000-000000000004',
     'SEED_CREATED', NULL,
     '{"external_id":"ACC-CORP-USD","balance":16500.00}',
     'seed-script', 'SYSTEM', gen_random_uuid());


-- ==============================================================================
-- 6. OUTBOX — evento pendente para a txn-008 (PENDING)
-- O relay vai publicá-lo no Kafka quando os serviços subirem.
-- ==============================================================================

INSERT INTO outbox_events
    (aggregate_type, aggregate_id, event_type, payload, topic, partition_key, status)
VALUES
    ('Transaction', '10000000-0000-0000-0000-000000000008',
     'TransactionSubmitted',
     '{
       "event_id": "30000000-0000-0000-0000-000000000001",
       "event_type": "TransactionSubmitted",
       "aggregate_type": "Transaction",
       "aggregate_id": "10000000-0000-0000-0000-000000000008",
       "aggregate_version": 1,
       "correlation_id": "30000000-0000-0000-0000-000000000002",
       "occurred_at": "2024-01-15T10:00:00Z",
       "payload": {
         "transaction_id": "10000000-0000-0000-0000-000000000008",
         "idempotency_key": "seed-txn-008",
         "transaction_type": "Transfer",
         "amount": "200.0000",
         "currency": "Usd",
         "source_account_id": "00000000-0000-0000-0000-000000000001",
         "dest_account_id": "00000000-0000-0000-0000-000000000003"
       }
     }',
     'financial.transactions.v1',
     '00000000-0000-0000-0000-000000000001',  -- partition by source account
     'PENDING')
ON CONFLICT DO NOTHING;


COMMIT;


-- ==============================================================================
-- RESUMO: exibe o estado do banco após o seed
-- ==============================================================================

SELECT '=== CONTAS ===' AS "---";
SELECT external_id, owner_id, currency, balance, is_active
FROM accounts
ORDER BY external_id;

SELECT '=== TRANSAÇÕES ===' AS "---";
SELECT
    SUBSTRING(id::text, 1, 8) || '...' AS id,
    transaction_type,
    status,
    amount,
    currency,
    SUBSTRING(description, 1, 40) AS description
FROM transactions
ORDER BY submitted_at;

SELECT '=== LEDGER ENTRIES ===' AS "---";
SELECT
    SUBSTRING(account_id::text, 1, 8) || '...' AS account,
    entry_type,
    amount,
    running_balance,
    posted_at::date
FROM ledger_entries
ORDER BY account_id, sequence_num;
