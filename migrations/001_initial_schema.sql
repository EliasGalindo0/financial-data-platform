-- =============================================================================
-- Financial Data Platform — Initial Schema
-- Design principles:
--   1. UUIDs (v7 = time-sortable, index-friendly) as primary keys
--   2. All monetary values stored as NUMERIC(19,4) — never FLOAT
--   3. Append-only audit_log table — never UPDATE or DELETE
--   4. Optimistic locking via `version` columns
--   5. Outbox pattern for reliable event publishing
-- =============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";    -- gen_random_uuid fallback
CREATE EXTENSION IF NOT EXISTS "btree_gist";  -- for exclusion constraints

-- =============================================================================
-- ENUM TYPES
-- =============================================================================

CREATE TYPE transaction_status AS ENUM (
    'PENDING',       -- received, not yet processed
    'PROCESSING',    -- worker has locked and is processing
    'SETTLED',       -- fully processed and posted to ledger
    'FAILED',        -- processing failed (non-retryable)
    'REVERSED'       -- reversal applied
);

CREATE TYPE transaction_type AS ENUM (
    'CREDIT',
    'DEBIT',
    'TRANSFER',
    'FEE',
    'REVERSAL',
    'ADJUSTMENT'
);

CREATE TYPE currency_code AS ENUM (
    'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD'
    -- Extend as needed; kept short for clarity
);

CREATE TYPE event_status AS ENUM (
    'PENDING',
    'PUBLISHED',
    'FAILED'
);

-- =============================================================================
-- ACCOUNTS
-- =============================================================================

CREATE TABLE accounts (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id     TEXT            NOT NULL UNIQUE,         -- client-facing ID
    owner_id        TEXT            NOT NULL,                -- user/entity ID
    currency        currency_code   NOT NULL,
    balance         NUMERIC(19, 4)  NOT NULL DEFAULT 0,
    version         BIGINT          NOT NULL DEFAULT 1,      -- optimistic lock
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Ensure balance never goes negative (enforced at DB level as safety net)
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_non_negative
    CHECK (balance >= 0);

CREATE INDEX idx_accounts_owner_id ON accounts (owner_id);
CREATE INDEX idx_accounts_external_id ON accounts (external_id);

-- =============================================================================
-- TRANSACTIONS  (write side — CQRS command model)
-- =============================================================================

CREATE TABLE transactions (
    id                  UUID                PRIMARY KEY,
    idempotency_key     TEXT                NOT NULL,        -- client-supplied
    transaction_type    transaction_type    NOT NULL,
    status              transaction_status  NOT NULL DEFAULT 'PENDING',

    -- Monetary fields
    amount              NUMERIC(19, 4)      NOT NULL,
    currency            currency_code       NOT NULL,

    -- Parties
    source_account_id   UUID                REFERENCES accounts(id),
    dest_account_id     UUID                REFERENCES accounts(id),

    -- Metadata
    description         TEXT,
    metadata            JSONB               NOT NULL DEFAULT '{}',

    -- Idempotency & dedup
    request_hash        TEXT                NOT NULL,        -- SHA-256(canonical request body)

    -- Lifecycle
    submitted_at        TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    processed_at        TIMESTAMPTZ,
    settled_at          TIMESTAMPTZ,

    -- Optimistic locking
    version             BIGINT              NOT NULL DEFAULT 1,

    -- Worker tracking
    worker_id           TEXT,
    retry_count         INT                 NOT NULL DEFAULT 0,
    last_error          TEXT,

    -- Compliance
    regulatory_flags    JSONB               NOT NULL DEFAULT '{}',
    risk_score          NUMERIC(5, 4),                      -- 0.0000 – 1.0000
    compliance_checked  BOOLEAN             NOT NULL DEFAULT FALSE
);

-- Critical: idempotency_key per source account must be unique
-- This prevents double-charges even if the client retries
CREATE UNIQUE INDEX idx_transactions_idempotency
    ON transactions (idempotency_key)
    WHERE status != 'REVERSED';

CREATE INDEX idx_transactions_status ON transactions (status)
    WHERE status IN ('PENDING', 'PROCESSING');

CREATE INDEX idx_transactions_submitted_at
    ON transactions (submitted_at DESC);

CREATE INDEX idx_transactions_source_account
    ON transactions (source_account_id, submitted_at DESC);

-- Monetary sanity
ALTER TABLE transactions ADD CONSTRAINT transactions_amount_positive
    CHECK (amount > 0);

-- =============================================================================
-- LEDGER ENTRIES  (immutable — event-sourced double-entry bookkeeping)
-- Purpose: source of truth for account balance reconciliation
-- Rule: for every transaction, debits = credits
-- =============================================================================

CREATE TABLE ledger_entries (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  UUID            NOT NULL REFERENCES transactions(id),
    account_id      UUID            NOT NULL REFERENCES accounts(id),
    entry_type      TEXT            NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount          NUMERIC(19, 4)  NOT NULL CHECK (amount > 0),
    currency        currency_code   NOT NULL,
    running_balance NUMERIC(19, 4)  NOT NULL,               -- snapshot at time of entry
    posted_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    sequence_num    BIGINT          NOT NULL                 -- monotonic per account
);

-- Immutability enforced via trigger (see below) + revoke UPDATE/DELETE from app role
CREATE UNIQUE INDEX idx_ledger_sequence
    ON ledger_entries (account_id, sequence_num);

CREATE INDEX idx_ledger_transaction
    ON ledger_entries (transaction_id);

CREATE INDEX idx_ledger_account_posted
    ON ledger_entries (account_id, posted_at DESC);

-- =============================================================================
-- AUDIT LOG  (compliance-grade immutable log)
-- Regulatory requirement: every state change must be recorded permanently
-- NEVER update or delete rows here — this is the regulatory record
-- =============================================================================

CREATE TABLE audit_log (
    id              BIGSERIAL       PRIMARY KEY,             -- sequential for ordering
    entity_type     TEXT            NOT NULL,
    entity_id       UUID            NOT NULL,
    action          TEXT            NOT NULL,
    old_state       JSONB,
    new_state       JSONB           NOT NULL,
    actor_id        TEXT            NOT NULL,               -- who triggered this
    actor_type      TEXT            NOT NULL,               -- 'USER', 'SYSTEM', 'WORKER'
    correlation_id  UUID            NOT NULL,               -- trace ID for cross-service correlation
    ip_address      INET,
    user_agent      TEXT,
    occurred_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_entity ON audit_log (entity_type, entity_id, occurred_at DESC);
CREATE INDEX idx_audit_correlation ON audit_log (correlation_id);
CREATE INDEX idx_audit_occurred_at ON audit_log (occurred_at DESC);

-- Partition by month for large-scale deployments
-- ALTER TABLE audit_log PARTITION BY RANGE (occurred_at);
-- CREATE TABLE audit_log_2024_01 PARTITION OF audit_log
--     FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- =============================================================================
-- OUTBOX (transactional outbox pattern)
-- Written in the SAME transaction as the business entity.
-- A separate relay process publishes to Kafka and marks PUBLISHED.
-- This guarantees at-least-once delivery without distributed transactions.
-- =============================================================================

CREATE TABLE outbox_events (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  TEXT            NOT NULL,               -- 'Transaction', 'Account'
    aggregate_id    UUID            NOT NULL,
    event_type      TEXT            NOT NULL,               -- 'TransactionSubmitted', etc.
    payload         JSONB           NOT NULL,
    status          event_status    NOT NULL DEFAULT 'PENDING',
    topic           TEXT            NOT NULL,               -- Kafka topic
    partition_key   TEXT            NOT NULL,               -- Usually account_id for ordering
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,
    retry_count     INT             NOT NULL DEFAULT 0,
    last_error      TEXT
);

CREATE INDEX idx_outbox_pending
    ON outbox_events (created_at ASC)
    WHERE status = 'PENDING';

-- =============================================================================
-- DEAD-LETTER QUEUE  (failed messages for manual review)
-- =============================================================================

CREATE TABLE dead_letter_queue (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    source_topic    TEXT            NOT NULL,
    partition_num   INT,
    offset_num      BIGINT,
    payload         JSONB           NOT NULL,
    error_message   TEXT            NOT NULL,
    error_count     INT             NOT NULL DEFAULT 1,
    first_failed_at TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    last_failed_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    resolved_by     TEXT
);

CREATE INDEX idx_dlq_unresolved ON dead_letter_queue (first_failed_at)
    WHERE resolved_at IS NULL;

-- =============================================================================
-- REGULATORY REPORTS
-- =============================================================================

CREATE TABLE regulatory_reports (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    report_type     TEXT            NOT NULL,               -- 'SAR', 'CTR', 'DAILY_SUMMARY'
    report_period   DATERANGE       NOT NULL,
    status          TEXT            NOT NULL DEFAULT 'DRAFT',
    content         JSONB           NOT NULL DEFAULT '{}',
    file_path       TEXT,                                   -- S3/GCS path of generated file
    generated_by    TEXT,
    generated_at    TIMESTAMPTZ,
    submitted_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Prevent updates/deletes on audit_log (belt + suspenders alongside app-level revoke)
CREATE OR REPLACE FUNCTION prevent_audit_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is immutable — modification not allowed';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_log_immutable
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION prevent_audit_modification();

-- Prevent updates/deletes on ledger_entries
CREATE OR REPLACE FUNCTION prevent_ledger_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entries is immutable — modification not allowed';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_entries_immutable
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION prevent_ledger_modification();

-- =============================================================================
-- ROLES & PERMISSIONS  (principle of least privilege)
-- =============================================================================

-- Application role: can read/write business tables, cannot modify audit/ledger
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_rw') THEN
        CREATE ROLE app_rw;
    END IF;
END $$;

GRANT SELECT, INSERT, UPDATE ON accounts TO app_rw;
GRANT SELECT, INSERT, UPDATE ON transactions TO app_rw;
GRANT SELECT, INSERT ON ledger_entries TO app_rw;   -- INSERT only, no UPDATE/DELETE
GRANT SELECT, INSERT ON audit_log TO app_rw;         -- INSERT only
GRANT SELECT, INSERT, UPDATE ON outbox_events TO app_rw;
GRANT SELECT, INSERT ON dead_letter_queue TO app_rw;
GRANT SELECT, INSERT, UPDATE ON regulatory_reports TO app_rw;

-- Reporting role: read-only
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'reporting_ro') THEN
        CREATE ROLE reporting_ro;
    END IF;
END $$;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO reporting_ro;
