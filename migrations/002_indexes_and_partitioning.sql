-- Performance-oriented migrations applied after initial schema
-- Run these on a schedule as data grows, not at startup

-- Partial index: pending transactions for worker polling
-- Covered index so the worker query hits index-only scan
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_pending_full
    ON transactions (submitted_at ASC)
    INCLUDE (id, amount, currency, source_account_id, dest_account_id, version, retry_count)
    WHERE status = 'PENDING';

-- For account statement queries (common read pattern)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_account_range
    ON ledger_entries (account_id, posted_at DESC)
    INCLUDE (entry_type, amount, running_balance, transaction_id);

-- For regulatory reporting: settled transactions in a time window
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_settled_window
    ON transactions (settled_at DESC, currency)
    INCLUDE (id, amount, source_account_id, dest_account_id)
    WHERE status = 'SETTLED';

-- Bloom filter for high-cardinality idempotency key lookups (optional, PostgreSQL 9.6+)
-- CREATE EXTENSION IF NOT EXISTS bloom;
-- CREATE INDEX idx_transactions_idem_bloom ON transactions USING bloom (idempotency_key);
