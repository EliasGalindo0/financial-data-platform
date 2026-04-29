# Financial Data Platform

A production-grade financial data processing platform built in **Rust** with **PostgreSQL**. Designed for high-throughput, fault-tolerant transaction ingestion, async processing, regulatory reporting, and full auditability.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Components](#components)
  - [Data Flow](#data-flow)
  - [Key Patterns](#key-patterns)
- [Data Model](#data-model)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [justfile — Commands](#justfile--commands)
- [Running the Platform](#running-the-platform)
  - [Option A — Docker Compose (recommended)](#option-a--docker-compose-recommended)
  - [Option B — Local development](#option-b--local-development)
  - [Data seeding](#data-seeding)
- [API Reference](#api-reference)
  - [Accounts](#accounts)
  - [Transactions](#transactions)
  - [Reports](#reports)
- [Configuration](#configuration)
- [Observability](#observability)
- [Design Decisions](#design-decisions)
  - [Idempotency](#idempotency)
  - [Outbox Pattern](#outbox-pattern)
  - [CQRS](#cqrs)
  - [Event Sourcing (Ledger)](#event-sourcing-ledger)
  - [Consistency vs Performance](#consistency-vs-performance)
- [Scaling Strategy](#scaling-strategy)
- [Fault Tolerance](#fault-tolerance)
- [Production Checklist](#production-checklist)

---

![Platform running — infra up, schema migrated, live transaction data](docs/images/platform-running.svg)

---

## Overview

This platform ingests financial transactions via a REST API, processes them asynchronously through Kafka-backed workers, maintains a double-entry ledger, enforces idempotency, and generates regulatory reports. Every state change is recorded in an immutable audit log.

**Tech stack:**

| Concern | Technology |
|---|---|
| Language | Rust 1.75+ |
| Web framework | Axum 0.7 |
| Database | PostgreSQL 16 |
| Message broker | Apache Kafka (KRaft mode) |
| Cache / rate limiting | Redis 7 |
| Async runtime | Tokio |
| DB access | sqlx (compile-time checked queries) |
| Observability | OpenTelemetry + Jaeger + Prometheus + Grafana |
| Containerization | Docker + Docker Compose |

---

## Architecture

```
+----------------------------------------------------------------------+
|                          CLIENT LAYER                                 |
|          Mobile / Backend / Payment Processors / Banks               |
+-----------------------------+----------------------------------------+
                              | HTTPS + mTLS
+-----------------------------v----------------------------------------+
|                      INGESTION SERVICE  (Rust/Axum)                  |
|  +--------------+  +-------------------+  +------------------------+ |
|  |  Rate Limit  |  |  Idempotency Gate |  |  Schema Validation     | |
|  |  (token bkt) |  |  (idempotency_key)|  |  (serde + validator)   | |
|  +--------------+  +-------------------+  +------------------------+ |
|                          | atomic DB write                            |
|  +-----------------------------------------------------------------+ |
|  |   OUTBOX WRITER -- INSERT transaction + INSERT outbox_event     | |
|  |                    (same PostgreSQL transaction)                 | |
|  +-----------------------------------------------------------------+ |
+-----------------------------+----------------------------------------+
                              |
         +--------------------+---------------------------+
         |                    |                          |
+--------v------+    +--------v--------+    +-----------v------------------+
|  PostgreSQL   |    |   Kafka         |    |   Redis                      |
|  (source of   |<---|   (event bus)   |    |   idempotency cache          |
|   truth)      |    |                 |    |   + rate limits              |
+--------+------+    +--------+--------+    +------------------------------+
         |                    |
         |         +----------v----------------------------------------------+
         |         |         PROCESSING WORKERS  (Rust/Tokio)                |
         |         |  +----------------------------------------------------+ |
         |         |  |  State Machine: PENDING -> PROCESSING -> SETTLED   | |
         |         |  |  Advisory locks (pg_try_advisory_lock)              | |
         |         |  |  Optimistic locking (version column)               | |
         |         |  |  Exponential backoff retries                       | |
         |         |  +----------------------------------------------------+ |
         |         |                |                                         |
         |         |     +----------+-----------+                            |
         |         |     |                      |                            |
         |         |  +--v-----------+  +-------v-----------+               |
         |         |  | Fraud Check  |  |  Ledger Posting   |               |
         |         |  | (rules eng)  |  |  (double-entry)   |               |
         |         |  +--------------+  +-------------------+               |
         |         +----------------------------------------------------------+
         |                    |
         |         +----------v----------------------------------------------+
         |         |              DEAD-LETTER QUEUE                          |
         |         |   Failed after N retries -> DLQ -> alert + manual      |
         |         +----------------------------------------------------------+
         |
+--------v------------------------------------------------------------------+
|                    READ MODEL  (CQRS query side)                          |
|   +-------------------+    +----------------------------------+           |
|   |  Reporting Service |    |       Audit Log Service          |          |
|   |  (regulatory RPTs) |    |  (immutable append-only ledger)  |          |
|   +-------------------+    +----------------------------------+           |
+---------------------------------------------------------------------------+
```

### Components

| Service | Port | Responsibility |
|---|---|---|
| `ingestion` | 8080 | REST API — receives transactions, validates, writes to DB + outbox |
| `worker` | — | Kafka consumer — processes transactions, posts to ledger |
| `outbox-relay` | — | Polls outbox table, publishes events to Kafka reliably |
| `reporting` | 8082 | Read-only query service — regulatory reports, summaries |
| PostgreSQL | 5432 | Primary data store — transactions, ledger, audit log, outbox |
| Kafka | 9092 | Event bus — decouples ingestion from processing |
| Redis | 6379 | Idempotency cache (hot path) + rate limiting |
| Prometheus | 9090 | Metrics scraping |
| Grafana | 3000 | Dashboards |
| Jaeger | 16686 | Distributed tracing UI |

### Data Flow

```
1.  Client sends POST /v1/transactions with Idempotency-Key header
2.  Ingestion service validates request body (schema + business rules)
3.  Idempotency gate: check Redis cache (fast path) -> DB unique key (authoritative)
4.  If new: INSERT transaction + INSERT outbox_event in a single DB transaction
5.  Return 202 Accepted with transaction_id
6.  Outbox relay polls outbox_events table, publishes to Kafka, marks PUBLISHED
7.  Worker consumes from Kafka:
      a. Acquire advisory lock on transaction_id
      b. Load transaction with SELECT FOR UPDATE
      c. Check idempotency (already SETTLED? skip safely)
      d. Mark PROCESSING (optimistic lock check)
      e. Run fraud check
      f. Execute business logic (debit/credit/transfer)
      g. Write ledger entries (double-entry, same DB transaction)
      h. Write audit log entry (same DB transaction)
      i. Mark SETTLED, commit
      j. Release advisory lock, commit Kafka offset
8.  Reporting service queries ledger + transactions for regulatory reports
```

### Key Patterns

| Pattern | Where Applied | Why |
|---|---|---|
| **Outbox** | Ingestion -> Kafka | Eliminates dual-write problem. Event only exits the DB after commit. |
| **Idempotency Key** | API gateway + DB | Clients can retry safely; deduplication is authoritative at DB level. |
| **CQRS** | Write (ingestion/worker) vs Read (reporting) | Write path optimized for ACID; read path for aggregations without locks. |
| **Event Sourcing** | Ledger entries | Every financial fact is appended — never updated. Full balance reconstruction possible. |
| **State Machine** | Transaction lifecycle | Explicit transitions prevent invalid states (e.g., SETTLED -> PROCESSING). |
| **Advisory Locks** | Worker per transaction | Prevents concurrent processing of the same transaction across worker instances. |
| **Optimistic Locking** | Transaction + Account updates | `version` column detects concurrent writes; retried automatically. |
| **Dead-Letter Queue** | Worker failures | Poison messages don't block the stream; held for manual review. |
| **Double-Entry Bookkeeping** | Ledger | Every credit has a matching debit; balance integrity verifiable at any time. |

---

## Data Model

```
accounts
  id (UUID v7)
  external_id  (UNIQUE)
  owner_id
  currency     (USD | EUR | GBP | ...)
  balance      (NUMERIC 19,4)  <- materialized projection of ledger
  version      (optimistic lock)
  is_active

transactions
  id (UUID v7, time-sortable)
  idempotency_key  (UNIQUE where status != REVERSED)
  transaction_type (CREDIT | DEBIT | TRANSFER | FEE | ...)
  status           (PENDING -> PROCESSING -> SETTLED | FAILED)
  amount           (NUMERIC 19,4)
  currency
  source_account_id -> accounts(id)
  dest_account_id   -> accounts(id)
  request_hash      (SHA-256 of canonical body -- detects key reuse)
  version           (optimistic lock)
  retry_count
  risk_score

ledger_entries  (IMMUTABLE -- trigger prevents UPDATE/DELETE)
  id
  transaction_id -> transactions(id)
  account_id     -> accounts(id)
  entry_type     (DEBIT | CREDIT)
  amount         (NUMERIC 19,4)
  running_balance (snapshot at time of posting)
  sequence_num    (monotonic per account -- ordering guarantee)

audit_log  (IMMUTABLE -- trigger prevents UPDATE/DELETE)
  id        (BIGSERIAL -- sequential, never gaps)
  entity_type / entity_id
  action    (SUBMITTED | PROCESSING | SETTLED | FAILED | ...)
  old_state / new_state  (JSONB snapshots)
  actor_id / actor_type  (USER | SYSTEM | WORKER)
  correlation_id  (trace ID -- links all entries for one request)
  occurred_at

outbox_events
  aggregate_type / aggregate_id
  event_type
  payload      (JSONB -- full EventEnvelope)
  topic        (Kafka topic name)
  partition_key (source_account_id for ordering within account)
  status       (PENDING -> PUBLISHED | FAILED)

dead_letter_queue
  source_topic / partition_num / offset_num
  payload      (original message)
  error_message
  error_count
  resolved_at  (NULL = unresolved, needs ops attention)
```

**DB-level safety rules:**
- `balance >= 0` check constraint on `accounts`
- `amount > 0` check constraint on `transactions` and `ledger_entries`
- Immutability triggers on `audit_log` and `ledger_entries`
- `UNIQUE (idempotency_key)` partial index (excludes REVERSED transactions)
- Role `app_rw` has INSERT-only on `audit_log` and `ledger_entries`; role `reporting_ro` is SELECT-only

---

## Project Structure

```
financial-data-platform/
|-- Cargo.toml                    # Workspace root
|-- config/
|   `-- default.toml              # Default configuration values
|-- migrations/
|   |-- 001_initial_schema.sql    # Tables, types, triggers, roles
|   `-- 002_indexes_and_partitioning.sql
|-- docker/
|   |-- docker-compose.yml        # Full local stack
|   |-- Dockerfile.ingestion      # Multi-stage, distroless runtime
|   `-- prometheus.yml
|-- scripts/
|   |-- setup-dev.sh              # Bootstrap local environment
|   `-- test-api.sh               # Smoke test the API
`-- crates/
    |-- shared/                   # Domain types, errors, events, audit
    |   `-- src/
    |       |-- domain.rs         # Aggregates: Transaction, Account, LedgerEntry
    |       |-- events.rs         # Domain events + EventEnvelope
    |       |-- error.rs          # DomainError with HTTP status mapping
    |       |-- idempotency.rs    # IdempotencyKey + request hash
    |       |-- audit.rs          # audit::record() helper
    |       `-- db.rs             # Connection pool factory
    |-- ingestion/                # REST API (Axum)
    |   `-- src/
    |       |-- main.rs           # Server bootstrap, tracing init
    |       |-- config.rs         # AppConfig (env + file)
    |       |-- routes.rs         # Router + AppState
    |       `-- handlers/
    |           |-- transactions.rs  # POST /v1/transactions (core logic)
    |           |-- accounts.rs
    |           |-- health.rs
    |           `-- metrics.rs
    |-- worker/                   # Kafka consumer + transaction processor
    |   `-- src/
    |       |-- main.rs           # Worker bootstrap, parallel consumers
    |       |-- consumer.rs       # Kafka loop, retry, DLQ routing
    |       |-- processor.rs      # Core: advisory lock -> validate -> ledger
    |       |-- fraud.rs          # Risk scoring (pluggable rules engine)
    |       `-- ledger.rs         # Balance reconciliation helpers
    |-- outbox-relay/             # Postgres -> Kafka relay
    |   `-- src/
    |       |-- main.rs           # Poll loop with SELECT FOR UPDATE SKIP LOCKED
    |       `-- config.rs
    `-- reporting/                # CQRS read side
        `-- src/
            |-- main.rs
            |-- reports.rs        # Daily summary, high-value, CTR
            `-- config.rs
```

---

## justfile — Commands

A `justfile` covers the entire project lifecycle. Run `just help` to see all targets.

```
$ just help

Financial Data Platform

Setup & Initialization
  setup                  Starts infra + migrates + creates topics + seed (all at once)
  infra-up               Starts only Postgres, Redis and Kafka via Docker
  infra-down             Stops infrastructure (preserves volumes)
  wait-db                Waits for Postgres to accept connections

Database
  migrate                Runs all pending migrations
  migrate-status         Shows migration status
  migrate-revert         Reverts the last migration
  db-reset               Drops and recreates the database (DESTROYS ALL DATA)
  db-shell               Opens interactive psql in the database
  db-dump                Dumps the database to backup.sql

Kafka
  kafka-topics           Creates required topics
  kafka-topics-list      Lists existing topics
  kafka-consumer-groups  Lists consumer groups and offsets

Seed
  seed                   Inserts data via SQL directly (idempotent)
  seed-api               Creates data via API (requires services running)
  seed-reset             Resets the database and re-runs seed from scratch

Local services
  dev                    Runs all services in parallel
  dev-ingestion          Runs only ingestion
  dev-worker             Runs only worker
  dev-relay              Runs only outbox relay
  dev-reporting          Runs only reporting

Docker Compose
  docker-up              Starts full stack (build + start)
  docker-down            Stops containers (preserves volumes)
  docker-destroy         Stops and removes volumes (DESTROYS DATA)
  docker-logs            Follows logs from all containers
  docker-ps              Lists containers and status

Tests
  test                   Runs all tests
  test-api               Smoke test against the API
  lint                   Runs clippy
  fmt                    Formats the code

Utilities
  stats                  Summary of system state
  balances               Balance of all accounts
  reconcile              Verifies ledger vs balance consistency
  audit-log              Last 20 entries in the audit log
  dlq-check              Messages in the Dead Letter Queue
  outbox-lag             Pending events in the outbox
  clean                  Removes build artifacts
```

### Most commonly used day-to-day commands

```bash
# Primeira vez — sobe tudo e popula com dados de teste
just setup

# Desenvolvimento local (após just setup)
just dev

# Ver estado do sistema a qualquer momento
just stats
just balances
just reconcile

# Inspecionar problemas
just dlq-check
just outbox-lag
just audit-log

# Reset completo (começa do zero)
just seed-reset
```

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Rust | 1.75+ | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| Docker | 24+ | [docs.docker.com/get-docker](https://docs.docker.com/get-docker/) |
| Docker Compose | v2 | Bundled with Docker Desktop |
| sqlx-cli | latest | `cargo install sqlx-cli --no-default-features --features postgres` |
| jq | any | `brew install jq` / `apt install jq` |

Verify:

```bash
rustc --version        # rustc 1.75.0 or newer
docker --version       # Docker version 24.x
docker compose version # Docker Compose version v2.x
sqlx --version         # sqlx-cli 0.7.x
```

---

## Running the Platform

### Option A — Docker Compose (recommended)

The fastest path. Builds all services and brings up the full stack including Postgres, Kafka, Redis, and the observability suite.

**Step 1 — Clone the project:**

```bash
git clone <repo-url> financial-data-platform
cd financial-data-platform
```

**Step 2 — Start the full stack:**

```bash
docker compose -f docker/docker-compose.yml up --build
```

This starts (in dependency order):
1. PostgreSQL 16
2. Redis 7
3. Kafka (KRaft mode — no Zookeeper)
4. Ingestion service (runs DB migrations on startup)
5. Outbox relay
6. Worker (2 replicas)
7. Reporting service
8. Prometheus + Grafana + Jaeger

Wait until you see `ingestion service listening on 0.0.0.0:8080` in the logs.

**Step 3 — Verify services are up:**

```bash
curl http://localhost:8080/health   # -> 200 OK
curl http://localhost:8080/ready    # -> {"status":"ready"}
curl http://localhost:8082/health   # -> ok
```

**Step 4 — Run the smoke test:**

```bash
bash scripts/test-api.sh
```

Expected output:

```
==> Create source account
Source: 019385a3-...

==> Create dest account
Dest: 019385b1-...

==> Submit transfer (first time)
{ "transaction_id": "...", "status": "PENDING", "was_duplicate": false }

==> Replay same request (must return was_duplicate=true)
{ "transaction_id": "...", "status": "PENDING", "was_duplicate": true }
```

**Step 5 — Open observability dashboards:**

| UI | URL | Login |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Jaeger (traces) | http://localhost:16686 | — |
| Prometheus | http://localhost:9090 | — |

**Stop the stack:**

```bash
# Stop, keep volumes (data preserved)
docker compose -f docker/docker-compose.yml down

# Stop and wipe all data
docker compose -f docker/docker-compose.yml down -v
```

---

### Option B — Local Development

Run services natively for fast iteration without Docker rebuild cycles on every change.

**Step 1 — Start infrastructure only:**

```bash
docker compose -f docker/docker-compose.yml up -d postgres redis kafka
```

**Step 2 — Wait for Postgres to be ready:**

```bash
until docker compose -f docker/docker-compose.yml exec postgres \
    pg_isready -U finplatform; do sleep 1; done
echo "Postgres ready"
```

**Step 3 — Run database migrations:**

```bash
export DATABASE_URL="postgresql://finplatform:dev_password_change_in_prod@localhost:5432/finplatform"
sqlx migrate run --source migrations/
```

**Step 4 — Create Kafka topics:**

```bash
docker compose -f docker/docker-compose.yml exec kafka \
    kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic financial.transactions.v1 \
    --partitions 12 --replication-factor 1

docker compose -f docker/docker-compose.yml exec kafka \
    kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic financial.transactions.dlq \
    --partitions 1 --replication-factor 1
```

**Step 5 — Create a `.env` file (never commit this):**

```bash
cat > .env <<'EOF'
APP__DATABASE_URL=postgresql://finplatform:dev_password_change_in_prod@localhost:5432/finplatform
APP__REDIS_URL=redis://localhost:6379
APP__LISTEN_ADDR=0.0.0.0:8080
APP__LOG_LEVEL=debug
APP__DB_MAX_CONNECTIONS=20
APP__RATE_LIMIT_PER_MINUTE=1000
APP__ENVIRONMENT=development

WORKER__DATABASE_URL=postgresql://finplatform:dev_password_change_in_prod@localhost:5432/finplatform
WORKER__KAFKA_BROKERS=localhost:9092
WORKER__KAFKA_GROUP_ID=transaction-workers
WORKER__WORKER_CONCURRENCY=4
WORKER__MAX_RETRIES=5
WORKER__RETRY_BACKOFF_MS=1000

RELAY__DATABASE_URL=postgresql://finplatform:dev_password_change_in_prod@localhost:5432/finplatform
RELAY__KAFKA_BROKERS=localhost:9092
RELAY__BATCH_SIZE=100
RELAY__POLL_INTERVAL_MS=200

REPORT__DATABASE_URL=postgresql://finplatform:dev_password_change_in_prod@localhost:5432/finplatform
REPORT__LISTEN_ADDR=0.0.0.0:8082
EOF
```

**Step 6 — Run each service in a separate terminal:**

```bash
# Terminal 1 — Ingestion API
cargo run -p ingestion

# Terminal 2 — Transaction worker
cargo run -p worker

# Terminal 3 — Outbox relay
cargo run -p outbox-relay

# Terminal 4 — Reporting service
cargo run -p reporting
```

**Step 7 — (Optional) Pretty-print JSON logs:**

```bash
# With jq
cargo run -p ingestion 2>&1 | jq .

# Or install bunyan for human-readable output
cargo install bunyan
cargo run -p worker 2>&1 | bunyan
```

**Build all crates (check for compile errors):**

```bash
cargo build --workspace
```

**Run tests:**

```bash
cargo test --workspace
```

---

### Data seeding

The seed populates the database with realistic data so you can test the system without having to create anything manually.

#### Via SQL (direct to database — idempotent)

Inserts accounts, historical transactions with ledger already posted, and a pending event in the outbox. Can be executed multiple times without duplicating data.

```bash
just seed
# or
psql $DATABASE_URL -f scripts/seed.sql
```

What is created:

| Account | Owner | Initial balance | Notes |
|---|---|---|---|
| ACC-ALICE-USD | user-alice | $33,190.11 | Main account with transaction history |
| ACC-BOB-USD | user-bob | $500.00 | Low balance — good for testing insufficient funds |
| ACC-CORP-USD | org-acme | $16,500.00 | Corporate account |
| ACC-MERCHANT-USD | org-shop | $25,299.99 | Online store |
| ACC-INACTIVE | user-charlie | $0.00 | Inactive account — tests rejection |
| ACC-DAVE-GBP | user-dave | £8,000.00 | GBP account |
| ACC-EVE-USD | user-eve | $0.00 | For fraud testing |

Included transactions:

| Short ID | Type | Status | Amount | Description |
|---|---|---|---|---|
| seed-txn-001 | CREDIT | SETTLED | $50,000 | Alice initial deposit |
| seed-txn-002 | CREDIT | SETTLED | $500 | Bob initial deposit |
| seed-txn-003 | TRANSFER | SETTLED | $1,500 | Alice -> Corp (invoice) |
| seed-txn-004 | TRANSFER | SETTLED | $299.99 | Alice -> Merchant (purchase) |
| seed-txn-005 | FEE | SETTLED | $9.90 | Alice monthly fee |
| seed-txn-006 | TRANSFER | FAILED | $9,999 | Bob insufficient funds — expected error |
| seed-txn-007 | TRANSFER | SETTLED | $15,000 | High value — appears in CTR |
| seed-txn-008 | TRANSFER | PENDING | $200 | Pending — processed by worker |

#### Via API (creates data in real-time)

Requires services running. Creates accounts via API and submits transactions including error scenarios and idempotency testing.

```bash
# Start services first
just dev        # local
# or
just docker-up  # Docker

# In another terminal
just seed-api
```

The `scripts/seed-api.sh` script executes in sequence:
1. Creates 4 accounts (Alice, Bob, Corp, Merchant)
2. Makes initial deposits to each account
3. Submits 4 transfers between accounts
4. Charges a fee (Fee)
5. Tests transfer with insufficient funds
6. Tests idempotent replay of the same transfer (verifies `was_duplicate: true`)
7. Saves generated IDs to `/tmp/seed-ids.env` for reuse

At the end, shows how to verify the state:

```bash
just stats        # summary of transactions and balances
just balances     # balance table per account
just reconcile    # verifies ledger vs balance (should show drift = 0)
just audit-log    # last entries in the audit log

# Regulatory reports
curl http://localhost:8082/v1/reports/daily
curl http://localhost:8082/v1/reports/high-value   # entries >= $10,000
curl http://localhost:8082/v1/reports/ctr           # CTR >= $10,000 USD
```

#### Complete reset

To return to the initial state at any time:

```bash
just seed-reset   # drops database, recreates, migrates, seeds SQL
```

---

## API Reference

All ingestion requests go to `http://localhost:8080`.
All reporting requests go to `http://localhost:8082`.

---

### Accounts

#### Create Account

```
POST /v1/accounts
Content-Type: application/json
```

```json
{
  "external_id": "ACC-001",
  "owner_id": "user-123",
  "currency": "Usd"
}
```

Currency values: `Usd`, `Eur`, `Gbp`, `Jpy`, `Chf`, `Cad`, `Aud`

**Response `201 Created`:**
```json
{
  "id": "019385a3-7b2c-7000-8000-000000000001",
  "external_id": "ACC-001"
}
```

#### Get Account

```
GET /v1/accounts/:id
```

**Response `200 OK`:**
```json
{
  "id": "019385a3-...",
  "external_id": "ACC-001",
  "owner_id": "user-123",
  "currency": "USD",
  "balance": "1000.0000",
  "is_active": true,
  "created_at": "2024-01-15T10:30:00Z"
}
```

---

### Transactions

All `POST /v1/transactions` requests **must** include the `Idempotency-Key` header.

**`Idempotency-Key` rules:**
- Required on every submission
- 1–64 characters: alphanumeric, `-`, `_`
- Must be unique per logical operation
- Safe to resend on network retries with the **same body**
- Reusing with a **different body** returns `422 IDEMPOTENCY_KEY_REUSED`

#### Submit Transaction

```
POST /v1/transactions
Content-Type: application/json
Idempotency-Key: <unique-key>
```

**Transfer between accounts:**
```json
{
  "transaction_type": "Transfer",
  "amount": "250.00",
  "currency": "Usd",
  "source_account_id": "019385a3-...",
  "dest_account_id": "019385b1-...",
  "description": "Invoice payment #INV-2024-001",
  "metadata": {
    "invoice_id": "INV-2024-001",
    "customer_ref": "CUST-789"
  }
}
```

**Credit (deposit to account):**
```json
{
  "transaction_type": "Credit",
  "amount": "1000.00",
  "currency": "Usd",
  "dest_account_id": "019385a3-...",
  "description": "Initial deposit"
}
```

**Debit (withdrawal from account):**
```json
{
  "transaction_type": "Debit",
  "amount": "75.50",
  "currency": "Usd",
  "source_account_id": "019385a3-...",
  "description": "ATM withdrawal"
}
```

**Response `202 Accepted` — new submission:**
```json
{
  "transaction_id": "019385c2-...",
  "status": "PENDING",
  "idempotency_key": "txn-abc123",
  "was_duplicate": false
}
```

**Response `200 OK` — idempotent replay (safe retry):**
```json
{
  "transaction_id": "019385c2-...",
  "status": "SETTLED",
  "idempotency_key": "txn-abc123",
  "was_duplicate": true
}
```

**Transaction status values:**

| Status | Meaning |
|---|---|
| `PENDING` | Received, waiting in queue |
| `PROCESSING` | Worker has locked and is executing |
| `SETTLED` | Fully processed, ledger updated, balances changed |
| `FAILED` | Non-retryable failure — inspect `last_error` |
| `REVERSED` | A reversal was applied |

#### Get Transaction

```
GET /v1/transactions/:id
```

**Response `200 OK`:**
```json
{
  "id": "019385c2-...",
  "status": "SETTLED",
  "amount": "250.0000",
  "currency": "USD",
  "transaction_type": "TRANSFER",
  "submitted_at": "2024-01-15T10:30:00Z",
  "settled_at": "2024-01-15T10:30:02Z"
}
```

**Error responses:**

| HTTP | Code | Cause |
|---|---|---|
| 400 | `MISSING_IDEMPOTENCY_KEY` | `Idempotency-Key` header absent |
| 400 | `INVALID_IDEMPOTENCY_KEY` | Key too long or invalid characters |
| 404 | `NOT_FOUND` | Transaction or account not found |
| 409 | `DOMAIN_ERROR` | Insufficient funds, inactive account, invalid state |
| 422 | `IDEMPOTENCY_KEY_REUSED` | Same key, different request body |
| 422 | `VALIDATION_ERROR` | Body fails schema validation |
| 500 | `INTERNAL_ERROR` | Unexpected server error |

---

### Reports

All go to `http://localhost:8082`. Read-only — point at a Postgres read replica in production.

#### Daily Transaction Summary

```
GET /v1/reports/daily
```

```json
{
  "date": "2024-01-15",
  "total_transactions": 12483,
  "total_volume": "8432150.2500",
  "settled_count": 12401,
  "failed_count": 82,
  "by_currency": { "USD": "7100000.0000", "EUR": "1332150.2500" },
  "by_type": { "TRANSFER": 9200, "CREDIT": 2100, "DEBIT": 1183 }
}
```

#### High-Value Transactions (last 24h)

```
GET /v1/reports/high-value
```

Returns all ledger entries >= $10,000 in the last 24 hours. Used for internal risk review and pre-CTR screening.

#### Currency Transaction Report (CTR)

```
GET /v1/reports/ctr
```

Returns settled USD transactions >= $10,000 in the last 24 hours. Required by FinCEN under the Bank Secrecy Act — must be filed within 15 days of the transaction date.

---

## Configuration

All services use **environment variables** with a double-underscore separator for nesting. `config/default.toml` provides fallback defaults.

### Ingestion (`APP__` prefix)

| Variable | Default | Description |
|---|---|---|
| `APP__DATABASE_URL` | — | PostgreSQL connection string |
| `APP__REDIS_URL` | — | Redis connection string |
| `APP__LISTEN_ADDR` | `0.0.0.0:8080` | Bind address |
| `APP__LOG_LEVEL` | `info` | `trace` / `debug` / `info` / `warn` / `error` |
| `APP__DB_MAX_CONNECTIONS` | `20` | Connection pool max size |
| `APP__RATE_LIMIT_PER_MINUTE` | `1000` | Per-IP rate limit |
| `APP__ENVIRONMENT` | `development` | Tag included in all log lines |
| `APP__OTLP_ENDPOINT` | — | Optional: `http://jaeger:4317` for trace export |

### Worker (`WORKER__` prefix)

| Variable | Default | Description |
|---|---|---|
| `WORKER__DATABASE_URL` | — | PostgreSQL connection string |
| `WORKER__KAFKA_BROKERS` | — | Comma-separated broker list |
| `WORKER__KAFKA_GROUP_ID` | — | Consumer group ID |
| `WORKER__WORKER_CONCURRENCY` | `4` | Parallel consumer tasks per pod |
| `WORKER__MAX_RETRIES` | `5` | Attempts before moving to DLQ |
| `WORKER__RETRY_BACKOFF_MS` | `1000` | Initial backoff (doubles each retry, max 30s) |

### Outbox relay (`RELAY__` prefix)

| Variable | Default | Description |
|---|---|---|
| `RELAY__DATABASE_URL` | — | PostgreSQL connection string |
| `RELAY__KAFKA_BROKERS` | — | Kafka broker list |
| `RELAY__BATCH_SIZE` | `100` | Max events per poll cycle |
| `RELAY__POLL_INTERVAL_MS` | `200` | Sleep between empty polls |

### Reporting (`REPORT__` prefix)

| Variable | Default | Description |
|---|---|---|
| `REPORT__DATABASE_URL` | — | Can point to a read replica |
| `REPORT__LISTEN_ADDR` | `0.0.0.0:8082` | Bind address |
| `REPORT__DB_MAX_CONNECTIONS` | `5` | Pool max size |

---

## Observability

### Structured Logs

Every service emits JSON logs to stdout. Every line includes `transaction_id`, `correlation_id`, `worker_id`, and `level`. Query by `correlation_id` to trace a request across all services:

```bash
# Follow logs and filter to one transaction
docker compose -f docker/docker-compose.yml logs -f worker | \
    jq 'select(.correlation_id == "019385a0-...")'
```

### Metrics

Scraped from `http://localhost:8080/metrics`. Key alerts:

| Metric | Alert threshold | Meaning |
|---|---|---|
| `http_requests_total{status=~"5.."}` | rate > 1% | Server errors |
| `http_request_duration_seconds{quantile="0.99"}` | > 500ms | Latency degradation |
| `outbox_relay_lag_seconds` | > 30s | Kafka delivery delay |
| `dead_letter_queue_depth` | > 0 | Processing failures need review |
| `db_pool_idle_connections` | < 2 | Pool starved — increase or add PgBouncer |

### Distributed Tracing

Open http://localhost:16686, select service `ingestion` or `worker`, and find a trace by `transaction_id`. Every span includes the full context from HTTP receive to DB commit to Kafka publish.

### Audit Log Queries

```sql
-- Full lifecycle of one transaction
SELECT action, old_state, new_state, actor_id, occurred_at
FROM audit_log
WHERE entity_type = 'Transaction'
  AND entity_id = '019385c2-...'
ORDER BY occurred_at;

-- Everything triggered by one API request (cross-service)
SELECT entity_type, entity_id, action, actor_type, occurred_at
FROM audit_log
WHERE correlation_id = '019385a0-...'
ORDER BY occurred_at;

-- All activity by a user in the last 30 days
SELECT entity_type, entity_id, action, occurred_at
FROM audit_log
WHERE actor_id = 'user-123'
  AND occurred_at >= NOW() - INTERVAL '30 days'
ORDER BY occurred_at DESC;
```

---

## Design Decisions

### Idempotency

**Problem:** Networks fail. Clients retry. Without idempotency a retried $500 transfer becomes two $500 transfers.

**Solution — two layers:**

```
Layer 1 (fast path)   Redis cache
  Hot keys (< 5 min)  ->  return without hitting Postgres

Layer 2 (authoritative)  PostgreSQL UNIQUE constraint
  INSERT ... ON CONFLICT (idempotency_key) DO NOTHING
  Atomic: no race between check and insert

Hash comparison: SHA-256(canonical sorted JSON body)
  Same key + same body  ->  safe replay, return cached response
  Same key + diff body  ->  422 (caught explicitly, not silently swallowed)
```

The idempotency key lives in Postgres — correctness does not depend on Redis. Redis is a latency optimization only.

### Outbox Pattern

**Problem:** You cannot atomically commit to Postgres and publish to Kafka. A crash between the two leaves data in an inconsistent state.

**Solution:**

```
1. BEGIN transaction
2. INSERT transaction row
3. INSERT outbox_events row    <- same transaction, atomic
4. COMMIT

Outbox relay (separate process):
  SELECT FOR UPDATE SKIP LOCKED  <- safe for multiple relay instances
  -> Publish to Kafka
  -> On ACK: UPDATE outbox_events SET status = 'PUBLISHED'
  -> On failure: increment retry_count; mark FAILED after threshold
```

If Kafka is unavailable, events accumulate in Postgres and are delivered when Kafka recovers — no data loss, no manual intervention.

### CQRS

The ingestion and worker services own all writes and enforce invariants. The reporting service is read-only. In production, point `REPORT__DATABASE_URL` at a Postgres streaming replica — all reporting traffic leaves the primary entirely.

This means:
- Slow reporting queries never block transaction processing
- The reporting schema can be independently evolved (materialized views, denormalized projections)
- Read replicas can scale horizontally for analytics load

### Event Sourcing (Ledger)

The `ledger_entries` table is an event log. DB triggers block UPDATE and DELETE. The `accounts.balance` column is a **materialized projection** maintained in the same transaction as each ledger entry.

This gives you:
- Full balance reconstruction from scratch: `SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE -amount END)`
- Regulatory-grade audit trail — every cent movement is permanently recorded
- Bug detection: `ledger.rs::verify_account_balance()` compares the ledger sum to `accounts.balance` and surfaces any drift

### Consistency vs Performance

| Decision | Choice | Why |
|---|---|---|
| Ledger write | Synchronous, same DB transaction as status update | Cannot defer — balance must reflect reality immediately |
| Audit log write | Synchronous, same DB transaction | Regulatory requirement; async risks loss on crash |
| Kafka publish | Async via outbox relay (~200ms delay) | Acceptable latency trade-off; correctness is not at risk |
| Idempotency authority | Postgres (not Redis) | Redis can lose data; money requires durability |
| Account balance locking | Pessimistic (`SELECT FOR UPDATE`) | Contention per account is bounded; prevents lost-update under concurrent debits |
| Transaction state | Optimistic (`version` column) | State transitions happen once per transaction; low contention |

---

## Scaling Strategy

### Current capacity

A single Postgres instance on 8 vCPU / 32 GB RAM comfortably handles:
- ~5,000 writes/second sustained
- ~10M transactions/day

The first bottleneck you will hit is **connection pool exhaustion**, not CPU. Add **PgBouncer** in transaction mode before scaling anything else.

### Growth phases

```
Phase 1 — Read offload (up to 50M/day)
  Point reporting service at a Postgres read replica.
  All audit queries, reports, and dashboards stop touching the primary.

Phase 2 — Table partitioning (50M–200M/day)
  Partition transactions and ledger_entries by month.
  Active partition stays hot in Postgres shared_buffers.
  Archive cold partitions to S3 as Parquet for long-term storage.

Phase 3 — Horizontal sharding (200M+/day)
  Shard accounts + ledger by hash(account_id).
  Use Citus extension on Postgres, or migrate to CockroachDB.
  Kafka topics partition by account_id — workers own specific account ranges.

Phase 4 — Analytics separation (any scale)
  Stream settled transactions to ClickHouse or BigQuery via Kafka Connect.
  All reporting queries move off Postgres entirely.
  Postgres handles only OLTP (current-state reads and writes).
```

### Worker scaling

Workers are stateless — add pods freely. Kafka rebalances partitions automatically across the consumer group. With 12 partitions you can run up to 12 worker pods before additional instances idle. Increase partition count before increasing pod count.

---

## Fault Tolerance

| Failure scenario | How this system responds |
|---|---|
| Ingestion pod crashes mid-request | No DB commit = transaction doesn't exist. Client retries with same `Idempotency-Key` — safe. |
| Worker crashes mid-processing | Kafka offset not committed. Message redelivered. Advisory lock auto-released. Idempotency check skips already-SETTLED transactions. |
| Kafka unavailable | Outbox events queue in Postgres. Relay catches up when Kafka recovers. No data loss. |
| Postgres primary failover | ~30s downtime during failover. Requests fail-fast with 500. Clients retry (all operations are idempotent). |
| Poison message (crashes every worker) | After `MAX_RETRIES` attempts: moved to `dead_letter_queue` table. Alert fires. Operator reviews. |
| Fraud rule blocks transaction | Marked FAILED with reason. No ledger entries written. No balance change. Full audit record. |
| Insufficient funds | `InsufficientFunds` domain error. Transaction marked FAILED. DB transaction rolled back. Account balance unchanged. |
| Two workers race on same transaction | First acquires advisory lock; second calls `pg_try_advisory_lock` → gets false → skips. No double-processing. |
| Balance drift (software bug) | `verify_account_balance()` reconciliation detects mismatch between `accounts.balance` and `SUM(ledger_entries)`. Triggers incident. |

### Chaos / failure simulation (fault injection)

For deterministic “real-world failure” simulation in local dev and CI, set `FDP_FAULTS` to inject faults at specific points:

- **`FDP_FAULTS=processor.transient=every:3`**: forces a transient processing failure (exercise retry/backoff).
- **`FDP_FAULTS=processor.permanent=once`**: forces a permanent failure (exercise FAILED + DLQ path).
- **`FDP_FAULTS=fraud.hang=once`**: simulates a hung fraud service (exercise the fraud timeout behavior).
- **`FDP_FAULTS=dlq.write=always`**: simulates DLQ DB write failure (exercise “do not commit offset” behavior).

---

## Production Checklist

Complete these before processing real money:

**Security**
- [ ] Replace all default passwords (`dev_password_change_in_prod`)
- [ ] Enable TLS on Postgres (`sslmode=verify-full`)
- [ ] Enable mTLS between services (Istio / Linkerd / client certs)
- [ ] Rotate database credentials via HashiCorp Vault or AWS Secrets Manager
- [ ] Enable Kafka SASL/SSL
- [ ] Revoke UPDATE/DELETE on `audit_log` and `ledger_entries` from the app DB role
- [ ] Network policy: ingestion accepts inbound only on 8080; worker has no inbound

**Data durability**
- [ ] Enable Postgres WAL archiving + PITR (Point-in-Time Recovery to any second)
- [ ] Verify daily snapshots to S3 or GCS with tested restore procedure
- [ ] Set Kafka topic retention to >= 7 days for full replay capability
- [ ] Schedule daily reconciliation job (`verify_account_balance` on all accounts)
- [ ] Archive `audit_log` to cold storage after 90 days (keep queryable for 7 years)

**Operations**
- [ ] Alert on: DLQ depth > 0, outbox relay lag > 30s, error rate > 1%, p99 > 500ms
- [ ] On-call rotation with PagerDuty or OpsGenie
- [ ] Runbook: how to replay a message from the DLQ
- [ ] Runbook: how to manually reverse a settled transaction
- [ ] Runbook: Postgres primary failover and replica promotion procedure
- [ ] Load test at 2x expected peak before go-live; baseline p99 latency

**Compliance**
- [ ] CTR workflow integrated with FinCEN BSA E-Filing (US requirement)
- [ ] SAR generation and filing process documented and tested
- [ ] Data retention policy enforced (7 years for financial records in most jurisdictions)
- [ ] PII encrypted at rest (Postgres TDE or column-level encryption for PII fields)
- [ ] Quarterly access review of who can query `audit_log`
