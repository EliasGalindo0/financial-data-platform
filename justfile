# ==============================================================================
# Financial Data Platform — justfile
# https://github.com/casey/just
#
# Instalação: cargo install just
#
# QUICK START:
#   just setup      — infra + migrate + kafka topics + seed (tudo de uma vez)
#   just dev        — roda todos os serviços localmente
#   just docker-up  — sobe stack completo via Docker Compose
# ==============================================================================

# Mostra receitas disponíveis por padrão
default:
    @just --list

# ------------------------------------------------------------------------------
# Variáveis
# ------------------------------------------------------------------------------

db_url      := "postgresql://finplatform:dev_password_change_in_prod@localhost:5432/finplatform"
api_base    := "http://localhost:8080"
compose     := "docker/docker-compose.yml"
migrations  := "migrations/"

# ------------------------------------------------------------------------------
# SETUP COMPLETO
# ------------------------------------------------------------------------------

# Sobe infra + migra + cria Kafka topics + seed (tudo de uma vez)
setup: infra-up wait-db migrate kafka-topics seed
    @echo ""
    @echo "✓ Ambiente pronto!"
    @echo ""
    @echo "  API Ingestion : {{api_base}}"
    @echo "  Reporting     : http://localhost:8082"
    @echo "  Grafana       : http://localhost:3000  (admin/admin)"
    @echo "  Jaeger        : http://localhost:16686"
    @echo "  Prometheus    : http://localhost:9090"
    @echo ""
    @echo "  Próximo passo : just dev        (serviços locais)"
    @echo "               : just docker-up  (stack completo via Docker)"
    @echo ""

# ------------------------------------------------------------------------------
# INFRAESTRUTURA
# ------------------------------------------------------------------------------

# Sobe apenas Postgres, Redis e Kafka via Docker
infra-up:
    docker compose -f {{compose}} up -d postgres redis kafka

# Para a infraestrutura (preserva volumes)
infra-down:
    docker compose -f {{compose}} stop postgres redis kafka

# Aguarda o Postgres aceitar conexões
wait-db:
    @echo "Aguardando Postgres..."
    @until docker compose -f {{compose}} exec -T postgres pg_isready -U finplatform -q; do \
        printf "."; sleep 1; \
    done
    @echo ""
    @echo "✓ Postgres pronto"

# ------------------------------------------------------------------------------
# BANCO DE DADOS
# ------------------------------------------------------------------------------

# Executa todas as migrations pendentes
migrate:
    DATABASE_URL="{{db_url}}" sqlx migrate run --source {{migrations}}

# Mostra o status de cada migration
migrate-status:
    DATABASE_URL="{{db_url}}" sqlx migrate info --source {{migrations}}

# Reverte a última migration aplicada
migrate-revert:
    DATABASE_URL="{{db_url}}" sqlx migrate revert --source {{migrations}}

# Dropa e recria o banco do zero (DESTRÓI TODOS OS DADOS — pede confirmação)
[confirm("ATENÇÃO: isso vai apagar todos os dados. Confirma? (s/N)")]
db-reset: _db-drop _db-create migrate
    @echo "✓ Banco recriado"

_db-drop:
    docker compose -f {{compose}} exec -T postgres \
        psql -U finplatform -c "DROP DATABASE IF EXISTS finplatform;"

_db-create:
    docker compose -f {{compose}} exec -T postgres \
        psql -U finplatform -c "CREATE DATABASE finplatform;"

# Abre psql interativo no banco
db-shell:
    docker compose -f {{compose}} exec postgres psql -U finplatform -d finplatform

# Faz dump do banco para backup.sql
db-dump:
    docker compose -f {{compose}} exec -T postgres \
        pg_dump -U finplatform finplatform > backup.sql
    @echo "✓ Dump salvo em backup.sql"

# ------------------------------------------------------------------------------
# KAFKA
# ------------------------------------------------------------------------------

# Cria os Kafka topics necessários
kafka-topics:
    docker compose -f {{compose}} exec -T kafka \
        kafka-topics --bootstrap-server localhost:9092 \
        --create --if-not-exists \
        --topic financial.transactions.v1 \
        --partitions 12 --replication-factor 1
    docker compose -f {{compose}} exec -T kafka \
        kafka-topics --bootstrap-server localhost:9092 \
        --create --if-not-exists \
        --topic financial.transactions.dlq \
        --partitions 1 --replication-factor 1
    @echo "✓ Topics criados"

# Lista os topics existentes no Kafka
kafka-topics-list:
    docker compose -f {{compose}} exec kafka \
        kafka-topics --bootstrap-server localhost:9092 --list

# Lista consumer groups e lag de offsets
kafka-consumer-groups:
    docker compose -f {{compose}} exec kafka \
        kafka-consumer-groups --bootstrap-server localhost:9092 \
        --group transaction-workers --describe

# ------------------------------------------------------------------------------
# SEED DE DADOS
# ------------------------------------------------------------------------------

# Insere dados iniciais via SQL (idempotente — pode rodar múltiplas vezes)
seed:
    psql "{{db_url}}" -f scripts/seed.sql

# Cria dados de teste via API (requer serviços rodando: just dev ou just docker-up)
seed-api:
    bash scripts/seed-api.sh

# Reseta o banco e refaz o seed do zero
seed-reset: db-reset seed

# ------------------------------------------------------------------------------
# SERVIÇOS LOCAIS
# ------------------------------------------------------------------------------

# Roda todos os serviços em paralelo (requer just infra-up)
dev:
    #!/usr/bin/env bash
    trap 'kill 0' INT
    cargo run -p ingestion    2>&1 | sed 's/^/[ingestion] /' &
    cargo run -p worker       2>&1 | sed 's/^/[worker   ] /' &
    cargo run -p outbox-relay 2>&1 | sed 's/^/[relay    ] /' &
    cargo run -p reporting    2>&1 | sed 's/^/[reporting] /' &
    wait

# Roda apenas o serviço de ingestion
dev-ingestion:
    cargo run -p ingestion

# Roda apenas o worker
dev-worker:
    cargo run -p worker

# Roda apenas o outbox relay
dev-relay:
    cargo run -p outbox-relay

# Roda apenas o serviço de reporting
dev-reporting:
    cargo run -p reporting

# ------------------------------------------------------------------------------
# DOCKER COMPOSE (stack completo)
# ------------------------------------------------------------------------------

# Sobe o stack completo via Docker Compose (build + start)
docker-up:
    docker compose -f {{compose}} up --build -d
    @echo ""
    @echo "✓ Stack iniciado"
    @echo "  API     : {{api_base}}"
    @echo "  Grafana : http://localhost:3000"
    @echo "  Jaeger  : http://localhost:16686"

# Para todos os containers (preserva volumes)
docker-down:
    docker compose -f {{compose}} down

# Para tudo e remove volumes (DESTRÓI TODOS OS DADOS — pede confirmação)
[confirm("ATENÇÃO: remove todos os volumes. Confirma? (s/N)")]
docker-destroy:
    docker compose -f {{compose}} down -v

# Acompanha logs de todos os containers
docker-logs:
    docker compose -f {{compose}} logs -f

# Acompanha logs apenas do worker
docker-logs-worker:
    docker compose -f {{compose}} logs -f worker

# Lista containers e seus status
docker-ps:
    docker compose -f {{compose}} ps

# Reconstrói as imagens sem subir os containers
docker-build:
    docker compose -f {{compose}} build

# ------------------------------------------------------------------------------
# TESTES E QUALIDADE
# ------------------------------------------------------------------------------

# Roda todos os testes do workspace
test:
    cargo test --workspace

# Roda `cargo test --workspace` em container (Rust 1.88) + Postgres temporário.
# Útil quando você não tem Rust/Cargo local ou quer reproduzir o ambiente de CI.
test-docker:
    MSYS2_ARG_CONV_EXCL='*' bash -lc 'set -euo pipefail; docker rm -f fdp-test-postgres >/dev/null 2>&1 || true; docker run -d --name fdp-test-postgres -e POSTGRES_USER=finplatform -e POSTGRES_PASSWORD=dev_password_change_in_prod -e POSTGRES_DB=finplatform -p 55432:5432 postgres:16 >/dev/null; docker run --rm --network host -v "c:/Dev/financial-data-platform/migrations:/migrations" -e PGPASSWORD=dev_password_change_in_prod postgres:16 bash -lc "until pg_isready -h 127.0.0.1 -p 55432 -U finplatform; do sleep 1; done && psql -h 127.0.0.1 -p 55432 -U finplatform -d finplatform -v ON_ERROR_STOP=1 -f /migrations/001_initial_schema.sql -f /migrations/002_indexes_and_partitioning.sql" >/dev/null; docker run --rm --network host -v "c:/Dev/financial-data-platform:/work" -w /work rust:1.88 sh -lc "PATH=\\"/usr/local/cargo/bin:\\$PATH\\"; export PATH; apt-get update >/dev/null; apt-get install -y --no-install-recommends cmake pkg-config build-essential libssl-dev zlib1g-dev ca-certificates >/dev/null; export DATABASE_URL=postgresql://finplatform:dev_password_change_in_prod@127.0.0.1:55432/finplatform; cargo test --workspace"; docker rm -f fdp-test-postgres >/dev/null'

# Roda clippy em container (não requer Rust instalado localmente)
lint-docker:
    MSYS2_ARG_CONV_EXCL='*' bash -lc 'set -euo pipefail; docker rm -f fdp-test-postgres >/dev/null 2>&1 || true; docker run -d --name fdp-test-postgres -e POSTGRES_USER=finplatform -e POSTGRES_PASSWORD=dev_password_change_in_prod -e POSTGRES_DB=finplatform -p 55432:5432 postgres:16 >/dev/null; docker run --rm --network host -v "c:/Dev/financial-data-platform/migrations:/migrations" -e PGPASSWORD=dev_password_change_in_prod postgres:16 bash -lc "until pg_isready -h 127.0.0.1 -p 55432 -U finplatform; do sleep 1; done && psql -h 127.0.0.1 -p 55432 -U finplatform -d finplatform -v ON_ERROR_STOP=1 -f /migrations/001_initial_schema.sql -f /migrations/002_indexes_and_partitioning.sql" >/dev/null; docker run --rm --network host -v "c:/Dev/financial-data-platform:/work" -w /work rust:1.88 sh -lc "PATH=\\"/usr/local/cargo/bin:\\$PATH\\"; export PATH; apt-get update >/dev/null; apt-get install -y --no-install-recommends cmake pkg-config build-essential libssl-dev zlib1g-dev ca-certificates >/dev/null; rustup component add clippy >/dev/null; export DATABASE_URL=postgresql://finplatform:dev_password_change_in_prod@127.0.0.1:55432/finplatform; cargo clippy --workspace -- -D warnings"; docker rm -f fdp-test-postgres >/dev/null'

# Verifica formatação em container (não requer Rust instalado localmente)
fmt-check-docker:
    MSYS2_ARG_CONV_EXCL='*' bash -lc 'set -euo pipefail; docker run --rm -v "c:/Dev/financial-data-platform:/work" -w /work rust:1.88 sh -lc "PATH=\\"/usr/local/cargo/bin:\\$PATH\\"; export PATH; rustup component add rustfmt >/dev/null; cargo fmt --all -- --check"'

# Smoke test completo contra a API rodando
test-api:
    bash scripts/test-api.sh

# Verifica compilação sem gerar binários
check:
    cargo check --workspace

# Roda clippy (linter)
lint:
    cargo clippy --workspace -- -D warnings

# Formata todo o código
fmt:
    cargo fmt --all

# Verifica formatação sem alterar arquivos
fmt-check:
    cargo fmt --all -- --check

# ------------------------------------------------------------------------------
# UTILITÁRIOS DE DIAGNÓSTICO
# ------------------------------------------------------------------------------

# Resumo rápido do estado do sistema
stats:
    @echo ""
    @echo "=== Transações ==="
    @psql "{{db_url}}" -c \
        "SELECT status, COUNT(*) AS total, COALESCE(SUM(amount), 0) AS volume \
         FROM transactions GROUP BY status ORDER BY status;"
    @echo "=== Contas ==="
    @psql "{{db_url}}" -c \
        "SELECT COUNT(*) AS total_accounts, SUM(balance) AS total_balance FROM accounts;"
    @echo "=== Outbox ==="
    @psql "{{db_url}}" -c \
        "SELECT status, COUNT(*) FROM outbox_events GROUP BY status;"
    @echo "=== DLQ ==="
    @psql "{{db_url}}" -c \
        "SELECT COUNT(*) AS unresolved FROM dead_letter_queue WHERE resolved_at IS NULL;"

# Exibe saldo de todas as contas
balances:
    @psql "{{db_url}}" -c \
        "SELECT external_id, owner_id, currency, balance, is_active \
         FROM accounts ORDER BY external_id;"

# Verifica consistência entre ledger e saldo das contas (drift deve ser 0)
reconcile:
    @psql "{{db_url}}" -c \
        "SELECT \
             a.external_id, \
             a.balance AS account_balance, \
             COALESCE(SUM(CASE WHEN l.entry_type = 'CREDIT' THEN l.amount \
                               ELSE -l.amount END), 0) AS ledger_balance, \
             a.balance - COALESCE(SUM(CASE WHEN l.entry_type = 'CREDIT' THEN l.amount \
                                     ELSE -l.amount END), 0) AS drift \
         FROM accounts a \
         LEFT JOIN ledger_entries l ON l.account_id = a.id \
         GROUP BY a.id, a.external_id, a.balance \
         ORDER BY ABS(drift) DESC;"

# Exibe as últimas 20 entradas do audit log
audit-log:
    @psql "{{db_url}}" -x -c \
        "SELECT entity_type, entity_id, action, actor_type, occurred_at \
         FROM audit_log ORDER BY occurred_at DESC LIMIT 20;"

# Mostra mensagens na Dead Letter Queue não resolvidas
dlq-check:
    @psql "{{db_url}}" -c \
        "SELECT id, source_topic, error_message, error_count, first_failed_at \
         FROM dead_letter_queue WHERE resolved_at IS NULL ORDER BY first_failed_at;"

# Mostra eventos pendentes no outbox (lag do relay)
outbox-lag:
    @psql "{{db_url}}" -c \
        "SELECT COUNT(*) AS pending, MIN(created_at) AS oldest \
         FROM outbox_events WHERE status = 'PENDING';"

# Remove artefatos de build
clean:
    cargo clean
