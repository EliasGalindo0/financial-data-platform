# ==============================================================================
# Financial Data Platform — Makefile
# ==============================================================================
#
# QUICK START:
#   make setup        — start infra + migrate + seed (tudo de uma vez)
#   make dev          — roda todos os serviços localmente (requer infra já up)
#   make docker-up    — sobe stack completo via Docker Compose
#
# HELP:
#   make help         — lista todos os targets disponíveis

.DEFAULT_GOAL := help

# ------------------------------------------------------------------------------
# Variáveis
# ------------------------------------------------------------------------------

DB_URL       ?= postgresql://finplatform:dev_password_change_in_prod@localhost:5432/finplatform
KAFKA_BROKER ?= localhost:9092
API_BASE     ?= http://localhost:8080
COMPOSE_FILE  = docker/docker-compose.yml
MIGRATIONS    = migrations/

# Cores para output legível
BOLD  = \033[1m
GREEN = \033[32m
CYAN  = \033[36m
RESET = \033[0m

# ------------------------------------------------------------------------------
# HELP
# ------------------------------------------------------------------------------

.PHONY: help
help: ## Mostra todos os targets disponíveis
	@echo ""
	@echo "$(BOLD)Financial Data Platform$(RESET)"
	@echo ""
	@echo "$(CYAN)Setup & Inicialização$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-22s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ------------------------------------------------------------------------------
# SETUP COMPLETO (único comando para começar)
# ------------------------------------------------------------------------------

.PHONY: setup
setup: infra-up wait-db migrate kafka-topics seed ## Sobe infra + migra + cria topics + seed (tudo de uma vez)
	@echo ""
	@echo "$(GREEN)$(BOLD)✓ Ambiente pronto!$(RESET)"
	@echo ""
	@echo "  API Ingestion : $(API_BASE)"
	@echo "  Reporting     : http://localhost:8082"
	@echo "  Grafana       : http://localhost:3000  (admin/admin)"
	@echo "  Jaeger        : http://localhost:16686"
	@echo "  Prometheus    : http://localhost:9090"
	@echo ""
	@echo "  Próximo passo : make dev   (serviços locais)"
	@echo "               : make docker-up  (stack completo via Docker)"
	@echo ""

# ------------------------------------------------------------------------------
# INFRAESTRUTURA (Postgres, Kafka, Redis)
# ------------------------------------------------------------------------------

.PHONY: infra-up
infra-up: ## Sobe apenas Postgres, Redis e Kafka via Docker
	@echo "$(CYAN)Subindo infraestrutura...$(RESET)"
	docker compose -f $(COMPOSE_FILE) up -d postgres redis kafka
	@echo "$(GREEN)✓ Infraestrutura iniciada$(RESET)"

.PHONY: infra-down
infra-down: ## Para a infraestrutura (preserva volumes)
	docker compose -f $(COMPOSE_FILE) stop postgres redis kafka

.PHONY: wait-db
wait-db: ## Aguarda Postgres aceitar conexões
	@echo "$(CYAN)Aguardando Postgres...$(RESET)"
	@until docker compose -f $(COMPOSE_FILE) exec -T postgres \
		pg_isready -U finplatform -q; do \
		printf "."; sleep 1; \
	done
	@echo ""
	@echo "$(GREEN)✓ Postgres pronto$(RESET)"

# ------------------------------------------------------------------------------
# BANCO DE DADOS
# ------------------------------------------------------------------------------

.PHONY: migrate
migrate: ## Executa todas as migrations pendentes
	@echo "$(CYAN)Executando migrations...$(RESET)"
	DATABASE_URL="$(DB_URL)" sqlx migrate run --source $(MIGRATIONS)
	@echo "$(GREEN)✓ Migrations aplicadas$(RESET)"

.PHONY: migrate-status
migrate-status: ## Mostra status das migrations
	DATABASE_URL="$(DB_URL)" sqlx migrate info --source $(MIGRATIONS)

.PHONY: migrate-revert
migrate-revert: ## Reverte a última migration
	@echo "$(CYAN)Revertendo última migration...$(RESET)"
	DATABASE_URL="$(DB_URL)" sqlx migrate revert --source $(MIGRATIONS)

.PHONY: db-reset
db-reset: ## Dropa e recria o banco (DESTRÓI TODOS OS DADOS)
	@echo "$(BOLD)ATENÇÃO: isso vai apagar todos os dados. Confirma? [y/N]$(RESET)" \
		&& read ans && [ $${ans:-N} = y ]
	@echo "$(CYAN)Recriando banco de dados...$(RESET)"
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		psql -U finplatform -c "DROP DATABASE IF EXISTS finplatform;"
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		psql -U finplatform -c "CREATE DATABASE finplatform;"
	$(MAKE) migrate
	@echo "$(GREEN)✓ Banco recriado$(RESET)"

.PHONY: db-shell
db-shell: ## Abre psql interativo no banco
	docker compose -f $(COMPOSE_FILE) exec postgres \
		psql -U finplatform -d finplatform

.PHONY: db-dump
db-dump: ## Faz dump do banco para backup.sql
	@echo "$(CYAN)Gerando dump...$(RESET)"
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		pg_dump -U finplatform finplatform > backup.sql
	@echo "$(GREEN)✓ Dump salvo em backup.sql$(RESET)"

# ------------------------------------------------------------------------------
# KAFKA
# ------------------------------------------------------------------------------

.PHONY: kafka-topics
kafka-topics: ## Cria os Kafka topics necessários
	@echo "$(CYAN)Criando Kafka topics...$(RESET)"
	@docker compose -f $(COMPOSE_FILE) exec -T kafka \
		kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic financial.transactions.v1 \
		--partitions 12 --replication-factor 1 2>&1 | grep -v "already exists" || true
	@docker compose -f $(COMPOSE_FILE) exec -T kafka \
		kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic financial.transactions.dlq \
		--partitions 1 --replication-factor 1 2>&1 | grep -v "already exists" || true
	@echo "$(GREEN)✓ Topics criados$(RESET)"

.PHONY: kafka-topics-list
kafka-topics-list: ## Lista os topics existentes no Kafka
	docker compose -f $(COMPOSE_FILE) exec kafka \
		kafka-topics --bootstrap-server localhost:9092 --list

.PHONY: kafka-consumer-groups
kafka-consumer-groups: ## Lista consumer groups e offsets
	docker compose -f $(COMPOSE_FILE) exec kafka \
		kafka-consumer-groups --bootstrap-server localhost:9092 \
		--group transaction-workers --describe

# ------------------------------------------------------------------------------
# SEED DE DADOS
# ------------------------------------------------------------------------------

.PHONY: seed
seed: ## Insere dados iniciais no banco via SQL (contas + transações de exemplo)
	@echo "$(CYAN)Inserindo seed de dados...$(RESET)"
	DATABASE_URL="$(DB_URL)" psql "$(DB_URL)" -f scripts/seed.sql
	@echo "$(GREEN)✓ Seed concluído$(RESET)"

.PHONY: seed-api
seed-api: ## Cria dados via API (requer serviços rodando — make dev ou make docker-up)
	@echo "$(CYAN)Criando dados de teste via API...$(RESET)"
	@bash scripts/seed-api.sh
	@echo "$(GREEN)✓ Seed via API concluído$(RESET)"

.PHONY: seed-reset
seed-reset: db-reset seed ## Reseta o banco e refaz o seed do zero

# ------------------------------------------------------------------------------
# SERVIÇOS LOCAIS (sem Docker)
# ------------------------------------------------------------------------------

.PHONY: dev
dev: ## Roda todos os serviços localmente em paralelo (requer infra-up)
	@echo "$(CYAN)Iniciando serviços locais...$(RESET)"
	@echo "  Use Ctrl+C para parar todos"
	@trap 'kill 0' INT; \
		cargo run -p ingestion 2>&1 | sed 's/^/[ingestion] /' & \
		cargo run -p worker    2>&1 | sed 's/^/[worker   ] /' & \
		cargo run -p outbox-relay 2>&1 | sed 's/^/[relay    ] /' & \
		cargo run -p reporting 2>&1 | sed 's/^/[reporting] /' & \
		wait

.PHONY: dev-ingestion
dev-ingestion: ## Roda apenas o serviço de ingestion
	cargo run -p ingestion

.PHONY: dev-worker
dev-worker: ## Roda apenas o worker
	cargo run -p worker

.PHONY: dev-relay
dev-relay: ## Roda apenas o outbox relay
	cargo run -p outbox-relay

.PHONY: dev-reporting
dev-reporting: ## Roda apenas o serviço de reporting
	cargo run -p reporting

# ------------------------------------------------------------------------------
# DOCKER COMPOSE (stack completo)
# ------------------------------------------------------------------------------

.PHONY: docker-up
docker-up: ## Sobe o stack completo via Docker Compose (build + start)
	@echo "$(CYAN)Subindo stack completo...$(RESET)"
	docker compose -f $(COMPOSE_FILE) up --build -d
	@echo "$(GREEN)✓ Stack iniciado$(RESET)"
	@echo ""
	@echo "  API   : $(API_BASE)"
	@echo "  Grafana : http://localhost:3000"
	@echo "  Jaeger  : http://localhost:16686"

.PHONY: docker-down
docker-down: ## Para todos os containers (preserva volumes)
	docker compose -f $(COMPOSE_FILE) down

.PHONY: docker-destroy
docker-destroy: ## Para tudo e remove volumes (DESTRÓI TODOS OS DADOS)
	@echo "$(BOLD)ATENÇÃO: remove todos os volumes. Confirma? [y/N]$(RESET)" \
		&& read ans && [ $${ans:-N} = y ]
	docker compose -f $(COMPOSE_FILE) down -v

.PHONY: docker-logs
docker-logs: ## Acompanha logs de todos os containers
	docker compose -f $(COMPOSE_FILE) logs -f

.PHONY: docker-logs-worker
docker-logs-worker: ## Acompanha logs apenas do worker
	docker compose -f $(COMPOSE_FILE) logs -f worker

.PHONY: docker-ps
docker-ps: ## Lista containers e status
	docker compose -f $(COMPOSE_FILE) ps

.PHONY: docker-build
docker-build: ## Reconstrói as imagens sem subir
	docker compose -f $(COMPOSE_FILE) build

# ------------------------------------------------------------------------------
# TESTES E QUALIDADE
# ------------------------------------------------------------------------------

.PHONY: test
test: ## Roda todos os testes
	cargo test --workspace

.PHONY: test-api
test-api: ## Roda smoke test completo contra a API rodando
	@bash scripts/test-api.sh

.PHONY: check
check: ## Verifica compilação sem gerar binários
	cargo check --workspace

.PHONY: lint
lint: ## Roda clippy (linter)
	cargo clippy --workspace -- -D warnings

.PHONY: fmt
fmt: ## Formata todo o código
	cargo fmt --all

.PHONY: fmt-check
fmt-check: ## Verifica formatação sem alterar
	cargo fmt --all -- --check

# ------------------------------------------------------------------------------
# UTILITÁRIOS
# ------------------------------------------------------------------------------

.PHONY: audit-log
audit-log: ## Exibe as últimas 20 entradas do audit log
	@psql "$(DB_URL)" -x -c \
		"SELECT entity_type, entity_id, action, actor_type, occurred_at \
		 FROM audit_log ORDER BY occurred_at DESC LIMIT 20;"

.PHONY: dlq-check
dlq-check: ## Mostra mensagens na Dead Letter Queue não resolvidas
	@psql "$(DB_URL)" -c \
		"SELECT id, source_topic, error_message, error_count, first_failed_at \
		 FROM dead_letter_queue WHERE resolved_at IS NULL ORDER BY first_failed_at;"

.PHONY: outbox-lag
outbox-lag: ## Mostra eventos pendentes no outbox (indica lag do relay)
	@psql "$(DB_URL)" -c \
		"SELECT COUNT(*) AS pending, MIN(created_at) AS oldest \
		 FROM outbox_events WHERE status = 'PENDING';"

.PHONY: balances
balances: ## Exibe saldo de todas as contas
	@psql "$(DB_URL)" -c \
		"SELECT external_id, owner_id, currency, balance, is_active \
		 FROM accounts ORDER BY external_id;"

.PHONY: reconcile
reconcile: ## Verifica consistência entre ledger e saldo das contas
	@psql "$(DB_URL)" -c \
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
		 ORDER BY ABS(a.balance - COALESCE(SUM(CASE WHEN l.entry_type = 'CREDIT' \
		              THEN l.amount ELSE -l.amount END), 0)) DESC;"

.PHONY: stats
stats: ## Resumo rápido do estado do sistema
	@echo ""
	@echo "$(BOLD)=== Transações ===$(RESET)"
	@psql "$(DB_URL)" -c \
		"SELECT status, COUNT(*) AS total, COALESCE(SUM(amount), 0) AS volume \
		 FROM transactions GROUP BY status ORDER BY status;"
	@echo "$(BOLD)=== Contas ===$(RESET)"
	@psql "$(DB_URL)" -c \
		"SELECT COUNT(*) AS total_accounts, SUM(balance) AS total_balance FROM accounts;"
	@echo "$(BOLD)=== Outbox ===$(RESET)"
	@psql "$(DB_URL)" -c \
		"SELECT status, COUNT(*) FROM outbox_events GROUP BY status;"
	@echo "$(BOLD)=== DLQ ===$(RESET)"
	@psql "$(DB_URL)" -c \
		"SELECT COUNT(*) AS unresolved FROM dead_letter_queue WHERE resolved_at IS NULL;"

.PHONY: clean
clean: ## Remove artefatos de build
	cargo clean
