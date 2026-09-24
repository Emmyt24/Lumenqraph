.PHONY: help db db-down seed build test test-db test-smoke fmt lint indexer api webhooks backfill up down

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

db: ## Start local Postgres
	docker compose up -d

db-down: ## Stop local Postgres
	docker compose down

seed: ## Populate demo dataset for local exploration
	@if [ -z "$$DATABASE_URL" ]; then \
	  export DATABASE_URL=postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph; \
	fi; \
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/seed.sql

build: ## Build the workspace
	cargo build --workspace

test: ## Run tests (unit + integration, no Postgres required)
	cargo test --workspace

test-db: db ## Run Postgres-backed tests (requires TEST_DATABASE_URL or a running local Postgres)
	@if [ -z "$$TEST_DATABASE_URL" ]; then \
	  export TEST_DATABASE_URL=postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph_test; \
	fi; \
	psql "$$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -c 'SELECT 1' >/dev/null 2>&1 || \
	  psql "postgres://lumenqraph:lumenqraph@localhost:5432/postgres" -v ON_ERROR_STOP=1 \
	    -c 'CREATE DATABASE lumenqraph_test' >/dev/null 2>&1 || true; \
	cargo test -p lumenqraph-indexer  -- --ignored; \
	cargo test -p lumenqraph-webhooks -- --ignored; \
	cargo test -p lumenqraph-api      -- --ignored; \
	cargo test -p lumenqraph-mcp      -- --ignored

test-smoke: db ## Run the gated end-to-end smoke test (requires TEST_DATABASE_URL or a running local Postgres)
	@if [ -z "$$TEST_DATABASE_URL" ]; then \
	  export TEST_DATABASE_URL=postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph_test; \
	fi; \
	psql "$$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -c 'SELECT 1' >/dev/null 2>&1 || \
	  psql "postgres://lumenqraph:lumenqraph@localhost:5432/postgres" -v ON_ERROR_STOP=1 \
	    -c 'CREATE DATABASE lumenqraph_test' >/dev/null 2>&1 || true; \
	cargo test -p lumenqraph-indexer --features smoke-tests smoke -- --ignored

fmt: ## Format
	cargo fmt --all

lint: ## Clippy (deny warnings)
	cargo clippy --workspace --all-targets -- -D warnings

indexer: ## Run the indexer (live)
	cargo run -p lumenqraph-indexer

backfill: ## Run backfill from START_LEDGER (make backfill LEDGER=123)
	cargo run -p lumenqraph-indexer -- backfill $(LEDGER)

api: ## Run the API
	cargo run -p lumenqraph-api

webhooks: ## Run the webhooks service
	cargo run -p lumenqraph-webhooks

up: ## Full stack in Docker
	docker compose -f docker-compose.full.yml up --build -d

down: ## Stop the full stack
	docker compose -f docker-compose.full.yml down
