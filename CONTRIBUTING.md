# Contributing

Before contributing, please review the [Troubleshooting & FAQ guide](docs/TROUBLESHOOTING.md) for details on common operational issues and setup challenges, and our [Code of Conduct](CODE_OF_CONDUCT.md) for community standards and expectations.

## Security

Please report security vulnerabilities responsibly by following our [Security Policy](SECURITY.md). Do not open public issues for security-related concerns.

## Dev setup

```bash
cp .env.example .env
docker compose up -d          # Postgres
cargo run -p lumenqraph-indexer
cargo run -p lumenqraph-api
```

## Before you push

Run the aggregate CI target to reproduce what CI checks locally (minus the
network-dependent E2E suite):

```bash
make ci
```

This runs `cargo fmt`, `cargo clippy`, the unit tests, the TypeScript and Python
SDK checks, the OpenAPI drift check, and `cargo deny`. Run `make help` to see
all available targets, including the individual steps (`fmt`, `lint`, `test`,
`sdk-ts`, `sdk-py`, `openapi-check`, `deny`, `audit`, `dashboards-check`).

If you prefer to run the steps by hand:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo audit
cargo deny check
```

The `cargo audit` and `cargo deny check` commands verify dependency security and license compliance. These must pass in CI; running them locally catches issues before pushing.

### Compile-time query checking

The codebase uses sqlx's offline mode for compile-time verification of SQL queries. After modifying any SQL queries in the code:

```bash
cargo sqlx prepare --database-url "$DATABASE_URL"
```

This generates `.sqlx/` metadata that must be committed alongside your code changes. CI will verify that the metadata is up-to-date; stale metadata will fail the build.

If you see "query not prepared" errors during build, run the command above with your database running (same as `docker compose up -d` in dev setup).

### Postgres-backed tests

`cargo test --workspace` skips anything marked `#[ignore]`, which is every test
that needs a real database (retention pruning, contract-spec versioning, webhook
enqueue). To run those locally with a single command:

```bash
make test-db
```

This target automatically:
- Ensures the Postgres container is running (`make db`)
- Sets `TEST_DATABASE_URL` to the local database
- Runs all ignored tests with `--test-threads=1` for proper isolation

Each test resets the schema to isolate itself. `--test-threads=1` is required, not a preference: each test runs `DROP SCHEMA public CASCADE` to start clean, so two running at once will drop the tables out from under each other.

If you need to run tests against a custom database, you can still use the manual approach:

```bash
export TEST_DATABASE_URL=postgres://user:password@host:port/dbname
cargo test -p lumenqraph-indexer  -- --ignored --test-threads=1
cargo test -p lumenqraph-webhooks -- --ignored --test-threads=1
cargo test -p lumenqraph-api      -- --ignored --test-threads=1
cargo test -p lumenqraph-mcp      -- --ignored --test-threads=1
```

CI runs all of the above against a Postgres service.

### Smoke tests

`crates/lumenqraph-indexer/src/smoke.rs` is a single heavy end-to-end test that
drives the whole pipeline (indexer → Postgres → API-shaped queries → webhook
enqueue → signed delivery). It uses an in-process mock Soroban RPC and a local
HTTP sink — **no live network** — but it is expensive and needs a database, so
it is kept out of the normal test run by two independent gates:

- `#[ignore]` — skipped by `cargo test` unless `--ignored` is passed.
- `#[cfg(feature = "smoke-tests")]` — the module is not even compiled without
  the `smoke-tests` cargo feature, so it can never run in an offline CI job that
  forgets to set `TEST_DATABASE_URL`.

Run it with:

```bash
make test-smoke
```

or manually:

```bash
export TEST_DATABASE_URL=postgres://user:password@host:port/dbname
cargo test -p lumenqraph-indexer --features smoke-tests smoke -- --ignored --test-threads=1
```

## Database changes

When modifying the schema or adding new tables/functions:

```bash
# 1. Create a new migration file
sqlx migrate add -r <description>

# 2. Edit the generated migrations/NNNN_<description>.sql and .down.sql
#    (Never edit an already-applied migration.)

# 3. Apply it to your local database
cargo run -p lumenqraph-indexer

# 4. Regenerate .sqlx offline metadata after the migration succeeds
cargo sqlx prepare --database-url "$DATABASE_URL"
```

After running migrations, you must commit `.sqlx/` metadata alongside your code changes. CI verifies this metadata is up-to-date; stale metadata fails the build.

To update schema objects like triggers (`contract_summaries`), edit the relevant migration and include the `CREATE TRIGGER` or `ALTER FUNCTION` statement. Test the change against a live database before committing.

## Conventions

- Shared types and decoding live in `lumenqraph-core`; don't duplicate models.
- DB writes must stay idempotent (key on `event_id`).
- New schema changes go in a new numbered `migrations/NNNN_*.sql` — never edit an
  applied migration.
- Keep raw base64 alongside any decoded representation; decoding is best-effort
  and must never break ingestion.

## Security Considerations

When contributing code, especially around authentication, cryptography, or sensitive data:

1. Use constant-time comparison for secrets and signatures (see [SECURITY.md](SECURITY.md))
2. Never hardcode secrets or API keys
3. Validate all user inputs and external data
4. Avoid leaking sensitive information in error messages
5. Keep dependencies up to date and audit transitive dependencies
