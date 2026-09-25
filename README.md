<div align="center">

# Lumenqraph

**An open-source, self-hostable event indexer for Soroban smart contracts on Stellar.**

Tail contract events from Soroban RPC, decode their XDR to clean JSON, store them in Postgres, and serve them over a plain REST API and signed webhooks — *curl and get JSON*, no VM or custom program to deploy.

[![CI](https://github.com/Lumen-Scribe/Lumenqraph/actions/workflows/ci.yml/badge.svg)](https://github.com/Lumen-Scribe/Lumenqraph/actions/workflows/ci.yml)
[![E2E](https://github.com/Lumen-Scribe/Lumenqraph/actions/workflows/e2e-test.yml/badge.svg)](https://github.com/Lumen-Scribe/Lumenqraph/actions/workflows/e2e-test.yml)
[![Coverage](https://codecov.io/gh/Lumen-Scribe/Lumenqraph/branch/main/graph/badge.svg)](https://codecov.io/gh/Lumen-Scribe/Lumenqraph)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)
[![Built for Stellar](https://img.shields.io/badge/built%20for-Stellar%20Soroban-black.svg)](https://stellar.org/soroban)

[Quick start](#quick-start) · [API](#api) · [Architecture](#architecture) · [Docs](docs/) · [Troubleshooting](docs/TROUBLESHOOTING.md) · [Security](SECURITY.md) · [Code of Conduct](CODE_OF_CONDUCT.md) · [Roadmap](#roadmap) · [Contributing](#contributing)

### 🔭 [Live demo → lumenqraph.onrender.com](https://lumenqraph.onrender.com)

Indexing Stellar **mainnet** right now. Below: the [Aquarius AMM](https://aqua.network) swap router's
75-function typed interface, parsed straight from its **on-chain** WASM spec — no ABI upload, no config.

![The Lumenqraph explorer showing the Aquarius router's typed interface, decoded live from the contract's on-chain spec](docs/images/explorer-interface.png)

</div>

---

## Table of contents

- [Why Lumenqraph](#why-lumenqraph)
- [Comparison with Alternatives](#comparison-with-alternatives)
- [Features](#features)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API](#api)
- [Typed, self-describing decoding](#typed-self-describing-decoding)
- [Contract upgrade watch](#contract-upgrade-watch)
- [Read layer — `eth_call` for Soroban](#read-layer--eth_call-for-soroban)
- [Contract state indexing](#contract-state-indexing)
- [Per-key state indexing](#per-key-state-indexing)
- [GraphQL](#graphql)
- [TypeScript SDK](#typescript-sdk)
- [AI-agent access — the MCP server](#ai-agent-access--the-mcp-server)
- [Webhooks](#webhooks)
- [Multi-network deployments](#multi-network-deployments)
- [Running in production](#running-in-production)
- [Project structure](#project-structure)
- [Development](#development)
- [Roadmap](#roadmap)
- [Known limitations](#known-limitations)
- [Troubleshooting & FAQ](docs/TROUBLESHOOTING.md)
- [Explorer UI & Configuration](docs/EXPLORER.md)
- [Migration Rollback Strategy](docs/MIGRATIONS.md)
- [Upgrading Between Versions](docs/UPGRADING.md)
- [Security](SECURITY.md)
- [Contributing](#contributing)
- [License](#license)

## Why Lumenqraph

Soroban contracts emit events, but raw on-chain data is nearly unusable for building a frontend or dashboard directly — you'd have to replay the ledger, decode XDR, and reconstruct state yourself. An indexer sits between the chain and your application: it watches events, decodes them, stores them queryably, and serves them over an API so your dApp makes a normal HTTP call instead of talking to the chain.

Lumenqraph's angle is **simplicity, self-hostability, and typed decoding that needs zero configuration**:

- **Typed, self-describing events — automatically.** Soroban contracts publish their full interface (function, type, and event schemas) *on-chain*, embedded in the deployed WASM. Lumenqraph reads that schema and turns a raw event into a **named, typed record** (`{ from: Address, to: Address, amount: i128 }`) with no ABI upload and no manual mapping. This is a Soroban-native advantage — on EVM chains the equivalent ABI lives off-chain and has to be verified or uploaded by hand. See [Typed, self-describing decoding](#typed-self-describing-decoding).
- **Zero learning curve** — a plain REST API and JSON webhooks. No custom VM, no programs to write and deploy.
- **Self-hostable and inspectable** — run it on your own infrastructure; the code is open and auditable.
- **Decoded, not raw** — XDR is decoded to friendly JSON (addresses as `G…`/`C…` strkeys, amounts as decimal strings), with the raw base64 always retained losslessly.

## Comparison with Alternatives

Lumenqraph occupies a specific niche. It trades unlimited historical indexing depth (managed services) for lightweight, self-hostable, zero-compile type safety. Below is a feature-by-feature comparison vs existing Soroban/Stellar indexing solutions:

| Feature / Axis | Lumenqraph | Mercury | Allium | SubQuery |
| --- | --- | --- | --- | --- |
| **Model** | Self-hosted | Managed Service | Managed Data Lake | Hybrid (Self / Managed) |
| **Decoding** | WASM spec-driven (Zero ABI upload) | Custom WASM mappings | Raw XDR schemas | Custom Javascript mappings |
| **Upgrade Watch** | Automatic semantic diffing | Manual update / tracking | N/A (raw decoding) | Manual subgraph update |
| **Read Simulator** | Built-in `/call` read simulation | Direct RPC simulate | N/A | N/A |
| **AI / MCP Native** | Yes (Model Context Protocol) | No | No | No |
| **Historical Depth** | Bounded (RPC limit or data-lake) | Unlimited / Archive | Unlimited / Archive | Unlimited / Archive |
| **Operations** | Zero-compile REST & webhooks | Managed (Zero ops) | SQL Interface (Zero ops) | Compile WASM & deploy subgraphs |
| **Pricing** | Infrastructure costs only | Volume-based pricing | Contract pricing | Subgraph hosting fees |

### Honest Tradeoffs: Where Managed Services Win
If your project requires **deep historical event analysis** going back years, or absolute guarantee of zero server operational overhead, a managed platform is a better fit:
- **Allium** excels at querying massive, cold, historical datasets across the entire Stellar ecosystem via SQL-based data lakes.
- **Mercury** provides an always-on, zero-ops event database that abstracts away all server maintenance, RPC endpoints, and database connection budgets.

### Where Lumenqraph Wins
Lumenqraph is ideal for developers seeking **complete ownership of their data** and **low-latency interaction** without vendor lock-in:
- **Zero-Configuration Type Safety**: Automatically reads on-chain metadata schemas. There are no ABI files to generate, host, or upload.
- **Contract Upgrade Diffing**: If a contract upgrades in-place, Lumenqraph records the version transition, highlights the changes (added/removed functions, breaking changes), and dispatches webhook alerts.
- **Integrated View-Simulation**: Simulates smart contract view calls through unified HTTP endpoints (`/call`) without needing separate Stellar SDK configuration.
- **AI-Agent Readiness**: Connects directly to LLMs (like Claude) using the built-in MCP server for automated contract interaction.

## Features

| | |
| --- | --- |
| 🧩 **Full XDR → JSON decoding** | ScVal decoded to friendly JSON: `i128`/`u128` as decimal strings, addresses as `G…`/`C…` strkeys, bytes as hex, vecs/maps as arrays/objects. Raw base64 always retained. |
| 🏷️ **Typed, spec-driven decoding** | Reads each contract's on-chain `contractspecv0` interface to emit **named, typed** events (`{from, to, amount: i128}`) — zero ABI upload. Serves the full decoded interface at `/contracts/:id/interface`. |
| 📖 **Read layer (`eth_call` for Soroban)** | Invoke any contract view function read-only over REST and get a **typed** result. Arguments are type-checked against the on-chain spec before simulation. |
| 🔮 **Transaction preview** | Dry-run *any* call (even state-changing ones) without signing or submitting, and see the typed result, the **events it would emit**, and its resource cost. Tenderly-style preview for Soroban. |
| 🚨 **Contract upgrade watch** | Soroban contracts are upgradable *in place*. Lumenqraph keeps every version of a contract's on-chain interface and **diffs them semantically** — which functions and events were added, removed, or changed, and whether that's **breaking** — then pushes a `contract.upgraded` webhook when it happens. |
| 🤖 **MCP server (AI-agent access)** | A [Model Context Protocol](https://modelcontextprotocol.io) server that lets Claude (or any MCP agent) discover, query, and call any indexed Soroban contract — typed and self-describing, zero hand-written schema. |
| 🗂️ **Contract state indexing** | Optional versioned snapshots of each contract's instance storage (admin, config, TVL, counters…), decoded to JSON — historical *state*, not just events. |
| 👛 **Per-key state indexing** | Optional versioned snapshots of *individual* storage entries — e.g. each token holder's `Balance(Address)` — discovered from the contract's own events. Per-key history, not just the instance map. |
| 💸 **Materialized token transfers** | SEP-41 `transfer` events projected into a queryable `from`/`to`/`amount` table. |
| 🔌 **REST + GraphQL API** | Contracts, events (filterable), transfers, state, per-key data, health, and Prometheus metrics over REST — plus a GraphQL endpoint with Relay-style cursor pagination. |
| 📦 **TypeScript SDK** | A typed, zero-dependency client (`@lumenqraph/sdk`) over the REST + GraphQL API, with a cursor-paginating async iterator. |
| 📣 **Signed webhooks** | Register a URL + filter, receive HMAC-SHA256-signed event pushes with retries and exponential backoff. |
| 🔑 **Auth & rate limiting** | SHA-256-hashed API keys with per-key request limits. |
| 📊 **Observability** | Prometheus `/metrics` and a `/health` endpoint reporting chain-tip lag. |
| ⏪ **Backfill mode** | One-shot historical catch-up (bounded by RPC retention). |
| 🛡️ **Robust ingestion** | Idempotent, reorg-tolerant writes; graceful shutdown; automatic retry with backoff. |
| 🐳 **Production-ready** | Dockerized, CI-gated (fmt + clippy + tests), fully documented. |

## Architecture

Lumenqraph is a Rust workspace of four service binaries sharing one core library. The services coordinate only through Postgres, so each can scale, restart, and fail independently — API traffic can never stall ingestion, and a decode bug can't take down the read path.

```
  Soroban RPC ──poll getEvents──▶ ┌───────────┐ ──write──▶ ┌────────────┐ ◀──read── ┌─────────┐ ──REST──▶ dApps
                                  │  indexer  │            │  Postgres  │           │   api   │
                                  └───────────┘            └────────────┘           └─────────┘
                                                                 ▲
                                                          read   │  write
                                                            ┌────────────┐
                                                            │  webhooks  │ ──signed POST──▶ subscribers
                                                            └────────────┘
```

| Crate | Role |
| --- | --- |
| [`lumenqraph-core`](crates/lumenqraph-core) | Shared models, error types, the self-contained XDR→JSON / strkey decoder, the on-chain contract-spec parser + event enricher, and the read-layer call encoder. |
| [`lumenqraph-indexer`](crates/lumenqraph-indexer) | Always-on process: polls `getEvents`, decodes, enriches against each contract's interface, writes to Postgres. |
| [`lumenqraph-api`](crates/lumenqraph-api) | Axum read + management API (auth, rate limiting, metrics) and the read layer (typed view-function calls via RPC). |
| [`lumenqraph-webhooks`](crates/lumenqraph-webhooks) | Matches events to subscriptions and delivers signed pushes. |
| [`lumenqraph-mcp`](crates/lumenqraph-mcp) | Model Context Protocol server: typed, self-describing contract access for AI agents. |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for details.

## Quick start

**Prerequisites:** [Rust](https://rustup.rs/) (stable) and [Docker](https://docs.docker.com/get-docker/).

```bash
git clone https://github.com/Lumen-Scribe/Lumenqraph.git
cd Lumenqraph
cp .env.example .env                 # defaults target Stellar testnet

docker compose up -d                 # local Postgres
cargo run -p lumenqraph-indexer      # polls RPC + auto-applies migrations
cargo run -p lumenqraph-api          # REST API on :8080 (separate shell)
cargo run -p lumenqraph-webhooks     # optional: webhook delivery (separate shell)
```

Query it:

```bash
curl localhost:8080/health
curl localhost:8080/contracts
curl 'localhost:8080/contracts/<CONTRACT_ID>/events?event_name=transfer&limit=5'
curl 'localhost:8080/contracts/<CONTRACT_ID>/transfers?limit=5'
curl localhost:8080/metrics
```

To run the entire stack (Postgres + all three services) in Docker:

```bash
docker compose -f docker-compose.full.yml up --build
```

Common tasks are wrapped in the [`Makefile`](Makefile) — run `make help`.

## Configuration

All configuration is via environment variables (see [`.env.example`](.env.example)).

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph` | Postgres connection string. |
| `RPC_URL` | `https://soroban-testnet.stellar.org` | Soroban RPC endpoint. Used by the indexer (polling) and the API (read-layer simulation). |
| `CONTRACT_IDS` | *(empty)* | Comma-separated contract IDs to index. Empty = **all** contract events. The Soroban RPC `getEvents` method supports at most **25 contract IDs** (5 filters × 5 IDs per filter); providing more causes the indexer to exit at startup with an actionable error. To index more than 25 contracts, run multiple indexer instances each covering a different subset. |
| `POLL_INTERVAL_SECS` | `5` | How often the indexer polls for new events. |
| `PAGE_SIZE` | `1000` | Events requested per `getEvents` page (1–10000). |
| `START_LEDGER` | `0` | Ledger to start a fresh index from. `0` = near the tip. Clamped to RPC retention. |
| `UPGRADE_WATCH` | on with `CONTRACT_IDS` | Detect contract upgrades, recording each new interface version and its diff. One RPC call per tracked contract per cycle, so it defaults on only when `CONTRACT_IDS` bounds that set. Free alongside `STATE_INDEXING`, which reads the same entry. See [Contract upgrade watch](#contract-upgrade-watch). |
| `STATE_INDEXING` | `false` | Snapshot contract instance storage into `contract_state` (versioned). One extra RPC call per tracked contract per cycle; best paired with `CONTRACT_IDS`. |
| `RETENTION_LEDGERS` | `0` | Keep only the last N ledgers of history, pruning older events (+ cascaded transfers) and superseded state versions as the tip advances. `0` = keep everything. Set it when the database has a hard size cap. ~17280 ≈ 1 day. |
| `SPEC_VERSION_RETENTION` | `0` | Keep at least N interface versions per contract in the spec history, even outside the retention window, for recent upgrade history. `0` = no minimum (follows `RETENTION_LEDGERS`). Only applies when `RETENTION_LEDGERS` is also set. Useful to always retain the last N upgrades: e.g. `5` keeps the last 5 interface versions per contract. |
| `SPEC_VERSION_RETENTION` | `0` | Keep only the last N versions of each contract's spec in `contract_spec_versions`, pruning older versions once they fall outside the retention window. `0` = keep all versions. The newest N versions per contract are always preserved even if outside the window. Pairs with `RETENTION_LEDGERS` to manage disk usage in index-all mode. |
| `SPEC_CACHE_MAX_ENTRIES` | `2000` | Maximum number of contract specs held in memory before evicting least-recently-used entries. Prevents unbounded memory growth in index-all mode. Evicted specs are re-fetched from the database on next miss. Increase for better cache hit rates with many contracts; decrease to reduce memory usage. |
| `ENRICHMENT_WARN_THRESHOLD` | `0.5` | Warn (emit a warn-level log) if the not-enriched fraction (events failing spec decode or fetch) exceeds this threshold in a single poll cycle. Range: 0.0–1.0. Default 0.5 = warn if >50% fail. Set to 0.0 to disable warnings; 1.0+ to warn only at 100%. |
| `API_BIND_ADDR` | `0.0.0.0:8080` | API listen address. |
| `INSTANCE_MOUNTS` | *(unset)* | Comma-separated list of sibling instances to reverse-proxy under path prefixes (e.g. `testnet=http://127.0.0.1:8081`). Enables serving multiple networks from one deployment. See [Multi-network deployments](#multi-network-deployments). |
| `CORS_ALLOWED_ORIGINS` | *(unset)* | Comma-separated list of allowed origins for CORS requests (e.g. `https://example.com`), `*` to allow all origins, or unset for same-origin only (no CORS headers added, default behavior). |
| `REQUIRE_API_KEY` | `false` | Require a valid API key on data routes. |
| `METRICS_REQUIRE_API_KEY` | `false` | Require a valid API key on `GET /metrics`. When `false` (default) the endpoint is public. Set to `true` in production when the API is internet-accessible to avoid leaking operational telemetry. |
| `MAX_REQUEST_BODY_BYTES` | `65536` | Maximum request body size in bytes (64 KB). Axum returns `413` for larger bodies. Protects `POST /call`, `POST /simulate`, and `POST /graphql` from large-payload denial-of-service. Replaces the old `API_MAX_BODY_BYTES` variable (kept as a fallback alias). |
| `ANON_RATE_LIMIT_PER_MIN` | `60` | Requests/min for unauthenticated callers. |
| `WEBHOOK_TICK_SECS` | `3` | Webhook dispatcher poll interval. |
| `WEBHOOK_BATCH_SIZE` | `100` | Deliveries processed per tick. |
| `WEBHOOK_MAX_ATTEMPTS` | `6` | Delivery attempts before a webhook is marked failed. |
| `READYZ_LAG_THRESHOLD` | `100` | Max ledger lag (in ledgers) for `/readyz` to return 200. |
| `READYZ_MAX_AGE_SECS` | `120` | Max age of indexer cursor (in seconds) for `/readyz` to return 200. |
| `HEALTH_MAX_LAG_LEDGERS` | `100` | Max ledger lag for `/health` to show "ok" status. |
| `HEALTH_MAX_STALE_SECS` | `120` | Max age of indexer cursor (in seconds) for `/health` to show "ok" status. |
| `RUST_LOG` | `info` | Log filter (`tracing` syntax). |

## API

Base URL defaults to `http://localhost:8080`. Full reference: [docs/API.md](docs/API.md).

**Authentication.** Data routes accept an API key via `Authorization: Bearer <key>` or `x-api-key: <key>`. When `REQUIRE_API_KEY=false` (default), unauthenticated callers are allowed up to `ANON_RATE_LIMIT_PER_MIN`. `/health`, `/livez`, and `/readyz` are always public. `/metrics` is public by default and can be restricted with `METRICS_REQUIRE_API_KEY=true`. Rate-limit breaches return `429`; invalid or revoked keys return `401`.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Indexing status and chain-tip lag. *(public)* |
| `GET` | `/livez` | Liveness probe: returns 200 if process is running. No database access. *(public)* |
| `GET` | `/readyz` | Readiness probe: returns 200 only when indexer is caught up and healthy; 503 otherwise. *(public)* |
| `GET` | `/metrics` | Prometheus metrics. *(public)* |
| `GET` | `/contracts` | Contracts seen, with per-contract counts. |
| `GET` | `/contracts/:id/interface` | The contract's decoded on-chain interface: functions, events, and user-defined types. Query: `version` (a historical version; default current). |
| `GET` | `/contracts/:id/interface/history` | Every interface version observed, newest first, each with its diff and `breaking` flag. Query: `limit`. |
| `GET` | `/contracts/:id/interface/diff` | What changed between two interface versions. Query: `from`, `to` (default: the latest upgrade). |
| `GET` | `/contracts/:id/sdk` | Generate a typed SDK client from the contract's on-chain spec. Query: `lang` (default `ts`), `version` (historical version). Returns a ready-to-use TypeScript client. |
| `GET` | `/contracts/:id/state` | Versioned snapshots of the contract's instance storage, newest first. Query: `limit` (1 = current state). |
| `GET` | `/contracts/:id/data` | Latest value of every per-key entry (e.g. holder balances). Query: `label`, `limit`. |
| `GET` | `/contracts/:id/data/:key_hash` | Version history of a single per-key entry. Query: `limit`. |
| `GET` | `/contracts/:id/functions` | The contract's callable functions with typed inputs/outputs. |
| `POST` | `/contracts/:id/call` | Invoke a view function read-only and return a typed result. Body: `{ function, args, source_account? }`. |
| `POST` | `/contracts/:id/simulate` | Dry-run any call; returns the typed result, the events it would emit, and its resource cost. Same body as `/call`. |
| `GET` | `/contracts/:id/events` | Events for a contract. Query: `limit`, `offset`, `event_name`. |
| `GET` | `/events/:event_id` | Fetch a single event by its unique id. `404` if not found. |
| `GET` | `/transactions/:tx_hash/events` | All indexed events emitted by a transaction, in emission order. Query: `limit`. |
| `GET` | `/contracts/:id/transfers` | Materialized token transfers. Query: `limit`, `offset`, `from`, `to`. |
| `GET` | `/contracts/:id/swaps` | Materialized AMM swap events. Query: `limit`, `offset`, `after`, `sender`, `sell_token`, `buy_token`. |
| `GET` | `/contracts/:id/nfts` | Materialized NFT events (mint/transfer/burn). Query: `limit`, `offset`, `after`, `kind`, `from`, `to`, `token_id`. |
| `GET` | `/contracts/:id/liquidity` | Materialized liquidity events (add/remove). Query: `limit`, `offset`, `after`, `kind`, `provider`. |
| `POST`/`GET` | `/graphql` | GraphQL endpoint (POST queries; GET serves the GraphiQL IDE). See [GraphQL](#graphql). |
| `POST` | `/webhooks` | Create a subscription. |
| `GET` | `/webhooks` | List subscriptions (secrets omitted). |
| `DELETE` | `/webhooks/:id` | Delete a subscription. |

**Multi-network paths:** When `INSTANCE_MOUNTS` is configured (see [Multi-network deployments](#multi-network-deployments)), all routes above are also available under `/<mount-name>/` prefixes. Example: `GET /<mount-name>/contracts` queries the mounted instance.

<details>
<summary><b>Example: a decoded transfer event</b></summary>

```jsonc
// GET /contracts/<CID>/events?event_name=transfer&limit=1
{
  "event_id": "0015250934946869248-0000000000",
  "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
  "ledger": 3550885,
  "event_name": "transfer",
  "decoded_topics": ["transfer", "GAIH3ULL...ZNSR", "GDN4OHYR...YQZ3", "native"],
  "decoded_value": "100000000000",
  // Named + typed, from the contract's on-chain spec (null if the contract
  // publishes no matching event schema):
  "enriched": {
    "event": "transfer",
    "params": {
      "from":   { "type": "Address", "value": "GAIH3ULL...ZNSR" },
      "to":     { "type": "Address", "value": "GDN4OHYR...YQZ3" },
      "amount": { "type": "i128",    "value": "100000000000" }
    }
  },
  "topics": ["AAAADwAA...", "..."],   // raw base64 XDR, always retained
  "value": "AAAACv//...",
  "tx_hash": "3664562a...",
  "in_successful_call": true
}
```
</details>

Generate an API key:

```bash
DATABASE_URL=... ./scripts/gen_api_key.sh myapp pro 600   # name, tier, requests/min
```

## Typed, self-describing decoding

Every generic decoder can tell you a value is an `i128` or an address. Lumenqraph goes further: it reads each contract's **on-chain interface** — the `contractspecv0` schema that Soroban embeds directly in the deployed WASM — and uses it to attach real **field names and types** to every event, automatically.

The first time the indexer sees a contract, it fetches the contract's WASM (two `getLedgerEntries` hops: instance → WASM hash → code), parses the interface once, caches it, and persists it. Every later event from that contract is enriched into a named record and stored in the `enriched` column. If a contract publishes no matching schema, `enriched` is simply `null` and the always-present `decoded_*` fields remain — nothing is ever lost.

Inspect any deployed contract's interface straight from the CLI (no database required):

```bash
cargo run -p lumenqraph-indexer -- inspect <CONTRACT_ID>
```

```jsonc
// GET /contracts/<CID>/interface  (or the `inspect` command above)
{
  "contract_id": "CB...",
  "has_events": true,
  "interface": {
    "events": [
      { "name": "transfer", "data_format": "single", "params": [
        { "name": "from",   "type": "Address", "location": "topic" },
        { "name": "to",     "type": "Address", "location": "topic" },
        { "name": "amount", "type": "i128",    "location": "data"  }
      ] }
    ],
    "functions": [ { "name": "transfer", "inputs": [ /* … */ ], "outputs": [] } ],
    "structs": [], "unions": [], "enums": []
  }
}
```

**Why this is a Soroban advantage.** On EVM chains, the ABI that names an event's fields lives *off-chain* — an indexer only produces human-readable data if someone verifies the contract or uploads its ABI. Soroban ships that schema *with the code*, so Lumenqraph delivers typed, self-describing data for any contract with **zero configuration**. Implementation: [`lumenqraph-core::spec`](crates/lumenqraph-core/src/spec.rs).

## Contract upgrade watch

A Soroban contract can be **upgraded in place**: the same `C…` address starts running new code, and exposing a new interface, at any ledger. If your dApp calls that contract, its signature can change underneath you with no warning and no notification — and today nothing in the ecosystem tells you.

Because the interface ships inside the WASM, Lumenqraph can capture it at every upgrade and say exactly what changed. The indexer keeps an append-only history in `contract_spec_versions` and diffs each version against the one before it:

```jsonc
// GET /contracts/<CID>/interface/diff?from=1&to=2
{
  "contract_id": "CB...", "from": 1, "to": 2,
  "diff": {
    "breaking": true,
    "summary": [
      "removed function withdraw(amount: i128) -> void",
      "changed event transfer: transfer(from: Address @topic, to: Address @topic) [single] became transfer(from: Address @topic, to: Address @data) [single]"
    ],
    "functions": { "added": ["pause() -> void"], "removed": ["withdraw(amount: i128) -> void"], "changed": [] },
    "events":    { "added": [], "removed": [], "changed": [ /* … */ ] },
    "types":     { "added": [], "removed": [], "changed": [] }
  }
}
```

A change is **breaking** if anything was removed or changed — i.e. an integration built against the old interface may stop working. Purely additive upgrades are not. Diffs compare each item's *rendered signature*, so a parameter that is renamed, retyped, or moved from topic to data all register — including the ones that fail silently rather than loudly.

| | |
| --- | --- |
| `GET /contracts/:id/interface/history` | Every version observed, newest first, each with its diff and `breaking` flag |
| `GET /contracts/:id/interface/diff?from=&to=` | Diff any two versions (defaults: the latest upgrade). Computed on demand, so `from=1&to=5` is one call |
| `GET /contracts/:id/interface?version=N` | The interface as it was at version N — what callers were binding to then |
| `POST /webhooks` with `{"kind": "upgrade"}` | Get pushed a signed `contract.upgraded` payload the moment it happens |

```jsonc
// The contract.upgraded webhook payload (HMAC-signed like any other delivery)
{
  "type": "contract.upgraded", "contract_id": "CB...", "version": 2,
  "wasm_hash": "…", "previous_wasm_hash": "…",
  "breaking": true, "diff": { /* as above */ }, "observed_at": "2026-07-15T…Z"
}
```

Version 1 is a *baseline*, not an upgrade: it's the first interface we ever saw, so it has no diff (`null`, distinct from an empty diff) and fires no webhook. An upgrade that changes only the code and not the interface — a bug fix — is still recorded, with an empty, non-breaking diff.

Enable it with `UPGRADE_WATCH=true`; it defaults to on whenever `CONTRACT_IDS` bounds the set of contracts to watch. See [Configuration](#configuration). Implementation: [`lumenqraph-core::diff`](crates/lumenqraph-core/src/diff.rs) and [`lumenqraph-indexer::specs`](crates/lumenqraph-indexer/src/specs.rs).

**Why this is a Soroban advantage.** The same argument as typed decoding, one step further: an EVM indexer can't diff a contract's ABI across an upgrade, because the ABI was never on-chain to begin with. Soroban's is — so this is ecosystem safety infrastructure that can be built *here* and nowhere else.

## Read layer — `eth_call` for Soroban

History tells you what *happened*; the read layer tells you the current *state*. `GET /contracts/:id/events` serves indexed events; `POST /contracts/:id/call` invokes a contract's **view functions** read-only — the Soroban equivalent of EVM's `eth_call`, a primitive no other Stellar indexer exposes as a service.

Under the hood it uses Soroban RPC's `simulateTransaction`. The friction that usually makes this hard — hand-building a transaction envelope and encoding/decoding XDR — is gone: Lumenqraph reads the arguments straight from JSON, **type-checks and encodes them against the contract's on-chain spec** (so a bad argument is rejected *before* the network call), simulates, and decodes the result into typed JSON.

```bash
# Discover what you can call:
curl localhost:8080/contracts/<CID>/functions
# → [{ "name": "balance", "inputs": [{ "name": "account", "type": "Address" }], "outputs": ["i128"] }, …]

# Call a view function (args by name or as a positional array):
curl -X POST localhost:8080/contracts/<CID>/call \
  -H 'Content-Type: application/json' \
  -d '{"function":"balance","args":{"account":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"}}'
```

```jsonc
{
  "contract_id": "CDTLXP6K…HIZA",
  "function": "balance",
  "result": { "type": "i128", "value": "0" },
  "simulated_at_ledger": 3585685
}
```

Errors are precise and client-facing: unknown function, missing/extra argument, or a wrong-typed argument all return `400` with a message (`argument "account": invalid address strkey`) rather than a failed simulation. Reads need the contract's interface, which the indexer captures on first sighting — Stellar Asset Contracts (no WASM spec) aren't callable this way. Supported argument types today: bool, all sized integers, `i128`/`u128`, `u256`/`i256` (as decimal strings), `Symbol`, `String`, `Address`, `Bytes`/`BytesN`, `Option`, `Vec`, `Tuple`, symbol-keyed `Map`, and **user-defined types** — structs, unit enums, and unions — resolved from the contract's own on-chain spec. Only `Val` (untyped by definition), `Result`, `Error`, and `MuxedAddress` remain unsupported. Implementation: [`lumenqraph-core::read`](crates/lumenqraph-core/src/read.rs).

User-defined arguments follow the shape the spec declares, so there is nothing to configure:

```jsonc
{ "order":  { "amount": "500", "buyer": "GABC…" } }  // struct -> its fields
{ "status": "Filled" }                               // unit enum -> a case name
{ "action": { "Bid": ["GABC…", "250"] } }            // union -> case + values
{ "action": "Cancel" }                               // union void case
```

The same naming applies coming back out: a unit enum that would decode to a bare `7` is enriched to `"Filled"`, and a union to `{"Bid": [...]}` — recursively, so UDTs nested inside `Option`/`Vec`/`Map`/tuples are named too.

### Transaction preview

`POST /contracts/:id/simulate` goes a step further: it dry-runs **any** call — including state-changing ones like `transfer` or `deposit` — and returns not just the typed result but **the events the call would emit** (decoded, and enriched with the contract's spec) and its estimated resource fee. Nothing is signed or broadcast; it runs through RPC simulation. This is Soroban's answer to Tenderly's transaction preview — *see exactly what a call would do before your users ever sign it*.

```jsonc
// POST /contracts/<CID>/simulate  { "function": "transfer", "args": { "from": "G…", "to": "G…", "amount": "1000" } }
{
  "contract_id": "C…",
  "function": "transfer",
  "result": { "type": "void", "value": null },
  "events": [
    {
      "type": "contract",
      "contract_id": "C…",
      "event": "transfer",
      "enriched": { "event": "transfer", "params": {
        "from":   { "type": "Address", "value": "G…" },
        "to":     { "type": "Address", "value": "G…" },
        "amount": { "type": "i128",    "value": "1000" }
      } }
    }
  ],
  "min_resource_fee": "34567",
  "simulated_at_ledger": 3585700
}
```

## Contract state indexing

Events are *history* (what happened); the read layer is current state *on demand* (one call, right now). Contract state indexing is the third leg: **versioned snapshots of a contract's on-chain state**, stored so you can query current state cheaply and see how it changed over time — something even `eth_call` can't do without an archive node.

Enable it with `STATE_INDEXING=true` (ideally alongside `CONTRACT_IDS`). Each cycle the indexer reads a tracked contract's **instance storage** — the enumerable key→value map Soroban keeps in the contract's instance entry (admin, config, global counters, TVL, …) — decodes it to JSON (the same shape as events), and writes a new `contract_state` row **only when the instance actually changed** (detected via the entry's `lastModifiedLedgerSeq`). The newest row is current state; older rows are history.

```jsonc
// GET /contracts/<CID>/state          → current state (limit=1, the default)
// GET /contracts/<CID>/state?limit=50 → the last 50 versions, newest first
{
  "contract_id": "CDTLXP6K…HIZA",
  "count": 1,
  "versions": [
    {
      "ledger": 3584379,
      "captured_at": "2026-07-13T13:11:59Z",
      "storage": [
        { "key": "METADATA", "val": { "name": "Stellars LP", "symbol": "sLP", "decimals": 13 } },
        { "key": ["TotalSupply"],  "val": "1624887782285314838" },
        { "key": ["ReservedUsdc"], "val": "768829008937" },
        { "key": ["IsPaused"],     "val": false }
        // …
      ]
    }
  ]
}
```

Reading the instance also reveals the contract's current WASM hash, so an **upgraded contract is detected automatically** — its interface is re-read immediately, keeping enrichment and the read/MCP layers correct across upgrades, and recording the new version and its diff (see [Contract upgrade watch](#contract-upgrade-watch)). Because state indexing already reads this entry, running it alongside the upgrade watch costs no extra RPC calls. Implementation: [`lumenqraph-indexer::state`](crates/lumenqraph-indexer/src/state.rs).

## Per-key state indexing

Instance storage is the one enumerable map RPC exposes. But the most interesting state usually lives in *separate*, **non-enumerable** ledger entries — a token's `Balance(Address)` per holder, say. You can only read one of those if you already know its exact key.

Lumenqraph derives the keys worth tracking from the events it already indexes: a token's own `transfer`/`mint`/`burn` events name its holders, so enabling `KEY_INDEXING=true` makes the indexer snapshot each holder's `Balance(Address)` entry (change-detected and versioned, exactly like instance state) into the `contract_data` table.

```jsonc
// GET /contracts/<CID>/data?label=balance      → latest value of every tracked key
// GET /contracts/<CID>/data/<KEY_HASH>?limit=50 → one holder's balance over time
{
  "contract_id": "CDLZFC3S…YSC",
  "count": 2,
  "keys": [
    { "key_hash": "9f2c…", "key": ["Balance", "GAIH3ULL…ZNSR"], "value": "100000000000", "ledger": 3584401, "durability": "persistent", "label": "balance" },
    { "key_hash": "1a7b…", "key": ["Balance", "GDN4OHYR…YQZ3"], "value": "250000000000", "ledger": 3584390, "durability": "persistent", "label": "balance" }
  ]
}
```

The balance key variant symbol (`Balance` in the soroban token reference) and its durability are configurable via `BALANCE_KEY_SYMBOL` / `BALANCE_KEY_DURABILITY` for tokens that differ. Because per-holder snapshots cost one RPC call per newly-active holder per cycle, pair `KEY_INDEXING` with `CONTRACT_IDS`. Implementation: [`lumenqraph-indexer::keys`](crates/lumenqraph-indexer/src/keys.rs) (discovery) and [`state::snapshot_data`](crates/lumenqraph-indexer/src/state.rs).

## GraphQL

REST stays the primary, zero-dependency interface; a **GraphQL endpoint is offered alongside it** at `/graphql` (POST for queries, GET for the in-browser GraphiQL IDE) for clients that want to select fields and page large histories with cursors. High-volume lists (`events`, `transfers`) are **Relay-style cursor connections**; naturally bounded lists (`contracts`, `contractState`, `contractData`) are plain lists.

```graphql
query Recent($id: String!, $after: String) {
  events(contractId: $id, eventName: "transfer", first: 50, after: $after) {
    edges { cursor node { ledger eventName enriched decodedValue } }
    pageInfo { hasNextPage endCursor }
  }
}
```

Pass a page's `pageInfo.endCursor` back as `after` to fetch the next page. It sits behind the same auth + rate-limiting as the REST data routes.

## Typed SDK generation

Every contract's on-chain interface is a complete, runtime-verified spec. Lumenqraph uses this to **generate production-ready typed clients on demand**, without requiring manual ABI uploads or hand-written type stubs — a strong differentiator since the spec is shipped *with the code*.

```bash
# Generate a TypeScript client for any contract, save it, and import it
curl -o MyContract.ts "https://lumenqraph.onrender.com/contracts/CB.../sdk?lang=ts"
```

The generated client is fully typed: functions, arguments, return types, and events all match the on-chain spec. Call it against the indexer's `/call` and `/simulate` endpoints:

```ts
import { ContractClient } from "./MyContract";
const c = new ContractClient({ baseUrl: "https://lumenqraph.onrender.com" });
const balance = await c.balance({ id: "G..." }); // typed, type-checked
```

**SDK endpoint**: `GET /contracts/:id/sdk`. Query parameters:
- `lang` — target language (`ts` / TypeScript, default; others return `400`)
- `version` — generate from a historical interface version (default: current)

**Limitations**: Stellar Asset Contracts lack WASM specs and cannot be code-generated; attempt to generate from them returns `404`.

## TypeScript SDK

[`sdk/typescript`](sdk/typescript) (`@lumenqraph/sdk`) is a typed, zero-runtime-dependency client over the REST + GraphQL API — it uses the platform `fetch` (Node 18+ or the browser).

```ts
import { LumenqraphClient } from "@lumenqraph/sdk";

const lq = new LumenqraphClient({ baseUrl: "http://localhost:8080" });
const balances = await lq.getData(contractId, { label: "balance" });

// Cursor pagination, as a transparent async iterator:
for await (const ev of lq.paginateEvents(contractId, { pageSize: 100 })) {
  console.log(ev.ledger, ev.event_name, ev.enriched ?? ev.decoded_value);
}
```

See the [SDK README](sdk/typescript/README.md) for the full method list.

## AI-agent access — the MCP server

Everything above — typed events, decoded interfaces, typed read calls — is exactly the structured, self-describing metadata an AI agent needs to work with a chain. [`lumenqraph-mcp`](crates/lumenqraph-mcp) exposes it as a [Model Context Protocol](https://modelcontextprotocol.io) server, so **Claude (Desktop or Code) or any MCP client can discover, query, and call any Soroban contract** — with no hand-written tool schemas, because the schemas come from each contract's on-chain interface.

It's a standard stdio JSON-RPC server that reuses the same Postgres and read-layer encoder as the API, and offers four tools:

| Tool | What the agent can do |
| --- | --- |
| `list_contracts` | See which contracts are indexed, with event counts. |
| `get_contract_interface` | Discover a contract's functions (typed inputs/outputs), events, and user-defined types. |
| `get_contract_upgrades` | Read a contract's interface history: every version, its diff, and whether the change was breaking. |
| `get_contract_state` | Read a contract's current (and historical) instance storage. |
| `get_contract_data` | Read a contract's per-key state — individual entries like token holder balances. |
| `query_events` | Read a contract's recent events, decoded and enriched. |
| `call_contract` | Invoke a view function read-only and get a typed result (args type-checked against the spec). |
| `simulate_call` | Dry-run any call and preview the result, the events it would emit, and its cost. |

### Quick start

**1. Build the server binary.**

```bash
cargo build --release -p lumenqraph-mcp
# → target/release/lumenqraph-mcp   (install it on your PATH, or use the full path below)
```

**2. Make sure the indexer has populated the database.** The MCP server is a *read* surface over the same Postgres the indexer writes — it needs data (and, for `call_contract`, the contract's interface, which the indexer captures on first sighting). If you followed the [Quick start](#quick-start) above, you already have this.

**3. Smoke-test it without an MCP client** by piping newline-delimited JSON-RPC to its stdin (protocol replies come back on stdout, one per line):

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_contracts","arguments":{}}}' \
| DATABASE_URL='postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph' \
  RPC_URL='https://soroban-testnet.stellar.org' \
  ./target/release/lumenqraph-mcp
# → an initialize result, the four tool definitions, then your indexed contracts.
```

**4. Register it with your MCP client** (e.g. Claude Desktop's `claude_desktop_config.json`, or Claude Code via `claude mcp add`):

```jsonc
{
  "mcpServers": {
    "lumenqraph": {
      "command": "lumenqraph-mcp",   // or the absolute path to the built binary
      "env": {
        "DATABASE_URL": "postgres://…",   // the same DB the indexer writes
        "RPC_URL": "https://soroban-testnet.stellar.org"
      }
    }
  }
}
```

**5. Restart the client and just ask** — in plain language:

> *"Using lumenqraph, what functions does contract `C…` expose? What's the balance of `G…`? Show me its last few `transfer` events."*

The agent calls `get_contract_interface` to discover the typed signatures, then `call_contract` and `query_events` to answer — all typed, all validated against the on-chain spec, with no schema you had to write.

## Webhooks

Register a URL (with optional contract/event filters) and receive event pushes as they're indexed. Each delivery carries an `X-Lumenqraph-Signature: sha256=<hmac>` header — an HMAC-SHA256 of the raw request body, keyed by the `secret` returned once at creation.

```bash
curl -X POST localhost:8080/webhooks \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/hook","event_name":"transfer"}'
```

A subscription has a `kind`: `event` (the default — a contract emitted an event) or `upgrade` (a contract's on-chain interface changed; see [Contract upgrade watch](#contract-upgrade-watch)). The two streams are independent, so an `event` subscriber never receives an upgrade payload.

```bash
# Alert me when this contract's interface changes under my dApp.
curl -X POST localhost:8080/webhooks \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/hook","kind":"upgrade","contract_id":"CB..."}'
```

Verifying a delivery (Node.js):

```js
const crypto = require("crypto");

function verify(rawBody, signatureHeader, secret) {
  const expected =
    "sha256=" + crypto.createHmac("sha256", secret).update(rawBody).digest("hex");
  return crypto.timingSafeEqual(
    Buffer.from(signatureHeader),
    Buffer.from(expected)
  );
}
```

Deliveries retry with exponential backoff up to `WEBHOOK_MAX_ATTEMPTS`.

## Multi-network deployments

Lumenqraph can serve multiple Stellar networks (mainnet, testnet, futurenet) from a single logical deployment using **instance mounts**. One Lumenqraph API reverse-proxies to sibling instances running on the same host, allowing clients to query different networks under a unified origin.

**Recommended pattern:** One instance per network + federation at the edge. This provides clean separation, independent scaling, and simple operations.

Example:
```bash
# Primary instance (mainnet)
RPC_URL=https://mainnet.sorobanrpc.com
INSTANCE_MOUNTS=testnet=http://127.0.0.1:8081

# GET /contracts → queries mainnet
# GET /testnet/contracts → proxied to testnet instance at :8081
```

The primary instance's `/health` endpoint advertises available networks so clients (like the explorer) can discover them automatically and switch networks with one click:

```json
{
  "network": "mainnet",
  "mounts": {
    "testnet": "/testnet"
  }
}
```

See [docs/MULTI_NETWORK.md](docs/MULTI_NETWORK.md) for detailed configuration, deployment patterns (free tier, separate services, fully isolated), security considerations, and monitoring.

## Running in production

Run three long-lived processes against one Postgres. Only the indexer applies migrations.

| Process | Notes |
| --- | --- |
| `lumenqraph-indexer` | Must run **24/7** — a sleeping poller falls behind the chain. |
| `lumenqraph-api` | Stateless; scale horizontally behind a load balancer. |
| `lumenqraph-webhooks` | A single instance suffices; delivery is idempotent per (subscription, event). |

Scrape `GET /metrics` and alert on `lumenqraph_indexer_lag_ledgers` climbing. For managed Postgres, point `DATABASE_URL` at Neon or Supabase. See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for scaling notes (RPC providers, Redis-backed rate limiting, caching).

When upgrading a running deployment, always consult [docs/UPGRADING.md](docs/UPGRADING.md) for breaking changes, required env var additions, and the database migration log for each release.

## Project structure

```
Lumenqraph/
├── crates/
│   ├── lumenqraph-core/       # shared models, errors, XDR↔JSON, spec parser, read encoder
│   ├── lumenqraph-indexer/    # polling, decoding, enrichment, state + per-key snapshots, backfill
│   ├── lumenqraph-api/        # Axum REST + GraphQL API, auth, rate limiting, metrics, read layer
│   ├── lumenqraph-webhooks/   # subscription matching + signed delivery
│   └── lumenqraph-mcp/        # Model Context Protocol server for AI agents
├── sdk/typescript/            # @lumenqraph/sdk — typed REST + GraphQL client
├── migrations/                # ordered sqlx SQL migrations
├── docs/                      # ARCHITECTURE, API, DEPLOYMENT
├── explorer/                  # zero-build explorer + self-host dashboard
├── scripts/                   # gen_api_key, backfill, setup_db
├── Dockerfile                 # multi-stage build (all four binaries)
├── docker-compose.yml         # local Postgres for dev
├── docker-compose.full.yml    # full stack
├── fly.toml                   # Fly.io deploy (3 process groups)
└── Makefile                   # common tasks (make help)
```

## Development

```bash
make db          # start local Postgres
make build       # cargo build --workspace
make test        # cargo test --workspace
make fmt         # cargo fmt --all
make lint        # cargo clippy -- -D warnings
```

CI runs formatting, Clippy (warnings denied), tests, and a release build against a Postgres service on every push and pull request. Please run `make fmt lint test` before opening a PR.

## Roadmap

- [x] Typed, self-describing decoding from each contract's on-chain interface (`contractspecv0`)
- [x] Read layer: typed, read-only view-function calls via `simulateTransaction` (`eth_call` for Soroban)
- [x] Transaction preview: dry-run any call for its typed result, emitted events, and cost (Tenderly-style)
- [x] MCP server: typed, self-describing Soroban access for AI agents (Model Context Protocol)
- [x] Contract state indexing: versioned instance-storage snapshots + automatic spec refresh on upgrade
- [x] State indexing: per-key persistent storage (e.g. individual balances), discovered from a token's own events
- [x] GraphQL endpoint alongside REST; cursor-based pagination
- [x] Client SDK: TypeScript (`sdk/typescript`)
- [x] Read layer: user-defined-type (struct/enum/union) and 256-bit-integer arguments
- [x] Enrichment for user-defined struct/enum/union values (naming nested UDT values, not just event params)
- [x] Contract upgrade watch: versioned interface history, semantic diffs, breaking-change detection, and `contract.upgraded` webhooks
- [x] In-memory spec cache in the API (hash-validated, so an upgrade is never served stale)
- [ ] Deep historical backfill via a captive-core / data-lake source (beyond RPC's ~7-day window)
- [ ] Additional materialized verticals (AMM swaps, liquidity, NFT mints/transfers)
- [ ] Redis-backed rate limiting and read caching for multi-instance deployments
- [ ] Client SDK: Python
- [ ] Grafana dashboards and alert rules

Contributions toward any of these are very welcome — see [Contributing](#contributing).

## Known limitations

- **History is bounded by RPC retention (~7 days).** Deep backfill needs a captive-core / data-lake source (on the roadmap); `START_LEDGER` is clamped to the retention window.
- **The rate limiter is per-instance** (in-memory). Running multiple API replicas means limits apply per replica — move the limiter to Redis for a global limit.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for the dev setup and conventions, and review our [Code of Conduct](CODE_OF_CONDUCT.md). Good first issues are labelled in the [issue tracker](https://github.com/Lumen-Scribe/Lumenqraph/issues).

See [CHANGELOG.md](CHANGELOG.md) for release history, versioning policy, and per-release migration notes. See [docs/UPGRADING.md](docs/UPGRADING.md) for step-by-step upgrade instructions.

## License

Licensed under the [MIT License](LICENSE).

---

<div align="center">
Built for the <a href="https://stellar.org/soroban">Stellar / Soroban</a> ecosystem.
</div>

## Handsoff notes

<!-- handsoff-issue-364 -->
- #364: Migration 0019 is a silent no-op on existing databases: `idx_events_enriched` already exists from 0004
