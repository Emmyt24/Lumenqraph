# API reference

Base URL defaults to `http://localhost:8080`.

Auth: data routes accept an API key via `Authorization: Bearer <key>` or
`x-api-key: <key>`. When `REQUIRE_API_KEY=false` (default), unauthenticated
callers are allowed up to `ANON_RATE_LIMIT_PER_MIN`. `/health` and `/metrics`
are always public. Rate-limit breaches return `429`; bad/revoked keys `401`.

## Error responses

Every error response includes a stable `code` field alongside the human-readable
`error` message:

```json
{ "code": "not_found", "error": "no event found with id '...'" }
```

Use the `code` field — not the `error` string — to branch in SDKs and integrations.
Error messages are intended for humans and may change between versions; codes are
stable and will not be renamed or removed.

| Code                | HTTP status | When                                                                  |
|---------------------|-------------|-----------------------------------------------------------------------|
| `bad_request`       | 400         | Malformed input, invalid parameter value, or wrong argument type.    |
| `unauthorized`      | 401         | Missing, invalid, or revoked API key.                                 |
| `not_found`         | 404         | The requested resource does not exist.                                |
| `rate_limited`      | 429         | Caller exceeded the requests-per-minute limit.                        |
| `simulation_failed` | 400         | RPC simulation returned an error (contract trap, bad call, etc.).    |
| `spec_unavailable`  | 404         | Contract has not been indexed yet. The indexer fetches the interface on first sighting — retry after the contract has been seen. |
| `sac_not_supported` | 422         | The contract is a Stellar Asset Contract (or has no WASM spec). Retrying will never help; use the token metadata endpoints instead of `/call` or `/simulate`. |
| `internal_error`    | 500         | Unexpected server-side failure. Details are logged, not exposed.     |

## Public

### `GET /health`
Human-readable status: indexing freshness and chain-tip lag.
```json
{ "status": "ok", "network": "mainnet",
  "network_passphrase": "Public Global Stellar Network ; September 2015",
  "last_processed_ledger": 3550886, "chain_tip_ledger": 3550886,
  "lag_ledgers": 0, "seconds_since_cursor_update": 1,
  "events_ingested_total": 4895, "errors_total": 0 }
```
`network` is which Stellar network this deployment indexes (`mainnet` /
`testnet` / `futurenet` / `custom`), asked of the RPC itself and cached — so
clients (like the explorer) can adapt instead of asking the user. `null` while
the RPC is unreachable.

When the operator has mounted sibling instances (`INSTANCE_MOUNTS`, e.g. the
hosted demo serving a testnet deployment under the same origin), `/health` also
advertises them as `"mounts": { "testnet": "/testnet" }` — every endpoint
documented here works under that prefix, served by the sibling.

### `GET /livez`
Kubernetes-style liveness probe: returns `200 OK` if the process is running,
nothing else. Requires no database access and no logic beyond "is the server
listening?" Used by orchestrators to detect dead or stuck processes and restart
them.

### `GET /readyz`
Kubernetes-style readiness probe: returns `200 OK` only when the indexer is
caught up and healthy; otherwise `503 Service Unavailable`. Checks:
- Database is reachable
- Cursor has been created (at least one pass through the event stream)
- Indexing lag is below the threshold (default: 100 ledgers)
- Cursor was updated recently (default: within 120 seconds)

Used by orchestrators to route traffic only to ready instances and avoid
cascading restarts during slow startup. Configurable via environment:
- `READYZ_LAG_THRESHOLD` (ledgers, default 100)
- `READYZ_MAX_AGE_SECS` (seconds, default 120)

### `GET /metrics`
Prometheus text: `lumenqraph_indexer_lag_ledgers`, `lumenqraph_events_total`,
`lumenqraph_indexer_ingested_total`, `lumenqraph_indexer_errors_total`,
`lumenqraph_api_requests_total`, …

## Data (authenticated / rate-limited)

### `GET /contracts`
Contracts seen, with `event_count`, `first_seen_ledger`, `last_seen_ledger`.

### `GET /contracts/:id/events`
Query: `limit` (1–1000, default 50), `offset`, `event_name` (e.g. `transfer`),
`after` (cursor for pagination).

**Pagination:** Use cursor pagination (`after` parameter) for production use.
Offset pagination is **deprecated** due to linear performance degradation and is
capped at 10,000 rows. For deeper pages, use the `next_cursor` from the previous
response as the `after` parameter.

**Filtering:** The `param` filter matches against the `enriched` JSON field using
containment queries (e.g., `?param={"from":"GXXX"}`). The enriched column has a
GIN index for efficient filtering.

Each row has raw base64 (`topics`, `value`) **and** decoded JSON
(`decoded_topics`, `decoded_value`), plus `event_name`, `tx_hash`, `ledger`, …

Response:
```json
{
  "data": [...],
  "has_more": true,
  "next_cursor": "3550885:0015250934946869248-0000000000"
}
```

### `GET /contracts/:id/stats`
Aggregated time-series event statistics for a contract, bucketed by a specified time resolution or ledger using Postgres `date_trunc`.

**Query parameters:**
- `resolution`: Time bucket granularity — `1m`, `1h`, `1d`, `1w`, or `1M` (also accepts `hour`, `day`). Default is `1d`.
- `window`: Relative lookback time window from now — e.g. `1h`, `24h`, `7d`, `30d`. Supported units: `m`, `h`, `d`, `w`.
- `bucket`: Alternative bucketing parameter (`hour`, `day`, `ledger`).
- `group_by`: Optional grouping (`event_name`) to split counts per event type.
- `from`: Minimum RFC3339 timestamp (inclusive).
- `to`: Maximum RFC3339 timestamp (inclusive).
- `from_ledger`: Minimum ledger sequence (inclusive).
- `to_ledger`: Maximum ledger sequence (inclusive).

Response:
```json
{
  "data": [
    {
      "bucket": "2026-07-15 12:00:00+00",
      "count": 142,
      "breakdown": {
        "transfer": 120,
        "mint": 22
      }
    }
  ],
  "total": 142
}
```

### `GET /events/:event_id`
Fetch a **single** event by its unique `event_id`. Returns the full event row
(raw XDR, decoded JSON, and enriched record); `404` if no event with that id is
indexed. Useful for re-fetching an event whose `event_id` was received in a
webhook delivery or other response.
```json
{
  "event_id": "0015250934946869248-0000000000",
  "contract_id": "CDLZFC3S...",
  "ledger": 3550885,
  "event_name": "transfer",
  "decoded_topics": ["transfer", "G...", "G...", "native"],
  "decoded_value": "100000000000",
  "enriched": { "event": "transfer", "params": { "from": { "type": "Address", "value": "G..." }, "to": { "type": "Address", "value": "G..." }, "amount": { "type": "i128", "value": "100000000000" } } },
  "tx_hash": "3664562a...",
  "in_successful_call": true
}
```

### `GET /transactions/:tx_hash/events`
All indexed events emitted by a transaction, in the order they were emitted
on-chain (`ledger ASC, event_id ASC`). Query: `limit` (1–1000, default 100).
Returns `{ "tx_hash": "...", "count": N, "data": [...] }`. Useful for debugging
"what did my transaction do?".
```json
{
  "tx_hash": "3664562a...",
  "count": 2,
  "data": [
    { "event_id": "...", "event_name": "transfer", "ledger": 3550885, "..." },
    { "event_id": "...", "event_name": "mint",     "ledger": 3550885, "..." }
  ]
}
```

### `GET /contracts/:id/transfers`
Materialized SEP-41 transfers. Query: `limit`, `offset`, `from`, `to`.
```json
[{ "from_addr": "G...", "to_addr": "G...", "amount": "100000000000",
   "ledger": 3550885, "event_id": "..." }]
```

### `GET /contracts/:id/swaps`
Materialized AMM swap events, newest first. Query: `limit` (1–1000, default 50),
`offset`, `after` (cursor), `sender`, `sell_token`, `buy_token`.
```json
{
  "data": [
    { "event_id": "...", "contract_id": "CB...", "sender": "G...",
      "sell_token": "C...", "buy_token": "C...",
      "sell_amount": "1000000000", "buy_amount": "987654321",
      "ledger": 3550885, "ledger_closed_at": "2026-07-15T..." }
  ],
  "has_more": false,
  "next_cursor": null
}
```

### `GET /contracts/:id/nfts`
Materialized NFT events (mint/transfer/burn), newest first. Query: `limit`
(1–1000, default 50), `offset`, `after` (cursor), `kind` (`mint`|`transfer`|`burn`),
`from`, `to`, `token_id`.
```json
{
  "data": [
    { "event_id": "...", "contract_id": "CB...", "event_kind": "transfer",
      "from_addr": "G...", "to_addr": "G...", "token_id": "42",
      "ledger": 3550885, "ledger_closed_at": "2026-07-15T..." }
  ],
  "has_more": false,
  "next_cursor": null
}
```

### `GET /contracts/:id/liquidity`
Materialized liquidity events (add/remove), newest first. Query: `limit`
(1–1000, default 50), `offset`, `after` (cursor), `kind` (`add`|`remove`),
`provider`.
```json
{
  "data": [
    { "event_id": "...", "contract_id": "CB...", "event_kind": "add",
      "provider": "G...", "amount_a": "500000000", "amount_b": "500000000",
      "shares": "999999", "ledger": 3550885, "ledger_closed_at": "2026-07-15T..." }
  ],
  "has_more": false,
  "next_cursor": null
}
```

### `GET /contracts/:id/interface`
The decoded on-chain interface: `functions`, `events`, `structs`, `unions`,
`enums`. Query: `version` (a historical version; default is the current one).
```json
{ "contract_id": "CB...", "has_events": true,
  "interface": { "functions": [...], "events": [...], "structs": [], "unions": [], "enums": [] },
  "fetched_at": "2026-07-15T..." }
```

### `GET /contracts/:id/interface/history`
Every interface version observed, newest first. Query: `limit` (1–1000, default
50), `after` (cursor for keyset pagination). Requires the indexer's
`UPGRADE_WATCH`.

**Pagination:** For contracts with many interface upgrades, use the `after`
parameter to page through the full history. Pass the `next_cursor` value from
a previous response as `after` to retrieve the next page. When `has_more` is
`false` and `next_cursor` is `null`, all versions have been returned.

Cursors are encoded as `version:<N>` (e.g. `version:42`) and are stable across
requests — the history is append-only from the top so pages do not shift.

```json
{ "contract_id": "CB...", "count": 2, "has_more": false, "next_cursor": null,
  "versions": [
  { "version": 2, "wasm_hash": "...", "previous_wasm_hash": "...",
    "breaking": true, "observed_at": "2026-07-15T...Z",
    "diff": { "breaking": true, "summary": ["removed function withdraw() -> void"],
              "functions": { "added": [], "removed": ["withdraw() -> void"], "changed": [] },
              "events": { "added": [], "removed": [], "changed": [] },
              "types":  { "added": [], "removed": [], "changed": [] } } },
  { "version": 1, "previous_wasm_hash": null, "breaking": false, "diff": null }
] }
```

### `GET /contracts/:id/interface/diff`
Diff any two versions. Query: `from`, `to` (default: the latest upgrade, i.e.
`to` = newest, `from` = the one before). `400` if the contract has only a
baseline version, or if `from` == `to`; `404` for an unknown version.
```json
{ "contract_id": "CB...", "from": 1, "to": 2,
  "diff": { "breaking": true,
    "summary": ["removed function withdraw(amount: i128) -> void"],
    "functions": { "added": [], "removed": ["withdraw(amount: i128) -> void"], "changed": [] },
    "events": { "added": [], "removed": [], "changed": [] },
    "types": { "added": [], "removed": [], "changed": [] } } }
```

`breaking` is true when anything was removed or changed — an integration built
against the old interface may no longer work. Additions alone are not breaking.

### `POST /contracts/:id/refresh`
Trigger an immediate manual re-fetch of a contract's on-chain interface from
Soroban RPC, clearing the cached spec and updating the contract's stored
interface and version history.

Useful when an initial spec fetch failed (e.g. due to a transient RPC error) or
when an operator needs to force a spec refresh for a specific contract without
restarting the indexer. Returns the refreshed interface on success or a
structured error on failure.
```json
{ "contract_id": "CB...", "has_events": true,
  "interface": { "functions": [...], "events": [...], "structs": [], "unions": [], "enums": [] },
  "fetched_at": "2026-07-15T..." }
```

### `GET /contracts/:id/state`
Versioned snapshots of the contract's **instance storage** (admin, config,
counters…), newest first. Query: `limit` (1–200, default 1 = current state).
Requires the indexer's `STATE_INDEXING`; `404` if there are no snapshots.
```json
{ "contract_id": "CB...", "count": 1, "versions": [
  { "ledger": 3550880, "storage": [
      { "key": "METADATA", "val": { "name": "Token", "symbol": "TKN" } },
      { "key": ["TotalSupply"], "val": "1000" }
    ], "captured_at": "2026-07-15T..." }] }
```

### `GET /contracts/:id/data`
The current value of every **per-key** entry snapshotted for this contract —
e.g. each tracked holder's `Balance(Address)`. One row per key (its latest
snapshot). Query: `label` (e.g. `balance`), `limit` (1–1000, default 100).
Requires the indexer's `KEY_INDEXING`.
```json
{ "contract_id": "CB...", "count": 2, "keys": [
  { "key_hash": "9f2c…", "key": ["Balance", "G..."], "durability": "persistent",
    "ledger": 3550881, "value": "500", "label": "balance",
    "captured_at": "2026-07-15T..." }] }
```

### `GET /contracts/:id/data/:key_hash`
The version history of a single per-key entry (one holder's balance over time),
newest first. Query: `limit` (1–500, default 1).
```json
{ "contract_id": "CB...", "key_hash": "9f2c…",
  "key": ["Balance", "G..."], "durability": "persistent", "label": "balance",
  "count": 3, "versions": [
    { "ledger": 3550881, "value": "500", "captured_at": "2026-07-15T..." },
    { "ledger": 3550870, "value": "450", "captured_at": "2026-07-15T..." },
    { "ledger": 3550850, "value": "400", "captured_at": "2026-07-15T..." }
  ] }
```

## Read layer (authenticated)

Invoke contracts through RPC simulation. Arguments are type-checked against the
contract's on-chain spec *before* the network call, so mistakes come back as a
`400` with a precise message rather than an opaque simulation failure. Nothing
is ever signed or submitted.

### `GET /contracts/:id/functions`
The contract's callable functions with typed inputs/outputs.
```json
{ "contract_id": "CB...", "functions": [
  { "name": "balance", "inputs": [{ "name": "id", "type": "Address" }],
    "outputs": ["i128"], "is_view": true }] }
```
`is_view` is a **best-effort** heuristic, not a guarantee: Soroban's
`contractspecv0` carries no `view`/`mutable` keyword (unlike Solidity's ABI),
so it is inferred from the function's output type (`void` usually means a
state change) and its name (a known mutating prefix like `set_`, `transfer`,
`mint`, `withdraw`, `upgrade`, …). A function marked `is_view: true` is
probably safe via `/call`; when in doubt, or when `is_view` is `false`, prefer
`/simulate`.

### `POST /contracts/:id/call`
Invoke a **view** function read-only and return a typed result.
Body: `{ "function": "balance", "args": { "id": "G..." }, "source_account": null }`
— `args` takes an object keyed by parameter name, or a positional array.
`source_account` accepts both a plain Ed25519 public key (`G…` strkey) and a
muxed account (`M…` strkey) for sub-account routing.
```json
{ "contract_id": "CB...", "function": "balance",
  "result": "500", "simulated_at_ledger": 3550886 }
```

### `POST /contracts/:id/simulate`
Dry-run **any** call, including state-changing ones, and get the typed result,
the events it would emit (decoded + enriched), and its estimated resource fee.
Same body as `/call`.

**Trust boundary:** This endpoint relies on the Soroban RPC's simulateTransaction
endpoint to return without submitting to the network. Lumenqraph does not sign or
broadcast anything itself, but the safety guarantee ultimately depends on the RPC
endpoint's simulation semantics. If the RPC endpoint's behavior changes (e.g., a
future Soroban upgrade), state-changing simulation calls could have unintended effects.
```json
{ "contract_id": "CB...", "function": "transfer", "result": null,
  "events": [
    { "contract_id": "CB...", "type": "contract", "event": "transfer",
      "topics": ["transfer", "G...", "G..."], "data": "500",
      "enriched": { "event": "transfer", "params": { "amount": { "type": "i128", "value": "500" } } } }],
  "min_resource_fee": "34561", "simulated_at_ledger": 3550886 }
```
`fn_call`/`fn_return` diagnostic noise is dropped; `enriched` is non-null only
for events emitted by the contract being simulated, since that's the only spec
in hand.

Errors are client-facing: a wrong-typed argument gives
`400 {"error": "argument \"id\": invalid address strkey"}`, an unknown function
`400 {"error": "contract has no function named \"nope\""}`, and a contract whose
interface isn't indexed (or a Stellar Asset Contract, which has no spec) gives
`404`. A contract trap is the caller's mistake, not a server fault, so it comes
back as `400 {"error": "simulation failed: ..."}`.

## GraphQL

### `POST /graphql`
Executes queries; `GET /graphql` serves the GraphiQL IDE in a browser. Behind
the same auth and rate limiting as the REST data routes.

REST stays the primary, zero-dependency interface; GraphQL is for clients that
want to select fields and page with cursors. High-volume lists (`events`,
`transfers`) are Relay-style cursor connections; naturally bounded ones
(`contracts`, `contractState`, `contractData`) are plain lists.

```graphql
query {
  events(contractId: "CB...", first: 20) {
    edges { cursor node { ledger eventName enriched } }
    pageInfo { hasNextPage endCursor }
  }
}
```

## Contract interface & upgrades

A Soroban contract can be upgraded in place, so its interface is a time series.
Version 1 is the first interface the indexer ever saw (a baseline: `diff` is
`null` and it fires no webhook); each later version is an upgrade. Requires the
indexer's `UPGRADE_WATCH` (on by default when `CONTRACT_IDS` is set).

### `GET /contracts/:id/interface`
The decoded on-chain interface: `functions`, `events`, `structs`, `unions`,
`enums`. Query: `version` (a historical version; default is the current one).

### `GET /contracts/:id/interface/history`
Every version observed, newest first. Query: `limit` (1–1000, default 50),
`after` (cursor for keyset pagination; pass `next_cursor` from a previous
response). Response includes `has_more` and `next_cursor` for full traversal.
```

### `GET /contracts/:id/interface/diff`
Diff any two versions. Query: `from`, `to` (default: the latest upgrade, i.e.
`to` = newest, `from` = the one before). `400` if the contract has only a
baseline version, or if `from` == `to`; `404` for an unknown version.

`breaking` is true when anything was removed or changed — an integration built
against the old interface may no longer work. Additions alone are not breaking.

### `POST /contracts/:id/refresh`
Trigger an immediate manual re-fetch of a contract's on-chain interface from
Soroban RPC, clearing the cached spec and updating the contract's stored
interface and version history. Useful when an initial spec fetch failed due to a
transient RPC error or to force a spec refresh without restarting the indexer.

## Generated typed clients

### `GET /contracts/:id/sdk`
A ready-to-use, typed TypeScript client for the contract, generated on demand
from its on-chain interface — the codegen equivalent of everything above. Save
it and call the contract with full type safety and zero dependencies:

```bash
curl -o contract.ts "$BASE/contracts/CB.../sdk?lang=ts"
```
```ts
import { ContractClient } from "./contract";
const c = new ContractClient({ baseUrl: "https://lumenqraph.onrender.com" });
const pool = await c.get_pool_info(); // typed from the chain's own schema
```

Query parameters:
- `lang` (`ts`, the default and only target so far; anything else is a `400`)
- `version` (generate from a historical interface version — the client your
  integration was built against *before* an upgrade; default: current)

The generator maps contract types to TypeScript:
- Structs → interfaces
- Unit enums → case-name literal types
- Unions → `"Case" | { Case: [...] }` shapes

Because `/call` results are named with the same spec, what a call returns is
exactly what the next call accepts. Generation is deterministic: same interface
version, same output.

**Limitations:**
- Stellar Asset Contracts have no WASM spec and cannot be code-generated.
  Attempt to generate from them returns `404`.
- Only TypeScript targets are currently supported (`lang=ts`).
- Some advanced Soroban types may not have complete TypeScript mappings.

**Success response:** The generated TypeScript client source code (content-type:
`text/plain`). The client is self-contained and zero-dependency.

Requires the contract's interface to be indexed (the first time the indexer saw
an event from that contract, or when `STATE_INDEXING` is enabled). Stellar Asset
Contracts (no WASM spec) cannot be generated from.

## Webhooks (authenticated)

### `POST /webhooks`
Body: `{ "url": "https://...", "kind": "event", "contract_id": null, "event_name": "transfer" }`
(all but `url` optional). Returns the subscription including a one-time `secret`
used to verify the `X-Lumenqraph-Signature: sha256=<hmac>` header on deliveries.

`kind` is `event` (default: a contract emitted an event; the payload is the event
row) or `upgrade` (a contract's interface changed). `event_name` doesn't apply to
`upgrade` subscriptions — scope them with `contract_id`, or omit it for all
contracts. An `upgrade` delivery is signed identically, and carries:
```json
{ "type": "contract.upgraded", "contract_id": "CB...", "version": 2,
  "wasm_hash": "...", "previous_wasm_hash": "...", "breaking": true,
  "diff": { "...": "as in /interface/diff" }, "observed_at": "2026-07-15T...Z" }
```

### `GET /webhooks`
Lists subscriptions (secrets omitted).

### `PATCH /webhooks/:id`
Pause/resume or update a subscription without losing its secret.
Body: `{ "active": false, "contract_id": null, "event_name": "transfer" }`
(all fields optional; omitted fields keep their current value).

Returns the updated subscription (secrets omitted). Allows:
- Toggling the `active` status (true = deliver, false = paused)
- Updating `contract_id` and `event_name` filters (same validation as creation)

### `GET /webhooks/:id/deliveries`
Delivery history and status for a subscription, most recent first.

Query parameters:
- `limit` — rows to return (1–500, default 50)
- `offset` — rows to skip for pagination (default 0)

Response includes `total_count` for computing total pages and `has_more` for
detecting whether another page exists.

```json
{
  "deliveries": [
    {
      "id": 1234,
      "status": "delivered",
      "attempts": 1,
      "last_error": null,
      "delivered_at": "2026-07-15T12:34:56Z",
      "created_at": "2026-07-15T12:34:55Z"
    }
  ],
  "total_count": 312,
  "limit": 50,
  "offset": 0,
  "has_more": true,
  "summary": {
    "delivered": 300,
    "failed": 12,
    "pending": 0
  }
}
```

### `DELETE /webhooks/:id`
Removes a subscription (and cascades its deliveries).

### `POST /webhooks/:id/rotate-secret`
Rotates the HMAC signing secret for a subscription. Returns the new secret
**once only** — store it immediately. The previous secret remains valid for a
configurable grace period (default 5 minutes, set via `WEBHOOK_SECRET_GRACE_SECS`)
so consumers can roll out the new secret without a verification gap.

```json
{
  "id": "...",
  "secret": "<new-hex-secret>",
  "previous_secret_valid_until": "2025-01-24T12:05:00Z",
  "message": "Store this secret immediately — it will not be shown again."
}
```

Delivery history and the subscription watermark are fully preserved — no
deliveries are replayed and no events are missed.

### Verifying a delivery
`HMAC-SHA256(secret, raw_request_body)` hex must equal the value after
`sha256=` in `X-Lumenqraph-Signature`.
