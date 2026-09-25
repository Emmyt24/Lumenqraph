# Deep Historical Backfill

> **Issue #84** — Gapless Soroban event history beyond the ~7-day RPC retention
> window via a captive-core / data-lake source.

## Background

The Lumenqraph live poller and the built-in `backfill` subcommand both call
`getEvents` on a Soroban RPC. SDF's public RPC retains ~7 days (~120 000
ledgers) of event history, and `START_LEDGER` is clamped to that window.
Analytics, audits, and "since inception" dashboards need history older than
7 days — that requires an alternate ingest source.

## Archive RPC timeouts

If you are backfilling recent history (inside the ~7-day RPC window) with the
RPC-based `backfill` subcommand — usually via `scripts/backfill.sh` — against a
slow archive or paid RPC endpoint, raise the RPC timeout.

`RPC_TIMEOUT_SECS` defaults to `30`, which suits the public SDF RPC. Archive
endpoints answering deep `getEvents` queries are frequently slower than that. A
timeout aborts the entire batch, and the automatic retry reuses the same
timeout, so a consistently slow RPC turns into a permanent failure until the
value is raised. **120 seconds** is a good starting point for archive RPCs;
increase further if you still see timeouts.

Set it any of these ways (highest precedence first):

```bash
# 1. Per-run flag on the backfill script:
./scripts/backfill.sh --rpc-timeout 120 <start_ledger>

# 2. Exported in the environment:
RPC_TIMEOUT_SECS=120 ./scripts/backfill.sh <start_ledger>

# 3. Persisted in .env (picked up by the indexer, not overridden if already set):
echo 'RPC_TIMEOUT_SECS=120' >> .env
```

The data-lake `deep-backfill` path below does not talk to an RPC, so
`RPC_TIMEOUT_SECS` has no effect there.

## Architecture

```
Data lake export               deep-backfill           Postgres
(Galexie NDJSON)  ──parse──▶  HistoricalSource  ──insert──▶  events
                                                              token_transfers

 Soroban RPC      ──poll──▶   live poller        ──insert──▶  (same tables)
```

The two paths write to the same tables. `INSERT … ON CONFLICT DO NOTHING` on
`event_id` makes every write idempotent, so:

- Running both paths concurrently is safe.
- Restarting a backfill from an earlier ledger is safe.
- Overlapping the backfill range with the live poller's window is safe.

## Supported sources

| Source | Flag | Description |
|---|---|---|
| Galexie / Stellar CDP | `--source galexie` | Newline-delimited JSON export |
| Stellar Horizon | `--source horizon` | Historical events from Horizon `/transactions` endpoint |

New sources can be added by implementing the `HistoricalSource` trait in
`crates/lumenqraph-indexer/src/deep_backfill.rs`.

## Obtaining a Galexie export

[Galexie](https://github.com/stellar/galexie) is the Stellar Development
Foundation's data-lake exporter. It writes per-ledger JSON to GCS/S3. The
[Stellar Hubble / CDP pipeline](https://github.com/stellar/stellar-etl) can
also export events as newline-delimited JSON.

For a quick test export from a captive-core instance:

```bash
# Install galexie: https://github.com/stellar/galexie
galexie export \
  --start-ledger 1000000 \
  --end-ledger   5000000 \
  --output events.ndjson
```

## Running a deep backfill

```bash
# Build the indexer binary first:
cargo build --release -p lumenqraph-indexer

# Backfill from ledger 1 000 000 to 5 000 000 from a single file:
DATABASE_URL=postgres://… ./target/release/lumenqraph-indexer deep-backfill \
  --from   1000000    \
  --to     5000000    \
  --source galexie    \
  --input  /data/events.ndjson

# Multiple files (processed in order, must be in ascending ledger order):
./target/release/lumenqraph-indexer deep-backfill \
  --from 1000000 \
  --to   9000000 \
  --input /data/2023.ndjson \
  --input /data/2024.ndjson \
  --input /data/2025.ndjson

# From stdin (e.g. pipe from gsutil or S3):
gsutil cat gs://my-bucket/events-*.ndjson | \
  ./target/release/lumenqraph-indexer deep-backfill \
    --from 1000000 \
    --input -

# From a Horizon endpoint:
./target/release/lumenqraph-indexer deep-backfill \
  --from   1000000 \
  --to     2000000 \
  --source horizon \
  --input  https://horizon.stellar.org
```

## HistoricalSource Trait Contract

Any historical ingest provider implements the `HistoricalSource` trait defined in `crates/lumenqraph-indexer/src/deep_backfill.rs`:

```rust
pub trait HistoricalSource: Send + Sync {
    /// Collect all events in `[from_ledger, to_ledger]` into `out`.
    ///
    /// Implementations should stream data and keep memory bounded.
    fn collect_range<'a>(
        &'a self,
        from_ledger: i64,
        to_ledger: i64,
        contract_ids: &'a [String],
        out: &'a mut Vec<HistoricalEvent>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>>;
}
```

### Event Shape: `HistoricalEvent`

Each event collected must populate `HistoricalEvent`:

```rust
pub struct HistoricalEvent {
    pub event_id: String,
    pub contract_id: String,
    pub ledger: i64,
    pub ledger_closed_at: DateTime<Utc>,
    pub event_type: String,
    pub topics: Vec<String>,     // Base64 XDR ScVal strings
    pub value: String,          // Base64 XDR ScVal string
    pub tx_hash: String,
    pub in_successful_call: bool,
    pub paging_token: String,
}
```

- **Ascending order:** Events returned in `out` must be ordered by ledger sequence in ascending order.
- **Contract filtering:** When `contract_ids` is non-empty, only events belonging to one of the specified contracts should be collected. When empty, all contract events are collected.
- **Idempotency:** Inserts into Postgres are executed with `ON CONFLICT (event_id) DO NOTHING`. Even if a source returns duplicate events or overlaps with the live poller, storage is idempotent.

## Extension Guide: Writing a New Source

To implement an archive source (e.g. Horizon, Stellar Core DB, BigQuery, AWS S3 / GCS bucket):

1. **Define your source struct:**
   Store configuration such as database connection pools, HTTP client endpoints, or credentials.
2. **Implement `HistoricalSource`:**
   In `collect_range`, query your storage layer for ledgers between `from_ledger` and `to_ledger`.
3. **Handle Pagination and Memory:**
   When reading from APIs or large dumps, iterate through pages or record streams using cursors, appending matching events to `out`. Stop once `to_ledger` is reached.
4. **Register in CLI:**
   Add your source variant to `run_deep_backfill` in `crates/lumenqraph-indexer/src/main.rs`.

## Reference Implementations

Lumenqraph provides two reference implementations:

### 1. `GalexieSource` (Data-Lake NDJSON Exports)
Reads newline-delimited JSON exports produced by Galexie or stellar-etl. Supports both native per-ledger envelopes and flat per-event records. Stream-reads from files or stdin (`--input -`).

### 2. `HorizonSource` (REST API Client)
Connects to a Stellar Horizon endpoint (e.g. `https://horizon.stellar.org/transactions`), paginates through transactions via HTTP query parameters (`order=asc`, `limit=200`, `cursor=...`), extracts contract events, and filters them against the requested ledger and contract bounds.


## Seam / hand-off to the live poller

The live poller picks up from where its cursor left off. To ensure no gap
between the deep backfill and the live tail:

1. Run the deep backfill with `--to <overlap_ledger>` where `overlap_ledger`
   is a few thousand ledgers inside the RPC retention window (~100k ledgers
   from the current tip).
2. Start the live poller with `START_LEDGER=<overlap_ledger>` (or just let it
   start from the tip — it will catch up the small overlap window
   automatically).

Because inserts are idempotent, any events in the overlap zone are safely
de-duplicated.

```
Timeline:
  ←──────────────────────────────────────────────────────────────────────→
  genesis           overlap_ledger - 100k      overlap_ledger         tip
  |←─── deep backfill covers this range ────→|
                                    |←── live poller covers this →|
                                              ↑ overlap is safe
```

## Input format

### Galexie envelope (native)

```json
{
  "ledger": 1000000,
  "ledger_close_time": "2024-01-01T00:00:00Z",
  "events": [
    {
      "id": "0001000000000000-000",
      "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
      "type": "contract",
      "topic": ["AAAADw==", "AAAAB3RyYW5zZmVy"],
      "value": "AAAACv//…",
      "tx_hash": "3664562a…",
      "in_successful_contract_call": true
    }
  ]
}
```

### Flat per-event record

```json
{
  "id": "0001000000000000-000",
  "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
  "ledger": 1000000,
  "ledger_closed_at": "2024-01-01T00:00:00Z",
  "type": "contract",
  "topic": ["AAAADw=="],
  "value": "AAAACv//…",
  "tx_hash": "3664562a…",
  "in_successful_contract_call": true
}
```

Both formats are auto-detected per line (by the presence of the `"events"` key).

### Notes

- Lines starting with `#` are treated as comments and ignored.
- Empty lines are ignored.
- Unknown fields are ignored.
- `in_successful_contract_call` defaults to `true` when absent.
- `type` defaults to `"contract"` when absent.
- `paging_token` defaults to `id` when absent.

## Known limitations

- The deep backfill path doesn't fetch contract specs from RPC (RPC is not
  available in data-lake mode). Events are still decoded (XDR → JSON) but the
  `enriched` field (named/typed record from the on-chain spec) will be `null`
  for contracts whose specs weren't already loaded by the live poller.
  Run the live poller for at least one cycle before starting the deep backfill
  to warm the spec cache for tracked contracts, or run the deep backfill first
  and then allow the poller to enrich on next encounter.
- Large exports (millions of ledgers) may use significant memory during the
  collect phase. A future improvement will stream batches to Postgres.
- Compressed inputs (`.gz`) are not decompressed automatically — decompress
  first with `gzip -d` or pipe through `gunzip`.
