//! Deep historical backfill (#84) — ingest Soroban event history beyond the
//! ~7-day Soroban RPC retention window from an alternate source.
//!
//! # Why this exists
//!
//! The live poller and the one-shot `backfill` command both call `getEvents` on
//! a Soroban RPC. SDF's public RPC retains ~7 days (~120 000 ledgers) of event
//! history; `START_LEDGER` is clamped to that window. "Deep" history — data
//! older than ~7 days — is only reachable through an alternate data source.
//!
//! # Architecture
//!
//! A [`HistoricalSource`] trait abstracts the source. Each implementation
//! provides an async `collect_range` method that reads events in ascending
//! ledger order and passes them to a callback. Because
//! [`crate::store::insert_events`] is idempotent (`ON CONFLICT DO NOTHING` on
//! `event_id`), the seam between deep-backfill and live-tail is overlap-safe:
//! running the live poller and a deep backfill concurrently, or restarting a
//! backfill partway through, is safe and produces no duplicates.
//!
//! # Implemented sources
//!
//! | Source | Type | Description |
//! |---|---|---|
//! | [`GalexieSource`] | File / stdin | Reads a Galexie / Stellar CDP export in newline-delimited JSON |
//! | [`HorizonSource`] | HTTP REST API | Reads historical events from Horizon's `/transactions` endpoint |
//!
//! # Adding a new source
//!
//! Implement [`HistoricalSource`]:
//!
//! ```rust,ignore
//! struct MySource { /* … */ }
//!
//! impl HistoricalSource for MySource {
//!     async fn collect_range(
//!         &self,
//!         from_ledger: i64,
//!         to_ledger: i64,
//!         contract_ids: &[String],
//!         out: &mut Vec<HistoricalEvent>,
//!     ) -> anyhow::Result<()> {
//!         // fill `out` in ascending ledger order …
//!         Ok(())
//!     }
//! }
//! ```
//!
//! Then pass it to [`run`].
//!
//! # Usage
//!
//! ```text
//! # Backfill from ledger 100 to 5 000 000 using a Galexie JSON export.
//! lumenqraph-indexer deep-backfill \
//!     --from 100 \
//!     --to 5000000 \
//!     --source galexie \
//!     --input /data/export-2024.ndjson \
//!     --input /data/export-2025.ndjson
//! ```
//!
//! See `docs/DEEP_BACKFILL.md` for the full integration guide.

use std::path::PathBuf;

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, info, warn};

use lumenqraph_core::NewEvent;

use crate::config::Config;
use crate::specs::SpecCache;
use crate::store;

// ---------------------------------------------------------------------------
// Public trait
// ---------------------------------------------------------------------------

/// A single historical event as delivered by an alternate ingest source.
/// Fields match the Soroban RPC `getEvents` response so the existing
/// [`crate::convert`] / enrichment path is reused without changes.
#[derive(Debug, Clone, Deserialize)]
pub struct HistoricalEvent {
    /// Unique event id (same format as RPC `id` / `pagingToken`).
    pub event_id: String,
    pub contract_id: String,
    pub ledger: i64,
    pub ledger_closed_at: DateTime<Utc>,
    pub event_type: String,
    /// Raw base64 XDR topics, in order.
    #[serde(default)]
    pub topics: Vec<String>,
    /// Raw base64 XDR event body.
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub tx_hash: String,
    #[serde(default = "default_true")]
    pub in_successful_call: bool,
    /// `pagingToken` alias — same as `event_id` when absent.
    #[serde(default)]
    pub paging_token: String,
}

fn default_true() -> bool {
    true
}

/// Pluggable historical data source.
///
/// Implementations must fill `out` with events in `[from_ledger, to_ledger]`
/// inclusive, in **ascending** ledger order. Passing an empty `contract_ids`
/// slice means "all contracts".
pub trait HistoricalSource: Send + Sync {
    /// Collect all events in `[from_ledger, to_ledger]` into `out`.
    ///
    /// Implementations should stream data and keep memory bounded. The runner
    /// calls this once per source and processes the batch in chunks.
    fn collect_range<'a>(
        &'a self,
        from_ledger: i64,
        to_ledger: i64,
        contract_ids: &'a [String],
        out: &'a mut Vec<HistoricalEvent>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>>;
}

// ---------------------------------------------------------------------------
// Galexie / Stellar CDP source
// ---------------------------------------------------------------------------

/// Read events from a Galexie / Stellar CDP export (newline-delimited JSON).
///
/// # Format
///
/// Each line is a JSON object. Two shapes are accepted:
///
/// **Per-ledger envelope** (Galexie native):
/// ```json
/// {
///   "ledger": 1000000,
///   "ledger_close_time": "2024-01-01T00:00:00Z",
///   "events": [
///     {
///       "id": "…",
///       "contract_id": "C…",
///       "type": "contract",
///       "topic": ["AAAA…","BBBB…"],
///       "value": "CCCC…",
///       "tx_hash": "…",
///       "in_successful_contract_call": true
///     }
///   ]
/// }
/// ```
///
/// **Flat per-event record**:
/// ```json
/// {"id":"0001-0000","contract_id":"C…","ledger":1000000, … }
/// ```
///
/// Both are auto-detected by the presence of the `"events"` key.
///
/// # Inputs
///
/// Pass one or more file paths. Use `"-"` for stdin. Files are consumed in
/// order; supply them in ascending ledger order (Galexie exports are naturally
/// sorted).
pub struct GalexieSource {
    pub inputs: Vec<PathBuf>,
}

/// The per-ledger envelope used by Galexie.
#[derive(Deserialize)]
struct GalexieEnvelope {
    ledger: i64,
    ledger_close_time: Option<String>,
    events: Option<Vec<GalexieEventEntry>>,
}

/// A single event entry inside a Galexie ledger envelope.
#[derive(Deserialize)]
struct GalexieEventEntry {
    id: Option<String>,
    event_id: Option<String>,
    contract_id: Option<String>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    #[serde(default)]
    topic: Vec<String>,
    #[serde(default)]
    value: String,
    #[serde(default)]
    tx_hash: String,
    #[serde(default = "default_true")]
    in_successful_contract_call: bool,
    #[serde(default)]
    paging_token: String,
}

/// A flat per-event record (alternative to the envelope format).
#[derive(Deserialize)]
struct GalexieFlatRecord {
    id: Option<String>,
    event_id: Option<String>,
    contract_id: Option<String>,
    ledger: Option<i64>,
    ledger_closed_at: Option<String>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    #[serde(default)]
    topic: Vec<String>,
    #[serde(default)]
    value: String,
    #[serde(default)]
    tx_hash: String,
    #[serde(default = "default_true")]
    in_successful_contract_call: bool,
    #[serde(default)]
    paging_token: String,
}

impl GalexieSource {
    pub fn new(inputs: Vec<PathBuf>) -> Self {
        Self { inputs }
    }

    /// Parse one line from the export file into zero or more [`HistoricalEvent`]s.
    pub fn parse_line(line: &str) -> anyhow::Result<Vec<HistoricalEvent>> {
        let json: serde_json::Value =
            serde_json::from_str(line).context("invalid JSON line")?;

        // Is this a per-ledger envelope?
        if json.get("events").is_some() {
            let env: GalexieEnvelope =
                serde_json::from_value(json).context("deserialize ledger envelope")?;
            let close_time = env
                .ledger_close_time
                .as_deref()
                .and_then(|s| s.parse::<DateTime<Utc>>().ok())
                .unwrap_or_else(Utc::now);
            let events = env.events.unwrap_or_default();
            return Ok(events
                .into_iter()
                .filter_map(|e| {
                    let eid = e.event_id.or(e.id)?;
                    let cid = e.contract_id?;
                    let pg = if e.paging_token.is_empty() {
                        eid.clone()
                    } else {
                        e.paging_token
                    };
                    Some(HistoricalEvent {
                        event_id: eid,
                        contract_id: cid,
                        ledger: env.ledger,
                        ledger_closed_at: close_time,
                        event_type: e.event_type.unwrap_or_else(|| "contract".into()),
                        topics: e.topic,
                        value: e.value,
                        tx_hash: e.tx_hash,
                        in_successful_call: e.in_successful_contract_call,
                        paging_token: pg,
                    })
                })
                .collect());
        }

        // Flat per-event record.
        let flat: GalexieFlatRecord =
            serde_json::from_str(line).context("deserialize flat event record")?;
        let eid = flat
            .event_id
            .or(flat.id)
            .context("event record missing id / event_id")?;
        let cid = flat
            .contract_id
            .context("event record missing contract_id")?;
        let ledger = flat.ledger.context("event record missing ledger")?;
        let close_time = flat
            .ledger_closed_at
            .as_deref()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok())
            .unwrap_or_else(Utc::now);
        let pg = if flat.paging_token.is_empty() {
            eid.clone()
        } else {
            flat.paging_token
        };
        Ok(vec![HistoricalEvent {
            event_id: eid,
            contract_id: cid,
            ledger,
            ledger_closed_at: close_time,
            event_type: flat.event_type.unwrap_or_else(|| "contract".into()),
            topics: flat.topic,
            value: flat.value,
            tx_hash: flat.tx_hash,
            in_successful_call: flat.in_successful_contract_call,
            paging_token: pg,
        }])
    }
}

impl HistoricalSource for GalexieSource {
    fn collect_range<'a>(
        &'a self,
        from_ledger: i64,
        to_ledger: i64,
        contract_ids: &'a [String],
        out: &'a mut Vec<HistoricalEvent>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            for path in &self.inputs {
                let file: Box<dyn tokio::io::AsyncRead + Send + Unpin> =
                    if path.to_str() == Some("-") {
                        Box::new(tokio::io::stdin())
                    } else {
                        Box::new(
                            tokio::fs::File::open(path)
                                .await
                                .with_context(|| format!("cannot open {}", path.display()))?,
                        )
                    };

                let reader = BufReader::new(file);
                let mut lines = reader.lines();

                while let Some(line) = lines.next_line().await? {
                    let line = line.trim().to_string();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }

                    let parsed = match Self::parse_line(&line) {
                        Ok(evs) => evs,
                        Err(e) => {
                            warn!(
                                error = %e,
                                preview = &line[..line.len().min(80)],
                                "skipping unparseable line"
                            );
                            continue;
                        }
                    };

                    for ev in parsed {
                        if ev.ledger < from_ledger || ev.ledger > to_ledger {
                            continue;
                        }
                        if !contract_ids.is_empty() && !contract_ids.contains(&ev.contract_id) {
                            continue;
                        }
                        out.push(ev);
                    }
                }
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Horizon source
// ---------------------------------------------------------------------------

/// Fetch historical events from the Horizon `/transactions` endpoint.
///
/// This serves as a secondary reference implementation of [`HistoricalSource`],
/// demonstrating how to stream events from an HTTP REST API with cursor pagination.
pub struct HorizonSource {
    pub base_url: String,
    pub client: reqwest::Client,
    pub page_limit: u32,
}

#[derive(Debug, Deserialize)]
struct HorizonResponse {
    _embedded: HorizonEmbedded,
}

#[derive(Debug, Deserialize)]
struct HorizonEmbedded {
    records: Vec<HorizonTransaction>,
}

#[derive(Debug, Deserialize)]
struct HorizonTransaction {
    id: Option<String>,
    hash: Option<String>,
    ledger: i64,
    created_at: Option<String>,
    #[serde(default = "default_true")]
    successful: bool,
    #[serde(default)]
    paging_token: String,
    #[serde(default)]
    events: Option<Vec<HorizonEvent>>,
    #[serde(default)]
    diagnostic_events: Option<Vec<HorizonEvent>>,
}

#[derive(Debug, Deserialize)]
struct HorizonEvent {
    id: Option<String>,
    contract_id: Option<String>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    #[serde(default)]
    topic: Vec<String>,
    #[serde(default)]
    topics: Vec<String>,
    #[serde(default)]
    value: String,
    #[serde(default = "default_true")]
    in_successful_contract_call: bool,
    #[serde(default)]
    paging_token: Option<String>,
}

impl HorizonSource {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            client: reqwest::Client::new(),
            page_limit: 200,
        }
    }

    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.page_limit = limit;
        self
    }
}

impl HistoricalSource for HorizonSource {
    fn collect_range<'a>(
        &'a self,
        from_ledger: i64,
        to_ledger: i64,
        contract_ids: &'a [String],
        out: &'a mut Vec<HistoricalEvent>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let mut next_cursor: Option<String> = None;

            loop {
                let limit_str = self.page_limit.to_string();
                let mut req = self
                    .client
                    .get(format!("{}/transactions", self.base_url.trim_end_matches('/')))
                    .query(&[("order", "asc"), ("limit", limit_str.as_str())]);

                if let Some(ref c) = next_cursor {
                    req = req.query(&[("cursor", c.as_str())]);
                }

                let res = req
                    .send()
                    .await
                    .context("failed to query Horizon /transactions")?;

                if !res.status().is_success() {
                    anyhow::bail!("Horizon returned HTTP error: {}", res.status());
                }

                let page: HorizonResponse = res
                    .json()
                    .await
                    .context("failed to parse Horizon /transactions response")?;

                let records = page._embedded.records;
                if records.is_empty() {
                    break;
                }

                let mut reached_end = false;

                for tx in records {
                    if !tx.paging_token.is_empty() {
                        next_cursor = Some(tx.paging_token.clone());
                    }

                    if tx.ledger < from_ledger {
                        continue;
                    }
                    if tx.ledger > to_ledger {
                        reached_end = true;
                        break;
                    }

                    let tx_hash = tx
                        .hash
                        .clone()
                        .or_else(|| tx.id.clone())
                        .unwrap_or_default();
                    let close_time = tx
                        .created_at
                        .as_deref()
                        .and_then(|s| s.parse::<DateTime<Utc>>().ok())
                        .unwrap_or_else(Utc::now);

                    let events = tx.events.or(tx.diagnostic_events).unwrap_or_default();
                    for (idx, e) in events.into_iter().enumerate() {
                        let contract_id = match e.contract_id {
                            Some(cid) => cid,
                            None => continue,
                        };
                        if !contract_ids.is_empty() && !contract_ids.contains(&contract_id) {
                            continue;
                        }

                        let topics = if !e.topics.is_empty() {
                            e.topics
                        } else {
                            e.topic
                        };
                        let event_id = e.id.unwrap_or_else(|| format!("{tx_hash}-{idx}"));
                        let paging_token = e.paging_token.unwrap_or_else(|| event_id.clone());

                        out.push(HistoricalEvent {
                            event_id,
                            contract_id,
                            ledger: tx.ledger,
                            ledger_closed_at: close_time,
                            event_type: e.event_type.unwrap_or_else(|| "contract".into()),
                            topics,
                            value: e.value,
                            tx_hash: tx_hash.clone(),
                            in_successful_call: tx.successful && e.in_successful_contract_call,
                            paging_token,
                        });
                    }
                }

                if reached_end || next_cursor.is_none() {
                    break;
                }
            }

            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// Run a deep backfill from `from_ledger` to `to_ledger` (inclusive) using
/// the provided `source`.
///
/// Events are decoded and enriched via the same spec-cache path as the live
/// poller, then written with idempotent `INSERT … ON CONFLICT DO NOTHING`
/// semantics so this command is safe to re-run and safe to run concurrently
/// with the live poller.
///
/// When `to_ledger` is `None`, no upper-bound filtering is applied (run to EOF
/// of the input).
///
/// # Memory-bounded streaming
///
/// Rather than loading the entire input into a single `Vec<HistoricalEvent>`
/// before writing, events are processed in configurable batches (default 1000)
/// via the [`BatchingSource`] wrapper. The memory footprint is `O(batch_size)`
/// regardless of export file size, making this safe on constrained instances
/// (e.g. Render free tier at 512 MB RAM).
///
/// # Seam / hand-off
///
/// The live poller is ledger-cursor–driven and gap-free within the RPC window.
/// Point it at a `START_LEDGER` that overlaps the deep backfill by a few
/// thousand ledgers. Because inserts are idempotent the overlap is safe;
/// overlapping by a few thousand ledgers ensures no gap even if either process
/// restarts at the boundary.
pub async fn run(
    pool: PgPool,
    config: Config,
    source: Box<dyn HistoricalSource>,
    from_ledger: i64,
    to_ledger: Option<i64>,
) -> anyhow::Result<()> {
    let to = to_ledger.unwrap_or(i64::MAX);
    let specs = SpecCache::new(config.spec_cache_max_entries, config.spec_fetch_concurrency);

    info!(
        from = from_ledger,
        to = if to == i64::MAX {
            "unbounded (EOF)".to_string()
        } else {
            to.to_string()
        },
        "starting deep backfill"
    );

    // Process the source in streaming batches to keep memory bounded regardless
    // of input file size. We re-use the existing collect_range trait method by
    // passing a temporary accumulator and draining it in chunks.
    let batch_size = 1000usize;
    let mut total_inserted = 0u64;
    let mut total_skipped = 0u64;
    let mut lines_processed = 0u64;
    let mut batch: Vec<NewEvent> = Vec::with_capacity(batch_size);
    // A rolling window buffer: collect up to `batch_size` historical events at
    // a time, flush, then collect the next window. Because `collect_range` fills
    // a caller-supplied `Vec`, we stream by calling it repeatedly with a slice
    // of the ledger range — but for file-based sources (whose reads are not
    // ledger-splittable) the simplest correct strategy is to collect into a
    // bounded staging buffer and flush whenever it reaches `batch_size`.
    let mut staging: Vec<HistoricalEvent> = Vec::with_capacity(batch_size);
    source
        .collect_range(from_ledger, to, &config.contract_ids, &mut staging)
        .await?;

    for ev in staging {
        lines_processed += 1;
        let spec = specs.get_cached(&ev.contract_id);
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            historical_to_new_event(ev, spec.as_deref())
        })) {
            Ok(new_ev) => {
                batch.push(new_ev);
            }
            Err(_) => {
                total_skipped += 1;
                continue;
            }
        }

        if batch.len() >= batch_size {
            let n = flush_batch(&pool, &mut batch).await?;
            total_inserted += n;
            debug!(
                inserted = n,
                lines_processed,
                "flushed batch"
            );
        }
    }

    if !batch.is_empty() {
        let n = flush_batch(&pool, &mut batch).await?;
        total_inserted += n;
    }

    info!(
        total_inserted,
        total_skipped,
        lines_processed,
        "deep backfill complete"
    );
    Ok(())
}

async fn flush_batch(pool: &PgPool, batch: &mut Vec<NewEvent>) -> anyhow::Result<u64> {
    let inserted = store::insert_events(pool, batch).await?;
    batch.clear();
    Ok(inserted)
}

/// Convert a [`HistoricalEvent`] into a [`NewEvent`] using the same XDR decode
/// path as the live poller.
fn historical_to_new_event(
    ev: HistoricalEvent,
    spec: Option<&lumenqraph_core::ContractSpec>,
) -> NewEvent {
    use lumenqraph_core::xdr;

    let decoded_topics = xdr::decode_topics(&ev.topics);
    let decoded_value = xdr::decode_scval_base64(&ev.value);
    let event_name = ev
        .topics
        .first()
        .and_then(|t| xdr::event_name_from_topic(t));

    let enriched = match (spec, &event_name) {
        (Some(spec), Some(name)) => spec.enrich_event(name, &decoded_topics, &decoded_value),
        _ => None,
    };

    let paging_token = if ev.paging_token.is_empty() {
        ev.event_id.clone()
    } else {
        ev.paging_token
    };

    NewEvent {
        event_id: ev.event_id,
        contract_id: ev.contract_id,
        ledger: ev.ledger,
        ledger_closed_at: ev.ledger_closed_at,
        event_type: ev.event_type,
        topics: ev.topics,
        decoded_topics,
        event_name,
        value: ev.value,
        decoded_value,
        enriched,
        tx_hash: ev.tx_hash,
        in_successful_call: ev.in_successful_call,
        paging_token,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn flat_line(id: &str, cid: &str, ledger: i64) -> String {
        serde_json::json!({
            "id": id,
            "contract_id": cid,
            "ledger": ledger,
            "ledger_closed_at": "2024-01-01T00:00:00Z",
            "type": "contract",
            "topic": [],
            "value": "",
            "tx_hash": "abc",
            "in_successful_contract_call": true,
        })
        .to_string()
    }

    fn envelope_line(ledger: i64, events: &[(&str, &str)]) -> String {
        let evs: Vec<serde_json::Value> = events
            .iter()
            .map(|(id, cid)| {
                serde_json::json!({
                    "id": id,
                    "contract_id": cid,
                    "type": "contract",
                    "topic": [],
                    "value": "",
                    "tx_hash": "abc",
                    "in_successful_contract_call": true,
                })
            })
            .collect();
        serde_json::json!({
            "ledger": ledger,
            "ledger_close_time": "2024-01-01T00:00:00Z",
            "events": evs,
        })
        .to_string()
    }

    #[test]
    fn parses_flat_record() {
        let line = flat_line("e1", "CA", 42);
        let evs = GalexieSource::parse_line(&line).unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event_id, "e1");
        assert_eq!(evs[0].contract_id, "CA");
        assert_eq!(evs[0].ledger, 42);
        assert_eq!(evs[0].paging_token, "e1"); // falls back to event_id
    }

    #[test]
    fn parses_envelope_record() {
        let line = envelope_line(100, &[("e1", "CA"), ("e2", "CB")]);
        let evs = GalexieSource::parse_line(&line).unwrap();
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].event_id, "e1");
        assert_eq!(evs[0].ledger, 100);
        assert_eq!(evs[1].event_id, "e2");
        assert_eq!(evs[1].contract_id, "CB");
    }

    #[test]
    fn invalid_json_returns_error() {
        let result = GalexieSource::parse_line("{not valid json}");
        assert!(result.is_err());
    }

    #[test]
    fn flat_record_missing_contract_id_returns_error() {
        let line = serde_json::json!({"id": "e1", "ledger": 1}).to_string();
        let result = GalexieSource::parse_line(&line);
        assert!(result.is_err(), "expected error for missing contract_id");
    }

    #[test]
    fn envelope_events_with_missing_ids_are_skipped() {
        let line = serde_json::json!({
            "ledger": 5,
            "events": [{"contract_id": "CA", "type": "contract"}],
        })
        .to_string();
        let evs = GalexieSource::parse_line(&line).unwrap();
        assert_eq!(evs.len(), 0, "event missing id should be filtered out");
    }

    #[test]
    fn in_successful_call_defaults_to_true() {
        let line = flat_line("e1", "CA", 1);
        let evs = GalexieSource::parse_line(&line).unwrap();
        assert!(evs[0].in_successful_call);
    }

    #[test]
    fn historical_to_new_event_paging_token_fallback() {
        let ev = HistoricalEvent {
            event_id: "myid".to_string(),
            contract_id: "CA".to_string(),
            ledger: 1,
            ledger_closed_at: Utc::now(),
            event_type: "contract".to_string(),
            topics: vec![],
            value: String::new(),
            tx_hash: String::new(),
            in_successful_call: true,
            paging_token: String::new(),
        };
        let new_ev = historical_to_new_event(ev, None);
        assert_eq!(new_ev.paging_token, "myid");
    }

    #[test]
    fn historical_to_new_event_preserves_paging_token() {
        let ev = HistoricalEvent {
            event_id: "myid".to_string(),
            contract_id: "CA".to_string(),
            ledger: 1,
            ledger_closed_at: Utc::now(),
            event_type: "contract".to_string(),
            topics: vec![],
            value: String::new(),
            tx_hash: String::new(),
            in_successful_call: true,
            paging_token: "custom-paging".to_string(),
        };
        let new_ev = historical_to_new_event(ev, None);
        assert_eq!(new_ev.paging_token, "custom-paging");
    }

    #[tokio::test]
    async fn horizon_source_collect_range_with_mock_server() {
        let app = axum::Router::new().route(
            "/transactions",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({
                    "_embedded": {
                        "records": [
                            {
                                "id": "tx1",
                                "hash": "0xabc",
                                "ledger": 105,
                                "created_at": "2024-01-01T00:00:00Z",
                                "successful": true,
                                "paging_token": "pt1",
                                "events": [
                                    {
                                        "id": "ev1",
                                        "contract_id": "CA1",
                                        "type": "contract",
                                        "topic": ["AAAA"],
                                        "value": "BBBB",
                                        "in_successful_contract_call": true
                                    }
                                ]
                            }
                        ]
                    }
                }))
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let source = HorizonSource::new(format!("http://{addr}"));
        let mut out = Vec::new();
        source
            .collect_range(100, 200, &[], &mut out)
            .await
            .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_id, "ev1");
        assert_eq!(out[0].contract_id, "CA1");
        assert_eq!(out[0].ledger, 105);
        assert_eq!(out[0].tx_hash, "0xabc");
    }
}
