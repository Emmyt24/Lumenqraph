//! `GET /contracts` — the set of contracts we've seen events for, with counts.
//! Derived on the fly from the events table so it can never drift from reality.
//!
//! `GET /contracts/:id/interface` — the contract's decoded on-chain interface
//! (functions, events, and user-defined types), parsed from its deployed WASM.
//!
//! `GET /contracts/:id/interface/history` and `/interface/diff` — the upgrade
//! watch: a Soroban contract can be upgraded in place, so its interface is a
//! time series, and these serve its versions and what changed between them.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use chrono::{DateTime, Utc};
use lumenqraph_core::{Contract, ContractSpec, SpecDiff};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::types::Json as SqlxJson;

use crate::error::{ApiError, ApiResult};
use crate::extract::ValidContractId;
use crate::specs::CachedSpec;
use crate::state::AppState;

/// Build a weak ETag value from an arbitrary string (e.g. wasm hash or version
/// composite). Returns a string like `W/"<hex>"`.
fn generate_etag(source: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    source.hash(&mut hasher);
    let h = hasher.finish();
    format!("W/\"{h:016x}\"")
}

/// Return `true` when the client's `If-None-Match` header matches the etag,
/// meaning the cached copy is still valid and we can respond 304.
fn check_if_none_match(headers: &HeaderMap, etag: &str) -> bool {
    if let Some(inm) = headers.get(header::IF_NONE_MATCH) {
        if let Ok(s) = inm.to_str() {
            return s == etag || s == "*";
        }
    }
    false
}

#[derive(Deserialize)]
pub struct ContractsQuery {
    /// Maximum number of contracts to return. Default 100, max 1000.
    #[serde(default = "default_limit")]
    limit: i64,
    /// Cursor-based pagination: the `contract_id` returned as `next_cursor`
    /// from a previous response. When supplied, returns contracts whose
    /// `contract_id` sorts after this value (within the same event-count order).
    #[serde(default)]
    after: Option<String>,
}

fn default_limit() -> i64 {
    100
}

#[derive(Serialize)]
pub struct ContractsResponse {
    pub data: Vec<Contract>,
    pub has_more: bool,
    /// The `after` value to pass on the next request to continue pagination.
    /// `null` when there are no more pages.
    pub next_cursor: Option<String>,
}

pub async fn list_contracts(
    State(state): State<AppState>,
    Query(q): Query<ContractsQuery>,
) -> ApiResult<Json<ContractsResponse>> {
    // Query from the contract_summaries table (maintained by a trigger on events inserts)
    // instead of computing a GROUP BY on every request. This provides constant-time
    // performance independent of the total event count, making the explorer's landing
    // page (which relies on this endpoint) performant at scale.
    //
    // Cursor pagination: when `after` is provided, resolve the event_count of the
    // cursor row and continue from there. Ties in event_count are broken by
    // contract_id (lexicographic), which gives a stable total order without a
    // sequential scan.
    let limit = q.limit.clamp(1, 1000);

    let contracts: Vec<Contract> = if let Some(ref cursor) = q.after {
        // Look up the event_count of the cursor contract so we can use a
        // keyset predicate instead of OFFSET, keeping the query O(log N).
        let cursor_count: Option<i64> =
            sqlx::query_scalar("SELECT event_count FROM contract_summaries WHERE contract_id = $1")
                .bind(cursor)
                .fetch_optional(&state.pool)
                .await?;

        match cursor_count {
            Some(cc) => {
                sqlx::query_as(
                    "SELECT contract_id,
                        event_count,
                        first_seen_ledger,
                        last_seen_ledger
                 FROM contract_summaries
                 WHERE event_count > 0
                   AND (event_count < $1
                        OR (event_count = $1 AND contract_id > $2))
                 ORDER BY event_count DESC, contract_id ASC
                 LIMIT $3",
                )
                .bind(cc)
                .bind(cursor)
                .bind(limit + 1)
                .fetch_all(&state.pool)
                .await?
            }
            // Unknown cursor — return empty rather than silently restarting.
            None => vec![],
        }
    } else {
        sqlx::query_as(
            "SELECT contract_id,
                    event_count,
                    first_seen_ledger,
                    last_seen_ledger
             FROM contract_summaries
             WHERE event_count > 0
             ORDER BY event_count DESC, contract_id ASC
             LIMIT $1",
        )
        .bind(limit + 1)
        .fetch_all(&state.pool)
        .await?
    };

    let has_more = contracts.len() as i64 > limit;
    let result_contracts: Vec<Contract> = if has_more {
        contracts.into_iter().take(limit as usize).collect()
    } else {
        contracts
    };

    let next_cursor = if has_more {
        result_contracts.last().map(|c| c.contract_id.clone())
    } else {
        None
    };

    Ok(Json(ContractsResponse {
        data: result_contracts,
        has_more,
        next_cursor,
    }))
}

#[derive(Deserialize)]
pub struct InterfaceQuery {
    /// A historical version to serve instead of the current interface.
    version: Option<i32>,
}

pub async fn contract_interface(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<InterfaceQuery>,
    headers: HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    if let Some(version) = q.version {
        return contract_interface_at_version(&state, &contract_id, version, &headers).await;
    }

    let row: Option<(SqlxJson<Value>, bool, DateTime<Utc>, String)> = sqlx::query_as(
        "SELECT interface, has_events, fetched_at, wasm_hash
         FROM contract_specs WHERE contract_id = $1",
    )
    .bind(&contract_id)
    .fetch_optional(&state.pool)
    .await?;

    match row {
        Some((interface, has_events, fetched_at, wasm_hash)) => {
            let etag = generate_etag(&wasm_hash);
            if check_if_none_match(&headers, &etag) {
                return Ok((StatusCode::NOT_MODIFIED, [(header::ETAG, etag)]).into_response());
            }
            let mut response = Json(json!({
                "contract_id": contract_id,
                "has_events": has_events,
                "fetched_at": fetched_at,
                "interface": interface.0,
            }))
            .into_response();
            response
                .headers_mut()
                .insert(header::ETAG, etag.parse().unwrap());
            Ok(response)
        }
        None => Err(ApiError::not_found(
            "no on-chain interface indexed for this contract yet",
        )),
    }
}

/// The interface as it was at one historical version — what the contract's
/// callers were binding to back then, which the current interface can't tell you
/// once the contract has been upgraded.
async fn contract_interface_at_version(
    state: &AppState,
    contract_id: &str,
    version: i32,
    headers: &HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    let row: Option<(SqlxJson<Value>, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT interface, wasm_hash, observed_at FROM contract_spec_versions
         WHERE contract_id = $1 AND version = $2",
    )
    .bind(contract_id)
    .bind(version)
    .fetch_optional(&state.pool)
    .await?;

    match row {
        Some((interface, wasm_hash, observed_at)) => {
            let etag = generate_etag(&wasm_hash);
            if check_if_none_match(headers, &etag) {
                return Ok((StatusCode::NOT_MODIFIED, [(header::ETAG, etag)]).into_response());
            }
            let mut response = Json(json!({
                "contract_id": contract_id,
                "version": version,
                "wasm_hash": wasm_hash,
                "observed_at": observed_at,
                "interface": interface.0,
            }))
            .into_response();
            response
                .headers_mut()
                .insert(header::ETAG, etag.parse().unwrap());
            Ok(response)
        }
        None => Err(ApiError::not_found(format!(
            "no version {version} recorded for this contract"
        ))),
    }
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    /// How many versions to return, newest first. Default 50, max 1000.
    #[serde(default = "default_history_limit")]
    limit: i64,
    /// Cursor for keyset pagination. Pass the `next_cursor` value from a
    /// previous response to retrieve the next page. The cursor encodes the
    /// last version seen as `version:<N>`.
    #[serde(default)]
    after: Option<String>,
}

fn default_history_limit() -> i64 {
    50
}

/// Parse a history cursor of the form `version:<N>` and return `N`.
///
/// Returns `Err` with a human-readable message on any malformed input so the
/// handler can surface a clean `400` instead of silently restarting at page 1.
fn parse_history_cursor(cursor: &str) -> Result<i32, ApiError> {
    let version_str = cursor
        .strip_prefix("version:")
        .ok_or_else(|| ApiError::bad_request("invalid cursor: expected format `version:<N>`"))?;
    version_str.parse::<i32>().map_err(|_| {
        ApiError::bad_request("invalid cursor: version component is not a valid integer")
    })
}

/// `GET /contracts/:id/interface/history` — every interface version we've
/// observed for this contract, newest first, each with the diff against the one
/// before it. The full interface of each version is omitted (fetch it with
/// `/interface?version=N`); the diff is what's interesting here.
///
/// Supports keyset cursor pagination via the `after` parameter so callers can
/// retrieve complete history for contracts with more than one page of versions.
/// Pass the `next_cursor` from a previous response as `after` to continue.
pub async fn contract_interface_history(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<HistoryQuery>,
    headers: HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    // Parse the cursor before hitting the DB so a malformed value returns 400
    // immediately rather than after an unnecessary round-trip.
    let after_version: Option<i32> = q.after.as_deref().map(parse_history_cursor).transpose()?;

    let limit = q.limit.clamp(1, 1000);

    // Get the latest version for ETag (independent of cursor / limit).
    let latest_version: Option<i32> = sqlx::query_scalar(
        "SELECT max(version) FROM contract_spec_versions WHERE contract_id = $1",
    )
    .bind(&contract_id)
    .fetch_one(&state.pool)
    .await?;

    let Some(latest_version) = latest_version else {
        return Err(ApiError::not_found(
            "no on-chain interface indexed for this contract yet",
        ));
    };

    // ETag covers the contract + newest version so it invalidates on every
    // upgrade regardless of which page is being requested.
    let etag = generate_etag(&format!("{}:{}", contract_id, latest_version));
    if check_if_none_match(&headers, &etag) {
        return Ok((StatusCode::NOT_MODIFIED, [(header::ETAG, etag)]).into_response());
    }

    // (version, wasm_hash, previous_wasm_hash, diff, breaking, observed_at)
    type VersionRow = (
        i32,
        String,
        Option<String>,
        Option<SqlxJson<Value>>,
        bool,
        DateTime<Utc>,
    );

    // Fetch limit+1 rows so we can detect whether a next page exists without
    // a separate COUNT query.
    let rows: Vec<VersionRow> = if let Some(after_v) = after_version {
        // Keyset predicate: only versions strictly older (lower number) than
        // the cursor. The unknown-cursor case returns empty rather than
        // silently restarting at the newest page.
        sqlx::query_as(
            "SELECT version, wasm_hash, previous_wasm_hash, diff, breaking, observed_at
             FROM contract_spec_versions
             WHERE contract_id = $1
               AND version < $2
             ORDER BY version DESC
             LIMIT $3",
        )
        .bind(&contract_id)
        .bind(after_v)
        .bind(limit + 1)
        .fetch_all(&state.pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT version, wasm_hash, previous_wasm_hash, diff, breaking, observed_at
             FROM contract_spec_versions
             WHERE contract_id = $1
             ORDER BY version DESC
             LIMIT $2",
        )
        .bind(&contract_id)
        .bind(limit + 1)
        .fetch_all(&state.pool)
        .await?
    };

    let has_more = rows.len() as i64 > limit;
    let rows: Vec<VersionRow> = if has_more {
        rows.into_iter().take(limit as usize).collect()
    } else {
        rows
    };

    // The next cursor points at the last version in this page; the next call
    // will return versions with a lower number than that.
    let next_cursor: Option<String> = if has_more {
        rows.last().map(|(v, ..)| format!("version:{v}"))
    } else {
        None
    };

    let versions: Vec<Value> = rows
        .into_iter()
        .map(
            |(version, wasm_hash, previous_wasm_hash, diff, breaking, observed_at)| {
                json!({
                    "version": version,
                    "wasm_hash": wasm_hash,
                    "previous_wasm_hash": previous_wasm_hash,
                    // Null on version 1: a baseline has nothing to be diffed
                    // against, which is not the same as an empty diff.
                    "diff": diff.map(|d| d.0),
                    "breaking": breaking,
                    "observed_at": observed_at,
                })
            },
        )
        .collect();

    let mut response = Json(json!({
        "contract_id": contract_id,
        "count": versions.len(),
        "has_more": has_more,
        "next_cursor": next_cursor,
        "versions": versions,
    }))
    .into_response();
    response
        .headers_mut()
        .insert(header::ETAG, etag.parse().unwrap());
    Ok(response)
}

#[derive(Deserialize)]
pub struct DiffQuery {
    /// Defaults to the version before `to`.
    from: Option<i32>,
    /// Defaults to the newest version.
    to: Option<i32>,
}

/// `GET /contracts/:id/interface/diff?from=&to=` — what changed between any two
/// interface versions.
///
/// Computed on demand from each version's stored spec section rather than read
/// from the `diff` column, which only ever holds consecutive diffs: this way you
/// can ask "what changed between v1 and v5" in one call, not four.
pub async fn contract_interface_diff(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<DiffQuery>,
) -> ApiResult<Json<Value>> {
    // Reject an explicit bad range before touching the database.
    if let (Some(from), Some(to)) = (q.from, q.to) {
        validate_diff_range(from, to)?;
    }
    let latest: Option<i32> = sqlx::query_scalar(
        "SELECT max(version) FROM contract_spec_versions WHERE contract_id = $1",
    )
    .bind(&contract_id)
    .fetch_one(&state.pool)
    .await?;
    let Some(latest) = latest else {
        return Err(ApiError::not_found(
            "no on-chain interface indexed for this contract yet",
        ));
    };

    let to = q.to.unwrap_or(latest);
    let from = q.from.unwrap_or(to - 1);
    if from < 1 && q.from.is_none() {
        return Err(ApiError::bad_request(format!(
            "no version to diff against: this contract has only version {latest} on record, \
             so there is no earlier interface to compare it to"
        )));
    }
    validate_diff_range(from, to)?;

    let old = load_spec(&state, &contract_id, from).await?;
    let new = load_spec(&state, &contract_id, to).await?;
    // Both are Some: load_spec rejects an unparseable version above.
    let diff = SpecDiff::between(old.parsed.as_ref().unwrap(), new.parsed.as_ref().unwrap());

    Ok(Json(json!({
        "contract_id": contract_id,
        "from": from,
        "to": to,
        "diff": diff.to_json(),
    })))
}

/// Validate a `from`..`to` interface-diff range (#211): `from` must be a real
/// version (>= 1) strictly older than `to`. A reversed range would produce a
/// backward diff (added items shown as removed and vice-versa).
fn validate_diff_range(from: i32, to: i32) -> Result<(), ApiError> {
    if from < 1 {
        return Err(ApiError::bad_request(format!(
            "`from` ({from}) must be >= 1: interface versions start at 1"
        )));
    }
    if from > to {
        return Err(ApiError::bad_request(format!(
            "`from` ({from}) must be less than `to` ({to}); \
             reversing the order would produce a backward diff where added items \
             appear as removed and vice-versa"
        )));
    }
    if from == to {
        return Err(ApiError::bad_request(
            "`from` and `to` are the same version; nothing to diff",
        ));
    }
    Ok(())
}

/// One version's parsed interface, from the cache (versions are immutable, so
/// this is a pure hit after the first read).
async fn load_spec(
    state: &AppState,
    contract_id: &str,
    version: i32,
) -> ApiResult<Arc<CachedSpec>> {
    let spec = state
        .specs
        .at_version(&state.pool, contract_id, version)
        .await?;
    if spec.parsed.is_none() {
        return Err(ApiError::Internal(anyhow::anyhow!(
            "stored spec section for version {version} could not be parsed"
        )));
    }
    Ok(spec)
}

#[derive(Deserialize)]
pub struct StateQuery {
    /// How many versions to return, newest first (1 = current state only).
    #[serde(default = "default_state_limit")]
    limit: i64,
}

fn default_state_limit() -> i64 {
    1
}

/// `GET /contracts/:id/state` — versioned snapshots of a contract's instance
/// storage, newest first. `limit=1` (default) is the current state.
pub async fn contract_state(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<StateQuery>,
    headers: HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    let limit = q.limit.clamp(1, 200);
    let rows: Vec<(i64, SqlxJson<Value>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT ledger, storage, captured_at
         FROM contract_state WHERE contract_id = $1
         ORDER BY ledger DESC LIMIT $2",
    )
    .bind(&contract_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;

    if rows.is_empty() {
        // Check if state indexing is disabled by seeing if any state exists at all.
        let any_state_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM contract_state LIMIT 1)")
                .fetch_one(&state.pool)
                .await?;

        if !any_state_exists {
            return Err(ApiError::feature_disabled("state indexing is disabled"));
        }

        return Err(ApiError::not_found("no state snapshots for this contract"));
    }

    let latest_ledger = rows[0].0;
    let etag = generate_etag(&format!("{}:{}", contract_id, latest_ledger));
    if check_if_none_match(&headers, &etag) {
        return Ok((StatusCode::NOT_MODIFIED, [(header::ETAG, etag)]).into_response());
    }

    let versions: Vec<Value> = rows
        .into_iter()
        .map(|(ledger, storage, captured_at)| {
            json!({ "ledger": ledger, "storage": storage.0, "captured_at": captured_at })
        })
        .collect();
    let mut response = Json(json!({
        "contract_id": contract_id,
        "count": versions.len(),
        "versions": versions,
    }))
    .into_response();
    response
        .headers_mut()
        .insert(header::ETAG, etag.parse().unwrap());
    Ok(response)
}

#[derive(Deserialize)]
pub struct DataQuery {
    /// Filter to a discovery label, e.g. `balance`.
    label: Option<String>,
    /// Max keys to return (latest value of each), default 100.
    #[serde(default = "default_data_limit")]
    limit: i64,
}

fn default_data_limit() -> i64 {
    100
}

/// One `contract_data` row as selected below: (key_hash, key, durability,
/// ledger, value, label, captured_at, deleted, archived).
type DataRow = (
    String,
    SqlxJson<Value>,
    String,
    i64,
    Option<SqlxJson<Value>>,
    Option<String>,
    DateTime<Utc>,
    bool,
    bool,
);

/// `GET /contracts/:id/data` — the current value of every *per-key* entry
/// snapshotted for this contract (e.g. every tracked holder balance), one row
/// per key (its latest snapshot). Requires the indexer's key indexing.
///
/// Keys whose newest row is a tombstone (the entry was deleted or expired, or
/// a persistent entry was archived) are reported as absent rather than with
/// their stale last value (#742): a deleted key is omitted from `keys` and
/// listed under `deleted` as `{ "key_hash", "key", "deleted_at_ledger" }`.
pub async fn contract_data(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<DataQuery>,
) -> ApiResult<Json<Value>> {
    let limit = q.limit.clamp(1, 1000);
    // DISTINCT ON gives the newest row per key_hash; the outer query orders and
    // bounds the set of keys returned. Tombstones are included here so the
    // newest row per key is always the truth, then split out below.
    let rows: Vec<DataRow> = sqlx::query_as(
        "SELECT key_hash, key, durability, ledger, value, label, captured_at, deleted, archived FROM (
                 SELECT DISTINCT ON (key_hash)
                        key_hash, key, durability, ledger, value, label, captured_at,
                        deleted, archived
                 FROM contract_data
                 WHERE contract_id = $1 AND ($2::text IS NULL OR label = $2)
                 ORDER BY key_hash, ledger DESC
             ) latest
             ORDER BY ledger DESC
             LIMIT $3",
    )
    .bind(&contract_id)
    .bind(&q.label)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;

    if rows.is_empty() {
        // Check if key indexing is disabled by seeing if any data exists at all.
        let any_data_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM contract_data LIMIT 1)")
                .fetch_one(&state.pool)
                .await?;

        if !any_data_exists {
            return Err(ApiError::feature_disabled("key indexing is disabled"));
        }

        return Err(ApiError::not_found(
            "no per-key data snapshots for this contract",
        ));
    }

    let mut keys: Vec<Value> = Vec::new();
    let mut deleted: Vec<Value> = Vec::new();
    for (key_hash, key, durability, ledger, value, label, captured_at, is_deleted, archived) in
        rows
    {
        if is_deleted || value.is_none() {
            // The entry is gone: report it as absent, never with its stale
            // last value. `archived` distinguishes a restorable persistent
            // entry from one that was actually deleted.
            deleted.push(json!({
                "key_hash": key_hash,
                "key": key.0,
                "durability": durability,
                "label": label,
                "deleted_at_ledger": ledger,
                "archived": archived,
            }));
            continue;
        }
        keys.push(json!({
            "key_hash": key_hash,
            "key": key.0,
            "durability": durability,
            "ledger": ledger,
            "value": value.map(|v| v.0),
            "label": label,
            "captured_at": captured_at,
        }));
    }
    Ok(Json(json!({
        "contract_id": contract_id,
        "count": keys.len(),
        "keys": keys,
        "deleted_count": deleted.len(),
        "deleted": deleted,
    })))
}

#[derive(Deserialize)]
pub struct DataHistoryQuery {
    /// How many versions to return, newest first.
    #[serde(default = "default_state_limit")]
    limit: i64,
}

/// `GET /contracts/:id/data/:key_hash` — the version history of a single
/// per-key entry (e.g. one holder's balance over time), newest first.
pub async fn contract_data_key(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Path((_, key_hash)): Path<(String, String)>,
    Query(q): Query<DataHistoryQuery>,
) -> ApiResult<Json<Value>> {
    let limit = q.limit.clamp(1, 500);
    // (key, durability, ledger, value, label, captured_at, deleted, archived)
    type HistRow = (
        SqlxJson<Value>,
        String,
        i64,
        Option<SqlxJson<Value>>,
        Option<String>,
        DateTime<Utc>,
        bool,
        bool,
    );
    let rows: Vec<HistRow> = sqlx::query_as(
        "SELECT key, durability, ledger, value, label, captured_at, deleted, archived
             FROM contract_data
             WHERE contract_id = $1 AND key_hash = $2
             ORDER BY ledger DESC LIMIT $3",
    )
    .bind(&contract_id)
    .bind(&key_hash)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;

    if rows.is_empty() {
        return Err(ApiError::not_found("no data snapshots for this key"));
    }

    // Key and durability are constant across a key's history; take them once.
    let key = rows[0].0 .0.clone();
    let durability = rows[0].1.clone();
    let label = rows[0].4.clone();
    // The newest row decides whether the key currently exists.
    let deleted = rows[0].6;
    let archived = rows[0].7;
    let versions: Vec<Value> = rows
        .into_iter()
        .map(|(_, _, ledger, value, _, captured_at, is_deleted, is_archived)| {
            // Tombstone rows carry no value; expose them so a caller can see
            // exactly when the entry was deleted or expired (#742).
            json!({
                "ledger": ledger,
                "value": value.map(|v| v.0),
                "deleted": is_deleted,
                "archived": is_archived,
                "captured_at": captured_at,
            })
        })
        .collect();
    Ok(Json(json!({
        "contract_id": contract_id,
        "key_hash": key_hash,
        "key": key,
        "durability": durability,
        "label": label,
        "deleted": deleted,
        "archived": archived,
        "count": versions.len(),
        "versions": versions,
    })))
}

/// `POST /contracts/:id/refresh` — drop the cached interface and re-read the
/// contract's WASM over Soroban RPC now. If the executable changed, the current
/// interface is replaced and a new version is appended to the history.
///
/// The indexer's spec load does the same work, but this crate can't call it,
/// so `record_spec_version` mirrors it. Mounted behind the RPC rate limiter.
pub async fn refresh_contract(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
) -> ApiResult<Json<Value>> {
    state.specs.invalidate(&contract_id);
    state.call_cache.invalidate_contract(&contract_id);

    let (wasm_hash, wasm, ledger) = state
        .rpc
        .get_contract_wasm(&contract_id)
        .await
        .map_err(|e| {
            ApiError::Internal(anyhow::anyhow!(
                "failed to fetch contract WASM from RPC: {e}"
            ))
        })?
        .ok_or_else(|| {
            ApiError::not_found(
                "no deployed WASM for this contract (it may be a Stellar Asset Contract)",
            )
        })?;

    let parsed = ContractSpec::from_wasm(&wasm).ok_or_else(|| {
        ApiError::bad_request("contract has deployed WASM but no valid contractspecv0 section")
    })?;
    let (spec, complete) = (parsed.spec, parsed.complete);
    let spec_section = lumenqraph_core::spec::spec_section_of(&wasm)
        .map(hex::encode)
        .unwrap_or_default();

    sqlx::query(
        "INSERT INTO contract_specs (contract_id, wasm_hash, interface, spec_section, has_events, spec_complete)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (contract_id) DO UPDATE
           SET wasm_hash     = EXCLUDED.wasm_hash,
               interface     = EXCLUDED.interface,
               spec_section  = EXCLUDED.spec_section,
               has_events    = EXCLUDED.has_events,
               spec_complete = EXCLUDED.spec_complete,
               fetched_at    = now()",
    )
    .bind(&contract_id)
    .bind(&wasm_hash)
    .bind(spec.to_interface_json())
    .bind(&spec_section)
    .bind(spec.has_events())
    .bind(complete)
    .execute(&state.pool)
    .await?;

    // The current interface is already stored, so a failed history write is
    // logged rather than failing the refresh.
    if let Err(e) = record_spec_version(
        &state.pool,
        &contract_id,
        &wasm_hash,
        &spec_section,
        &spec,
        complete,
        ledger,
    )
    .await
    {
        tracing::warn!(contract_id = %contract_id, error = %e, "failed to record refreshed spec version");
    }

    Ok(Json(json!({
        "contract_id": contract_id,
        "has_events": spec.has_events(),
        "fetched_at": Utc::now(),
        "interface": spec.to_interface_json(),
    })))
}

/// Append this interface to the contract's version history, unless it matches
/// the newest stored version. The diff is skipped when the new spec is
/// incomplete (#401), so a partial parse can't report removals that never
/// happened.
async fn record_spec_version(
    pool: &sqlx::PgPool,
    contract_id: &str,
    wasm_hash: &str,
    spec_section: &str,
    spec: &ContractSpec,
    complete: bool,
    ledger: i64,
) -> anyhow::Result<()> {
    let previous: Option<(i32, String, String)> = sqlx::query_as(
        "SELECT version, wasm_hash, spec_section FROM contract_spec_versions
         WHERE contract_id = $1 ORDER BY version DESC LIMIT 1",
    )
    .bind(contract_id)
    .fetch_optional(pool)
    .await?;

    let (version, previous_hash, diff) = match previous {
        Some((_, ref prev_hash, _)) if prev_hash == wasm_hash => return Ok(()),
        Some((prev_version, prev_hash, prev_section)) => {
            let diff = if complete {
                diff_against(&prev_section, spec)
            } else {
                None
            };
            (prev_version + 1, Some(prev_hash), diff)
        }
        None => (1, None, None),
    };

    sqlx::query(
        "INSERT INTO contract_spec_versions
            (contract_id, version, wasm_hash, previous_wasm_hash, interface, spec_section, diff, breaking, ledger, spec_complete)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (contract_id, version) DO NOTHING",
    )
    .bind(contract_id)
    .bind(version)
    .bind(wasm_hash)
    .bind(previous_hash)
    .bind(spec.to_interface_json())
    .bind(spec_section)
    .bind(diff.as_ref().map(|d| d.to_json()))
    .bind(diff.as_ref().is_some_and(|d| d.breaking))
    .bind(ledger)
    .bind(complete)
    .execute(pool)
    .await?;
    Ok(())
}

/// Diff against the stored raw section. `None` when that section can't be
/// re-parsed: "upgraded, diff unavailable" is more honest than diffing against
/// an empty interface.
fn diff_against(previous_section: &str, new_spec: &ContractSpec) -> Option<SpecDiff> {
    let bytes = hex::decode(previous_section).ok()?;
    let previous = ContractSpec::from_spec_xdr(&bytes)?.spec;
    Some(SpecDiff::between(&previous, new_spec))
}

#[cfg(test)]
mod tests {
    use super::parse_history_cursor;

    // -----------------------------------------------------------------------
    // History cursor parsing (#253)
    // -----------------------------------------------------------------------

    #[test]
    fn history_cursor_valid() {
        let v = parse_history_cursor("version:42").expect("should parse");
        assert_eq!(v, 42);
    }

    #[test]
    fn history_cursor_version_one() {
        let v = parse_history_cursor("version:1").expect("should parse");
        assert_eq!(v, 1);
    }

    #[test]
    fn history_cursor_missing_prefix_rejected() {
        assert!(
            parse_history_cursor("42").is_err(),
            "bare integer without prefix must be rejected"
        );
    }

    #[test]
    fn history_cursor_wrong_prefix_rejected() {
        assert!(
            parse_history_cursor("ledger:42").is_err(),
            "wrong prefix must be rejected"
        );
    }

    #[test]
    fn history_cursor_non_integer_version_rejected() {
        assert!(
            parse_history_cursor("version:abc").is_err(),
            "non-integer version must be rejected"
        );
    }

    #[test]
    fn history_cursor_empty_string_rejected() {
        assert!(
            parse_history_cursor("").is_err(),
            "empty cursor must be rejected"
        );
    }

    // -----------------------------------------------------------------------
    // Diff parameter validation (#211, #444)
    //
    // These call the production `validate_diff_range` that
    // `contract_interface_diff` uses, plus one request through a router, so
    // removing the guard from the handler fails a test.
    // -----------------------------------------------------------------------

    use super::validate_diff_range;

    #[test]
    fn diff_from_greater_than_to_is_rejected() {
        // from=5, to=2 is the canonical bad case from the issue.
        let err = validate_diff_range(5, 2).expect_err("from > to must be rejected");
        assert!(
            matches!(
                err,
                crate::error::ApiError::Status(axum::http::StatusCode::BAD_REQUEST, _, _)
            ),
            "{err:?}"
        );
    }

    #[test]
    fn diff_from_equal_to_to_is_rejected() {
        assert!(
            validate_diff_range(3, 3).is_err(),
            "from == to must be rejected"
        );
    }

    #[test]
    fn diff_valid_range_is_accepted() {
        assert!(
            validate_diff_range(1, 2).is_ok(),
            "from=1, to=2 is a valid range"
        );
        assert!(
            validate_diff_range(1, 5).is_ok(),
            "from=1, to=5 is a valid range"
        );
    }

    #[test]
    fn diff_from_below_one_is_rejected() {
        assert!(
            validate_diff_range(0, 1).is_err(),
            "from=0 is not a valid version"
        );
    }

    mod http {
        use std::sync::atomic::AtomicU64;
        use std::sync::Arc;

        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use axum::routing::get;
        use axum::Router;
        use serde_json::Value;
        use tower::ServiceExt;

        use super::super::contract_interface_diff;
        use crate::call_cache::CallCache;
        use crate::concurrency_limit::ConcurrencyLimiter;
        use crate::rate_limit::RateLimiter;
        use crate::read_cost_limit::ReadCostLimitConfig;
        use crate::rpc::RpcClient;
        use crate::specs::SpecCache;
        use crate::state::AppState;

        const CONTRACT: &str = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";

        /// `AppState` over a lazy pool that never connects: the explicit
        /// `from > to` guard must fire before any database access.
        fn make_state() -> AppState {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://test:test@localhost:5432/test")
                .unwrap();
            AppState {
                pool,
                event_fanout: Arc::new(crate::routes::stream::EventFanout::new()),
                require_auth: false,
                anon_rate_limit: 1_000_000,
                limiter: Arc::new(RateLimiter::memory()),
                http_requests: Arc::new(AtomicU64::new(0)),
                rpc: RpcClient::new("http://127.0.0.1:0", 30),
                specs: Arc::new(SpecCache::new()),
                mounts: Arc::new(vec![]),
                rpc_limiter: Arc::new(RateLimiter::memory()),
                rpc_require_auth: false,
                rpc_anon_rate_limit: 1_000_000,
                metrics: Arc::new(crate::metrics_middleware::MetricsCollector::new()),
                call_cache: Arc::new(CallCache::new(100, 5)),
                build_info: Arc::new(crate::state::BuildInfo {
                    version: "test".to_string(),
                    commit: "test".to_string(),
                    build_time: "test".to_string(),
                }),
                concurrency_limiter: Arc::new(ConcurrencyLimiter::new()),
                max_concurrent_per_ip: 100,
                read_cost_limit_config: ReadCostLimitConfig::default(),
                readyz_lag_threshold: 100,
                readyz_max_age_secs: 120,
                health_max_lag_ledgers: 100,
                health_max_stale_secs: 120,
                metrics_require_auth: false,
                proxy_limiter: Arc::new(RateLimiter::memory()),
                webhook_limiter: Arc::new(RateLimiter::memory()),
                config: Arc::new(crate::config::ApiConfig::test_default()),
                key_cache: Arc::new(crate::key_cache::KeyCache::new(
                    256,
                    std::time::Duration::from_secs(30),
                    std::time::Duration::from_secs(5),
                )),
                ip_config: crate::auth::IpConfig {
                    trusted_proxy_hops: 0,
                    platform_header: None,
                },
                audit_tx: None,
                audit_dropped: Arc::new(AtomicU64::new(0)),
                shutdown: tokio_util::sync::CancellationToken::new(),
            }
        }

        #[tokio::test]
        async fn diff_from_greater_than_to_returns_400_through_router() {
            let app = Router::new()
                .route(
                    "/contracts/:contract_id/interface/diff",
                    get(contract_interface_diff),
                )
                .with_state(make_state());

            let req = Request::builder()
                .method("GET")
                .uri(format!("/contracts/{CONTRACT}/interface/diff?from=5&to=2"))
                .body(Body::empty())
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

            let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["code"], "bad_request");
            assert!(
                body["error"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("must be less than"),
                "unexpected body: {body}"
            );
        }
    }
}
