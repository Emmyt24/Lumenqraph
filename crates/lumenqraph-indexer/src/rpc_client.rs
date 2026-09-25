//! Thin JSON-RPC client for the Soroban RPC `getEvents` / `getLatestLedger`
//! methods. Only the fields we use are modeled.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context};
use rand::Rng;
use serde::{Deserialize, Serialize};
use stellar_xdr::curr::{
    ContractDataDurability, ContractExecutable, LedgerEntryData, LedgerKey, LedgerKeyContractCode,
    LedgerKeyContractData, Limits, ReadXdr, ScAddress, ScVal, WriteXdr,
};

pub struct RpcClient {
    http: reqwest::Client,
    url: String,
    /// Track RPC call metrics for observability
    pub calls_made: std::sync::atomic::AtomicU64,
    pub calls_failed: std::sync::atomic::AtomicU64,
    pub calls_failed_32001: std::sync::atomic::AtomicU64,
}

/// Identifies the sending binary's actual release to RPC operators who key off
/// `User-Agent` for debugging, so request logs can be traced back to the
/// version that sent them.
const USER_AGENT: &str = concat!("lumenqraph-indexer/", env!("CARGO_PKG_VERSION"));

#[derive(Serialize)]
struct RpcRequest<'a, P> {
    jsonrpc: &'a str,
    id: u32,
    method: &'a str,
    params: P,
}

#[derive(Deserialize)]
struct RpcResponse<R> {
    result: Option<R>,
    error: Option<RpcError>,
}

#[derive(Deserialize, Debug)]
struct RpcError {
    code: i64,
    message: String,
}

/// Soroban RPC / JSON-RPC error codes distinguishing retryable (transient)
/// from non-retryable (permanent) errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SorobanRpcErrorCode {
    /// -32700: Parse error (invalid JSON received by the server).
    ParseError,
    /// -32600: Invalid Request (the JSON sent is not a valid Request object).
    InvalidRequest,
    /// -32601: Method not found (the method does not exist / is not available).
    MethodNotFound,
    /// -32602: Invalid params (invalid method parameter(s), e.g. invalid contract ID).
    InvalidParams,
    /// -32603: Internal JSON-RPC error.
    InternalError,
    /// -32000: Generic server error.
    ServerError,
    /// -32001: Soroban RPC resource/processing limit exceeded.
    ProcessingLimitExceeded,
    /// Any other RPC error code.
    Other(i64),
}

impl SorobanRpcErrorCode {
    pub fn from_code(code: i64) -> Self {
        match code {
            -32700 => Self::ParseError,
            -32600 => Self::InvalidRequest,
            -32601 => Self::MethodNotFound,
            -32602 => Self::InvalidParams,
            -32603 => Self::InternalError,
            -32000 => Self::ServerError,
            -32001 => Self::ProcessingLimitExceeded,
            other => Self::Other(other),
        }
    }

    pub fn code(&self) -> i64 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::InternalError => -32603,
            Self::ServerError => -32000,
            Self::ProcessingLimitExceeded => -32001,
            Self::Other(c) => *c,
        }
    }

    /// Whether this error represents a transient, server-side condition that is safe to retry.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::ProcessingLimitExceeded | Self::InternalError | Self::ServerError
        )
    }

    /// Whether this error represents a permanent, client-side condition (e.g. invalid params, method not found).
    pub fn is_permanent(&self) -> bool {
        !self.is_retryable()
    }
}

/// Represents a classified error returned by Soroban RPC.
#[derive(Debug, Clone)]
pub struct SorobanRpcError {
    pub code: i64,
    pub message: String,
    pub method: String,
    pub error_code: SorobanRpcErrorCode,
}

impl SorobanRpcError {
    pub fn new(code: i64, message: impl Into<String>, method: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            method: method.into(),
            error_code: SorobanRpcErrorCode::from_code(code),
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.error_code.is_retryable()
    }

    pub fn is_permanent(&self) -> bool {
        self.error_code.is_permanent()
    }
}

impl std::fmt::Display for SorobanRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rpc {} error {}: {}",
            self.method, self.code, self.message
        )
    }
}

impl std::error::Error for SorobanRpcError {}

/// Returns true if the given error contains a non-retryable Soroban RPC error.
pub fn is_non_retryable_rpc_error(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(e) = cause.downcast_ref::<SorobanRpcError>() {
            return e.is_permanent();
        }
    }
    false
}

// ---- getLatestLedger ----

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LatestLedgerResult {
    sequence: i64,
}

// ---- getEvents ----

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetEventsParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    start_ledger: Option<i64>,
    filters: Vec<EventFilter>,
    pagination: Pagination,
    xdr_format: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventFilter {
    #[serde(rename = "type")]
    filter_type: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    contract_ids: Vec<String>,
}

#[derive(Serialize)]
struct Pagination {
    limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

/// getEvents allows at most this many contract IDs in one filter…
const MAX_IDS_PER_FILTER: usize = 5;
/// …and at most this many filters in one request.
const MAX_FILTERS: usize = 5;

/// Spread the watchlist across getEvents filters (the filters are OR'd by the
/// RPC). An empty watchlist stays a single bare `contract` filter, which
/// matches every contract's events.
fn event_filters(contract_ids: &[String]) -> anyhow::Result<Vec<EventFilter>> {
    if contract_ids.len() > MAX_IDS_PER_FILTER * MAX_FILTERS {
        return Err(anyhow!(
            "getEvents supports at most {} contract IDs ({} filters x {} IDs); got {}",
            MAX_IDS_PER_FILTER * MAX_FILTERS,
            MAX_FILTERS,
            MAX_IDS_PER_FILTER,
            contract_ids.len()
        ));
    }
    if contract_ids.is_empty() {
        return Ok(vec![EventFilter {
            filter_type: "contract",
            contract_ids: vec![],
        }]);
    }
    Ok(contract_ids
        .chunks(MAX_IDS_PER_FILTER)
        .map(|chunk| EventFilter {
            filter_type: "contract",
            contract_ids: chunk.to_vec(),
        })
        .collect())
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetEventsResult {
    /// Present in the RPC response; the poller reads the tip from
    /// getLatestLedger instead, so this is retained only for completeness.
    #[allow(dead_code)]
    pub latest_ledger: i64,
    pub events: Vec<EventInfo>,
    pub cursor: Option<String>,
}

/// One event as returned by RPC. `topic` and `value` are base64 XDR.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventInfo {
    #[serde(rename = "type")]
    pub event_type: String,
    pub ledger: i64,
    pub ledger_closed_at: String,
    pub contract_id: String,
    pub id: String,
    /// Newer RPC responses omit `pagingToken` (the unique `id` serves the same
    /// role), so treat it as optional.
    #[serde(default)]
    pub paging_token: String,
    #[serde(default)]
    pub in_successful_contract_call: bool,
    #[serde(default)]
    pub tx_hash: String,
    #[serde(default)]
    pub topic: Vec<String>,
    #[serde(default)]
    pub value: String,
}

impl RpcClient {
    pub fn new(url: impl Into<String>, timeout_secs: u64) -> Self {
        // A request timeout is essential for a 24/7 poller: without it, a hung
        // RPC connection blocks the poll loop indefinitely and the backoff path
        // (which only fires on an error) is never reached.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .expect("failed to build HTTP client");
        Self {
            http,
            url: url.into(),
            calls_made: std::sync::atomic::AtomicU64::new(0),
            calls_failed: std::sync::atomic::AtomicU64::new(0),
            calls_failed_32001: std::sync::atomic::AtomicU64::new(0),
        }
    }

    async fn call<P: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: P,
    ) -> anyhow::Result<R> {
        let req = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        let body = serde_json::to_vec(&req).with_context(|| format!("rpc {method} serialize"))?;

        // Bounded retry with jittered exponential backoff for transient failures.
        // Attempt 1 → delay ~1s → Attempt 2 → delay ~2s → Attempt 3 (final).
        const MAX_ATTEMPTS: u32 = 3;
        // Base delays in millis between successive attempts (before jitter).
        const BASE_DELAYS_MS: [u64; 2] = [1_000, 2_000];

        let mut last_err: Option<anyhow::Error> = None;

        for attempt in 0..MAX_ATTEMPTS {
            // Count each actual HTTP attempt so operators can see retry activity
            // in Prometheus without needing to diff success/error counters.
            self.calls_made.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            if attempt > 0 {
                let base_ms = BASE_DELAYS_MS[(attempt - 1) as usize];
                // ±25% uniform jitter to avoid retry storms from multiple pollers.
                let jitter_ms = rand::thread_rng().gen_range(0..=(base_ms / 2)) as i64
                    - (base_ms / 4) as i64;
                let delay_ms = ((base_ms as i64) + jitter_ms).max(1) as u64;
                tracing::warn!(
                    method,
                    attempt,
                    delay_ms,
                    "transient rpc failure; retrying after delay"
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }

            // Each attempt gets a fresh serialized body clone (reqwest consumes it).
            let result = self
                .http
                .post(&self.url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header(reqwest::header::USER_AGENT, USER_AGENT)
                .body(body.clone())
                .send()
                .await;

            // Classify network-level failures: always retryable.
            let response = match result {
                Err(e) => {
                    last_err = Some(anyhow!(e).context(format!("rpc {method} request failed")));
                    continue; // retry
                }
                Ok(r) => r,
            };

            // Classify HTTP-level failures.
            let response = match response.error_for_status() {
                Err(e) => {
                    let status = e.status();
                    let is_5xx = status.map_or(false, |s| s.is_server_error());
                    let ctx_err = anyhow!(e).context(format!("rpc {method} returned http error"));
                    if is_5xx {
                        last_err = Some(ctx_err);
                        continue; // retry on 5xx
                    }
                    // 4xx and other client errors are permanent — fail fast.
                    self.calls_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Err(ctx_err);
                }
                Ok(r) => r,
            };

            let resp: RpcResponse<R> = match response.json().await {
                Err(e) => {
                    // A JSON decode failure on an otherwise-successful HTTP response
                    // is usually a truncated/garbled body — treat as transient.
                    last_err = Some(anyhow!(e).context(format!("rpc {method} response decode failed")));
                    continue; // retry
                }
                Ok(r) => r,
            };

            if let Some(err) = resp.error {
                self.calls_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let rpc_err = SorobanRpcError::new(err.code, err.message, method);
                if rpc_err.is_retryable() {
                    if err.code == -32001 {
                        // -32001: "processing limit" — the RPC is overloaded.
                        // Transient; retry with backoff.
                        self.calls_failed_32001.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    last_err = Some(anyhow!(rpc_err));
                    continue; // retry
                }
                // Non-retryable error (invalid params, method not found, etc.)
                // is permanent — propagate immediately as anyhow::Error without retrying.
                return Err(anyhow!(rpc_err));
            }

            return resp
                .result
                .ok_or_else(|| anyhow!("rpc {method} returned no result"));
        }

        // All attempts exhausted.
        self.calls_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(last_err.unwrap_or_else(|| anyhow!("rpc {method} failed after {MAX_ATTEMPTS} attempts")))
    }

    /// Current tip ledger sequence.
    pub async fn get_latest_ledger(&self) -> anyhow::Result<i64> {
        let r: LatestLedgerResult = self.call("getLatestLedger", serde_json::json!({})).await?;
        Ok(r.sequence)
    }

    /// Fetch a page of events. Pass `start_ledger` on the first page of a scan,
    /// or `cursor` to continue a previous page (never both).
    ///
    /// RPC caps a getEvents filter at 5 contract IDs and a request at 5
    /// filters, so the watchlist is spread across filters — up to 25 IDs total.
    pub async fn get_events(
        &self,
        start_ledger: Option<i64>,
        contract_ids: &[String],
        cursor: Option<String>,
        limit: u32,
    ) -> anyhow::Result<GetEventsResult> {
        let params = GetEventsParams {
            start_ledger: if cursor.is_some() { None } else { start_ledger },
            filters: event_filters(contract_ids)?,
            pagination: Pagination { limit, cursor },
            xdr_format: "base64",
        };
        self.call("getEvents", params).await
    }

    /// Fetch a contract's deployed WASM (hex hash + bytes), so its on-chain
    /// interface spec can be parsed. Returns `Ok(None)` for contracts with no
    /// WASM (e.g. a Stellar Asset Contract) or when the entries aren't found.
    ///
    /// Two hops, both via `getLedgerEntries`: the contract's *instance* entry
    /// names the WASM hash of its executable; the *code* entry holds the bytes.
    pub async fn get_contract_wasm(
        &self,
        contract_id: &str,
    ) -> anyhow::Result<Option<(String, Vec<u8>)>> {
        let addr = ScAddress::from_str(contract_id)
            .with_context(|| format!("invalid contract id {contract_id}"))?;

        let instance_key = LedgerKey::ContractData(LedgerKeyContractData {
            contract: addr,
            key: ScVal::LedgerKeyContractInstance,
            durability: ContractDataDurability::Persistent,
        });
        let Some((entry, _)) = self.get_ledger_entry(&instance_key).await? else {
            return Ok(None);
        };
        let wasm_hash = match entry {
            LedgerEntryData::ContractData(cd) => match cd.val {
                ScVal::ContractInstance(inst) => match inst.executable {
                    ContractExecutable::Wasm(hash) => hash,
                    ContractExecutable::StellarAsset => return Ok(None),
                },
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        let hash_hex = hex::encode(wasm_hash.0);
        let code_key = LedgerKey::ContractCode(LedgerKeyContractCode { hash: wasm_hash });
        let Some((entry, _)) = self.get_ledger_entry(&code_key).await? else {
            return Ok(None);
        };
        match entry {
            LedgerEntryData::ContractCode(cc) => Ok(Some((hash_hex, cc.code.into()))),
            _ => Ok(None),
        }
    }

    /// Fetch a contract's *instance* ledger entry: its current executable hash
    /// (`None` for a Stellar Asset Contract), its instance storage as a single
    /// `ScVal::Map`, and the ledger at which the instance last changed. Used for
    /// state snapshots and upgrade detection. `Ok(None)` if the contract's
    /// instance entry isn't found.
    pub async fn get_contract_instance(
        &self,
        contract_id: &str,
    ) -> anyhow::Result<Option<InstanceEntry>> {
        let addr = ScAddress::from_str(contract_id)
            .with_context(|| format!("invalid contract id {contract_id}"))?;
        let key = LedgerKey::ContractData(LedgerKeyContractData {
            contract: addr,
            key: ScVal::LedgerKeyContractInstance,
            durability: ContractDataDurability::Persistent,
        });
        let Some((data, last_modified_ledger)) = self.get_ledger_entry(&key).await? else {
            return Ok(None);
        };
        let LedgerEntryData::ContractData(cd) = data else {
            return Ok(None);
        };
        let ScVal::ContractInstance(inst) = cd.val else {
            return Ok(None);
        };
        let wasm_hash = match inst.executable {
            ContractExecutable::Wasm(h) => Some(hex::encode(h.0)),
            ContractExecutable::StellarAsset => None,
        };
        Ok(Some(InstanceEntry {
            wasm_hash,
            // The instance storage map (may be empty/None) as one decodable ScVal.
            storage: ScVal::Map(inst.storage),
            last_modified_ledger,
        }))
    }

    /// Fetch a batch of contract instance entries in a single `getLedgerEntries` call.
    /// The returned `Vec` is positionally aligned with `contract_ids`: `None` means the
    /// instance entry was absent. Callers should chunk large slices to avoid RPC limits.
    pub async fn get_contract_instances_batch(
        &self,
        contract_ids: &[String],
    ) -> anyhow::Result<Vec<Option<InstanceEntry>>> {
        if contract_ids.is_empty() {
            return Ok(vec![]);
        }

        // Encode all instance keys.
        let mut keys_with_idx: Vec<(String, usize)> = Vec::with_capacity(contract_ids.len());
        for (idx, contract_id) in contract_ids.iter().enumerate() {
            let addr = ScAddress::from_str(contract_id)
                .with_context(|| format!("invalid contract id {contract_id}"))?;
            let key = LedgerKey::ContractData(LedgerKeyContractData {
                contract: addr,
                key: ScVal::LedgerKeyContractInstance,
                durability: ContractDataDurability::Persistent,
            });
            let key_b64 = key.to_xdr_base64(Limits::none()).context("encode ledger key")?;
            keys_with_idx.push((key_b64, idx));
        }

        // Extract just the keys for the RPC call.
        let keys: Vec<String> = keys_with_idx.iter().map(|(k, _)| k.clone()).collect();

        let result: LedgerEntriesResult = self
            .call("getLedgerEntries", serde_json::json!({ "keys": keys }))
            .await?;

        let mut output = vec![None; contract_ids.len()];
        for item in result.entries.unwrap_or_default() {
            let data = match LedgerEntryData::from_xdr_base64(&item.xdr, Limits::none()) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let LedgerEntryData::ContractData(cd) = data else {
                continue;
            };
            let ScVal::ContractInstance(inst) = cd.val else {
                continue;
            };

            // Determine which contract this is by reconstructing the key.
            let entry_key = LedgerKey::ContractData(LedgerKeyContractData {
                contract: cd.contract.clone(),
                key: cd.key,
                durability: cd.durability,
            });
            if let Ok(key_b64) = entry_key.to_xdr_base64(Limits::none()) {
                if let Some((_, idx)) = keys_with_idx.iter().find(|(k, _)| k == &key_b64) {
                    let wasm_hash = match inst.executable {
                        ContractExecutable::Wasm(h) => Some(hex::encode(h.0)),
                        ContractExecutable::StellarAsset => None,
                    };
                    output[*idx] = Some(InstanceEntry {
                        wasm_hash,
                        storage: ScVal::Map(inst.storage),
                        last_modified_ledger: item.last_modified_ledger_seq,
                    });
                }
            }
        }
        Ok(output)
    }

    /// Fetch a single contract-data entry by its exact storage `key` and
    /// `durability`. Unlike instance storage, these per-key entries aren't
    /// enumerable, so the caller must know the key (e.g. a `Balance(Address)`).
    /// Returns the decoded value `ScVal` and the ledger it last changed at, or
    /// `Ok(None)` if no such entry exists (e.g. a holder with a zero balance
    /// whose entry was never written or has expired).
    ///
    /// For bulk fetches use [`get_contract_data_batch`] instead.
    #[allow(dead_code)]
    pub async fn get_contract_data(
        &self,
        contract_id: &str,
        key: &ScVal,
        durability: ContractDataDurability,
    ) -> anyhow::Result<Option<DataEntry>> {
        let addr = ScAddress::from_str(contract_id)
            .with_context(|| format!("invalid contract id {contract_id}"))?;
        let ledger_key = LedgerKey::ContractData(LedgerKeyContractData {
            contract: addr,
            key: key.clone(),
            durability,
        });
        let Some((data, last_modified_ledger)) = self.get_ledger_entry(&ledger_key).await? else {
            return Ok(None);
        };
        let LedgerEntryData::ContractData(cd) = data else {
            return Ok(None);
        };
        Ok(Some(DataEntry {
            val: cd.val,
            last_modified_ledger,
        }))
    }

    /// Reset and return the accumulated RPC metrics from this client instance.
    /// Used by the indexer to report metrics periodically.
    ///
    /// Memory ordering rationale:
    ///   - `fetch_add` on the counter paths uses `Relaxed` because counter
    ///     increments are independent — we only care about the final aggregate,
    ///     not any ordering relative to other memory operations.
    ///   - `swap(0, Acquire)` here ensures that all preceding `Relaxed`
    ///     `fetch_add` operations on *this thread* (and any thread that
    ///     synchronised with this one) are visible before the counters are
    ///     reset. This prevents a stale read where increments that happened
    ///     before the swap are not yet visible to the Prometheus reporter on
    ///     weakly-ordered architectures such as ARM.
    pub fn take_metrics(&self) -> (u64, u64, u64) {
        let calls = self.calls_made.swap(0, std::sync::atomic::Ordering::Acquire);
        let errors = self.calls_failed.swap(0, std::sync::atomic::Ordering::Acquire);
        let errors_32001 = self.calls_failed_32001.swap(0, std::sync::atomic::Ordering::Acquire);
        (calls, errors, errors_32001)
    }

    /// Fetch and XDR-decode a single ledger entry by key, with the ledger it was
    /// last modified at. `None` if absent.
    async fn get_ledger_entry(
        &self,
        key: &LedgerKey,
    ) -> anyhow::Result<Option<(LedgerEntryData, i64)>> {
        let key_b64 = key
            .to_xdr_base64(Limits::none())
            .context("encode ledger key")?;
        let result: LedgerEntriesResult = self
            .call("getLedgerEntries", serde_json::json!({ "keys": [key_b64] }))
            .await?;
        let Some(first) = result.entries.unwrap_or_default().into_iter().next() else {
            return Ok(None);
        };
        let data = LedgerEntryData::from_xdr_base64(&first.xdr, Limits::none())
            .context("decode ledger entry")?;
        Ok(Some((data, first.last_modified_ledger_seq)))
    }

    /// Fetch a batch of contract-data entries in a single `getLedgerEntries` call.
    /// The returned `Vec` is positionally aligned with `keys`: `None` means the
    /// entry was absent (e.g. a zero-balance holder whose entry never existed or
    /// has expired). Callers should chunk large slices to `MAX_BATCH_KEYS`.
    pub async fn get_contract_data_batch(
        &self,
        keys: &[(String, ScVal, ContractDataDurability)],
    ) -> anyhow::Result<Vec<Option<DataEntry>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Encode all input keys and record their index for matching response entries.
        let mut key_b64s: Vec<String> = Vec::with_capacity(keys.len());
        for (contract_id, key, durability) in keys {
            let addr = ScAddress::from_str(contract_id)
                .with_context(|| format!("invalid contract id {contract_id}"))?;
            let lkey = LedgerKey::ContractData(LedgerKeyContractData {
                contract: addr,
                key: key.clone(),
                durability: *durability,
            });
            key_b64s.push(lkey.to_xdr_base64(Limits::none()).context("encode ledger key")?);
        }

        let key_to_idx: std::collections::HashMap<&str, usize> = key_b64s
            .iter()
            .enumerate()
            .map(|(i, k)| (k.as_str(), i))
            .collect();

        let result: LedgerEntriesResult = self
            .call("getLedgerEntries", serde_json::json!({ "keys": key_b64s }))
            .await?;

        let mut output = vec![None; keys.len()];
        for item in result.entries.into_iter().flatten() {
            let data = LedgerEntryData::from_xdr_base64(&item.xdr, Limits::none())
                .context("decode ledger entry")?;
            // Re-derive the LedgerKey from the returned entry so we can match it
            // back to its position in the input slice.
            if let LedgerEntryData::ContractData(ref cd) = data {
                let entry_key = LedgerKey::ContractData(LedgerKeyContractData {
                    contract: cd.contract.clone(),
                    key: cd.key.clone(),
                    durability: cd.durability,
                });
                if let Ok(k) = entry_key.to_xdr_base64(Limits::none()) {
                    if let Some(&idx) = key_to_idx.get(k.as_str()) {
                        if let LedgerEntryData::ContractData(cd) = data {
                            output[idx] = Some(DataEntry {
                                val: cd.val,
                                last_modified_ledger: item.last_modified_ledger_seq,
                            });
                        }
                    }
                }
            }
        }

        Ok(output)
    }
}

/// A contract's instance entry: executable hash, instance storage, and the
/// ledger it last changed at.
#[derive(Clone)]
pub struct InstanceEntry {
    pub wasm_hash: Option<String>,
    pub storage: ScVal,
    pub last_modified_ledger: i64,
}

/// A single contract-data entry: its decoded value and the ledger it last
/// changed at.
#[derive(Clone)]
pub struct DataEntry {
    pub val: ScVal,
    pub last_modified_ledger: i64,
}

#[derive(Deserialize)]
struct LedgerEntriesResult {
    #[serde(default)]
    entries: Option<Vec<LedgerEntryItem>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LedgerEntryItem {
    xdr: String,
    #[serde(default)]
    last_modified_ledger_seq: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("C{i}")).collect()
    }

    #[test]
    fn empty_watchlist_is_one_bare_filter() {
        let filters = event_filters(&[]).unwrap();
        assert_eq!(filters.len(), 1);
        assert!(filters[0].contract_ids.is_empty());
    }

    #[test]
    fn five_ids_fit_in_one_filter() {
        let filters = event_filters(&ids(5)).unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].contract_ids.len(), 5);
    }

    #[test]
    fn seven_ids_split_across_two_filters() {
        // The regression that stalled the live indexer: >5 IDs in one filter
        // is rejected by RPC with -32602.
        let filters = event_filters(&ids(7)).unwrap();
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].contract_ids.len(), 5);
        assert_eq!(filters[1].contract_ids.len(), 2);
    }

    #[test]
    fn twenty_five_ids_are_the_ceiling() {
        let filters = event_filters(&ids(25)).unwrap();
        assert_eq!(filters.len(), 5);
        assert!(filters.iter().all(|f| f.contract_ids.len() == 5));

        let err = event_filters(&ids(26)).err().unwrap().to_string();
        assert!(err.contains("at most 25"), "unexpected error: {err}");
    }

    // ── Retry logic tests ─────────────────────────────────────────────────
    //
    // Spin up a tiny axum server that counts requests and returns a controlled
    // sequence of responses; verify the RpcClient retries on transient errors
    // and surfaces them correctly when all attempts are exhausted.

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use axum::extract::State;
    use axum::routing::post;
    use axum::Router;
    use tokio::net::TcpListener;

    // ── helpers ───────────────────────────────────────────────────────────

    /// Spin up a mock server whose handler decides what to return based on
    /// `attempt_count` (incremented each request). Returns the server URL.
    async fn spawn_counting_mock(
        count: Arc<AtomicUsize>,
        make_response: Arc<dyn Fn(usize) -> (u16, &'static str) + Send + Sync + 'static>,
    ) -> String {
        #[derive(Clone)]
        struct S {
            count: Arc<AtomicUsize>,
            responder: Arc<dyn Fn(usize) -> (u16, &'static str) + Send + Sync + 'static>,
        }
        async fn handler(State(s): State<S>) -> axum::response::Response {
            let n = s.count.fetch_add(1, Ordering::SeqCst);
            let (status, body) = (s.responder)(n);
            axum::response::Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap()
        }
        let state = S { count, responder: make_response };
        let app = Router::new().route("/", post(handler)).with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    // Success response body for getLatestLedger.
    const OK_SEQ_1000: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"sequence":1000}}"#;
    const OK_SEQ_2000: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"sequence":2000}}"#;
    const ERR_32001: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"processing limit reached"}}"#;
    const ERR_32602: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}"#;

    #[tokio::test]
    async fn retries_on_5xx_and_eventually_succeeds() {
        // One 500 then a 200 — should succeed on the second attempt.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|n| if n < 1 { (500, "") } else { (200, OK_SEQ_1000) }),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let seq: i64 = client
            .get_latest_ledger()
            .await
            .expect("should succeed after retry");
        assert_eq!(seq, 1000);
        assert_eq!(count.load(Ordering::SeqCst), 2, "must have made exactly 2 attempts");
    }

    #[tokio::test]
    async fn exhausts_all_attempts_on_persistent_5xx() {
        // Always 500 — all MAX_ATTEMPTS (3) should be exhausted.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|_| (500, "")),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_latest_ledger()
            .await
            .expect_err("should fail after all attempts exhausted");
        assert!(
            err.to_string().contains("http error"),
            "unexpected error message: {err}"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            3,
            "must attempt exactly MAX_ATTEMPTS=3 times"
        );
    }

    #[tokio::test]
    async fn does_not_retry_on_4xx() {
        // HTTP 400 is a client error — should fail immediately without retrying.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|_| (400, "")),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_latest_ledger()
            .await
            .expect_err("4xx should fail immediately");
        assert!(
            err.to_string().contains("http error"),
            "unexpected error message: {err}"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1, "must not retry on 4xx");
    }

    #[tokio::test]
    async fn does_not_retry_on_non_transient_rpc_error() {
        // RPC error -32602 (bad params) should surface immediately without retrying.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|_| (200, ERR_32602)),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_latest_ledger()
            .await
            .expect_err("non-transient rpc error should fail immediately");
        assert!(
            err.to_string().contains("-32602"),
            "unexpected error message: {err}"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "must not retry on non-transient rpc error codes"
        );
        assert!(is_non_retryable_rpc_error(&err));
    }

    #[tokio::test]
    async fn does_not_retry_on_method_not_found_rpc_error() {
        // RPC error -32601 (method not found) should surface immediately without retrying.
        const ERR_32601: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#;
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|_| (200, ERR_32601)),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_latest_ledger()
            .await
            .expect_err("method not found rpc error should fail immediately");
        assert!(
            err.to_string().contains("-32601"),
            "unexpected error message: {err}"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "must not retry on method not found"
        );
        assert!(is_non_retryable_rpc_error(&err));
    }

    #[test]
    fn soroban_rpc_error_code_classification() {
        assert!(SorobanRpcErrorCode::ProcessingLimitExceeded.is_retryable());
        assert!(SorobanRpcErrorCode::InternalError.is_retryable());
        assert!(SorobanRpcErrorCode::ServerError.is_retryable());

        assert!(SorobanRpcErrorCode::InvalidParams.is_permanent());
        assert!(SorobanRpcErrorCode::MethodNotFound.is_permanent());
        assert!(SorobanRpcErrorCode::InvalidRequest.is_permanent());
        assert!(SorobanRpcErrorCode::ParseError.is_permanent());
        assert!(SorobanRpcErrorCode::Other(-99999).is_permanent());

        assert!(!SorobanRpcErrorCode::InvalidParams.is_retryable());
        assert!(!SorobanRpcErrorCode::MethodNotFound.is_retryable());
    }

    #[tokio::test]
    async fn retries_on_rpc_32001_processing_limit() {
        // -32001 twice then success — should succeed on the third attempt.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|n| if n < 2 { (200, ERR_32001) } else { (200, OK_SEQ_2000) }),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let seq: i64 = client
            .get_latest_ledger()
            .await
            .expect("should succeed on third attempt after two -32001 errors");
        assert_eq!(seq, 2000);
        assert_eq!(count.load(Ordering::SeqCst), 3, "must have made exactly 3 attempts");
    }
}