//! Contract state indexing: versioned snapshots of a contract's instance
//! storage.
//!
//! Events tell you what *happened*; this tells you what a contract currently
//! *holds*. We read the contract's instance ledger entry (the same one used to
//! find its WASM), decode its storage map to JSON, and store a new row whenever
//! the instance has changed since our last snapshot — so `contract_state`
//! becomes a time series: the newest row is current state, older rows are
//! history.
//!
//! Reading the instance also gives us the contract's current WASM hash, which we
//! feed back to the [`SpecCache`] so an upgraded contract's interface is
//! refreshed.

use lumenqraph_core::xdr;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use stellar_xdr::curr::{ContractDataDurability, Limits, ScVal, WriteXdr};
use tracing::{debug, warn};

use crate::rpc_client::{DataEntry, RpcClient};
use crate::specs::SpecCache;

/// Max keys per `getLedgerEntries` RPC call when batching holder balance snapshots.
const MAX_BATCH_KEYS: usize = 100;

/// Snapshot a batch of contract instances. Contract IDs are chunked into
/// `getLedgerEntries` calls, and each chunk's change detection and writes are
/// done with a single grouped `SELECT max(ledger)` plus one multi-row
/// `INSERT … ON CONFLICT DO NOTHING` (no per-contract round-trips). State
/// indexing and upgrade detection both use the instance entry, so this bundles
/// both. Best-effort: errors are logged, never propagated to the poller.
pub async fn snapshot_instances_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    specs: &SpecCache,
    contract_ids: &[String],
) {
    if contract_ids.is_empty() {
        return;
    }

    // Chunk contract IDs for RPC calls (avoid sending too many at once).
    const MAX_BATCH_INSTANCES: usize = 50;
    for chunk in contract_ids.chunks(MAX_BATCH_INSTANCES) {
        if let Err(e) = try_snapshot_instances_batch(pool, rpc, specs, chunk).await {
            warn!(error = %e, "batch instance snapshot failed");
        }
    }
}

async fn try_snapshot_instances_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    specs: &SpecCache,
    contract_ids: &[String],
) -> anyhow::Result<()> {
    let instances = rpc.get_contract_instances_batch(contract_ids).await?;

    // Reading the instance revealed the current executable — detect upgrades.
    for (contract_id, instance_opt) in contract_ids.iter().zip(instances.iter()) {
        if let Some(instance) = instance_opt {
            if let Some(hash) = &instance.wasm_hash {
                specs
                    .note_wasm_hash(pool, rpc, contract_id, hash, instance.last_modified_ledger)
                    .await;
            }
        }
    }

    // One grouped query fetches the latest ledger for every contract in the chunk.
    let latest_rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT contract_id, max(ledger) FROM contract_state
         WHERE contract_id = ANY($1)
         GROUP BY contract_id",
    )
    .bind(contract_ids)
    .fetch_all(pool)
    .await?;
    let latest: std::collections::HashMap<String, i64> = latest_rows.into_iter().collect();

    // Build the changed rows. The instance's lastModifiedLedgerSeq only advances
    // when the instance (incl. its storage) actually changes.
    let mut changed_ids: Vec<String> = Vec::new();
    let mut changed_ledgers: Vec<i64> = Vec::new();
    let mut changed_storage: Vec<serde_json::Value> = Vec::new();
    for (contract_id, instance_opt) in contract_ids.iter().zip(instances.iter()) {
        let Some(instance) = instance_opt else {
            continue;
        };
        if latest.get(contract_id) == Some(&instance.last_modified_ledger) {
            continue;
        }
        changed_ids.push(contract_id.clone());
        changed_ledgers.push(instance.last_modified_ledger);
        changed_storage.push(decode_storage(&instance.storage));
    }

    if changed_ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO contract_state (contract_id, ledger, storage)
         SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::jsonb[])
         ON CONFLICT (contract_id, ledger) DO NOTHING",
    )
    .bind(&changed_ids)
    .bind(&changed_ledgers)
    .bind(&changed_storage)
    .execute(pool)
    .await?;
    debug!(
        count = changed_ids.len(),
        "state snapshots recorded"
    );
    Ok(())
}

/// Snapshot a single contract-data entry (one key/value pair, e.g. a holder's
/// `Balance(Address)`) if it has changed since the last snapshot. Best-effort:
/// errors are logged, never propagated to the poller. `label` is an optional
/// grouping tag stored alongside the row (e.g. `"balance"`).
///
/// For bulk per-holder snapshots use [`snapshot_balances_batch`] instead.
#[allow(dead_code)]
pub async fn snapshot_data(
    pool: &PgPool,
    rpc: &RpcClient,
    contract_id: &str,
    key: &ScVal,
    durability: ContractDataDurability,
    label: Option<&str>,
) {
    if let Err(e) = try_snapshot_data(pool, rpc, contract_id, key, durability, label).await {
        warn!(contract_id, error = %e, "contract-data snapshot failed");
    }
}

async fn try_snapshot_data(
    pool: &PgPool,
    rpc: &RpcClient,
    contract_id: &str,
    key: &ScVal,
    durability: ContractDataDurability,
    label: Option<&str>,
) -> anyhow::Result<()> {
    let entry_opt = rpc.get_contract_data(contract_id, key, durability).await?;
    let key_xdr = key.to_xdr_base64(Limits::none())?;
    let key_hash = hex::encode(Sha256::digest(key_xdr.as_bytes()));
    match entry_opt {
        Some(entry) => {
            try_write_snapshot_data(pool, contract_id, key, durability, label, &entry).await
        }
        None => {
            // No entry (e.g. a holder whose balance was never written / has expired).
            // If we have history for this key, record a tombstone so `/data` stops
            // reporting the stale value.
            write_tombstone_if_needed(pool, contract_id, &key_hash, durability, label).await
        }
    }
}

/// Snapshot per-holder balances discovered during a cycle in a small number of
/// batched `getLedgerEntries` calls instead of one call per holder.
///
/// Keys are deduplicated by `(contract_id, key_hash)` and chunked to
/// `MAX_BATCH_KEYS` entries per RPC call. Per chunk, change detection is one
/// grouped `SELECT max(ledger)` and writes are one multi-row `UNNEST` insert.
/// Keys that return no entry but have history get a tombstone row.
pub async fn snapshot_balances_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    holders_by_contract: &std::collections::HashMap<String, std::collections::HashSet<String>>,
    contract_ids_filter: &[String],
    balance_key_symbol: &str,
    durability: ContractDataDurability,
) {
    use crate::keys;

    // Build the flat list of (contract_id, key, durability, label) to fetch,
    // deduplicated by (contract_id, key_hash).
    let mut batch: Vec<(String, ScVal, ContractDataDurability, &str)> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for (contract_id, holders) in holders_by_contract {
        if !contract_ids_filter.is_empty() && !contract_ids_filter.contains(contract_id) {
            continue;
        }
        for holder in holders {
            match keys::balance_key(balance_key_symbol, holder) {
                Ok(key) => {
                    let key_hash = match key_hash_of(&key) {
                        Ok(h) => h,
                        Err(e) => {
                            debug!(holder, error = %e, "skipping unhashable balance key");
                            continue;
                        }
                    };
                    if seen.insert((contract_id.clone(), key_hash)) {
                        batch.push((contract_id.clone(), key, durability, "balance"));
                    }
                }
                Err(e) => debug!(holder, error = %e, "skipping unbuildable balance key"),
            }
        }
    }

    if batch.is_empty() {
        return;
    }

    debug!(count = batch.len(), "batching holder balance key snapshots");

    for chunk in batch.chunks(MAX_BATCH_KEYS) {
        if let Err(e) = snapshot_data_chunk(pool, rpc, chunk).await {
            warn!(error = %e, "batch contract-data snapshot failed");
        }
    }
}

/// Snapshot per-key entries from configurable templates discovered during a cycle.
/// Keys are deduplicated by `(contract_id, key_hash)` and chunked to
/// `MAX_BATCH_KEYS` entries per RPC call. Per chunk, change detection is one
/// grouped `SELECT max(ledger)` and writes are one multi-row `UNNEST` insert.
/// Keys that return no entry but have history get a tombstone row.
pub async fn snapshot_template_keys_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    template_keys_by_contract: &std::collections::HashMap<
        String,
        std::collections::HashSet<(usize, Vec<String>)>,
    >,
    contract_ids_filter: &[String],
    templates: &[crate::keys::KeyTemplate],
) {
    // Build the flat list of (contract_id, key, durability, label) to fetch,
    // deduplicated by (contract_id, key_hash).
    let mut batch: Vec<(String, ScVal, ContractDataDurability, Option<String>)> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for (contract_id, entries) in template_keys_by_contract {
        if !contract_ids_filter.is_empty() && !contract_ids_filter.contains(contract_id) {
            continue;
        }
        for (template_idx, params) in entries {
            if let Some(template) = templates.get(*template_idx) {
                match template.build_key(params) {
                    Ok(key) => {
                        let key_hash = match key_hash_of(&key) {
                            Ok(h) => h,
                            Err(e) => {
                                debug!(error = %e, "skipping unhashable template key");
                                continue;
                            }
                        };
                        if seen.insert((contract_id.clone(), key_hash)) {
                            batch.push((
                                contract_id.clone(),
                                key,
                                template.durability,
                                template.label.clone(),
                            ));
                        }
                    }
                    Err(e) => debug!(error = %e, "skipping unbuildable template key"),
                }
            }
        }
    }

    if batch.is_empty() {
        return;
    }

    debug!(count = batch.len(), "batching template key snapshots");

    for chunk in batch.chunks(MAX_BATCH_KEYS) {
        if let Err(e) = snapshot_data_chunk(pool, rpc, chunk).await {
            warn!(error = %e, "batch contract-data snapshot failed");
        }
    }
}

/// Fetch one chunk of contract-data keys via a single `getLedgerEntries` call,
/// then do change detection with one grouped query and write all changed rows
/// (including tombstones for keys that vanished) with one multi-row insert.
async fn snapshot_data_chunk(
    pool: &PgPool,
    rpc: &RpcClient,
    chunk: &[(String, ScVal, ContractDataDurability, Option<String>)],
) -> anyhow::Result<()> {
    let rpc_keys: Vec<(String, ScVal, ContractDataDurability)> = chunk
        .iter()
        .map(|(cid, key, dur, _)| (cid.clone(), key.clone(), *dur))
        .collect();

    let results = rpc.get_contract_data_batch(&rpc_keys).await?;

    // Precompute key hashes and durability strings for the chunk.
    let mut key_hashes: Vec<String> = Vec::with_capacity(chunk.len());
    let mut durability_strs: Vec<&str> = Vec::with_capacity(chunk.len());
    for (_, key, dur, _) in chunk {
        key_hashes.push(key_hash_of(key)?);
        durability_strs.push(durability_str(*dur));
    }

    // One grouped query fetches the latest ledger for every key in the chunk.
    let contract_ids: Vec<String> = chunk.iter().map(|(cid, _, _, _)| cid.clone()).collect();
    let latest_rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT contract_id, key_hash, max(ledger) FROM contract_data
         WHERE contract_id = ANY($1) AND key_hash = ANY($2)
         GROUP BY contract_id, key_hash",
    )
    .bind(&contract_ids)
    .bind(&key_hashes)
    .fetch_all(pool)
    .await?;
    let latest: std::collections::HashMap<(String, String), i64> = latest_rows
        .into_iter()
        .map(|(cid, kh, ledger)| ((cid, kh), ledger))
        .collect();

    // Build the changed rows (live values and tombstones).
    let mut ins_contract: Vec<String> = Vec::new();
    let mut ins_key_hash: Vec<String> = Vec::new();
    let mut ins_key: Vec<serde_json::Value> = Vec::new();
    let mut ins_key_xdr: Vec<String> = Vec::new();
    let mut ins_durability: Vec<String> = Vec::new();
    let mut ins_ledger: Vec<i64> = Vec::new();
    let mut ins_value: Vec<Option<serde_json::Value>> = Vec::new();
    let mut ins_label: Vec<Option<String>> = Vec::new();
    let mut ins_deleted: Vec<bool> = Vec::new();
    let mut ins_archived: Vec<bool> = Vec::new();

    for (idx, ((contract_id, key, dur, label), entry_opt)) in
        chunk.iter().zip(results.iter()).enumerate()
    {
        let key_hash = &key_hashes[idx];
        let latest_ledger = latest.get(&(contract_id.clone(), key_hash.clone())).copied();

        match entry_opt {
            Some(entry) => {
                if latest_ledger == Some(entry.last_modified_ledger) {
                    continue;
                }
                ins_contract.push(contract_id.clone());
                ins_key_hash.push(key_hash.clone());
                ins_key.push(decode_scval(key));
                ins_key_xdr.push(key.to_xdr_base64(Limits::none())?);
                ins_durability.push(durability_strs[idx].to_string());
                ins_ledger.push(entry.last_modified_ledger);
                ins_value.push(Some(decode_scval(&entry.val)));
                ins_label.push(label.clone());
                ins_deleted.push(false);
                ins_archived.push(false);
            }
            None => {
                // No entry returned. Only record a tombstone if we have history
                // for this key; otherwise there is nothing stale to correct.
                if latest_ledger.is_none() {
                    continue;
                }
                let tip = rpc.latest_ledger().await;
                let archived = *dur == ContractDataDurability::Persistent
                    && entry_archived(rpc, contract_id, key, tip).await;
                ins_contract.push(contract_id.clone());
                ins_key_hash.push(key_hash.clone());
                ins_key.push(decode_scval(key));
                ins_key_xdr.push(key.to_xdr_base64(Limits::none())?);
                ins_durability.push(durability_strs[idx].to_string());
                ins_ledger.push(tip);
                ins_value.push(None);
                ins_label.push(label.clone());
                ins_deleted.push(true);
                ins_archived.push(archived);
            }
        }
    }

    if ins_contract.is_empty() {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO contract_data
            (contract_id, key_hash, key, key_xdr, durability, ledger, value, label, deleted, archived)
         SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::jsonb[], $4::text[], $5::text[],
            $6::bigint[], $7::jsonb[], $8::text[], $9::boolean[], $10::boolean[]
         )
         ON CONFLICT (contract_id, key_hash, ledger) DO NOTHING",
    )
    .bind(&ins_contract)
    .bind(&ins_key_hash)
    .bind(&ins_key)
    .bind(&ins_key_xdr)
    .bind(&ins_durability)
    .bind(&ins_ledger)
    .bind(&ins_value)
    .bind(&ins_label)
    .bind(&ins_deleted)
    .bind(&ins_archived)
    .execute(pool)
    .await?;
    debug!(
        count = ins_contract.len(),
        "contract-data snapshots recorded"
    );
    Ok(())
}

/// Compute the hex-encoded SHA-256 of a key's XDR base64 encoding.
fn key_hash_of(key: &ScVal) -> anyhow::Result<String> {
    let key_xdr = key.to_xdr_base64(Limits::none())?;
    Ok(hex::encode(Sha256::digest(key_xdr.as_bytes())))
}

/// Map a durability to its stored string form.
fn durability_str(durability: ContractDataDurability) -> &'static str {
    match durability {
        ContractDataDurability::Persistent => "persistent",
        ContractDataDurability::Temporary => "temporary",
    }
}

/// Best-effort check whether a persistent entry that returned no data is
/// archived (restorable) rather than deleted. Uses the entry's TTL /
/// `liveUntilLedgerSeq` when the RPC provides it; defaults to `false`.
async fn entry_archived(
    rpc: &RpcClient,
    contract_id: &str,
    key: &ScVal,
    tip: i64,
) -> bool {
    match rpc
        .get_contract_data_ttl(contract_id, key, ContractDataDurability::Persistent)
        .await
    {
        Ok(Some(live_until)) => live_until > tip,
        Ok(None) => false,
        Err(e) => {
            debug!(contract_id, error = %e, "TTL lookup failed; assuming deleted");
            false
        }
    }
}

/// Write a tombstone row for a key that has history but returned no entry.
async fn write_tombstone_if_needed(
    pool: &PgPool,
    contract_id: &str,
    key_hash: &str,
    durability: ContractDataDurability,
    label: Option<&str>,
) -> anyhow::Result<()> {
    let latest: Option<i64> = sqlx::query_scalar(
        "SELECT max(ledger) FROM contract_data WHERE contract_id = $1 AND key_hash = $2",
    )
    .bind(contract_id)
    .bind(key_hash)
    .fetch_one(pool)
    .await?;
    let Some(latest_ledger) = latest else {
        return Ok(());
    };

    // Already a tombstone at the latest ledger? Nothing to do.
    let already_deleted: Option<bool> = sqlx::query_scalar(
        "SELECT deleted FROM contract_data
         WHERE contract_id = $1 AND key_hash = $2 AND ledger = $3",
    )
    .bind(contract_id)
    .bind(key_hash)
    .bind(latest_ledger)
    .fetch_optional(pool)
    .await?;
    if already_deleted == Some(true) {
        return Ok(());
    }

    let tip = sqlx::query_scalar::<_, i64>("SELECT max(ledger) FROM contract_data")
        .fetch_optional(pool)
        .await?
        .unwrap_or(latest_ledger);

    sqlx::query(
        "INSERT INTO contract_data
            (contract_id, key_hash, key, key_xdr, durability, ledger, value, label, deleted, archived)
         VALUES ($1, $2, NULL, NULL, $3, $4, NULL, $5, true, false)
         ON CONFLICT (contract_id, key_hash, ledger) DO NOTHING",
    )
    .bind(contract_id)
    .bind(key_hash)
    .bind(durability_str(durability))
    .bind(tip)
    .bind(label)
    .execute(pool)
    .await?;
    debug!(contract_id, ledger = tip, "contract-data tombstone recorded");
    Ok(())
}

/// Write a single contract-data snapshot row after change detection. Shared by
/// the per-entry path (`try_snapshot_data`) and the batch path (`snapshot_balances_batch`).
async fn try_write_snapshot_data(
    pool: &PgPool,
    contract_id: &str,
    key: &ScVal,
    durability: ContractDataDurability,
    label: Option<&str>,
    entry: &DataEntry,
) -> anyhow::Result<()> {
    let key_xdr = key.to_xdr_base64(Limits::none())?;
    let key_hash = hex::encode(Sha256::digest(key_xdr.as_bytes()));

    // Change detection: skip if we already have this key at this ledger.
    let latest: Option<i64> = sqlx::query_scalar(
        "SELECT max(ledger) FROM contract_data WHERE contract_id = $1 AND key_hash = $2",
    )
    .bind(contract_id)
    .bind(&key_hash)
    .fetch_one(pool)
    .await?;
    if latest == Some(entry.last_modified_ledger) {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO contract_data
            (contract_id, key_hash, key, key_xdr, durability, ledger, value, label, deleted, archived)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, false, false)
         ON CONFLICT (contract_id, key_hash, ledger) DO NOTHING",
    )
    .bind(contract_id)
    .bind(&key_hash)
    .bind(decode_scval(key))
    .bind(&key_xdr)
    .bind(durability_str(durability))
    .bind(entry.last_modified_ledger)
    .bind(decode_scval(&entry.val))
    .bind(label)
    .execute(pool)
    .await?;
    debug!(
        contract_id,
        ledger = entry.last_modified_ledger,
        label,
        "contract-data snapshot recorded"
    );
    Ok(())
}

/// Decode an instance-storage `ScVal` to friendly JSON directly, without any
/// base64 round-trip, using the shared decoder so state and events share one
/// JSON shape (symbol-keyed maps become objects, i128 as decimal strings…).
fn decode_storage(storage: &ScVal) -> serde_json::Value {
    decode_scval(storage)
}

/// Decode any `ScVal` to friendly JSON via the shared event decoder.
/// No base64 encoding/decoding is performed — the value is decoded directly
/// from the already-parsed typed enum.
fn decode_scval(v: &ScVal) -> serde_json::Value {
    xdr::decode_scval(v)
}
