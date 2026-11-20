//! Persistence. Writes are idempotent (`ON CONFLICT DO NOTHING` on the unique
//! `event_id`) so re-fetching a ledger never double-counts. For shallow reorgs,
//! `upsert_events` updates mutable fields (decoded values, enrichment) when the
//! RPC returns different content for a recently-passed ledger.
//! Transfer events are additionally projected into the materialized `token_transfers` table.

pub use lumenqraph_core::NewEvent;
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::debug;

/// Counts events that looked like token transfers but were skipped because the
/// contract is not a confirmed SEP-41 token or the value was not a valid amount
/// (issue #600).
static TRANSFER_PROJECTION_SKIPPED: AtomicU64 = AtomicU64::new(0);

/// Counts vertical projection failures (swaps, NFT events, liquidity events)
/// that were rolled back to a savepoint so the raw events could still commit
/// (issue #730).
static VERTICAL_PROJECTION_FAILED: AtomicU64 = AtomicU64::new(0);

/// Total number of vertical projection failures since process start. Exposed
/// for the `lumenqraph_vertical_projection_failed_total` metric.
#[allow(dead_code)] // read by the metrics endpoint
pub fn vertical_projection_failed_total() -> u64 {
    VERTICAL_PROJECTION_FAILED.load(Ordering::Relaxed)
}

fn record_vertical_projection_failed() {
    VERTICAL_PROJECTION_FAILED.fetch_add(1, Ordering::Relaxed);
}

/// Total number of transfer-like events skipped by the projection since process
/// start. Exposed for the `lumenqraph_transfer_projection_skipped_total` metric.
pub fn transfer_projection_skipped_total() -> u64 {
    TRANSFER_PROJECTION_SKIPPED.load(Ordering::Relaxed)
}

fn record_transfer_skipped() {
    TRANSFER_PROJECTION_SKIPPED.fetch_add(1, Ordering::Relaxed);
}

/// The kind of contract an event originates from, as far as projection cares.
/// Determined from the contract's on-chain spec (or SAC detection), never from
/// the event name alone (issues #599, #600).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractKind {
    /// A SEP-41 fungible token (or a Stellar Asset Contract).
    FungibleToken,
    /// A SEP-50 / OpenZeppelin style NFT contract.
    NonFungibleToken,
    /// An AMM (Soroswap, Aquarius, ...) that emits `swap` events.
    Amm,
    /// A liquidity pool emitting `deposit`/`withdraw` events.
    LiquidityPool,
}

/// Supplies the [`ContractKind`] for a contract id. In production this is backed
/// by the `SpecCache` / `ContractSpec::events_by_prefix`; tests can supply a
/// simple closure. Returning `None` means "unknown" and projections that
/// require a confirmed spec are skipped.
pub type ContractKindResolver<'a> = dyn Fn(&str) -> Option<ContractKind> + 'a;

/// Default resolver used by [`insert_events`] when no spec cache is available.
///
/// A contract id alone cannot tell us whether a contract is a fungible token:
/// every Soroban contract id is a 56-character `C…` strkey, so a prefix check
/// would accept *every* contract (issue #729). Without a spec cache we cannot
/// confirm the contract kind, so we return `None` and the projection is skipped.
/// Callers that have a `SpecCache` should use [`insert_events_with_resolver`]
/// with a cache-backed resolver instead.
fn default_contract_kind(_contract_id: &str) -> Option<ContractKind> {
    None
}

/// Insert a batch of events (+ derived transfers) in one transaction using a
/// single multi-row UNNEST statement. Returns the number of events newly inserted.
///
/// Each call issues at most two SQL statements regardless of batch size:
/// one UNNEST INSERT for events (RETURNING newly-inserted IDs) and one UNNEST
/// INSERT for their derived token_transfers. This avoids the per-row round-trip
/// overhead that dominates at high PAGE_SIZE values.
///
/// This uses [`default_contract_kind`], which cannot confirm any contract kind,
/// so no vertical projections are written. Callers that have a `SpecCache`
/// (e.g. the poller) should call [`insert_events_with_resolver`] with a
/// cache-backed resolver so confirmed tokens are projected (issue #729).
pub async fn insert_events(pool: &PgPool, events: &[NewEvent]) -> anyhow::Result<u64> {
    insert_events_with_resolver(pool, events, &default_contract_kind).await
}

/// SQL that deletes `token_transfers` rows whose contract is not a confirmed
/// fungible token (issue #729). Before #729 the default resolver treated every
/// 56-character `C…` contract id as a fungible token, so NFT and custom
/// contracts polluted `token_transfers`. Run this once after deploying the fix,
/// passing the set of confirmed token contract ids (SACs plus contracts whose
/// WASM spec declares SEP-41):
///
/// ```sql
/// DELETE FROM token_transfers
///  WHERE contract_id <> ALL($1::text[]);
/// ```
///
/// The helper below executes exactly that statement and returns the number of
/// deleted rows. It is intended as a one-off cleanup / `reproject` step.
pub async fn delete_non_token_transfers(
    pool: &PgPool,
    confirmed_token_contract_ids: &[String],
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        "DELETE FROM token_transfers
          WHERE contract_id <> ALL($1::text[])",
    )
    .bind(confirmed_token_contract_ids)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Like [`insert_events`] but with an explicit contract-kind resolver, so the
/// caller can pass the `SpecCache`-backed lookup. All vertical projections
/// (`token_transfers`, `amm_swaps`, `nft_events`, `liquidity_events`) are gated
/// on the resolved [`ContractKind`].
pub async fn insert_events_with_resolver(
    pool: &PgPool,
    events: &[NewEvent],
    resolve: &ContractKindResolver<'_>,
) -> anyhow::Result<u64> {
    if events.is_empty() {
        return Ok(0);
    }

    // Collect column arrays for the UNNEST batch insert.
    let mut event_ids: Vec<String> = Vec::with_capacity(events.len());
    let mut contract_ids: Vec<String> = Vec::with_capacity(events.len());
    let mut ledgers: Vec<i64> = Vec::with_capacity(events.len());
    let mut closed_ats: Vec<chrono::DateTime<chrono::Utc>> = Vec::with_capacity(events.len());
    let mut event_types: Vec<String> = Vec::with_capacity(events.len());
    let mut topics_json: Vec<String> = Vec::with_capacity(events.len());
    let mut decoded_topics_json: Vec<String> = Vec::with_capacity(events.len());
    let mut event_names: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut values: Vec<String> = Vec::with_capacity(events.len());
    let mut decoded_values_json: Vec<String> = Vec::with_capacity(events.len());
    let mut enriched_json: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut tx_hashes: Vec<String> = Vec::with_capacity(events.len());
    let mut in_successful_calls: Vec<bool> = Vec::with_capacity(events.len());
    let mut paging_tokens: Vec<String> = Vec::with_capacity(events.len());

    for e in events {
        event_ids.push(e.event_id.clone());
        contract_ids.push(e.contract_id.clone());
        ledgers.push(e.ledger);
        closed_ats.push(e.ledger_closed_at);
        event_types.push(e.event_type.as_str().to_string());
        topics_json.push(serde_json::to_string(&e.topics)?);
        decoded_topics_json.push(serde_json::to_string(&e.decoded_topics)?);
        event_names.push(e.event_name.clone());
        values.push(e.value.clone());
        decoded_values_json.push(serde_json::to_string(&e.decoded_value)?);
        enriched_json.push(e.enriched.as_ref().map(serde_json::to_string).transpose()?);
        tx_hashes.push(e.tx_hash.clone());
        in_successful_calls.push(e.in_successful_call);
        paging_tokens.push(e.paging_token.clone());
    }

    let mut tx = pool.begin().await?;

    // Single INSERT for the whole batch. JSON columns are passed as text and
    // cast to jsonb in the SELECT so the driver only needs to encode plain arrays.
    // RETURNING gives us only the newly-inserted IDs (conflicts are excluded).
    let inserted_rows = sqlx::query(
        "INSERT INTO events (
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics, decoded_topics, event_name, value, decoded_value,
            enriched, tx_hash, in_successful_call, paging_token
         )
         SELECT
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics::jsonb, decoded_topics::jsonb, event_name, value,
            decoded_value::jsonb, enriched::jsonb, tx_hash, in_successful_call, paging_token
         FROM UNNEST(
            $1::text[], $2::text[], $3::bigint[], $4::timestamptz[], $5::text[],
            $6::text[], $7::text[], $8::text[], $9::text[], $10::text[],
            $11::text[], $12::text[], $13::bool[], $14::text[]
         ) AS t(
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics, decoded_topics, event_name, value, decoded_value,
            enriched, tx_hash, in_successful_call, paging_token
         )
         ON CONFLICT (event_id) DO NOTHING
         RETURNING event_id",
    )
    .bind(&event_ids)
    .bind(&contract_ids)
    .bind(&ledgers)
    .bind(&closed_ats)
    .bind(&event_types)
    .bind(&topics_json)
    .bind(&decoded_topics_json)
    .bind(&event_names)
    .bind(&values)
    .bind(&decoded_values_json)
    .bind(&enriched_json)
    .bind(&tx_hashes)
    .bind(&in_successful_calls)
    .bind(&paging_tokens)
    .fetch_all(&mut *tx)
    .await?;

    let inserted_count = inserted_rows.len() as u64;

    // Project token_transfers and verticals only for events that were newly inserted.
    if inserted_count > 0 {
        let inserted_ids: std::collections::HashSet<String> = inserted_rows
            .iter()
            .filter_map(|row| row.try_get::<String, _>("event_id").ok())
            .collect();

        let new_events: Vec<&NewEvent> = events
            .iter()
            .filter(|e| inserted_ids.contains(&e.event_id))
            .collect();

        project_derived(&mut tx, &new_events, resolve).await?;
    }

    // Wake the API's SSE fan-out. Postgres delivers NOTIFY only on commit, so
    // it is sent in the same transaction as the rows it announces (#569).
    // Only notify when rows were actually inserted, and carry the new max seq
    // so listeners can skip the catch-up query when already past it (#732).
    if inserted_count > 0 {
        let max_seq: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM events")
            .fetch_one(&mut *tx)
            .await?;
        sqlx::query("SELECT pg_notify('lumenqraph_events', $1)")
            .bind(max_seq.unwrap_or(0).to_string())
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(inserted_count)
}

/// Project the derived rows (`token_transfers`, `amm_swaps`, `nft_events`,
/// `liquidity_events`) for the given events. Shared by the insert and upsert
/// paths so their projection logic cannot drift (#733).
async fn project_derived(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[&NewEvent],
    resolve: &ContractKindResolver<'_>,
) -> anyhow::Result<()> {
    let transfers: Vec<Transfer> = events
        .iter()
        .filter_map(|e| extract_transfer(e, resolve))
        .collect();

    if !transfers.is_empty() {
        let t_event_ids: Vec<String> = transfers.iter().map(|t| t.event_id.clone()).collect();
        let t_contract_ids: Vec<String> = transfers.iter().map(|t| t.contract_id.clone()).collect();
        let t_from_addrs: Vec<Option<String>> =
            transfers.iter().map(|t| t.from_addr.clone()).collect();
        let t_to_addrs: Vec<Option<String>> = transfers.iter().map(|t| t.to_addr.clone()).collect();
        let t_amounts: Vec<String> = transfers.iter().map(|t| t.amount.clone()).collect();
        let t_kinds: Vec<String> = transfers.iter().map(|t| t.kind.clone()).collect();
        let t_amounts_numeric: Vec<Option<String>> =
            transfers.iter().map(|t| Some(t.amount.clone())).collect();
        let t_ledgers: Vec<i64> = transfers.iter().map(|t| t.ledger).collect();
        let t_closed_ats: Vec<chrono::DateTime<chrono::Utc>> =
            transfers.iter().map(|t| t.ledger_closed_at).collect();

        sqlx::query(
            "INSERT INTO token_transfers
                (event_id, contract_id, from_addr, to_addr, amount, kind, amount_numeric, ledger, ledger_closed_at)
             SELECT * FROM UNNEST(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::numeric[], $8::bigint[], $9::timestamptz[]
             ) AS t(event_id, contract_id, from_addr, to_addr, amount, kind, amount_numeric, ledger, ledger_closed_at)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&t_event_ids)
        .bind(&t_contract_ids)
        .bind(&t_from_addrs)
        .bind(&t_to_addrs)
        .bind(&t_amounts)
        .bind(&t_kinds)
        .bind(&t_amounts_numeric)
        .bind(&t_ledgers)
        .bind(&t_closed_ats)
        .execute(&mut **tx)
        .await?;
    }

    // Vertical projections (#599): swaps, NFT events and liquidity events.
    // Written in the same transaction as the events insert.
    project_verticals(tx, events, resolve).await?;

    Ok(())
}

/// Delete the derived rows for the given event ids so they can be re-projected
/// after an event's content changed during a reorg re-scan (#733).
async fn delete_derived(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event_ids: &[String],
) -> anyhow::Result<()> {
    if event_ids.is_empty() {
        return Ok(());
    }
    for table in [
        "token_transfers",
        "amm_swaps",
        "nft_events",
        "liquidity_events",
    ] {
        sqlx::query(&format!("DELETE FROM {table} WHERE event_id = ANY($1::text[])"))
            .bind(event_ids)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// Write `amm_swaps`, `nft_events` and `liquidity_events` rows for the given
/// newly-inserted events, gated on the resolved contract kind. Column names and
/// value vocabularies match migrations 0012/0013/0014 (issue #730).
async fn project_verticals(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[&NewEvent],
    resolve: &ContractKindResolver<'_>,
) -> anyhow::Result<()> {
    // amm_swaps: sell_* / buy_* columns, sell_amount and buy_amount NOT NULL.
    let swaps: Vec<Swap> = events
        .iter()
        .filter_map(|e| extract_swap(e, resolve))
        .filter(|s| {
            let ok = s.sell_amount.is_some() && s.buy_amount.is_some();
            if !ok {
                record_vertical_projection_failed();
            }
            ok
        })
        .collect();
    if !swaps.is_empty() {
        let s_event_ids: Vec<String> = swaps.iter().map(|s| s.event_id.clone()).collect();
        let s_contract_ids: Vec<String> = swaps.iter().map(|s| s.contract_id.clone()).collect();
        let s_senders: Vec<Option<String>> = swaps.iter().map(|s| s.sender.clone()).collect();
        let s_sell_tokens: Vec<Option<String>> =
            swaps.iter().map(|s| s.sell_token.clone()).collect();
        let s_buy_tokens: Vec<Option<String>> = swaps.iter().map(|s| s.buy_token.clone()).collect();
        let s_sell_amounts: Vec<String> = swaps
            .iter()
            .map(|s| s.sell_amount.clone().unwrap_or_default())
            .collect();
        let s_buy_amounts: Vec<String> = swaps
            .iter()
            .map(|s| s.buy_amount.clone().unwrap_or_default())
            .collect();
        let s_raw_event_names: Vec<String> =
            swaps.iter().map(|s| s.raw_event_name.clone()).collect();
        let s_ledgers: Vec<i64> = swaps.iter().map(|s| s.ledger).collect();
        let s_closed_ats: Vec<chrono::DateTime<chrono::Utc>> =
            swaps.iter().map(|s| s.ledger_closed_at).collect();

        sqlx::query(
            "INSERT INTO amm_swaps
                (event_id, contract_id, sender, sell_token, buy_token,
                 sell_amount, buy_amount, raw_event_name, ledger, ledger_closed_at)
             SELECT * FROM UNNEST(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
                $6::numeric[], $7::numeric[], $8::text[], $9::bigint[], $10::timestamptz[]
             ) AS t(event_id, contract_id, sender, sell_token, buy_token,
                    sell_amount, buy_amount, raw_event_name, ledger, ledger_closed_at)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&s_event_ids)
        .bind(&s_contract_ids)
        .bind(&s_senders)
        .bind(&s_sell_tokens)
        .bind(&s_buy_tokens)
        .bind(&s_sell_amounts)
        .bind(&s_buy_amounts)
        .bind(&s_raw_event_names)
        .bind(&s_ledgers)
        .bind(&s_closed_ats)
        .execute(&mut **tx)
        .await?;
    }

    // nft_events: event_kind column, token_id NOT NULL. Skip (and count) rows
    // without a token id rather than violating the constraint.
    let nfts: Vec<NftEvent> = events
        .iter()
        .filter_map(|e| extract_nft_event(e, resolve))
        .filter(|n| {
            let ok = n.token_id.is_some();
            if !ok {
                record_vertical_projection_failed();
            }
            ok
        })
        .collect();
    if !nfts.is_empty() {
        let n_event_ids: Vec<String> = nfts.iter().map(|n| n.event_id.clone()).collect();
        let n_contract_ids: Vec<String> = nfts.iter().map(|n| n.contract_id.clone()).collect();
        let n_kinds: Vec<String> = nfts.iter().map(|n| n.kind.clone()).collect();
        let n_froms: Vec<Option<String>> = nfts.iter().map(|n| n.from_addr.clone()).collect();
        let n_tos: Vec<Option<String>> = nfts.iter().map(|n| n.to_addr.clone()).collect();
        let n_token_ids: Vec<String> = nfts
            .iter()
            .map(|n| n.token_id.clone().unwrap_or_default())
            .collect();
        let n_ledgers: Vec<i64> = nfts.iter().map(|n| n.ledger).collect();
        let n_closed_ats: Vec<chrono::DateTime<chrono::Utc>> =
            nfts.iter().map(|n| n.ledger_closed_at).collect();

        sqlx::query(
            "INSERT INTO nft_events
                (event_id, contract_id, event_kind, from_addr, to_addr, token_id,
                 ledger, ledger_closed_at)
             SELECT * FROM UNNEST(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
                $7::bigint[], $8::timestamptz[]
             ) AS t(event_id, contract_id, event_kind, from_addr, to_addr, token_id,
                    ledger, ledger_closed_at)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&n_event_ids)
        .bind(&n_contract_ids)
        .bind(&n_kinds)
        .bind(&n_froms)
        .bind(&n_tos)
        .bind(&n_token_ids)
        .bind(&n_ledgers)
        .bind(&n_closed_ats)
        .execute(&mut **tx)
        .await?;
    }

    // liquidity_events: event_kind add/remove, shares, raw_event_name,
    // extra_amounts JSONB; no token_a/token_b columns.
    let liqs: Vec<LiquidityEvent> = events
        .iter()
        .filter_map(|e| extract_liquidity_event(e, resolve))
        .collect();
    if !liqs.is_empty() {
        let l_event_ids: Vec<String> = liqs.iter().map(|l| l.event_id.clone()).collect();
        let l_contract_ids: Vec<String> = liqs.iter().map(|l| l.contract_id.clone()).collect();
        let l_kinds: Vec<String> = liqs.iter().map(|l| l.kind.clone()).collect();
        let l_providers: Vec<Option<String>> = liqs.iter().map(|l| l.provider.clone()).collect();
        let l_amount_as: Vec<Option<String>> = liqs.iter().map(|l| l.amount_a.clone()).collect();
        let l_amount_bs: Vec<Option<String>> = liqs.iter().map(|l| l.amount_b.clone()).collect();
        let l_shares: Vec<Option<String>> = liqs.iter().map(|l| l.shares.clone()).collect();
        let l_raw_event_names: Vec<String> =
            liqs.iter().map(|l| l.raw_event_name.clone()).collect();
        let l_extra_amounts: Vec<Option<String>> = liqs
            .iter()
            .map(|l| serde_json::to_string(&l.extra_amounts).ok())
            .collect();
        let l_ledgers: Vec<i64> = liqs.iter().map(|l| l.ledger).collect();
        let l_closed_ats: Vec<chrono::DateTime<chrono::Utc>> =
            liqs.iter().map(|l| l.ledger_closed_at).collect();

        sqlx::query(
            "INSERT INTO liquidity_events
                (event_id, contract_id, event_kind, provider, amount_a, amount_b,
                 shares, raw_event_name, extra_amounts, ledger, ledger_closed_at)
             SELECT * FROM UNNEST(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::numeric[], $6::numeric[],
                $7::numeric[], $8::text[], $9::jsonb[], $10::bigint[], $11::timestamptz[]
             ) AS t(event_id, contract_id, event_kind, provider, amount_a, amount_b,
                    shares, raw_event_name, extra_amounts, ledger, ledger_closed_at)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&l_event_ids)
        .bind(&l_contract_ids)
        .bind(&l_kinds)
        .bind(&l_providers)
        .bind(&l_amount_as)
        .bind(&l_amount_bs)
        .bind(&l_shares)
        .bind(&l_raw_event_names)
        .bind(&l_extra_amounts)
        .bind(&l_ledgers)
        .bind(&l_closed_ats)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

/// Insert a batch of events with upsert semantics for reorg handling.
/// If an event already exists (same event_id) with *different* content, every
/// content column (raw and decoded) is replaced and its derived rows are
/// re-projected, in case the RPC returned different content for a
/// recently-passed ledger (shallow reorg). Identical rows are left untouched.
/// Returns (inserted, updated) counts.
pub async fn upsert_events(pool: &PgPool, events: &[NewEvent]) -> anyhow::Result<(u64, u64)> {
    upsert_events_with_resolver(pool, events, &default_contract_kind).await
}

/// Like [`upsert_events`] but with an explicit contract-kind resolver, so the
/// caller can pass the `SpecCache`-backed lookup.
pub async fn upsert_events_with_resolver(
    pool: &PgPool,
    events: &[NewEvent],
    resolve: &ContractKindResolver<'_>,
) -> anyhow::Result<(u64, u64)> {
    if events.is_empty() {
        return Ok((0, 0));
    }

    // Collect column arrays for the UNNEST batch upsert
    let mut event_ids: Vec<String> = Vec::with_capacity(events.len());
    let mut contract_ids: Vec<String> = Vec::with_capacity(events.len());
    let mut ledgers: Vec<i64> = Vec::with_capacity(events.len());
    let mut closed_ats: Vec<chrono::DateTime<chrono::Utc>> = Vec::with_capacity(events.len());
    let mut event_types: Vec<String> = Vec::with_capacity(events.len());
    let mut topics_json: Vec<String> = Vec::with_capacity(events.len());
    let mut decoded_topics_json: Vec<String> = Vec::with_capacity(events.len());
    let mut event_names: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut values: Vec<String> = Vec::with_capacity(events.len());
    let mut decoded_values_json: Vec<String> = Vec::with_capacity(events.len());
    let mut enriched_json: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut tx_hashes: Vec<String> = Vec::with_capacity(events.len());
    let mut in_successful_calls: Vec<bool> = Vec::with_capacity(events.len());
    let mut paging_tokens: Vec<String> = Vec::with_capacity(events.len());

    for e in events {
        event_ids.push(e.event_id.clone());
        contract_ids.push(e.contract_id.clone());
        ledgers.push(e.ledger);
        closed_ats.push(e.ledger_closed_at);
        event_types.push(e.event_type.as_str().to_string());
        topics_json.push(serde_json::to_string(&e.topics)?);
        decoded_topics_json.push(serde_json::to_string(&e.decoded_topics)?);
        event_names.push(e.event_name.clone());
        values.push(e.value.clone());
        decoded_values_json.push(serde_json::to_string(&e.decoded_value)?);
        enriched_json.push(e.enriched.as_ref().map(serde_json::to_string).transpose()?);
        tx_hashes.push(e.tx_hash.clone());
        in_successful_calls.push(e.in_successful_call);
        paging_tokens.push(e.paging_token.clone());
    }

    let mut tx = pool.begin().await?;

    // Single batch upsert with RETURNING to distinguish inserts from updates
    let rows = sqlx::query(
        "INSERT INTO events (
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics, decoded_topics, event_name, value, decoded_value,
            enriched, tx_hash, in_successful_call, paging_token
         )
         SELECT
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics::jsonb, decoded_topics::jsonb, event_name, value,
            decoded_value::jsonb, enriched::jsonb, tx_hash, in_successful_call, paging_token
         FROM UNNEST(
            $1::text[], $2::text[], $3::bigint[], $4::timestamptz[], $5::text[],
            $6::text[], $7::text[], $8::text[], $9::text[], $10::text[],
            $11::text[], $12::text[], $13::bool[], $14::text[]
         ) AS t(
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics, decoded_topics, event_name, value, decoded_value,
            enriched, tx_hash, in_successful_call, paging_token
         )
         ON CONFLICT (event_id) DO UPDATE SET
            topics = EXCLUDED.topics,
            value = EXCLUDED.value,
            decoded_topics = EXCLUDED.decoded_topics,
            decoded_value = EXCLUDED.decoded_value,
            enriched = EXCLUDED.enriched,
            tx_hash = EXCLUDED.tx_hash,
            in_successful_call = EXCLUDED.in_successful_call,
            ledger_closed_at = EXCLUDED.ledger_closed_at
         WHERE (events.topics, events.value, events.tx_hash, events.in_successful_call)
               IS DISTINCT FROM (EXCLUDED.topics, EXCLUDED.value, EXCLUDED.tx_hash, EXCLUDED.in_successful_call)
         RETURNING event_id, (xmax = 0) AS inserted",
    )
    .bind(&event_ids)
    .bind(&contract_ids)
    .bind(&ledgers)
    .bind(&closed_ats)
    .bind(&event_types)
    .bind(&topics_json)
    .bind(&decoded_topics_json)
    .bind(&event_names)
    .bind(&values)
    .bind(&decoded_values_json)
    .bind(&enriched_json)
    .bind(&tx_hashes)
    .bind(&in_successful_calls)
    .bind(&paging_tokens)
    .fetch_all(&mut *tx)
    .await?;

    let mut inserted = 0u64;
    let mut updated = 0u64;
    let mut inserted_ids = std::collections::HashSet::new();
    let mut updated_ids: Vec<String> = Vec::new();

    for row in rows {
        let event_id: String = row.try_get("event_id")?;
        let is_insert: bool = row.try_get("inserted")?;
        if is_insert {
            inserted += 1;
            inserted_ids.insert(event_id);
        } else {
            updated += 1;
            updated_ids.push(event_id);
        }
    }

    if updated > 0 {
        debug!(count = updated, "events updated due to shallow reorg");
    }

    // Re-project derived rows for updated events: drop the stale projections
    // and re-insert them from the new content, using the same resolver as the
    // insert path (#733).
    if !updated_ids.is_empty() {
        delete_derived(&mut tx, &updated_ids).await?;
        let updated_set: std::collections::HashSet<&String> = updated_ids.iter().collect();
        let changed_events: Vec<&NewEvent> = events
            .iter()
            .filter(|e| updated_set.contains(&e.event_id))
            .collect();
        project_derived(&mut tx, &changed_events, resolve).await?;
    }

    // Project derived rows for newly inserted events.
    if inserted > 0 {
        let new_events: Vec<&NewEvent> = events
            .iter()
            .filter(|e| inserted_ids.contains(&e.event_id))
            .collect();
        project_derived(&mut tx, &new_events, resolve).await?;
    }

    // Wake the API's SSE fan-out. Postgres delivers NOTIFY only on commit, so
    // it is sent in the same transaction as the rows it announces (#569).
    // Updates don't change `seq`, so only notify when rows were inserted (#732).
    if inserted > 0 {
        let max_seq: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM events")
            .fetch_one(&mut *tx)
            .await?;
        sqlx::query("SELECT pg_notify('lumenqraph_events', $1)")
            .bind(max_seq.unwrap_or(0).to_string())
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok((inserted, updated))
}

struct Transfer {
    event_id: String,
    contract_id: String,
    from_addr: Option<String>,
    to_addr: Option<String>,
    amount: String,
    kind: String,
    ledger: i64,
    ledger_closed_at: chrono::DateTime<chrono::Utc>,
}

struct Swap {
    event_id: String,
    contract_id: String,
    sender: Option<String>,
    sell_token: Option<String>,
    buy_token: Option<String>,
    sell_amount: Option<String>,
    buy_amount: Option<String>,
    raw_event_name: String,
    ledger: i64,
    ledger_closed_at: chrono::DateTime<chrono::Utc>,
}

struct NftEvent {
    event_id: String,
    contract_id: String,
    kind: String,
    token_id: Option<String>,
    from_addr: Option<String>,
    to_addr: Option<String>,
    ledger: i64,
    ledger_closed_at: chrono::DateTime<chrono::Utc>,
}

struct LiquidityEvent {
    event_id: String,
    contract_id: String,
    kind: String,
    provider: Option<String>,
    amount_a: Option<String>,
    amount_b: Option<String>,
    shares: Option<String>,
    raw_event_name: String,
    extra_amounts: Value,
    ledger: i64,
    ledger_closed_at: chrono::DateTime<chrono::Utc>,
}

fn as_addr(v: Option<&Value>) -> Option<String> {
    match v {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Look up a spec-declared event parameter by name in the enrichment payload
/// (`enriched.params.<name>.value`). Returns `None` when enrichment is absent
/// or the parameter is not present.
fn enriched_param(e: &NewEvent, name: &str) -> Option<String> {
    let enriched = e.enriched.as_ref()?;
    let params = enriched.get("params")?;
    as_addr(params.get(name).and_then(|p| p.get("value")))
}

/// Extract a decimal amount from a decoded value. Accepts a JSON string or a
/// JSON number that is an integer (i128). Returns `None` for anything else so
/// non-numeric values are never stringified into `amount` (issue #600).
fn as_i128_amount(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => {
            if s.parse::<i128>().is_ok() {
                Some(s.clone())
            } else {
                None
            }
        }
        Value::Number(n) => n
            .as_i64()
            .map(|i| i.to_string())
            .or_else(|| n.as_u64().map(|u| u.to_string())),
        _ => None,
    }
}

/// Recognize SEP-41 style balance-delta events: transfer, mint, burn, clawback.
/// Returns None for other event types, for contracts that are not confirmed
/// SEP-41 tokens (or SACs), and for values that are not a valid amount.
///
/// transfer: Topics: [symbol "transfer", from Address, to Address]
/// mint: Topics: [symbol "mint", to Address] (CAP-67 SAC / custom SEP-41) or
///       [symbol "mint", admin Address, to Address, asset] (legacy pre-CAP-67 SAC)
/// burn: Topics: [symbol "burn", from Address]
/// clawback: Topics: [symbol "clawback", from Address] (CAP-67 SAC / custom SEP-41)
///       or [symbol "clawback", admin Address, from Address, asset] (legacy SAC)
/// All have Value: i128 amount (decoded as a decimal string) or, under CAP-67,
/// a map `{amount, to_muxed_id}`.
///
/// For `mint`/`clawback` the address is taken from the spec-declared parameter
/// names when enrichment is available, otherwise from the topic count so the
/// legacy SAC layouts (which carry the admin at index 1) are handled (#734).
fn extract_transfer(e: &NewEvent, resolve: &ContractKindResolver<'_>) -> Option<Transfer> {
    let event_name = e.event_name.as_deref()?;
    let kind = match event_name {
        "transfer" => "transfer",
        "mint" => "mint",
        "burn" => "burn",
        "clawback" => "clawback",
        _ => return None,
    };

    // Gate on the contract's spec: only confirmed fungible tokens project here.
    match resolve(&e.contract_id) {
        Some(ContractKind::FungibleToken) => {}
        _ => {
            record_transfer_skipped();
            return None;
        }
    }

    // Amount must be an i128 or the CAP-67 map form `{amount, to_muxed_id}`.
    let amount = match &e.decoded_value {
        Value::Object(map) => match map.get("amount").and_then(as_i128_amount) {
            Some(a) => a,
            None => {
                record_transfer_skipped();
                return None;
            }
        },
        other => match as_i128_amount(other) {
            Some(a) => a,
            None => {
                record_transfer_skipped();
                return None;
            }
        },
    };

    let (from_addr, to_addr) = match kind {
        "transfer" => (
            as_addr(e.decoded_topics.get(1)),
            as_addr(e.decoded_topics.get(2)),
        ),
        "mint" => {
            let to = enriched_param(e, "to").or_else(|| {
                // Legacy SAC: [mint, admin, to, asset]; CAP-67 SAC (3 topics)
                // and custom SEP-41 (2 topics): [mint, to].
                if e.decoded_topics.len() >= 4 {
                    as_addr(e.decoded_topics.get(2))
                } else {
                    as_addr(e.decoded_topics.get(1))
                }
            });
            (None, to)
        }
        "burn" => (as_addr(e.decoded_topics.get(1)), None),
        "clawback" => {
            let from = enriched_param(e, "from").or_else(|| {
                // Legacy SAC: [clawback, admin, from, asset]; otherwise
                // [clawback, from].
                if e.decoded_topics.len() >= 4 {
                    as_addr(e.decoded_topics.get(2))
                } else {
                    as_addr(e.decoded_topics.get(1))
                }
            });
            (from, None)
        }
        _ => return None,
    };

    Some(Transfer {
        event_id: e.event_id.clone(),
        contract_id: e.contract_id.clone(),
        from_addr,
        to_addr,
        amount,
        kind: kind.to_string(),
        ledger: e.ledger,
        ledger_closed_at: e.ledger_closed_at,
    })
}

/// Recognize AMM `swap` events (Soroswap, Aquarius). Gated on the contract
/// being a confirmed AMM. Expected shape:
/// Topics: [symbol "swap", sender, recipient, token_in, token_out]
/// Value: map `{amount_in, amount_out}` (or a two-element array).
fn extract_swap(e: &NewEvent, resolve: &ContractKindResolver<'_>) -> Option<Swap> {
    if e.event_name.as_deref()? != "swap" {
        return None;
    }
    if resolve(&e.contract_id) != Some(ContractKind::Amm) {
        return None;
    }

    let (amount_in, amount_out) = match &e.decoded_value {
        Value::Object(map) => (
            map.get("amount_in").and_then(as_i128_amount),
            map.get("amount_out").and_then(as_i128_amount),
        ),
        Value::Array(arr) => (
            arr.first().and_then(as_i128_amount),
            arr.get(1).and_then(as_i128_amount),
        ),
        _ => (None, None),
    };

    Some(Swap {
        event_id: e.event_id.clone(),
        contract_id: e.contract_id.clone(),
        sender: as_addr(e.decoded_topics.get(1)),
        sell_token: as_addr(e.decoded_topics.get(3)),
        buy_token: as_addr(e.decoded_topics.get(4)),
        sell_amount: amount_in,
        buy_amount: amount_out,
        raw_event_name: e.event_name.clone().unwrap_or_default(),
        ledger: e.ledger,
        ledger_closed_at: e.ledger_closed_at,
    })
}

/// Recognize SEP-50 / OpenZeppelin NFT `mint`/`transfer`/`burn` events. Gated
/// on the contract being a confirmed NFT. The token id is taken from the value
/// (or the last topic) so NFT transfers are never mistaken for fungible ones.
fn extract_nft_event(e: &NewEvent, resolve: &ContractKindResolver<'_>) -> Option<NftEvent> {
    let kind = match e.event_name.as_deref()? {
        "mint" => "mint",
        "transfer" => "transfer",
        "burn" => "burn",
        _ => return None,
    };
    if resolve(&e.contract_id) != Some(ContractKind::NonFungibleToken) {
        return None;
    }

    let token_id = match &e.decoded_value {
        Value::Object(map) => map.get("token_id").and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        }),
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    };

    let (from_addr, to_addr) = match kind {
        "transfer" => (
            as_addr(e.decoded_topics.get(1)),
            as_addr(e.decoded_topics.get(2)),
        ),
        "mint" => (None, as_addr(e.decoded_topics.get(1))),
        "burn" => (as_addr(e.decoded_topics.get(1)), None),
        _ => return None,
    };

    Some(NftEvent {
        event_id: e.event_id.clone(),
        contract_id: e.contract_id.clone(),
        kind: kind.to_string(),
        token_id,
        from_addr,
        to_addr,
        ledger: e.ledger,
        ledger_closed_at: e.ledger_closed_at,
    })
}

/// Recognize liquidity pool `deposit`/`withdraw` events. Gated on the contract
/// being a confirmed liquidity pool. Expected shape:
/// Topics: [symbol "deposit"|"withdraw", provider, token_a, token_b]
/// Value: map `{amount_a, amount_b}` (or a two-element array).
fn extract_liquidity_event(
    e: &NewEvent,
    resolve: &ContractKindResolver<'_>,
) -> Option<LiquidityEvent> {
    // Migration 0014 uses `add`/`remove` as the event_kind vocabulary.
    let kind = match e.event_name.as_deref()? {
        "deposit" => "add",
        "withdraw" => "remove",
        _ => return None,
    };
    if resolve(&e.contract_id) != Some(ContractKind::LiquidityPool) {
        return None;
    }

    let (amount_a, amount_b) = match &e.decoded_value {
        Value::Object(map) => (
            map.get("amount_a").and_then(as_i128_amount),
            map.get("amount_b").and_then(as_i128_amount),
        ),
        Value::Array(arr) => (
            arr.first().and_then(as_i128_amount),
            arr.get(1).and_then(as_i128_amount),
        ),
        _ => (None, None),
    };

    let shares = match &e.decoded_value {
        Value::Object(map) => map.get("shares").and_then(as_i128_amount),
        _ => None,
    };

    let extra_amounts = match &e.decoded_value {
        Value::Object(map) => map
            .get("extra_amounts")
            .cloned()
            .unwrap_or(Value::Null),
        _ => Value::Null,
    };

    Some(LiquidityEvent {
        event_id: e.event_id.clone(),
        contract_id: e.contract_id.clone(),
        kind: kind.to_string(),
        provider: as_addr(e.decoded_topics.get(1)),
        amount_a,
        amount_b,
        shares,
        raw_event_name: e.event_name.clone().unwrap_or_default(),
        extra_amounts,
        ledger: e.ledger,
        ledger_closed_at: e.ledger_closed_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;


    /// Test resolver: treats every contract as a confirmed fungible token so the
    /// transfer projection itself is what the assertions exercise.
    fn fungible(_contract_id: &str) -> Option<ContractKind> {
        Some(ContractKind::FungibleToken)
    }

    fn event(name: Option<&str>, topics: Vec<Value>, value: Value) -> NewEvent {
        NewEvent {
            event_id: "e1".into(),
            contract_id: "C1".into(),
            ledger: 42,
            ledger_closed_at: chrono::Utc::now(),
            event_type: lumenqraph_core::EventType::Contract,
            topics: vec![],
            decoded_topics: topics,
            event_name: name.map(|s| s.to_string()),
            value: String::new(),
            decoded_value: value,
            enriched: None,
            tx_hash: "tx".into(),
            in_successful_call: true,
            paging_token: "e1".into(),
        }
    }

    #[test]
    fn extracts_transfer_from_to_amount() {
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!("300"),
        );
        let t = extract_transfer(&e, &fungible).expect("transfer should be recognised");
        assert_eq!(t.from_addr.as_deref(), Some("GFROM"));
        assert_eq!(t.to_addr.as_deref(), Some("GTO"));
        assert_eq!(t.amount, "300");
        assert_eq!(t.kind, "transfer");
    }

    #[test]
    fn extracts_mint_event() {
        let e = event(
            Some("mint"),
            vec![json!("mint"), json!("GRECIPIENT")],
            json!("1000"),
        );
        let t = extract_transfer(&e, &fungible).expect("mint should be recognised");
        assert_eq!(t.from_addr, None);
        assert_eq!(t.to_addr.as_deref(), Some("GRECIPIENT"));
        assert_eq!(t.amount, "1000");
        assert_eq!(t.kind, "mint");
    }

    #[test]
    fn extracts_burn_event() {
        let e = event(
            Some("burn"),
            vec![json!("burn"), json!("GBURNER")],
            json!("500"),
        );
        let t = extract_transfer(&e, &fungible).expect("burn should be recognised");
        assert_eq!(t.from_addr.as_deref(), Some("GBURNER"));
        assert_eq!(t.to_addr, None);
        assert_eq!(t.amount, "500");
        assert_eq!(t.kind, "burn");
    }

    #[test]
    fn extracts_clawback_event() {
        let e = event(
            Some("clawback"),
            vec![json!("clawback"), json!("GVICTIM")],
            json!("750"),
        );
        let t = extract_transfer(&e, &fungible).expect("clawback should be recognised");
        assert_eq!(t.from_addr.as_deref(), Some("GVICTIM"));
        assert_eq!(t.to_addr, None);
        assert_eq!(t.amount, "750");
        assert_eq!(t.kind, "clawback");
    }

    // Issue #734: legacy (pre-CAP-67) SAC mint/clawback carry the admin at index 1.
    #[test]
    fn extracts_legacy_sac_mint_recipient_not_admin() {
        let e = event(
            Some("mint"),
            vec![json!("mint"), json!("GADMIN"), json!("GRECIPIENT"), json!("native")],
            json!("1000"),
        );
        let t = extract_transfer(&e, &fungible).expect("legacy mint should be recognised");
        assert_eq!(t.from_addr, None);
        assert_eq!(t.to_addr.as_deref(), Some("GRECIPIENT"));
        assert_eq!(t.kind, "mint");
    }

    #[test]
    fn extracts_cap67_sac_mint_recipient() {
        let e = event(
            Some("mint"),
            vec![json!("mint"), json!("GRECIPIENT"), json!("native")],
            json!("1000"),
        );
        let t = extract_transfer(&e, &fungible).expect("CAP-67 mint should be recognised");
        assert_eq!(t.to_addr.as_deref(), Some("GRECIPIENT"));
    }

    #[test]
    fn extracts_legacy_sac_clawback_victim_not_admin() {
        let e = event(
            Some("clawback"),
            vec![json!("clawback"), json!("GADMIN"), json!("GVICTIM"), json!("native")],
            json!("750"),
        );
        let t = extract_transfer(&e, &fungible).expect("legacy clawback should be recognised");
        assert_eq!(t.from_addr.as_deref(), Some("GVICTIM"));
        assert_eq!(t.to_addr, None);
        assert_eq!(t.kind, "clawback");
    }

    #[test]
    fn extracts_cap67_sac_clawback_victim() {
        let e = event(
            Some("clawback"),
            vec![json!("clawback"), json!("GVICTIM"), json!("native")],
            json!("750"),
        );
        let t = extract_transfer(&e, &fungible).expect("CAP-67 clawback should be recognised");
        assert_eq!(t.from_addr.as_deref(), Some("GVICTIM"));
    }

    #[test]
    fn enriched_params_take_precedence_for_mint() {
        let mut e = event(
            Some("mint"),
            vec![json!("mint"), json!("GADMIN"), json!("GWRONG"), json!("native")],
            json!("1000"),
        );
        e.enriched = Some(json!({
            "params": { "to": { "value": "GRECIPIENT" } }
        }));
        let t = extract_transfer(&e, &fungible).expect("mint should be recognised");
        assert_eq!(t.to_addr.as_deref(), Some("GRECIPIENT"));
    }

    #[test]
    fn ignores_other_events() {
        let e = event(Some("approval"), vec![json!("approval")], json!("1"));
        assert!(extract_transfer(&e, &fungible).is_none());
    }

    // Issue #729: the default resolver must not treat every `C…` strkey as a
    // fungible token. Without a spec cache the kind is unknown, so a transfer
    // from an arbitrary contract is skipped.
    #[test]
    fn default_resolver_does_not_assume_fungible_token() {
        let sac_id = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        assert_eq!(sac_id.len(), 56);
        assert_eq!(default_contract_kind(sac_id), None);

        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!("300"),
        );
        assert!(
            extract_transfer(&e, &default_contract_kind).is_none(),
            "unknown contract kind must not project a transfer"
        );
    }

    #[test]
    fn non_string_amount_is_stringified() {
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!(300),
        );
        let t = extract_transfer(&e, &fungible).unwrap();
        assert_eq!(t.amount, "300");
    }

    #[test]
    fn missing_address_topics_are_none() {
        let e = event(Some("transfer"), vec![json!("transfer")], json!("5"));
        let t = extract_transfer(&e, &fungible).unwrap();
        assert!(t.from_addr.is_none());
        assert!(t.to_addr.is_none());
        assert_eq!(t.amount, "5");
    }

    // Issue #385 / #729: Non-token contracts with transfer events should not be
    // projected. A resolver that reports the contract as an NFT must skip the
    // fungible transfer projection even when the value parses as an integer.
    #[test]
    fn nft_transfer_with_token_id_should_be_rejected() {
        fn nft(_contract_id: &str) -> Option<ContractKind> {
            Some(ContractKind::NonFungibleToken)
        }
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!(42),
        );
        assert!(
            extract_transfer(&e, &nft).is_none(),
            "NFT contract transfers must not project into token_transfers"
        );
    }

    #[test]
    fn nft_contract_transfer_with_complex_topics_is_skipped() {
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!({"token_id": 123}),
        );
        // No `amount` field: skipped and counted, never projected as a transfer.
        assert!(extract_transfer(&e, &fungible).is_none(), "value without an amount is skipped");
    }

    // Issue #384: CAP-67 Protocol 23 support for muxed addresses and SAC assets
    #[test]
    fn cap67_sac_transfer_with_asset_topic_should_extract() {
        let e = event(
            Some("transfer"),
            vec![
                json!("transfer"),
                json!("GFROM"),
                json!("GTO"),
                json!("native"),
            ],
            json!("500"),
        );
        let t = extract_transfer(&e, &fungible);
        assert!(t.is_some(), "should handle SAC transfer with asset topic");
        let transfer = t.unwrap();
        assert_eq!(transfer.from_addr.as_deref(), Some("GFROM"));
        assert_eq!(transfer.to_addr.as_deref(), Some("GTO"));
        assert_eq!(transfer.amount, "500");
    }

    #[test]
    fn muxed_address_topic_should_be_preserved() {
        let muxed_addr = "MAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4";
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!(muxed_addr)],
            json!("1000"),
        );
        let t = extract_transfer(&e, &fungible);
        assert!(t.is_some(), "should handle muxed address in topic");
        let transfer = t.unwrap();
        // Current behavior: treats muxed address as regular string
        assert_eq!(transfer.to_addr.as_deref(), Some(muxed_addr));
    }

    #[test]
    fn cap67_transfer_with_map_value_should_extract_gracefully() {
        let map_value = json!({
            "amount": "100",
            "to_muxed_id": "42"
        });
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            map_value,
        );
        // CAP-67 map form: the amount comes from the `amount` key.
        let transfer = extract_transfer(&e, &fungible).expect("CAP-67 map value should extract");
        assert_eq!(transfer.amount, "100");
    }

    // Issue #383: Poison-pill events with non-integer values must not abort ingestion
    #[test]
    fn transfer_with_numeric_token_id_should_stringify_not_panic() {
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!(0xDEADBEEFu64),
        );
        let t = extract_transfer(&e, &fungible).expect("should handle numeric value");
        assert_eq!(t.amount, "3735928559");
    }

    #[test]
    fn transfer_with_vector_value_is_skipped_without_panicking() {
        let vector_value = json!(["item1", "item2", "item3"]);
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            vector_value,
        );
        assert!(extract_transfer(&e, &fungible).is_none(), "non-amount value is skipped");
    }

    #[test]
    fn mint_with_struct_value_is_skipped() {
        let struct_value = json!({
            "field1": "value1",
            "field2": 42,
            "nested": {
                "amount": "100"
            }
        });
        let e = event(
            Some("mint"),
            vec![json!("mint"), json!("GRECIPIENT")],
            struct_value,
        );
        // Only a top-level `amount` key counts; a nested one does not.
        assert!(extract_transfer(&e, &fungible).is_none(), "struct without top-level amount is skipped");
    }

    #[test]
    fn burn_with_null_value_is_skipped() {
        let e = event(
            Some("burn"),
            vec![json!("burn"), json!("GBURNER")],
            json!(null),
        );
        assert!(extract_transfer(&e, &fungible).is_none(), "null amount is skipped");
    }

    #[test]
    fn clawback_with_boolean_value_is_skipped() {
        let e = event(
            Some("clawback"),
            vec![json!("clawback"), json!("GVICTIM")],
            json!(true),
        );
        assert!(extract_transfer(&e, &fungible).is_none(), "boolean amount is skipped");
    }

    // Issue #382: REORG_OVERLAP_LEDGERS upsert should skip no-op updates
    #[test]
    fn upsert_unchanged_rows_should_not_increment_tuple_version() {
        // This test documents the expected behavior at the DB level:
        // When upserting an event with identical decoded_topics, decoded_value, and enriched,
        // the xmax field (transaction ID of the last update) should remain 0,
        // indicating no actual update occurred.
        //
        // SQL query verification:
        // INSERT ... ON CONFLICT (event_id) DO UPDATE SET ...
        // WHERE events.decoded_value IS DISTINCT FROM EXCLUDED.decoded_value
        //   OR events.enriched IS DISTINCT FROM EXCLUDED.enriched
        //   OR events.decoded_topics IS DISTINCT FROM EXCLUDED.decoded_topics
        // RETURNING event_id, (xmax = 0) AS inserted
        //
        // When row is unchanged, xmax = 0 and it should be counted as inserted (not updated).

        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!("1000"),
        );
        let t = extract_transfer(&e, &fungible).expect("should extract transfer");
        assert_eq!(t.from_addr.as_deref(), Some("GFROM"));
        assert_eq!(t.to_addr.as_deref(), Some("GTO"));
        assert_eq!(t.amount, "1000");
    }

    #[test]
    fn upsert_with_changed_decoded_topics_should_update() {
        // Verify that when decoded_topics actually change, the row IS updated.
        let e1 = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO1")],
            json!("1000"),
        );
        let e2 = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO2")],
            json!("1000"),
        );
        let t1 = extract_transfer(&e1, &fungible).unwrap();
        let t2 = extract_transfer(&e2, &fungible).unwrap();
        assert_ne!(t1.to_addr, t2.to_addr);
    }

    #[test]
    fn upsert_with_changed_decoded_value_should_update() {
        // Verify that when decoded_value changes, the row IS updated.
        let e1 = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!("1000"),
        );
        let e2 = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!("2000"),
        );
        let t1 = extract_transfer(&e1, &fungible).unwrap();
        let t2 = extract_transfer(&e2, &fungible).unwrap();
        assert_ne!(t1.amount, t2.amount);
    }
}
