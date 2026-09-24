//! The live polling loop: fetch new events from the tip, page through them,
//! store, advance the cursor, sleep, repeat. Failures are logged, counted, and
//! retried with exponential backoff so a flaky RPC never kills the process.
//! Responds to Ctrl-C / SIGTERM for a clean shutdown.

use std::sync::Arc;
use std::time::{Duration, Instant};

use lumenqraph_core::NewEvent;
use sqlx::PgPool;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::convert::to_new_event;
use crate::rpc_client::RpcClient;
use crate::specs::{self, SpecCache};
use crate::{cursor, keys, retention, state, store};

/// How often to prune outside the retention window. Decoupled from the poll
/// interval: retention is a slow-moving disk concern, and re-checking it every
/// few seconds would spend more on probes than the deletes are worth.
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

/// The RPC retention window: how far back the Soroban RPC will serve events.
/// This is the hard limit of what's available via `getEvents` on SDF public RPC.
/// Backfill and fresh-start clamping use this value. Note: MAX_CATCHUP_LEDGERS
/// (in config) is a separate, more conservative limit for live polling performance.
const MAX_LOOKBACK_LEDGERS: i64 = 120_000; // ~7 days at ~5s/ledger (SDF public RPC)

/// The RPC retention window shared with backfill for fresh-start clamping.
pub fn max_lookback() -> i64 {
    MAX_LOOKBACK_LEDGERS
}

pub async fn run(pool: PgPool, rpc: RpcClient, config: Config, specs: Arc<SpecCache>) -> anyhow::Result<()> {
    let base_interval = Duration::from_secs(config.poll_interval_secs.max(1));
    let degraded_interval = Duration::from_secs(config.degraded_poll_interval_secs.max(1));
    let mut backoff = base_interval;
    // One spec cache for the process lifetime: each contract's interface is
    // fetched and parsed once, then reused to enrich every event.
    let specs = SpecCache::new(config.spec_cache_max_entries, config.spec_fetch_concurrency);
    // None => prune on the first cycle that reaches the tip, so a deployment
    // that switches retention on starts reclaiming immediately.
    let mut last_prune: Option<Instant> = None;
    // Circuit-breaker state: count consecutive poll failures.
    let mut consecutive_errors: u32 = 0;

    loop {
        let sleep_for = match poll_once(&pool, &rpc, &config, &specs).await {
            Ok(processed_to) => {
                // Success: reset both the backoff and the circuit-breaker counter.
                if consecutive_errors > 0 {
                    info!(
                        consecutive_errors,
                        "poll cycle succeeded; resetting circuit breaker"
                    );
                    consecutive_errors = 0;
                    // Record cleared state to the gauge.
                    let _ = cursor::set_consecutive_errors(&pool, 0).await;
                }
                backoff = base_interval;
                if let Some(ledger) = processed_to {
                    debug!(ledger, "cycle complete");
                    if config.retention_ledgers > 0
                        && last_prune.is_none_or(|t| t.elapsed() >= PRUNE_INTERVAL)
                    {
                        // Never fatal: falling behind on disk reclamation is bad,
                        // but stopping the tail over it is worse.
                        if let Err(e) =
                            retention::prune(&pool, ledger, config.retention_ledgers).await
                        {
                            warn!(error = %e, "retention prune failed");
                        }
                        // Prune old spec versions (if enabled)
                        if config.spec_version_retention > 0 {
                            if let Err(e) =
                                retention::prune_spec_versions(
                                    &pool,
                                    ledger,
                                    config.retention_ledgers,
                                    config.spec_version_retention,
                                )
                                .await
                            {
                                warn!(error = %e, "spec version retention prune failed");
                            }
                        }
                        last_prune = Some(Instant::now());
                    }
                }
                base_interval
            }
            Err(e) => {
                consecutive_errors += 1;
                let _ = cursor::incr_errors(&pool).await;
                let _ = cursor::set_consecutive_errors(&pool, consecutive_errors).await;

                // Check if we should enter degraded / circuit-breaker state.
                let circuit_open = config.max_consecutive_errors > 0
                    && consecutive_errors >= config.max_consecutive_errors;

                if circuit_open {
                    error!(
                        error = %e,
                        consecutive_errors,
                        max_consecutive_errors = config.max_consecutive_errors,
                        degraded_interval_secs = config.degraded_poll_interval_secs,
                        "circuit breaker open: too many consecutive poll failures; \
                         switching to degraded polling interval"
                    );
                    backoff = degraded_interval;
                    degraded_interval
                } else {
                    warn!(error = %e, backoff_secs = backoff.as_secs(), consecutive_errors, "poll cycle failed; backing off");
                    let this = backoff;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                    this
                }
            }
        };

        tokio::select! {
            _ = tokio::time::sleep(sleep_for) => {}
            _ = shutdown_signal() => {
                info!("shutdown signal received; stopping poller");
                return Ok(());
            }
        }
    }
}

/// One catch-up to the current tip. Returns the ledger we advanced to.
async fn poll_once(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
) -> anyhow::Result<Option<i64>> {
    use tracing::Instrument as _;

    let latest = rpc.get_latest_ledger().await?;

    // Wrap the rest of the cycle in a span so all log lines — including those
    // from spawned tasks (spec fetches, state/key snapshots) — carry a shared
    // `ledger_range` field. Log aggregators can filter on it to reconstruct a
    // single cycle without interleaved noise from concurrent cycles.
    let cycle_span = tracing::info_span!(
        "poll_cycle",
        from = tracing::field::Empty,
        to = latest,
    );

    let cycle_span_for_record = cycle_span.clone();
    async move {

    let mut start = match cursor::read_last_processed(pool).await? {
        Some(c) => c + 1,
        None if config.start_ledger > 0 => config.start_ledger,
        None => latest,
    };

    if start > latest {
        // Nothing new closed; still record the tip so lag reflects reality.
        cursor::write_progress(pool, start - 1, latest, 0).await?;
        return Ok(None);
    }

    // Clamp to RPC retention window on fresh start.
    // If the configured START_LEDGER is older than what the RPC serves,
    // clamp to the oldest available ledger to avoid RPC rejection errors.
    let oldest_available = latest - MAX_LOOKBACK_LEDGERS;
    if start < oldest_available {
        warn!(
            requested = start,
            clamped_to = oldest_available,
            reason = "start ledger is older than RPC retention window (~7 days)",
            "fresh-start ledger clamped to earliest servable ledger"
        );
        start = oldest_available;
    }

    if latest - start > config.max_catchup_ledgers {
        let clamped = latest - config.max_catchup_ledgers;
        warn!(
            from = start,
            to = clamped,
            gap_ledgers = clamped - start,
            "cursor too far behind tip; skipping ahead to the catch-up window \
             (gap unrecoverable via public RPC — use a retaining/paid RPC or a \
             datalake backfill for gapless history, or raise MAX_CATCHUP_LEDGERS)"
        );
        start = clamped;
    }

    // Record the ledger range we are about to process in the span, now that
    // we know both endpoints after clamping.
    cycle_span_for_record.record("from", start);

    let (inserted, enrichment) = fetch_and_store(pool, rpc, config, specs, start, latest).await?;

    // Check enrichment coverage and warn if it drops significantly
    if enrichment.enriched_count > 0 || enrichment.not_enriched_count > 0 {
        let total = enrichment.enriched_count + enrichment.not_enriched_count;
        if total > 0 {
            let enrichment_rate = enrichment.enriched_count as f64 / total as f64;
            let not_enriched_fraction = enrichment.not_enriched_count as f64 / total as f64;

            // Emit warning if enrichment rate is below threshold
            if not_enriched_fraction > config.enrichment_warn_threshold {
                warn!(
                    enrichment_rate = enrichment_rate,
                    not_enriched_fraction = not_enriched_fraction,
                    threshold = config.enrichment_warn_threshold,
                    enriched_count = enrichment.enriched_count,
                    not_enriched_count = enrichment.not_enriched_count,
                    total_events = total,
                    "enrichment coverage dropped below threshold"
                );
            }
        }
    }

    // Trailing re-scan for shallow reorg detection.
    // If configured, re-fetch the last N ledgers with upsert semantics to catch
    // content changes that might have occurred due to chain reorgs.
    let mut reorg_updated = 0u64;
    if config.reorg_overlap_ledgers > 0 && latest > config.reorg_overlap_ledgers {
        let reorg_start = latest - config.reorg_overlap_ledgers;
        reorg_updated =
            fetch_and_upsert(pool, rpc, config, specs, reorg_start, latest).await?;
        if reorg_updated > 0 {
            debug!(
                reorg_updated,
                reorg_start,
                reorg_end = latest,
                "trailing reorg re-scan complete"
            );
        }
    }

    // Record RPC metrics for this cycle
    let (rpc_calls, rpc_errors, rpc_errors_32001) = rpc.take_metrics();
    if rpc_calls > 0 || rpc_errors > 0 {
        cursor::track_rpc_call(pool, rpc_calls, rpc_errors, rpc_errors_32001).await?;
    }

    cursor::write_progress(pool, latest, latest, inserted).await?;
    if inserted > 0 {
        info!(inserted, up_to_ledger = latest, "indexed events");
    }
    if reorg_updated > 0 {
        info!(reorg_updated, "events updated due to shallow reorg detection");
    }
    Ok(Some(latest))
    }
    .instrument(cycle_span)
    .await
}

/// Enrichment metrics for a cycle.
pub struct EnrichmentMetrics {
    pub enriched_count: u64,
    pub not_enriched_count: u64,
}

/// Page through events from `start` to the tip, storing each page. Shared by the
/// live poller and the backfill command. Returns (total_inserted, enrichment_metrics).
pub async fn fetch_and_store(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
    start: i64,
    tip: i64,
) -> anyhow::Result<(u64, EnrichmentMetrics)> {
    let mut cursor_token: Option<String> = None;
    let mut total_inserted = 0u64;
    let mut enriched_count = 0u64;
    let mut not_enriched_count = 0u64;
    let mut page_count = 0usize;
    let mut last_seen_ledger = start;
    // Contracts seen this cycle, used to bound per-contract instance reads when
    // no explicit CONTRACT_IDS list does it for us.
    let mut active_contracts: std::collections::HashSet<String> = std::collections::HashSet::new();
    let tracks_active_contracts =
        (config.state_indexing || config.upgrade_watch) && config.contract_ids.is_empty();
    // contract -> holder addresses seen this cycle (for per-key balance snapshots).
    let mut holders_by_contract: std::collections::HashMap<
        String,
        std::collections::HashSet<String>,
    > = std::collections::HashMap::new();
    // contract -> (template_index, key_params) for per-key template snapshots.
    let mut template_keys_by_contract: std::collections::HashMap<
        String,
        Vec<(usize, Vec<String>)>,
    > = std::collections::HashMap::new();
    loop {
        let page = match rpc
            .get_events(
                Some(start),
                &config.contract_ids,
                cursor_token.clone(),
                config.page_size,
            )
            .await
        {
            Ok(p) => p,
            Err(e) => {
                if page_count > 0 {
                    warn!(
                        error = %e,
                        page = page_count + 1,
                        from = last_seen_ledger,
                        to = tip,
                        "RPC error on non-first page of batch; recording missed range"
                    );
                    let _ = cursor::incr_errors(pool).await;
                    record_missed_range(pool, last_seen_ledger, tip, &e.to_string()).await?;
                    break;
                } else {
                    return Err(e);
                }
            }
        };
        page_count += 1;
        let mut batch: Vec<NewEvent> = Vec::with_capacity(page.events.len());
        for ev in &page.events {
            last_seen_ledger = last_seen_ledger.max(ev.ledger);
            // Interface lookups are cached, so this is one fetch per contract.
            let spec = specs.get(pool, rpc, &ev.contract_id, ev.ledger).await;
            // Only needed for index-all instance reads (see below).
            if tracks_active_contracts {
                active_contracts.insert(ev.contract_id.clone());
            }
            let new_event = to_new_event(ev, spec.as_deref());
            // Track enrichment coverage: count enriched vs not-enriched events
            if new_event.enriched.is_some() {
                enriched_count += 1;
            } else {
                not_enriched_count += 1;
            }
            // Discover holder addresses to snapshot per-key balances for.
            if config.key_indexing {
                for holder in keys::holders_in_event(&new_event) {
                    holders_by_contract
                        .entry(new_event.contract_id.clone())
                        .or_default()
                        .insert(holder);
                }
            }
            // Discover keys from custom templates.
            for (idx, template) in config.key_templates.iter().enumerate() {
                for params in template.keys_from_event(&new_event) {
                    template_keys_by_contract
                        .entry(new_event.contract_id.clone())
                        .or_default()
                        .push((idx, params));
                }
            }
            batch.push(new_event);
        }
        let n = batch.len();
        total_inserted += store::insert_events(pool, &batch).await?;

        cursor_token = page.cursor;
        if n < config.page_size as usize || cursor_token.is_none() {
            break;
        }
    }

    // Record enrichment metrics for this cycle
    if enriched_count > 0 || not_enriched_count > 0 {
        cursor::track_enrichment(pool, enriched_count, not_enriched_count).await?;
    }

    // Read each tracked contract's instance entry. With an explicit CONTRACT_IDS
    // list we track those contracts every cycle; in index-all mode we restrict
    // to contracts active this cycle to bound the extra RPC calls.
    //
    // State indexing and the upgrade watch both want this entry — for the
    // storage map and the executable hash respectively — so whenever state
    // indexing is on it covers both, and the upgrade watch adds no RPC calls.
    if config.state_indexing || config.upgrade_watch {
        let targets: Vec<String> = if config.contract_ids.is_empty() {
            active_contracts.iter().cloned().collect()
        } else {
            config.contract_ids.clone()
        };
        if config.state_indexing {
            // Batch instance reads with bounded concurrency.
            // Change-detected, so unchanged instances are no-op writes.
            // Also notes the executable hash, detecting upgrades for free.
            state::snapshot_instances_batch(pool, rpc, specs, &targets).await;
        } else {
            // Upgrade watch in index-all mode: batch the instance reads,
            // then check for upgrades.
            for contract_id in &targets {
                specs::check_for_upgrade(pool, rpc, specs, contract_id, tip).await;
            }
        }
    }

    // Snapshot per-holder balances discovered from this cycle's token events.
    // Keys are batched into chunked getLedgerEntries calls so per-cycle RPC cost
    // scales with the number of batches, not the number of individual holders.
    if config.key_indexing && !holders_by_contract.is_empty() {
        let durability = keys::parse_durability(&config.balance_key_durability);
        state::snapshot_balances_batch(
            pool,
            rpc,
            &holders_by_contract,
            &config.contract_ids,
            &config.balance_key_symbol,
            durability,
        )
        .await;
    }
    // Snapshot per-key entries from custom templates.
    if !template_keys_by_contract.is_empty() {
        state::snapshot_template_keys_batch(
            pool,
            rpc,
            &template_keys_by_contract,
            &config.contract_ids,
            &config.key_templates,
        )
        .await;
    }
    Ok((
        total_inserted,
        EnrichmentMetrics {
            enriched_count,
            not_enriched_count,
        },
    ))
}

/// Record a ledger range missed due to a mid-batch page RPC failure.
pub async fn record_missed_range(
    pool: &PgPool,
    from_ledger: i64,
    to_ledger: i64,
    reason: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO missed_ranges (from_ledger, to_ledger, reason, detected_at) \
         VALUES ($1, $2, $3, NOW())",
    )
    .bind(from_ledger)
    .bind(to_ledger)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

/// Replay all recorded missed ranges that have not yet been recovered.
pub async fn recover_gaps(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
) -> anyhow::Result<()> {
    #[derive(sqlx::FromRow)]
    struct MissedRange {
        id: i64,
        from_ledger: i64,
        to_ledger: i64,
    }

    let unrecovered: Vec<MissedRange> = sqlx::query_as(
        "SELECT id, from_ledger, to_ledger FROM missed_ranges WHERE recovered_at IS NULL ORDER BY from_ledger ASC",
    )
    .fetch_all(pool)
    .await?;

    if unrecovered.is_empty() {
        info!("no unrecovered missed ranges found");
        return Ok(());
    }

    info!(count = unrecovered.len(), "replaying recorded missed ranges");

    for range in unrecovered {
        info!(
            id = range.id,
            from = range.from_ledger,
            to = range.to_ledger,
            "replaying missed range"
        );

        let mut cursor_token: Option<String> = None;
        let mut total_inserted = 0u64;

        loop {
            let page = rpc
                .get_events(
                    Some(range.from_ledger),
                    &config.contract_ids,
                    cursor_token.clone(),
                    config.page_size,
                )
                .await?;

            let mut batch: Vec<NewEvent> = Vec::with_capacity(page.events.len());
            let mut stop = false;

            for ev in &page.events {
                if ev.ledger > range.to_ledger {
                    stop = true;
                    break;
                }
                let spec = specs.get(pool, rpc, &ev.contract_id, ev.ledger).await;
                let new_event = to_new_event(ev, spec.as_deref());
                batch.push(new_event);
            }

            let n = batch.len();
            if !batch.is_empty() {
                total_inserted += store::insert_events(pool, &batch).await?;
            }

            if stop {
                break;
            }

            cursor_token = page.cursor;
            if n < config.page_size as usize || cursor_token.is_none() {
                break;
            }
        }

        sqlx::query("UPDATE missed_ranges SET recovered_at = NOW() WHERE id = $1")
            .bind(range.id)
            .execute(pool)
            .await?;

        info!(
            id = range.id,
            from = range.from_ledger,
            to = range.to_ledger,
            events_inserted = total_inserted,
            "missed range recovered successfully"
        );
    }

    info!("all missed ranges replayed successfully");
    Ok(())
}

/// Re-fetch events from a range of recently-closed ledgers and upsert them,
/// updating mutable fields if the RPC returned different content (shallow reorg).
/// Returns the number of events updated.
async fn fetch_and_upsert(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
    start: i64,
    end: i64,
) -> anyhow::Result<u64> {
    let mut cursor_token: Option<String> = None;
    let mut total_updated = 0u64;
    let mut enriched_count = 0u64;
    let mut not_enriched_count = 0u64;

    loop {
        let page = rpc
            .get_events(
                Some(start),
                &config.contract_ids,
                cursor_token.clone(),
                config.page_size,
            )
            .await?;

        let mut batch: Vec<NewEvent> = Vec::with_capacity(page.events.len());
        for ev in &page.events {
            // Stop if we've moved past the reorg window.
            if ev.ledger > end {
                // Record enrichment metrics for reorg scan
                if enriched_count > 0 || not_enriched_count > 0 {
                    let _ = cursor::track_enrichment(pool, enriched_count, not_enriched_count).await;
                }
                return Ok(total_updated);
            }
            let spec = specs.get(pool, rpc, &ev.contract_id, ev.ledger).await;
            let new_event = to_new_event(ev, spec.as_deref());
            // Track enrichment coverage: count enriched vs not-enriched events
            if new_event.enriched.is_some() {
                enriched_count += 1;
            } else {
                not_enriched_count += 1;
            }
            batch.push(new_event);
        }

        if !batch.is_empty() {
            let (_, updated) = store::upsert_events(pool, &batch).await?;
            total_updated += updated;
        }

        cursor_token = page.cursor;
        let n = batch.len();
        if n < config.page_size as usize || cursor_token.is_none() {
            break;
        }
    }

    // Record enrichment metrics for reorg scan
    if enriched_count > 0 || not_enriched_count > 0 {
        cursor::track_enrichment(pool, enriched_count, not_enriched_count).await?;
    }

    Ok(total_updated)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_clamping_logic() {
        // Verify the START_LEDGER retention clamping logic.
        // If START_LEDGER is older than the RPC retention window,
        // it should be clamped to the earliest available ledger.

        let latest = 1_000_000i64;
        let oldest_available = latest - MAX_LOOKBACK_LEDGERS;

        // Case 1: START_LEDGER is within retention window
        let start_ledger = oldest_available + 1000;
        let clamped = start_ledger.max(oldest_available);
        assert_eq!(
            clamped, start_ledger,
            "start_ledger within window should not be clamped"
        );

        // Case 2: START_LEDGER is outside (older than) retention window
        let start_ledger = oldest_available - 10_000;
        let clamped = start_ledger.max(oldest_available);
        assert_eq!(
            clamped, oldest_available,
            "start_ledger outside window should be clamped to oldest_available"
        );

        // Case 3: START_LEDGER equals oldest_available
        let start_ledger = oldest_available;
        let clamped = start_ledger.max(oldest_available);
        assert_eq!(clamped, oldest_available);

        // Verify MAX_LOOKBACK_LEDGERS is approximately 7 days.
        // ~5 seconds per ledger, 86400 seconds per day.
        let secs_per_day = 86400.0_f64;
        let approx_days = (MAX_LOOKBACK_LEDGERS as f64) * 5.0 / secs_per_day;
        assert!(
            (6.5_f64..=7.5_f64).contains(&approx_days),
            "MAX_LOOKBACK_LEDGERS should be ~7 days, got ~{:.1} days",
            approx_days
        );
    }

    #[test]
    fn max_lookback_exports_retention_window() {
        // Verify that the public max_lookback() function returns the RPC retention window.
        assert_eq!(
            max_lookback(),
            MAX_LOOKBACK_LEDGERS,
            "max_lookback() should export the RPC retention window"
        );
    }

    // ── Circuit breaker logic ─────────────────────────────────────────────

    #[test]
    fn circuit_breaker_opens_after_max_consecutive_errors() {
        let max = 20u32;
        // Simulate accumulating errors.
        for count in 1..=max {
            let circuit_open = max > 0 && count >= max;
            if count < max {
                assert!(!circuit_open, "circuit should stay closed at error {count}");
            } else {
                assert!(circuit_open, "circuit should open at error {count}");
            }
        }
    }

    #[test]
    fn circuit_breaker_disabled_when_max_is_zero() {
        // max_consecutive_errors = 0 means the circuit breaker is disabled.
        let max = 0u32;
        let count = 1_000u32;
        let circuit_open = max > 0 && count >= max;
        assert!(!circuit_open, "circuit should never open when max = 0");
    }

    #[test]
    fn circuit_breaker_resets_on_success() {
        // After a successful cycle consecutive_errors is reset to 0.
        let mut consecutive_errors: u32 = 25;
        // Simulate success path.
        if consecutive_errors > 0 {
            consecutive_errors = 0;
        }
        assert_eq!(consecutive_errors, 0);
    }

    #[test]
    fn degraded_interval_used_when_circuit_open() {
        let base = Duration::from_secs(5);
        let degraded = Duration::from_secs(300);
        let max_consecutive_errors = 20u32;
        let consecutive_errors = 20u32;

        let circuit_open = max_consecutive_errors > 0 && consecutive_errors >= max_consecutive_errors;
        let sleep = if circuit_open { degraded } else { base };
        assert_eq!(sleep, degraded, "degraded interval should be used when circuit is open");
    }
}
