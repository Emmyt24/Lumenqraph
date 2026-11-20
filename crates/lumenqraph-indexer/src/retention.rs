//! Retention: drop indexed history older than a rolling ledger window.
//!
//! Off by default — the indexer's normal posture is "keep everything". It exists
//! for deployments with a hard disk budget (free-tier Postgres is typically
//! ~500MB, while a hyperactive SAC emits ~500 events/ledger), where an unbounded
//! index would hit the cap in hours and stop accepting writes.
//!
//! Deletes run in bounded batches with a per-pass ceiling, so pruning a large
//! backlog costs many small transactions rather than one long lock — a shared-CPU
//! instance stays responsive to reads while it catches up.
//!
//! `token_transfers` cascades from `events` (FK `ON DELETE CASCADE`), so pruning
//! events prunes the projection with it.
//!
//! `webhook_deliveries` also cascades from `events`, but webhooks promise
//! at-least-once delivery with retries and redrive — so an event that still has
//! a *pending* delivery is skipped by the pruner, however far outside the window
//! it is. A subscriber that is down, auto-disabled, or backlogged therefore never
//! silently loses events. If a delivery is dropped anyway (e.g. a non-pending
//! delivery cascading away with its event), the count is reported in the
//! `lumenqraph_webhook_deliveries_dropped_by_retention_total` counter and logged.
//!
//! The versioned tables (`contract_state`, `contract_data`) are per-key time
//! series, so their *newest* row per key is current state, not history — it is
//! kept however old it is, and only superseded versions are pruned. Otherwise a
//! contract whose state last changed before the window would read as having no
//! state at all.

use sqlx::PgPool;
use tracing::{info, warn};

/// Cumulative count of `webhook_deliveries` rows removed as a side effect of
/// retention pruning (via the `events` FK cascade). Exposed as the
/// `lumenqraph_webhook_deliveries_dropped_by_retention_total` metric.
static WEBHOOK_DELIVERIES_DROPPED_BY_RETENTION: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Total `webhook_deliveries` rows dropped by retention since process start.
/// Exported for the metrics endpoint.
pub fn webhook_deliveries_dropped_by_retention_total() -> u64 {
    WEBHOOK_DELIVERIES_DROPPED_BY_RETENTION.load(std::sync::atomic::Ordering::Relaxed)
}

/// Rows per DELETE. Small enough that each transaction is short.
const BATCH: i64 = 5_000;

/// Max batches per table per pass, bounding the work one pass can do (and so the
/// time before the poller returns to the tip). A backlog drains over many passes.
const MAX_BATCHES: usize = 10;

/// Delete indexed data older than `tip - retention_ledgers`.
///
/// Returns the total rows deleted. A no-op (and cheap — an index-only probe)
/// once the tables are inside the window, which is the steady state.
pub async fn prune(pool: &PgPool, tip: i64, retention_ledgers: i64) -> anyhow::Result<u64> {
    if retention_ledgers <= 0 {
        return Ok(0);
    }
    let cutoff = tip - retention_ledgers;
    if cutoff <= 0 {
        // Window is longer than the chain's history so far; nothing to drop.
        return Ok(0);
    }

    // Skip events that still have a pending webhook delivery: deleting them would
    // cascade the delivery away and break the at-least-once promise. Such events
    // stay until their delivery is delivered or dropped by the webhook layer.
    let events = prune_batched(
        pool,
        "DELETE FROM events WHERE event_id IN (
             SELECT e.event_id FROM events e
              WHERE e.ledger < $1
                AND NOT EXISTS (
                    SELECT 1 FROM webhook_deliveries wd
                     WHERE wd.event_id = e.event_id AND wd.status = 'pending'
                )
              ORDER BY e.ledger
              LIMIT $2
         )",
        cutoff,
    )
    .await?;

    // Count deliveries that were dropped anyway (non-pending deliveries whose
    // event was pruned). Pending ones are protected above, so this is normally 0.
    let dropped = count_dropped_deliveries(pool, cutoff).await?;
    if dropped > 0 {
        WEBHOOK_DELIVERIES_DROPPED_BY_RETENTION
            .fetch_add(dropped, std::sync::atomic::Ordering::Relaxed);
        warn!(
            cutoff_ledger = cutoff,
            dropped, "retention dropped webhook deliveries (non-pending) with their events"
        );
    }

    // `ledger < MAX(ledger) for this key` is what spares the current version:
    // the newest row can never satisfy it.
    let state = prune_batched(
        pool,
        "DELETE FROM contract_state WHERE ctid IN (
             SELECT cs.ctid FROM contract_state cs
              WHERE cs.ledger < $1
                AND cs.ledger < (
                    SELECT MAX(x.ledger) FROM contract_state x
                     WHERE x.contract_id = cs.contract_id
                )
              LIMIT $2
         )",
        cutoff,
    )
    .await?;

    let data = prune_batched(
        pool,
        "DELETE FROM contract_data WHERE ctid IN (
             SELECT cd.ctid FROM contract_data cd
              WHERE cd.ledger < $1
                AND cd.ledger < (
                    SELECT MAX(x.ledger) FROM contract_data x
                     WHERE x.contract_id = cd.contract_id
                       AND x.key_hash = cd.key_hash
                )
              LIMIT $2
         )",
        cutoff,
    )
    .await?;

    let specs = prune_batched(
        pool,
        "DELETE FROM contract_spec_versions WHERE ctid IN (
             SELECT csv.ctid FROM contract_spec_versions csv
              WHERE csv.ledger < $1
                AND csv.version < (
                    SELECT MAX(x.version) FROM contract_spec_versions x
                     WHERE x.contract_id = csv.contract_id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM webhook_deliveries wd
                     WHERE wd.upgrade_id = csv.id
                       AND wd.status = 'pending'
                )
              LIMIT $2
         )",
        cutoff,
    )
    .await?;

    let total = events + state + data + specs;
    if total > 0 {
        info!(
            cutoff_ledger = cutoff,
            events, state, data, specs, "pruned history outside the retention window"
        );
    }
    Ok(total)
}

/// Delete old contract spec versions, respecting both the retention window and the
/// minimum number of versions to keep per contract.
///
/// Deletes versions that are:
/// 1. Outside the retention window (ledger < cutoff), AND
/// 2. NOT among the newest N versions per contract (where N = spec_version_retention)
///
/// If spec_version_retention is 0, uses only the retention window (like other tables).
pub async fn prune_spec_versions(
    pool: &PgPool,
    tip: i64,
    retention_ledgers: i64,
    spec_version_retention: i64,
) -> anyhow::Result<u64> {
    if retention_ledgers <= 0 {
        return Ok(0);
    }
    let cutoff = tip - retention_ledgers;
    if cutoff <= 0 {
        // Window is longer than the chain's history so far; nothing to drop.
        return Ok(0);
    }

    if spec_version_retention <= 0 {
        // No minimum version requirement; use only the retention window.
        // Keep the current version (newest version per contract).
        return prune_batched(
            pool,
            "DELETE FROM contract_spec_versions WHERE ctid IN (
                 SELECT csv.ctid FROM contract_spec_versions csv
                  WHERE csv.ledger < $1
                    AND csv.version < (
                        SELECT MAX(x.version) FROM contract_spec_versions x
                         WHERE x.contract_id = csv.contract_id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM webhook_deliveries wd
                         WHERE wd.upgrade_id = csv.id
                           AND wd.status = 'pending'
                    )
                  LIMIT $2
             )",
            cutoff,
        )
        .await;
    }

    // Keep the newest spec_version_retention versions per contract, even if outside window.
    let mut deleted = 0u64;
    for _ in 0..MAX_BATCHES {
        let n = sqlx::query(
            "DELETE FROM contract_spec_versions WHERE ctid IN (
                 SELECT csv.ctid FROM contract_spec_versions csv
                  WHERE csv.ledger < $1
                    AND csv.version < (
                        SELECT COALESCE(
                            (SELECT MIN(v.version) FROM (
                                SELECT version FROM contract_spec_versions
                                 WHERE contract_id = csv.contract_id
                                 ORDER BY version DESC
                                 LIMIT $3 OFFSET 1
                            ) v),
                            0
                        )
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM webhook_deliveries wd
                         WHERE wd.upgrade_id = csv.id
                           AND wd.status = 'pending'
                    )
                  LIMIT $2
             )",
        )
        .bind(cutoff)
        .bind(BATCH)
        .bind(spec_version_retention)
        .execute(pool)
        .await?
        .rows_affected();
        deleted += n;
        if n < BATCH as u64 {
            break;
        }
    }

    if deleted > 0 {
        info!(
            cutoff_ledger = cutoff,
            spec_version_retention,
            deleted,
            "pruned old contract spec versions outside the retention window"
        );
    }
    Ok(deleted)
}

/// Count `webhook_deliveries` rows that retention is about to drop: deliveries
/// whose event is outside the window and which are not pending (pending ones are
/// skipped by the event pruner). Must run *before* the events DELETE.
async fn count_dropped_deliveries(pool: &PgPool, cutoff: i64) -> anyhow::Result<u64> {
    let row: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM webhook_deliveries wd
          WHERE wd.status <> 'pending'
            AND EXISTS (
                SELECT 1 FROM events e
                 WHERE e.event_id = wd.event_id AND e.ledger < $1
            )",
    )
    .bind(cutoff)
    .fetch_one(pool)
    .await?;
    Ok(row.0 as u64)
}

/// Run `sql` (bound: $1 = cutoff ledger, $2 = batch size) until it stops
/// deleting or hits the per-pass ceiling.
async fn prune_batched(pool: &PgPool, sql: &str, cutoff: i64) -> anyhow::Result<u64> {
    let mut deleted = 0u64;
    for _ in 0..MAX_BATCHES {
        let n = sqlx::query(sql)
            .bind(cutoff)
            .bind(BATCH)
            .execute(pool)
            .await?
            .rows_affected();
        deleted += n;
        if n < BATCH as u64 {
            return Ok(deleted);
        }
    }
    // Still more to delete than one pass allows. Expected when retention is first
    // switched on over a big index; a standing warning means the write rate is
    // outrunning the pruner and the window (or BATCH) needs revisiting.
    warn!(
        deleted,
        cutoff_ledger = cutoff,
        "retention pass hit its batch ceiling; more rows remain (will continue next pass)"
    );
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    //! Needs a Postgres (schema is created by the migrations). Ignored by
    //! default; run with:
    //!   TEST_DATABASE_URL=postgres://…/lumenqraph \
    //!     cargo test -p lumenqraph-indexer -- --ignored --nocapture

    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Row;

    /// Fresh, isolated schema per test — safe for parallel execution.
    async fn fixture() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());

        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
            .execute(&admin)
            .await
            .expect("create test schema");
        admin.close().await;

        let option = format!("-c search_path={schema},public");
        let sep = if url.contains('?') { "&" } else { "?" };
        let schema_url = format!("{url}{sep}options={}", percent_encode(&option));
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&schema_url)
            .await
            .expect("connect with search_path");
        // Separate statements: sqlx prepares, and a prepared statement can only
        // carry one command.
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("migrate");
        pool
    }

    fn percent_encode(s: &str) -> String {
        s.chars()
            .flat_map(|c| match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => vec![c],
                c => format!("%{:02X}", c as u32).chars().collect(),
            })
            .collect()
    }

    async fn insert_event(pool: &PgPool, id: &str, ledger: i64) {
        sqlx::query(
            "INSERT INTO events (event_id, contract_id, ledger, ledger_closed_at, event_type,
                                 topics, event_name, value, tx_hash, in_successful_call, paging_token)
             VALUES ($1,'C1',$2,now(),'contract','[]','transfer','v','tx',true,$1)",
        )
        .bind(id)
        .bind(ledger)
        .execute(pool)
        .await
        .expect("insert event");
        // The projection that must disappear with its parent event.
        sqlx::query(
            "INSERT INTO token_transfers (event_id, contract_id, from_addr, to_addr, amount, ledger, ledger_closed_at)
             VALUES ($1,'C1','GA','GB','1',$2,now())",
        )
        .bind(id)
        .bind(ledger)
        .execute(pool)
        .await
        .expect("insert transfer");
    }

    async fn count(pool: &PgPool, sql: &str) -> i64 {
        sqlx::query(sql).fetch_one(pool).await.unwrap().get(0)
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn drops_events_older_than_the_window_and_cascades_transfers() {
        let pool = fixture().await;
        insert_event(&pool, "old", 100).await;
        insert_event(&pool, "new", 900).await;

        // tip 1000, keep 500 => cutoff 500.
        let deleted = prune(&pool, 1000, 500).await.expect("prune");

        assert_eq!(deleted, 1, "only the pre-cutoff event should go");
        assert_eq!(count(&pool, "SELECT count(*) FROM events").await, 1);
        assert_eq!(
            count(&pool, "SELECT count(*) FROM events WHERE event_id='new'").await,
            1,
            "in-window event must survive"
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM token_transfers").await,
            1,
            "the pruned event's transfer should cascade away with it"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn keeps_the_newest_state_version_even_when_older_than_the_window() {
        let pool = fixture().await;
        // C1 changed twice, both before the cutoff: the newer row is still C1's
        // *current* state, so pruning it would blank the contract entirely.
        for ledger in [10i64, 20] {
            sqlx::query(
                "INSERT INTO contract_state (contract_id, ledger, storage) VALUES ('C1',$1,'{}')",
            )
            .bind(ledger)
            .execute(&pool)
            .await
            .unwrap();
        }
        // Two versions of one key, plus a different key on the same contract.
        for (key_hash, ledger) in [("k1", 10i64), ("k1", 20), ("k2", 10)] {
            sqlx::query(
                "INSERT INTO contract_data (contract_id, key_hash, key, key_xdr, durability, ledger, value)
                 VALUES ('C1',$1,'[]','xdr','persistent',$2,'{}')",
            )
            .bind(key_hash)
            .bind(ledger)
            .execute(&pool)
            .await
            .unwrap();
        }

        prune(&pool, 1000, 500).await.expect("prune");

        assert_eq!(
            count(&pool, "SELECT count(*) FROM contract_state").await,
            1,
            "superseded state versions go, the latest stays"
        );
        assert_eq!(
            count(&pool, "SELECT ledger FROM contract_state").await,
            20,
            "the surviving state row must be the newest"
        );
        // k1 keeps only its ledger-20 row; k2's sole row is current, so it stays.
        assert_eq!(count(&pool, "SELECT count(*) FROM contract_data").await, 2);
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM contract_data WHERE key_hash='k1' AND ledger=20"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM contract_data WHERE key_hash='k2'"
            )
            .await,
            1,
            "a key with only one version must never be pruned to nothing"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn boundary_ledger_is_kept() {
        let pool = fixture().await;
        // cutoff = tip - retention_ledgers = 1000 - 500 = 500.
        // The DELETE condition is `ledger < 500` (strict less-than), so:
        //   ledger 499 → deleted (strictly below cutoff)
        //   ledger 500 → kept   (equal to cutoff — not strictly below)
        //   ledger 501 → kept   (above cutoff)
        insert_event(&pool, "below", 499).await;
        insert_event(&pool, "at_floor", 500).await;
        insert_event(&pool, "above", 501).await;

        let deleted = prune(&pool, 1000, 500).await.expect("prune");
        assert_eq!(
            deleted, 1,
            "only the row strictly below the cutoff is pruned"
        );

        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM events WHERE event_id = 'at_floor'"
            )
            .await,
            1,
            "row at ledger == cutoff must survive (condition is `ledger < cutoff`, not `<=`)"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM events WHERE event_id = 'above'"
            )
            .await,
            1,
            "row above the cutoff must survive"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM events WHERE event_id = 'below'"
            )
            .await,
            0,
            "row strictly below the cutoff must be removed"
        );
        // token_transfers cascade: the transfer for 'below' is gone, the two others remain.
        assert_eq!(
            count(&pool, "SELECT count(*) FROM token_transfers").await,
            2,
            "only the pruned event's transfer cascades away; in-window transfers survive"
        );
    }

    // -------------------------------------------------------------------------
    // Issue #601: retention must not drop pending webhook deliveries
    // -------------------------------------------------------------------------

    async fn insert_delivery(pool: &PgPool, id: &str, event_id: &str, status: &str) {
        sqlx::query(
            "INSERT INTO webhook_deliveries (delivery_id, subscription_id, event_id, status)
             VALUES ($1, 'sub1', $2, $3)",
        )
        .bind(id)
        .bind(event_id)
        .bind(status)
        .execute(pool)
        .await
        .expect("insert delivery");
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn pruning_keeps_events_with_pending_deliveries() {
        let pool = fixture().await;
        insert_event(&pool, "pending_evt", 100).await;
        insert_event(&pool, "plain_evt", 150).await;
        insert_delivery(&pool, "d1", "pending_evt", "pending").await;

        // tip 1000, keep 500 => cutoff 500; both events are outside the window.
        let deleted = prune(&pool, 1000, 500).await.expect("prune");

        assert_eq!(
            deleted, 1,
            "only the event without a pending delivery should be pruned"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM events WHERE event_id = 'pending_evt'"
            )
            .await,
            1,
            "an event with a pending delivery must survive retention"
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM webhook_deliveries").await,
            1,
            "the pending delivery must not be cascaded away"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM webhook_deliveries WHERE status = 'pending'"
            )
            .await,
            1,
            "the surviving delivery must still be pending"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn disabled_and_young_chain_are_no_ops() {
        let pool = fixture().await;
        insert_event(&pool, "old", 1).await;

        assert_eq!(
            prune(&pool, 1000, 0).await.unwrap(),
            0,
            "0 = keep everything"
        );
        // Window reaches back past ledger 0 — nothing is outside it yet.
        assert_eq!(prune(&pool, 100, 500).await.unwrap(), 0);
        assert_eq!(count(&pool, "SELECT count(*) FROM events").await, 1);
    }

    // -------------------------------------------------------------------------
    // Issue #225: contract_summaries trigger DELETE handling
    // -------------------------------------------------------------------------

    async fn summary_event_count(pool: &PgPool, contract_id: &str) -> Option<i64> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT event_count FROM contract_summaries WHERE contract_id = $1")
                .bind(contract_id)
                .fetch_optional(pool)
                .await
                .expect("query contract_summaries");
        row.map(|(c,)| c)
    }

    /// Retention pruning must decrement `contract_summaries.event_count` for every
    /// deleted event, and remove the row entirely when the count reaches zero.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn contract_summaries_decrements_on_event_delete() {
        let pool = fixture().await;

        // Insert three events for the same contract.
        insert_event(&pool, "e1", 100).await;
        insert_event(&pool, "e2", 200).await;
        insert_event(&pool, "e3", 300).await;

        // Sanity check: the trigger should have maintained the count during inserts.
        let before = summary_event_count(&pool, "C1")
            .await
            .expect("summary row should exist after inserts");
        assert_eq!(
            before, 3,
            "insert trigger must have counted all three events"
        );

        // Prune events older than ledger 250 (tip=1000, retention=750 → cutoff=250).
        // Events e1 (ledger=100) and e2 (ledger=200) are below the cutoff; e3 survives.
        let deleted = prune(&pool, 1000, 750).await.expect("prune");
        assert_eq!(deleted, 2, "two events should be pruned");

        let after = summary_event_count(&pool, "C1")
            .await
            .expect("summary row should still exist because one event survives");
        assert_eq!(after, 1, "event_count must reflect the one surviving event");

        // Prune the last event (cutoff=350 > ledger=300).
        let deleted2 = prune(&pool, 1000, 650).await.expect("prune again");
        assert_eq!(deleted2, 1, "the final event should be pruned");

        let final_row = summary_event_count(&pool, "C1").await;
        assert!(
            final_row.is_none(),
            "summary row must be removed when event_count would reach zero; \
             list_contracts must never expose contracts with zero events"
        );
    }

    /// first_seen_ledger and last_seen_ledger must stay accurate after pruning.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn contract_summaries_ledger_bounds_after_prune() {
        let pool = fixture().await;

        // Three events spanning a wide ledger range.
        insert_event(&pool, "b1", 50).await;
        insert_event(&pool, "b2", 500).await;
        insert_event(&pool, "b3", 900).await;

        // Prune events before ledger 200 (cutoff=200 → only b1 deleted).
        prune(&pool, 1000, 800).await.expect("prune");

        let row: (i64, i64, i64) = sqlx::query_as(
            "SELECT event_count, first_seen_ledger, last_seen_ledger
               FROM contract_summaries WHERE contract_id = 'C1'",
        )
        .fetch_one(&pool)
        .await
        .expect("summary row");

        let (count_val, first, last) = row;
        assert_eq!(count_val, 2, "two events survive");
        assert_eq!(
            first, 500,
            "first_seen_ledger must advance to the next surviving event"
        );
        assert_eq!(
            last, 900,
            "last_seen_ledger must remain at the newest event"
        );
    }

    // -------------------------------------------------------------------------
    // Issue #602: pruning a spec version referenced by a pending upgrade
    // delivery must not strand the delivery (no retries, no auto-disable).
    // -------------------------------------------------------------------------

    /// Insert a spec version row and return its id.
    async fn insert_spec_version(
        pool: &PgPool,
        contract_id: &str,
        version: i32,
        ledger: i64,
    ) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO contract_spec_versions (contract_id, version, ledger, spec_hash, spec)
             VALUES ($1,$2,$3,'hash','{}')
             RETURNING id",
        )
        .bind(contract_id)
        .bind(version)
        .bind(ledger)
        .fetch_one(pool)
        .await
        .expect("insert spec version")
    }

    /// A pending upgrade delivery referencing an old spec version must keep that
    /// version alive through pruning, so the dispatcher can still resolve it and
    /// the delivery is not retried into failure (which would count toward the
    /// subscriber's auto-disable threshold).
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn prune_keeps_spec_version_referenced_by_pending_upgrade_delivery() {
        let pool = fixture().await;

        // Two versions of C1; the older one is referenced by a pending delivery.
        let old_id = insert_spec_version(&pool, "C1", 1, 10).await;
        let _new_id = insert_spec_version(&pool, "C1", 2, 20).await;

        // A subscription and a pending upgrade delivery pointing at the old version.
        let sub_id: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_subscriptions (url, secret, contract_id, events)
             VALUES ('https://example.test/hook','secret','C1','[\"upgrade\"]')
             RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .expect("insert subscription");
        sqlx::query(
            "INSERT INTO webhook_deliveries
                 (subscription_id, event_id, upgrade_id, payload, status, attempts, next_attempt_at)
             VALUES ($1,NULL,$2,'{}','pending',0,now())",
        )
        .bind(sub_id)
        .bind(old_id)
        .execute(&pool)
        .await
        .expect("insert delivery");

        // Prune with a window that would otherwise drop the old version.
        prune_spec_versions(&pool, 1000, 500, 1)
            .await
            .expect("prune spec versions");

        // The referenced version must survive so the delivery can still resolve it.
        assert_eq!(
            count(
                &pool,
                &format!("SELECT count(*) FROM contract_spec_versions WHERE id = {old_id}")
            )
            .await,
            1,
            "a spec version referenced by a pending upgrade delivery must not be pruned"
        );

        // The delivery is still pending (never retried into failure).
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM webhook_deliveries WHERE status = 'pending' AND attempts = 0"
            )
            .await,
            1,
            "the pending upgrade delivery must remain pending with no attempts consumed"
        );

        // The subscription must not have been auto-disabled.
        assert_eq!(
            count(
                &pool,
                &format!("SELECT count(*) FROM webhook_subscriptions WHERE id = {sub_id} AND enabled = true")
            )
            .await,
            1,
            "a healthy upgrade subscriber must not be auto-disabled by pruning"
        );
    }

    /// A contract that was never pruned must not be affected by pruning another
    /// contract's events.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn contract_summaries_isolation_across_contracts() {
        let pool = fixture().await;

        // Two events for C1 and one for a second contract C2.
        insert_event(&pool, "c1e1", 100).await;
        insert_event(&pool, "c1e2", 800).await;
        // Insert a C2 event manually (insert_event always uses contract_id='C1').
        sqlx::query(
            "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type,
                  topics, event_name, value, tx_hash, in_successful_call, paging_token)
             VALUES ('c2e1','C2',600,now(),'contract','[]','mint','v','tx',true,'c2e1')",
        )
        .execute(&pool)
        .await
        .expect("insert C2 event");

        // Prune events before ledger 200 (removes c1e1 only).
        prune(&pool, 1000, 800).await.expect("prune");

        let c1 = summary_event_count(&pool, "C1").await.expect("C1 row");
        let c2 = summary_event_count(&pool, "C2").await.expect("C2 row");
        assert_eq!(c1, 1, "C1 should have one surviving event");
        assert_eq!(c2, 1, "C2 should be unaffected by C1's prune");
    }
}
