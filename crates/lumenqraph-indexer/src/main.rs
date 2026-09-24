//! Lumenqraph indexer — an always-on process that tails Soroban RPC and writes
//! decoded events into Postgres. It talks to nothing but the RPC and its own DB.
//!
//! Usage:
//!   lumenqraph-indexer                    # live tail (default)
//!   lumenqraph-indexer backfill [LEDGER]  # one-shot catch-up within RPC window (~7 days) then exit
//!   lumenqraph-indexer deep-backfill [OPTIONS]  # gapless history from a data-lake export (#84)
//!   lumenqraph-indexer reenrich          # re-enrich historical events with newly-available specs
//!   lumenqraph-indexer inspect <CONTRACT> # print a contract's on-chain interface
//!
//! deep-backfill options:
//!   --from <LEDGER>   Start ledger (required)
//!   --to   <LEDGER>   End ledger   (default: max / run to EOF of input)
//!   --source <TYPE>   Source type: galexie (default: galexie)
//!   --input <PATH>    Input file(s); use '-' for stdin; may be repeated
//!
//! Concurrency model:
//!   The live poller elects a single active instance via the leader advisory
//!   lock (`INDEXER_LOCK_ID`). One-shot maintenance commands (`backfill`,
//!   `reenrich`, `deep-backfill`) do NOT take the leader lock: their writes are
//!   idempotent (`ON CONFLICT DO NOTHING`) and they never advance the live
//!   cursor, so they can safely run alongside a live indexer. Migrations run
//!   under a short, separate migration lock (`MIGRATION_LOCK_ID`) so they are
//!   serialized without blocking on the leader lock.

mod backfill;
mod config;
mod convert;
mod cursor;
mod deep_backfill;
mod http;
mod keys;
mod poller;
mod reenrich;
mod retention;
mod rpc_client;
mod specs;
mod state;
mod store;
// The end-to-end smoke test is gated behind the `smoke-tests` feature (as well
// as `#[ignore]`) so it is never compiled or run by a plain `cargo test`,
// including in offline CI. See CONTRIBUTING.md → "Smoke tests".
#[cfg(all(test, feature = "smoke-tests"))]
mod smoke;

use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use config::Config;
use rpc_client::RpcClient;

/// Postgres advisory lock id used to elect a single active indexer.
const INDEXER_LOCK_ID: i64 = 0x6c756d656e717261; // "lumenqra" as i64

/// Postgres advisory lock id used to serialize migrations across processes.
/// This is deliberately distinct from `INDEXER_LOCK_ID` so that running
/// migrations never blocks on (or is blocked by) the live leader lock.
const MIGRATION_LOCK_ID: i64 = 0x6c756d656e717262; // "lumenqrb" as i64

/// A leader lock held on a dedicated Postgres connection that is never
/// returned to the pool. Advisory locks are session-scoped, so the lock lives
/// exactly as long as this connection. If the connection drops (idle timeout,
/// network blip, `pg_terminate_backend`), the lock is released by Postgres and
/// the holder must stop polling so a standby can take over.
struct LeaderLock {
    conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
}

impl LeaderLock {
    /// Acquire the leader lock on a dedicated connection. If another instance
    /// holds it, block until it is released (hot standby).
    async fn acquire(pool: &sqlx::PgPool) -> anyhow::Result<Self> {
        // Detach a connection from the pool so it is never handed back while
        // we hold the session-scoped advisory lock.
        let mut conn = pool
            .acquire()
            .await
            .context("failed to acquire dedicated connection for leader lock")?
            .detach();

        info!("acquiring indexer leader lock (id {})", INDEXER_LOCK_ID);
        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(INDEXER_LOCK_ID)
            .fetch_one(&mut *conn)
            .await
            .context("failed to acquire advisory lock")?;

        if !acquired {
            info!(
                "another indexer instance holds the leader lock; \
                 blocking until it releases (this instance will become a hot standby)"
            );
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(INDEXER_LOCK_ID)
                .execute(&mut *conn)
                .await
                .context("failed to acquire advisory lock (blocking)")?;
        }

        info!("indexer leader lock acquired; this instance is now active");
        Ok(Self { conn })
    }

    /// Spawn a task that pings the lock-holding connection periodically. If the
    /// connection is lost, the task exits the process so the orchestrator can
    /// restart it and a standby can take over. This makes leadership loss fail
    /// fast instead of silently continuing to poll without the lock.
    fn spawn_keepalive(&self) {
        let mut conn = self.conn.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if let Err(err) = sqlx::query("SELECT 1").execute(&mut *conn).await {
                    tracing::error!(
                        error = %err,
                        "leader lock connection lost; stopping indexer to avoid split-brain"
                    );
                    std::process::exit(1);
                }
            }
        });
    }
}

/// Run migrations under a short, dedicated advisory lock so concurrent
/// processes serialize migrations without contending on the leader lock.
/// The lock is held on a dedicated connection for the duration of the run and
/// released when that connection is dropped.
async fn run_migrations(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire dedicated connection for migration lock")?
        .detach();

    info!("acquiring migration lock (id {})", MIGRATION_LOCK_ID);
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut *conn)
        .await
        .context("failed to acquire migration lock")?;

    let result = sqlx::migrate!("../../migrations")
        .run(pool)
        .await
        .context("failed to run migrations");

    // Release the migration lock by dropping the dedicated connection.
    drop(conn);

    result
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.get(1).map(|s| s.as_str()) == Some("--version") {
        println!(
            "lumenqraph-indexer {}\ncommit: {}\nbuilt: {}",
            env!("CARGO_PKG_VERSION"),
            option_env!("LUMENQRAPH_GIT_SHA").unwrap_or("unknown"),
            option_env!("LUMENQRAPH_BUILD_TIME").unwrap_or("unknown"),
        );
        return Ok(());
    }

    let _ = dotenvy::dotenv();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    let config = Config::from_env()?;
    let rpc = RpcClient::new(config.rpc_url.clone(), config.rpc_timeout_secs);

    // `inspect` needs only RPC — handle it before touching the database.
    if args.get(1).map(String::as_str) == Some("inspect") {
        let contract_id = args
            .get(2)
            .context("usage: lumenqraph-indexer inspect <contract_id>")?;
        return inspect(&rpc, contract_id).await;
    }

    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .min_connections(config.database_min_connections)
        .acquire_timeout(Duration::from_secs(env_parse_u64(
            "DATABASE_ACQUIRE_TIMEOUT_SECS",
            30,
        )))
        .idle_timeout(Duration::from_secs(env_parse_u64(
            "DATABASE_IDLE_TIMEOUT_SECS",
            600,
        )))
        .connect(&config.database_url)
        .await
        .context("failed to connect to Postgres")?;

    // One-shot maintenance commands (`backfill`, `reenrich`, `deep-backfill`)
    // must be runnable alongside a live indexer. Their writes are idempotent
    // (`ON CONFLICT DO NOTHING`) and they never advance the live cursor, so
    // they take neither the leader lock nor any other exclusive lock. Only the
    // live poller elects a leader.
    let subcommand = args.get(1).map(String::as_str);
    let is_maintenance = matches!(subcommand, Some("backfill") | Some("reenrich") | Some("deep-backfill"));

    // Migrations run under a short, separate migration lock so they are
    // serialized across processes without contending on the leader lock.
    run_migrations(&pool).await?;

    if subcommand == Some("reenrich") {
        info!("running in reenrich mode");
        return reenrich::run_reenrich(pool.clone(), rpc, config).await;
    }

    if subcommand == Some("backfill") {
        let from = args
            .get(2)
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(config.start_ledger);
        info!(from, "running in backfill mode");
        return backfill::run(pool.clone(), rpc, config, from).await;
    }

    // deep-backfill: ingest beyond the RPC retention window from a data-lake
    // source. Parse manual args: --from, --to, --source, --input (repeatable).
    if subcommand == Some("deep-backfill") {
        return run_deep_backfill(args, pool.clone(), config).await;
    }

    // Live tail: elect a single active indexer via the leader lock. Others
    // block here and become hot standbys that take over on failure.
    debug_assert!(!is_maintenance);
    let leader_lock = LeaderLock::acquire(&pool).await?;
    leader_lock.spawn_keepalive();

    let result = poller::run(pool.clone(), rpc, config).await;

    // Release the lock by dropping the dedicated connection.
    info!("releasing indexer leader lock");
    drop(leader_lock);

    result
}
