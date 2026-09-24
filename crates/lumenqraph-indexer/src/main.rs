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

    // Acquire a Postgres advisory lock to prevent concurrent indexer instances
    // from both running migrations and polling. The lock is held on a dedicated
    // connection that is never returned to the pool, so it cannot be silently
    // released while this process keeps running. Only one indexer can be active;
    // others block here and become hot standbys that take over on failure.
    let leader_lock = LeaderLock::acquire(&pool).await?;
    leader_lock.spawn_keepalive();

    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .context("failed to run migrations")?;

    if args.get(1).map(String::as_str) == Some("reenrich") {
        info!("running in reenrich mode");
        let result = reenrich::run_reenrich(pool.clone(), rpc, config).await;

        // Release the lock by dropping the dedicated connection.
        info!("releasing indexer leader lock");
        drop(leader_lock);

        return result;
    }

    if args.get(1).map(String::as_str) == Some("backfill") {
        let from = args
            .get(2)
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(config.start_ledger);
        info!(from, "running in backfill mode");
        let result = backfill::run(pool.clone(), rpc, config, from).await;

        // Release the lock by dropping the dedicated connection.
        info!("releasing indexer leader lock");
        drop(leader_lock);

        return result;
    }

    // deep-backfill: ingest beyond the RPC retention window from a data-lake
    // source. Parse manual args: --from, --to, --source, --input (repeatable).
    if args.get(1).map(String::as_str) == Some("deep-backfill") {
        let result = run_deep_backfill(args, pool.clone(), config).await;

        // Release the lock by dropping the dedicated connection.
        info!("releasing indexer leader lock");
        drop(leader_lock);

        return result;
    }

    info!(
        rpc = %config.rpc_url,
        contracts = ?config.contract_ids,
        poll_secs = config.poll_interval_secs,
        "starting lumenqraph indexer (live)"
    );

    // Start health/metrics HTTP server if configured
    if let Ok(health_addr) = std::env::var("INDEXER_HEALTH_ADDR") {
        let pool_arc = std::sync::Arc::new(pool.clone());
        let spec_cache_arc = std::sync::Arc::new(specs::SpecCache::new(config.spec_cache_max_entries));
        let spec_cache_for_poller = spec_cache_arc.clone();
        http::start_http_server(pool_arc, spec_cache_arc, &health_addr).await?;
        let result = poller::run(pool.clone(), rpc, config, spec_cache_for_poller).await;
        info!("releasing indexer leader lock");
        drop(leader_lock);
        return result;
    }

    let spec_cache = std::sync::Arc::new(specs::SpecCache::new(config.spec_cache_max_entries));
    let result = poller::run(pool.clone(), rpc, config, spec_cache).await;

    // Release the lock on shutdown by dropping the dedicated connection.
    info!("releasing indexer leader lock");
    drop(leader_lock);

    result
}

fn env_parse_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_parse_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// Fetch a contract's deployed WASM and print its parsed interface as JSON.
async fn inspect(rpc: &RpcClient, contract_id: &str) -> anyhow::Result<()> {
    if !lumenqraph_core::is_valid_contract_id(contract_id) {
        anyhow::bail!("invalid contract id {contract_id:?}:

/* … truncated 4326 chars — edit only what you need near the top … */
