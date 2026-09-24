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
use clap::{Parser, Subcommand};
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

/// Lumenqraph indexer — tails Soroban RPC and writes decoded events into Postgres.
#[derive(Parser)]
#[command(
    name = "lumenqraph-indexer",
    version,
    about = "Lumenqraph indexer — tails Soroban RPC and writes decoded events into Postgres",
    long_version = concat!(
        env!("CARGO_PKG_VERSION"),
        "\ncommit: ",
        option_env!("LUMENQRAPH_GIT_SHA").unwrap_or("unknown"),
        "\nbuilt: ",
        option_env!("LUMENQRAPH_BUILD_TIME").unwrap_or("unknown"),
    ),
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Live tail (default when no subcommand is given).
    Run,
    /// One-shot catch-up within the RPC window (~7 days) then exit.
    Backfill {
        /// Start ledger (defaults to the current cursor).
        ledger: Option<u32>,
    },
    /// Gapless history from a data-lake export (#84).
    DeepBackfill {
        /// Start ledger (required).
        #[arg(long)]
        from: u32,
        /// End ledger (default: max / run to EOF of input).
        #[arg(long)]
        to: Option<u32>,
        /// Source type: galexie (default: galexie).
        #[arg(long, default_value = "galexie")]
        source: String,
        /// Input file(s); use '-' for stdin; may be repeated.
        #[arg(long = "input")]
        input: Vec<String>,
    },
    /// Re-enrich historical events with newly-available specs.
    Reenrich {
        /// Restrict re-enrichment to a single contract.
        #[arg(long)]
        contract: Option<String>,
        /// Re-enrich even events that already have a spec.
        #[arg(long)]
        force: bool,
    },
    /// Print a contract's on-chain interface.
    Inspect {
        /// Contract id to inspect.
        contract_id: String,
    },
    /// Run database migrations and exit.
    Migrate,
}

/// Parse the optional `backfill` ledger argument.
///
/// Returns `Ok(None)` when the argument is absent (caller falls back to
/// `START_LEDGER`). Returns an error when the argument is present but is not a
/// valid positive integer, so a typo like `51_000_000` or `5100000O` fails
/// loudly instead of silently backfilling a different range.
fn parse_backfill_ledger(arg: Option<&str>) -> anyhow::Result<Option<u32>> {
    match arg {
        None => Ok(None),
        Some(raw) => match raw.parse::<u32>() {
            Ok(ledger) if ledger > 0 => Ok(Some(ledger)),
            _ => anyhow::bail!(
                "invalid ledger \"{}\": expected a positive integer",
                raw
            ),
        },
    }
}

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
        

/* … truncated 1692 chars — edit only what you need near the top … */
