//! HTTP server for health and metrics endpoints on the indexer.

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};

use crate::specs::SpecCache;

#[derive(Clone)]
pub struct HttpState {
    pub pool: Arc<PgPool>,
    /// Reference to the live spec cache so its size can be reported in metrics.
    pub spec_cache: Arc<SpecCache>,
    /// Optional bearer token required for `/metrics` (#605). When `None`, the
    /// endpoint is unauthenticated (rely on binding to a private address).
    pub metrics_bearer_token: Option<Arc<String>>,
}

/// Reads the optional metrics bearer token from the environment.
///
/// `METRICS_BEARER_TOKEN` takes precedence; for parity with the API's
/// `METRICS_REQUIRE_API_KEY` semantics, `METRICS_API_KEY` is also accepted.
/// Empty values are treated as "no auth configured".
pub fn metrics_bearer_token_from_env() -> Option<Arc<String>> {
    for key in ["METRICS_BEARER_TOKEN", "METRICS_API_KEY"] {
        if let Ok(v) = std::env::var(key) {
            let v = v.trim();
            if !v.is_empty() {
                return Some(Arc::new(v.to_string()));
            }
        }
    }
    None
}

pub async fn start_http_server(
    pool: Arc<PgPool>,
    spec_cache: Arc<SpecCache>,
    bind_addr: &str,
) -> anyhow::Result<()> {
    let state = HttpState {
        pool,
        spec_cache,
        metrics_bearer_token: metrics_bearer_token_from_env(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let addr: SocketAddr = listener.local_addr()?;

    info!(addr = %addr, "indexer health/metrics listener started");

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        // Never signals shutdown — health/metrics server runs until process dies
        std::future::pending().await
    });

    tokio::spawn(async move {
        if let Err(e) = server.await {
            error!(error = %e, "health/metrics server error");
        }
    });

    Ok(())
}

async fn health(State(state): State<HttpState>) -> impl IntoResponse {
    match get_indexer_status(&state.pool).await {
        Ok(status) => {
            let health_status = if status.is_healthy { "ok" } else { "degraded" };
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "status": health_status,
                    "last_processed_ledger": status.last_processed_ledger,
                    "chain_tip_ledger": status.chain_tip_ledger,
                    "lag_ledgers": status.lag_ledgers,
                    "seconds_since_cursor_update": status.secs_since_update,
                    "unrecovered_missed_ranges": status.unrecovered_missed_ranges,
                    "recoverable_missed_ranges": status.recoverable_missed_ranges,
                    "unrecoverable_missed_ranges": status.unrecoverable_missed_ranges,
                })),
            )
        }
        Err(_) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "unavailable",
                "error": "failed to fetch indexer status"
            })),
        ),
    }
}

async fn metrics(
    State(state): State<HttpState>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Some(expected) = state.metrics_bearer_token.as_deref() {
        if !bearer_token_matches(&headers, expected) {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "unauthorized".to_string(),
            );
        }
    }

    let mut body = String::new();

    match gather_metrics(&state.pool).await {
        Ok(metrics) => {
            body.push_str("# HELP lumenqraph_indexer_last_processed_ledger The last ledger processed by the indexer\n");
            body.push_str("# TYPE lumenqraph_indexer_last_processed_ledger gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_last_processed_ledger {}\n",
                metrics.last_processed_ledger
            ));

            body.push_str(
                "# HELP lumenqraph_indexer_chain_tip_ledger The current chain tip ledger\n",
            );
            body.push_str("# TYPE lumenqraph_indexer_chain_tip_ledger gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_chain_tip_ledger {}\n",
                metrics.chain_tip_ledger
            ));

            body.push_str("# HELP lumenqraph_indexer_lag_ledgers Lag between indexer and chain tip in ledgers\n");
            body.push_str("# TYPE lumenqraph_indexer_lag_ledgers gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_lag_ledgers {}\n",
                metrics.lag_ledgers
            ));

            body.push_str(
                "# HELP lumenqraph_indexer_seconds_since_update Seconds since last cursor update\n",
            );
            body.push_str("# TYPE lumenqraph_indexer_seconds_since_update gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_seconds_since_update {}\n",
                metrics.secs_since_update
            ));

            body.push_str(
                "# HELP lumenqraph_indexer_events_ingested_total Total events ingested\n",
            );
            body.push_str("# TYPE lumenqraph_indexer_events_ingested_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_events_ingested_total {}\n",
                metrics.events_ingested_total
            ));

            body.push_str("# HELP lumenqraph_indexer_errors_total Total indexer errors\n");
            body.push_str("# TYPE lumenqraph_indexer_errors_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_errors_total {}\n",
                metrics.errors_total
            ));

            body.push_str("# HELP lumenqraph_enrichment_rate Fraction of events successfully enriched (0.0 to 1.0)\n");
            body.push_str("# TYPE lumenqraph_enrichment_rate gauge\n");
            body.push_str(&format!(
                "lumenqraph_enrichment_rate {}\n",
                metrics.enrichment_rate
            ));

            // Spec cache size gauge (issue #268).
            let cache_size = state.spec_cache.cache_size();
            body.push_str("# HELP lumenqraph_spec_cache_size Current number of entries in the in-memory spec cache\n");
            body.push_str("# TYPE lumenqraph_spec_cache_size gauge\n");
            body.push_str(&format!("lumenqraph_spec_cache_size {cache_size}\n"));

            // RPC failover counter (#398).
            body.push_str("# HELP lumenqraph_indexer_rpc_failovers_total Total number of RPC endpoint failovers\n");
            body.push_str("# TYPE lumenqraph_indexer_rpc_failovers_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_rpc_failovers_total {}\n",
                metrics.rpc_failovers_total
            ));

            // Timestamp parse error counter (#399).
            body.push_str("# HELP lumenqraph_indexer_timestamp_parse_errors_total Total ledgerClosedAt parse failures (never stored as Utc::now())\n");
            body.push_str("# TYPE lumenqraph_indexer_timestamp_parse_errors_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_timestamp_parse_errors_total {}\n",
                metrics.timestamp_parse_errors_total
            ));

            body.push_str("# HELP lumenqraph_rpc_oldest_ledger Oldest ledger currently available from the RPC provider\n");
            body.push_str("# TYPE lumenqraph_rpc_oldest_ledger gauge\n");
            body.push_str(&format!(
                "lumenqraph_rpc_oldest_ledger {}\n",
                crate::poller::rpc_oldest_ledger()
            ));
            body.push_str("# HELP lumenqraph_indexer_unrecovered_missed_ranges Number of ledger ranges not yet recovered\n");
            body.push_str("# TYPE lumenqraph_indexer_unrecovered_missed_ranges gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_unrecovered_missed_ranges {}\n",
                metrics.unrecovered_missed_ranges
            ));
            body.push_str("# HELP lumenqraph_indexer_recoverable_missed_ranges Unrecovered ranges still within RPC retention\n");
            body.push_str("# TYPE lumenqraph_indexer_recoverable_missed_ranges gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_recoverable_missed_ranges {}\n",
                metrics.recoverable_missed_ranges
            ));
            body.push_str("# HELP lumenqraph_indexer_unrecoverable_missed_ranges Unrecovered ranges outside RPC retention\n");
            body.push_str("# TYPE lumenqraph_indexer_unrecoverable_missed_ranges gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_unrecoverable_missed_ranges {}\n",
                metrics.unrecoverable_missed_ranges
            ));
            render_process_counters(&mut body);
        }
        Err(e) => {
            error!(error = %e, "failed to gather indexer metrics");
        }
    }

    (
        axum::http::StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

/// Renders the process-wide counters that do not require a database round-trip.
///
/// Kept as a pure function so the rendering path can be unit tested without a
/// live `PgPool` (see `tests::metrics_render_process_counters`).
fn render_process_counters(body: &mut String) {
    // Transfer-like events skipped by the SEP-41 projection (#600).
    body.push_str("# HELP lumenqraph_transfer_projection_skipped_total Transfer-like events skipped by the SEP-41 projection since start\n");
    body.push_str("# TYPE lumenqraph_transfer_projection_skipped_total counter\n");
    body.push_str(&format!(
        "lumenqraph_transfer_projection_skipped_total {}\n",
        crate::store::transfer_projection_skipped_total()
    ));
    // webhook_deliveries rows dropped by retention (#601).
    body.push_str("# HELP lumenqraph_webhook_deliveries_dropped_by_retention_total webhook_deliveries rows dropped by retention since start\n");
    body.push_str("# TYPE lumenqraph_webhook_deliveries_dropped_by_retention_total counter\n");
    body.push_str(&format!(
        "lumenqraph_webhook_deliveries_dropped_by_retention_total {}\n",
        crate::retention::webhook_deliveries_dropped_by_retention_total()
    ));
}

/// Constant-time-ish comparison of the `Authorization: Bearer <token>` header.
fn bearer_token_matches(headers: &axum::http::HeaderMap, expected: &str) -> bool {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Some(token) = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
    else {
        return false;
    };
    let token = token.trim();
    // Length check first, then byte-wise comparison without early exit.
    if token.len() != expected.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in token.bytes().zip(expected.bytes()) {
        diff |= a ^ b;
    }
    diff == 0
}

struct IndexerStatus {
    last_processed_ledger: i64,
    chain_tip_ledger: i64,
    lag_ledgers: i64,
    secs_since_update: i64,
    is_healthy: bool,
    unrecovered_missed_ranges: i64,
    recoverable_missed_ranges: i64,
    unrecoverable_missed_ranges: i64,
}

async fn get_indexer_status(pool: &PgPool) -> anyhow::Result<IndexerStatus> {
    let status: Option<(i64, i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, updated_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    let Some((last, tip, updated_at)) = status else {
        anyhow::bail!("indexer cursor not initialized");
    };

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (Utc::now() - updated_at).num_seconds();

    let max_stale_secs = env_parse_i64("HEALTH_MAX_STALE_SECS", 120);
    let max_lag_ledgers = env_parse_i64("HEALTH_MAX_LAG_LEDGERS", 100);
    let is_healthy = secs_since_update <= max_stale_secs && lag_ledgers < max_lag_ledgers;
    let (recoverable_missed_ranges, unrecoverable_missed_ranges): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) FILTER (WHERE recoverable), COUNT(*) FILTER (WHERE NOT recoverable)
         FROM missed_ranges WHERE recovered_at IS NULL",
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0, 0));
    let unrecovered_missed_ranges = recoverable_missed_ranges + unrecoverable_missed_ranges;

    Ok(IndexerStatus {
        last_processed_ledger: last,
        chain_tip_ledger: tip,
        lag_ledgers,
        secs_since_update,
        is_healthy,
        unrecovered_missed_ranges,
        recoverable_missed_ranges,
        unrecoverable_missed_ranges,
    })
}

struct IndexerMetrics {
    last_processed_ledger: i64,
    chain_tip_ledger: i64,
    lag_ledgers: i64,
    secs_since_update: i64,
    events_ingested_total: i64,
    errors_total: i64,
    enrichment_rate: f64,
    /// Cumulative RPC endpoint failovers (#398).
    rpc_failovers_total: i64,
    /// Cumulative timestamp parse errors (#399).
    timestamp_parse_errors_total: i64,
    unrecovered_missed_ranges: i64,
    recoverable_missed_ranges: i64,
    unrecoverable_missed_ranges: i64,
}

#[allow(clippy::type_complexity)] // one row mirrors the indexer_cursor columns
async fn gather_metrics(pool: &PgPool) -> anyhow::Result<IndexerMetrics> {
    // Column order matches the SELECT below.
    type CursorRow = (
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        chrono::DateTime<chrono::Utc>,
    );
    let status: Option<CursorRow> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, events_ingested_total, errors_total, events_enriched_total, events_not_enriched_total,
                COALESCE(rpc_failovers_total, 0),
                COALESCE(timestamp_parse_errors_total, 0),
                updated_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    let Some((
        last,
        tip,
        events,
        errors,
        enriched,
        not_enriched,
        rpc_failovers,
        ts_errors,
        updated_at,
    )) = status
    else {
        anyhow::bail!("indexer cursor not initialized");
    };

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (Utc::now() - updated_at).num_seconds();

    let total_enriched = enriched + not_enriched;
    let enrichment_rate = if total_enriched > 0 {
        enriched as f64 / total_enriched as f64
    } else {
        1.0
    };
    let (recoverable_missed_ranges, unrecoverable_missed_ranges): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) FILTER (WHERE recoverable), COUNT(*) FILTER (WHERE NOT recoverable)
         FROM missed_ranges WHERE recovered_at IS NULL",
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0, 0));
    let unrecovered_missed_ranges = recoverable_missed_ranges + unrecoverable_missed_ranges;

    Ok(IndexerMetrics {
        last_processed_ledger: last,
        chain_tip_ledger: tip,
        lag_ledgers,
        secs_since_update,
        events_ingested_total: events,
        errors_total: errors,
        enrichment_rate,
        rpc_failovers_total: rpc_failovers,
        timestamp_parse_errors_total: ts_errors,
        unrecovered_missed_ranges,
        recoverable_missed_ranges,
        unrecoverable_missed_ranges,
    })
}

fn env_parse_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn test_state(token: Option<&str>) -> HttpState {
        // The pool is never touched by the auth check, so a lazy pool is fine.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost:5432/db")
            .expect("lazy pool");
        HttpState {
            pool: Arc::new(pool),
            spec_cache: Arc::new(SpecCache::new(16, 2)),
            metrics_bearer_token: token.map(|t| Arc::new(t.to_string())),
        }
    }

    fn app_with(state: HttpState) -> Router {
        Router::new()
            .route("/health", get(health))
            .route("/metrics", get(metrics))
            .with_state(state)
    }

    #[tokio::test]
    async fn metrics_rejects_missing_token_when_configured() {
        let app = app_with(test_state(Some("s3cret")));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn metrics_rejects_wrong_token_when_configured() {
        let app = app_with(test_state(Some("s3cret")));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header(header::AUTHORIZATION, "Bearer nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn bearer_token_matches_accepts_correct_token() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer s3cret".parse().unwrap());
        assert!(bearer_token_matches(&headers, "s3cret"));
        assert!(!bearer_token_matches(&headers, "other"));
    }

    #[test]
    fn metrics_render_process_counters() {
        let mut body = String::new();
        render_process_counters(&mut body);

        // Both process-wide counters must be exported with HELP/TYPE lines.
        assert!(
            body.contains("lumenqraph_transfer_projection_skipped_total"),
            "missing transfer_projection_skipped_total in metrics body:\n{body}"
        );
        assert!(
            body.contains("# TYPE lumenqraph_transfer_projection_skipped_total counter"),
            "missing TYPE line for transfer_projection_skipped_total:\n{body}"
        );
        assert!(
            body.contains("lumenqraph_webhook_deliveries_dropped_by_retention_total"),
            "missing webhook_deliveries_dropped_by_retention_total in metrics body:\n{body}"
        );
        assert!(
            body.contains(
                "# TYPE lumenqraph_webhook_deliveries_dropped_by_retention_total counter"
            ),
            "missing TYPE line for webhook_deliveries_dropped_by_retention_total:\n{body}"
        );
    }
}
