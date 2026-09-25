//! Health endpoints: `/health` for status, `/livez` for liveness, `/readyz` for readiness.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};

use crate::error::ApiResult;
use crate::state::AppState;

pub async fn health(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    let status: Option<(i64, i64, i64, i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, events_ingested_total, errors_total, updated_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(&state.pool)
    .await?;

    // Which network this deployment indexes, from the RPC itself (cached after
    // the first successful probe) — lets clients like the explorer adapt
    // instead of asking the user. `null` if the RPC is unreachable right now.
    let passphrase = state.rpc.network_passphrase().await;
    let network = passphrase.as_deref().map(network_name);
    // Sibling instances mounted under this origin (e.g. {"testnet": "/testnet"})
    // so clients can discover other networks without configuration.
    let mounts = (!state.mounts.is_empty()).then(|| {
        state
            .mounts
            .iter()
            .map(|(name, _)| (name.clone(), Value::String(format!("/{name}"))))
            .collect::<serde_json::Map<String, Value>>()
    });

    let Some((last, tip, ingested, errors, updated_at)) = status else {
        return Ok(Json(json!({
            "status": "starting",
            "network": network,
            "network_passphrase": passphrase,
            "mounts": mounts,
            "unrecovered_missed_ranges": 0,
            "version": state.build_info.version,
            "commit": state.build_info.commit,
            "build_time": state.build_info.build_time,
        })));
    };

    let unrecovered_missed_ranges: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM missed_ranges WHERE recovered_at IS NULL",
    )
    .fetch_one(&state.pool)
    .await
    .unwrap_or(0);

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (chrono::Utc::now() - updated_at).num_seconds();
    // Stale if the cursor hasn't advanced recently, or we're far behind.
    let healthy = secs_since_update <= state.health_max_stale_secs && lag_ledgers < state.health_max_lag_ledgers;

    Ok(Json(json!({
        "status": if healthy { "ok" } else { "degraded" },
        "network": network,
        "network_passphrase": passphrase,
        "mounts": mounts,
        "last_processed_ledger": last,
        "chain_tip_ledger": tip,
        "lag_ledgers": lag_ledgers,
        "seconds_since_cursor_update": secs_since_update,
        "events_ingested_total": ingested,
        "errors_total": errors,
        "unrecovered_missed_ranges": unrecovered_missed_ranges,
        "version": state.build_info.version,
        "commit": state.build_info.commit,
        "build_time": state.build_info.build_time,
    })))
}

/// Liveness probe: returns 200 if the process is running.
/// No database access required; used by orchestrators to detect dead processes.
pub async fn livez() -> StatusCode {
    StatusCode::OK
}

/// Readiness probe: returns 200 only when the indexer is caught up and healthy.
/// Returns 503 if the database is unreachable or lag is too high.
/// Used by orchestrators to route traffic only to ready instances.
pub async fn readyz(State(state): State<AppState>) -> (StatusCode, Option<Json<Value>>) {
    let status: Option<(i64, i64, i64, chrono::DateTime<chrono::Utc>)> = match sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, errors_total, updated_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(&state.pool)
    .await
    {
        Ok(Some(row)) => Some(row),
        Ok(None) => {
            // Cursor not yet created; not ready
            return (StatusCode::SERVICE_UNAVAILABLE, None);
        }
        Err(_) => {
            // Database error; not ready
            return (StatusCode::SERVICE_UNAVAILABLE, None);
        }
    };

    if let Some((last, tip, errors, updated_at)) = status {
        let lag_ledgers = (tip - last).max(0);
        let secs_since_update = (chrono::Utc::now() - updated_at).num_seconds();

        // Ready if recent activity and not too far behind
        if secs_since_update <= state.readyz_max_age_secs && lag_ledgers < state.readyz_lag_threshold {
            (StatusCode::OK, None)
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, None)
        }
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, None)
    }
}

/// Short name for the well-known Stellar network passphrases.
fn network_name(passphrase: &str) -> &'static str {
    match passphrase {
        "Public Global Stellar Network ; September 2015" => "mainnet",
        "Test SDF Network ; September 2015" => "testnet",
        "Test SDF Future Network ; October 2022" => "futurenet",
        _ => "custom",
    }
}
