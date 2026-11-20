# Lumenqraph Monitoring

This directory contains Prometheus alert rules and Grafana dashboards for monitoring Lumenqraph deployments.

## Files

- **`prometheus_alerts.yml`** - Prometheus alert rules for indexer health, error rates, and lag
- **`grafana_dashboard.json`** - Grafana dashboard visualizing key metrics

## Quick Start

### Using Docker Compose

Add to your `docker-compose.yml`:

```yaml
prometheus:
  image: prom/prometheus:latest
  ports:
    - "9090:9090"
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    - ./monitoring/prometheus_alerts.yml:/etc/prometheus/prometheus_alerts.yml
  command:
    - '--config.file=/etc/prometheus/prometheus.yml'
    - '--storage.tsdb.path=/prometheus'

grafana:
  image: grafana/grafana:latest
  ports:
    - "3000:3000"
  environment:
    - GF_SECURITY_ADMIN_PASSWORD=admin
  volumes:
    - grafana_data:/var/lib/grafana
  depends_on:
    - prometheus

volumes:
  grafana_data:
```

### Prometheus Configuration

`monitoring/prometheus.yml` already scrapes all three services (API, indexer and webhooks) plus Prometheus itself, using docker-compose service names. Use it as-is with the compose snippet above, or adapt the targets for bare metal:

| Job | Default target | Service setting |
|-----|----------------|-----------------|
| `lumenqraph-api` | `api:8080` | `API_BIND_ADDR` |
| `lumenqraph-indexer` | `indexer:9090` | `INDEXER_HTTP_BIND_ADDR` (must be `0.0.0.0:9090` inside compose) |
| `lumenqraph-webhooks` | `webhooks:9091` | `WEBHOOKS_METRICS_BIND_ADDR` (must be `0.0.0.0:9091` inside compose) |

The indexer and webhooks default to `127.0.0.1`, which is unreachable from another container. `docker-compose.full.yml` sets both to `0.0.0.0` on the private compose network without publishing the ports. If the indexer sets `METRICS_BEARER_TOKEN`, uncomment the `authorization` block in the indexer job.

To send alerts to Alertmanager, set the `alerting.alertmanagers` targets in `prometheus.yml` (the file has a commented example).

### Importing the Grafana Dashboard

1. **Open Grafana** (default: `http://localhost:3000`, admin/admin)
2. **Add Prometheus data source**:
   - Click "Configuration" → "Data Sources" → "Add data source"
   - Select "Prometheus"
   - URL: `http://prometheus:9090`
   - Click "Save & Test"

3. **Import dashboard**:
   - Click "+" (Create) → "Import"
   - Upload `grafana_dashboard.json` or paste the JSON directly
   - Select the Prometheus data source
   - Click "Import"

## Metrics Overview

### Indexer Metrics

- **`lumenqraph_indexer_lag_ledgers`** - Ledgers behind chain tip (lower is better)
- **`lumenqraph_indexer_last_processed_ledger`** - Most recent ledger processed
- **`lumenqraph_indexer_chain_tip_ledger`** - Latest ledger on chain
- **`lumenqraph_indexer_events_ingested_total`** - Events ingested by the indexer process (cumulative). The API also exposes `lumenqraph_indexer_ingested_total`, derived from the database.
- **`lumenqraph_indexer_errors_total`** - Total indexer poll-cycle errors (cumulative)
- **`lumenqraph_transfer_projection_skipped_total`** - Cumulative count of
  transfer-like events skipped by the SEP-41 projection gate (#600). A
  non-zero, rising value means events are being dropped before projection;
  investigate the gate's classification rules.
- **`lumenqraph_webhook_deliveries_dropped_by_retention_total`** - Cumulative
  count of `webhook_deliveries` rows dropped by the retention job (#601). A
  rising value means deliveries are being lost to retention before they are
  dispatched; consider raising the retention window or draining the backlog.

Both counters are exported on the indexer `/metrics` endpoint and have
dedicated Grafana dashboard panels.

### API Metrics

- **`lumenqraph_api_requests_total`** - Total HTTP requests served (cumulative)

### Data Metrics

- **`lumenqraph_events_total`** - Total events stored in database

### Webhook Metrics

Exposed by the `lumenqraph-webhooks` service on its own `/metrics` endpoint
(default `127.0.0.1:9091`, `WEBHOOKS_METRICS_BIND_ADDR`; use `0.0.0.0:9091` when Prometheus scrapes from another container):

- **`lumenqraph_webhooks_pending_deliveries`** - Deliveries in the `pending`
  state (queue depth), refreshed once per dispatcher tick. A sustained rise
  means the dispatcher is falling behind a slow or unresponsive subscriber.
- **`lumenqraph_webhook_oldest_pending_age_seconds`** - Age of the oldest
  pending delivery.
- **`lumenqraph_webhook_delivered_total`** / **`lumenqraph_webhook_failed_total`**
  - Lifetime delivery outcomes.

## Alert Rules

### Critical Alerts

- **IndexerStalled** - No events ingested in 5+ minutes but lag exists
- **IndexerLagCritical** - Lag > 500 ledgers for 2+ minutes
- **LumenqraphWebhookBacklogCritical** - Pending webhook deliveries > 5000 for 5+ minutes

### Warning Alerts

- **IndexerLagHigh** - Lag > 100 ledgers for 5+ minutes
- **IndexerErrorRateHigh** - Error rate > 1% over 5 minutes
- **LargeLagGrowth** - Lag grew > 1000 ledgers/hour
- **IngestRateLow** - Processing < 1 event/sec for 10+ minutes
- **APINoRequests** - No API requests for 5+ minutes
- **LumenqraphWebhookBacklogHigh** - Pending webhook deliveries > 500 for 5+ minutes

## Validating the Configuration

CI (`monitoring-config` job) runs these checks; run them locally before changing the files:

```bash
# Alert and scrape consistency: every metric in an alert expression must be
# emitted by a service that has a scrape job.
python3 scripts/check_alert_metrics.py

# Prometheus syntax checks, pinned to the same image as CI
docker run --rm -v "$PWD/monitoring:/etc/prometheus:ro" \
  --entrypoint promtool prom/prometheus:v2.54.1 \
  check config /etc/prometheus/prometheus.yml
docker run --rm -v "$PWD/monitoring:/etc/prometheus:ro" \
  --entrypoint promtool prom/prometheus:v2.54.1 \
  check rules /etc/prometheus/prometheus_alerts.yml
```

## Customization

### Adjusting Alert Thresholds

Edit `prometheus_alerts.yml` to adjust:
- Lag thresholds (currently 100/500 ledgers)
- Duration before firing (currently 2-10 minutes)
- Error rate thresholds (currently 1%)

### Adding More Panels

The Grafana dashboard can be extended by:
1. Opening it in edit mode in Grafana
2. Adding new panels with additional metrics
3. Exporting the updated JSON back to this directory

## Production Recommendations

1. **Set up AlertManager** for alert routing/aggregation
2. **Configure persistent storage** for Prometheus data
3. **Use external Grafana instance** for high availability
4. **Set up log aggregation** (ELK/Loki) for detailed debugging
5. **Monitor Prometheus itself** with additional scrape jobs
6. **Establish on-call rotation** for critical alerts

## Integration with CI/CD

Store these files in your repository and:
- Deploy Prometheus with the alert rules via your deployment pipeline
- Use Grafana provisioning API to auto-import the dashboard
- Version control all monitoring configurations

## Troubleshooting

**Metrics not appearing in Grafana?**
- Verify Prometheus can reach `/metrics` endpoint
- Check Prometheus targets at `http://prometheus:9090/targets`

**Alerts not firing?**
- Check Prometheus rule evaluation at `http://prometheus:9090/alerts`
- Verify alert thresholds match your environment

**Dashboard showing no data?**
- Ensure Prometheus data source is configured correctly
- Check time range selection in dashboard
- Verify metrics are being scraped (check Prometheus targets)
