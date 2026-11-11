# cloudflare-exporter

A Prometheus exporter for Cloudflare zone, Workers and R2 analytics that works on the Free plan.

Other exporters bundle datasets the Free plan rejects into one GraphQL query, so the whole query fails; this one only queries datasets Free allows, one query per dataset.

Each dataset is collected in minute-aligned windows behind its own watermark, and datasets are collected concurrently.
A window is applied to the counters fully once, or not at all and retried on the next tick, so a failed request to the Cloudflare API does not lose data.
Collection runs `COLLECT_DELAY` behind real time because Cloudflare analytics arrive late.
After an outage the exporter catches up in 10-minute windows, as far as each tick's `COLLECT_INTERVAL` budget allows.
Only the last hour is backfilled, and any older gap is reported in `cloudflare_exporter_skipped_seconds_total`.
Zones and their proxied DNS records are read once at startup, so a new zone or DNS record needs a restart.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `CF_API_TOKEN` | required | API token, see permissions below. |
| `CF_ACCOUNT_ID` | required | Account to collect from. |
| `CF_ZONES` | all zones at startup | Comma-separated zone names to include. Unknown names fail startup. |
| `LISTEN_ADDRESS` | `:9184` | HTTP listen address. |
| `COLLECT_INTERVAL` | `60s` | How often to collect. |
| `COLLECT_DELAY` | `5m` | How far behind real time windows end. |

The token needs these permissions:

- Account: Account Analytics Read, Account Settings Read.
- Zone: Analytics Read, Firewall Services Read, DNS Read.

The server exposes `/metrics`, `/healthz` and `/`.

## Local stack

`deploy/compose.yaml` runs the exporter built from source, Prometheus and Grafana on localhost.

```sh
cp deploy/.env.example deploy/.env
```

Fill in `CF_API_TOKEN` and `CF_ACCOUNT_ID` in `deploy/.env`, then start the stack:

```sh
docker compose -f deploy/compose.yaml up --build
```

Open http://localhost:3000 to see the dashboard, and http://localhost:9090 for Prometheus.
Grafana allows anonymous viewing; sign in as `admin` / `admin` to edit.
Set `EXPORTER_PORT`, `PROMETHEUS_PORT` or `GRAFANA_PORT` in `deploy/.env` when the default ports are taken.

## Dashboard

The dashboards are importable into any Grafana with a Prometheus datasource.

- `dashboards/cloudflare-edge.json` shows zone traffic, 5xx by host and status, cache and firewall.
- `dashboards/cloudflare-workers.json` shows Workers requests, errors, CPU time and subrequests.
- `dashboards/cloudflare-r2.json` shows R2 storage, objects and operations per bucket.
- `dashboards/cloudflare-exporter.json` shows collection lag, errors, gaps and build info.

## Metrics

| Metric | Type | Labels |
| --- | --- | --- |
| `cloudflare_zone_requests_total` | counter | `zone`, `host`, `status`, `cache_status` |
| `cloudflare_zone_response_bytes_total` | counter | `zone`, `host`, `status`, `cache_status` |
| `cloudflare_zone_firewall_events_total` | counter | `zone`, `action`, `source` |
| `cloudflare_worker_requests_total` | counter | `script`, `status` |
| `cloudflare_worker_errors_total` | counter | `script`, `status` |
| `cloudflare_worker_subrequests_total` | counter | `script`, `status` |
| `cloudflare_worker_cpu_seconds_total` | counter | `script`, `status` |
| `cloudflare_r2_operations_total` | counter | `bucket`, `action`, `status_code` |
| `cloudflare_r2_stored_bytes` | gauge | `bucket`, `storage_class` |
| `cloudflare_r2_objects` | gauge | `bucket`, `storage_class` |
| `cloudflare_exporter_errors_total` | counter | `source` |
| `cloudflare_exporter_last_success_timestamp_seconds` | gauge | `source` |
| `cloudflare_exporter_watermark_timestamp_seconds` | gauge | `source` |
| `cloudflare_exporter_truncated_windows_total` | counter | `source` |
| `cloudflare_exporter_skipped_seconds_total` | counter | `source` |
| `cloudflare_exporter_build_info` | gauge | `version`, `revision`, `goversion` |

`zone` is the zone name.
`host` is the request Host header, lowercased and without its port, matched against the zone's proxied DNS records.
An exact record match keeps the host, otherwise the most specific wildcard record that covers it becomes the label (for example `*.easygpu.ai`, which does not cover `easygpu.ai` itself).
Any other host, such as scanner traffic with made-up names, becomes `other`, so the label stays bounded without configuration.
The `source` label is one of `http_requests`, `firewall_events`, `workers`, `r2_operations` and `r2_storage`.
Values come from Cloudflare's adaptive datasets, which are sample-adjusted estimates.
Firewall events are weighted by their `sampleInterval`.
A counter series seen for the first time is exported at 0 and gets its first value one tick later, so `rate()` and `increase()` count that first value.
`cloudflare_exporter_truncated_windows_total` counts one-minute windows that still hit the 10000-row query limit, which means those windows are undercounted.
For `r2_storage` it counts snapshots that hit the limit, which can miss buckets whose newest sample is older than the cut-off.
The R2 gauges come from the newest sample per bucket and storage class in the last 6 hours.
They disappear when the last successful refresh is more than 15 minutes old, rather than reporting stale values.
Every `source` series of the `cloudflare_exporter_*` metrics exists from startup, and `cloudflare_exporter_last_success_timestamp_seconds` starts at the process start time, so a source that never succeeds shows up as stale.

## Example queries

Share of 5xx responses per zone:

```promql
sum by (zone) (rate(cloudflare_zone_requests_total{status=~"5.."}[15m]))
/ sum by (zone) (rate(cloudflare_zone_requests_total[15m]))
```

Cache hit ratio per zone:

```promql
sum by (zone) (rate(cloudflare_zone_requests_total{cache_status=~"hit|stale|updating|revalidated"}[1h]))
/ sum by (zone) (rate(cloudflare_zone_requests_total[1h]))
```

Bytes stored per R2 bucket:

```promql
sum by (bucket) (cloudflare_r2_stored_bytes)
```

Data lag per source, which grows when collection is failing:

```promql
time() - cloudflare_exporter_watermark_timestamp_seconds
```
