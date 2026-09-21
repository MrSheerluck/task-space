# Task Space observability

This directory contains the private Prometheus scrape configuration and the
first alert policy for the sync/auth/billing service. The metrics endpoint is
never routed through the public application origin or Cloudflare Tunnel. Put Prometheus
on the same private network as the API, mount `prometheus.yml` and
`task-space-alerts.yml` read-only, and mount the value of
`TASK_SPACE_METRICS_TOKEN` at `/etc/prometheus/task-space-metrics-token` with
mode `0600`.

Import `grafana-task-space.json` into the private Grafana instance and bind
its Prometheus datasource through the `${DS_PROMETHEUS}` import variable. The
dashboard covers API errors, reconciliation outcomes and latency, SSE replay
health, readiness/webhook failures, compaction/retention, and sync payload
sizes.

The endpoint accepts both `X-Metrics-Token` for manual checks and standard
`Authorization: Bearer` credentials for Prometheus. Metric labels are limited
to bounded route/status/kind values; account IDs and provider payloads must
not be added as labels.

## Ownership and response

These are the minimum designated operational roles. Replace the role names
with on-call identities in the deployment system:

| Area | Owner | First action |
| --- | --- | --- |
| API, sync, SSE, client convergence | `sync-service` | Check 5xx/reconcile/cursor-reset alerts and the sync runbook |
| PostgreSQL, migrations, backups, restore | `database` | Check readiness, locks, retention, and the latest backup checksum |
| Dodo webhooks and entitlement state | `billing` | Check webhook inbox, provider status, and reconciliation candidates |
| Authentication, cookies, origin policy | `security` | Check WorkOS readiness, origin allowlist, and request IDs |
| Incident coordination and rollout | `incident-commander` | Freeze rollout, preserve diagnostics, and choose rollback/forward-fix |

Page on sustained API 5xx or webhook failures. Ticket sync cursor resets,
rate limiting, and a stalled compaction worker. During an incident, capture
the request ID, account-safe diagnostics, metric timestamp, migration
version, and deployment revision; never capture access tokens, cookies,
provider secrets, or document content.

## Installation smoke test

After mounting the configuration, verify that Prometheus can scrape the
private endpoint and that alert rules load without errors. Separately verify
the endpoint directly:

The repository-level syntax gate uses a pinned Prometheus image and a
temporary empty credentials file; it never needs the real metrics secret:

```sh
make observability-check
```

Exercise actual local alert delivery through Prometheus and Alertmanager with
a synthetic readiness-failure metric and a disposable webhook receiver:

```sh
make alert-delivery
```

This does not contact a production notification service or use production
credentials. It proves that a firing rule reaches the configured notification
boundary; staging/production still need a real receiver delivery and paging
acknowledgement during rollout.

The same gate runs semantic `promtool test rules` fixtures for readiness
failures, API 5xx responses, and cursor-reset alerts in addition to validating
the scrape configuration, dashboard JSON, and all alert-rule syntax.

```sh
curl -fsS -H "X-Metrics-Token: $TASK_SPACE_METRICS_TOKEN" \
  http://127.0.0.1:3000/internal/metrics
```

The API's `/healthz` probe checks process liveness. `/readyz` also checks the
PostgreSQL connection and should be used for traffic readiness.
