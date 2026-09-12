#!/usr/bin/env bash
set -euo pipefail

# Verify the local notification path without using production credentials:
# synthetic metrics -> Prometheus rule -> Alertmanager -> webhook receiver.
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
network="task-space-alerts-${run_id}"
receiver="task-space-alert-receiver-${run_id}"
prometheus="task-space-alert-prometheus-${run_id}"
alertmanager="task-space-alertmanager-${run_id}"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/task-space-alert-delivery.XXXXXX")"
prometheus_image="${TASK_SPACE_PROMETHEUS_IMAGE:-prom/prometheus:v2.55.1}"
alertmanager_image="${TASK_SPACE_ALERTMANAGER_IMAGE:-prom/alertmanager:v0.27.0}"
receiver_image="${TASK_SPACE_ALERT_RECEIVER_IMAGE:-python:3.12-alpine}"

cleanup() {
  docker rm -f "$prometheus" "$alertmanager" "$receiver" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -rf -- "$tmp_dir"
}
trap cleanup EXIT

cat > "$tmp_dir/prometheus.yml" <<'EOF'
global:
  scrape_interval: 1s
  evaluation_interval: 1s
rule_files:
  - /etc/prometheus/alert-rules.yml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ["alertmanager:9093"]
scrape_configs:
  - job_name: task-space-alert-test
    static_configs:
      - targets: ["receiver:8000"]
    metrics_path: /metrics
EOF

cat > "$tmp_dir/alert-rules.yml" <<'EOF'
groups:
  - name: task-space-alert-delivery-test
    rules:
      - alert: TaskSpaceReadinessFailures
        expr: task_space_readiness_failures_total > 0
        for: 0s
        labels:
          severity: page
          owner: database
        annotations:
          summary: synthetic readiness failure delivery test
EOF

cat > "$tmp_dir/alertmanager.yml" <<'EOF'
route:
  receiver: task-space-test-webhook
  group_wait: 0s
  group_interval: 1s
  repeat_interval: 1h
receivers:
  - name: task-space-test-webhook
    webhook_configs:
      - url: http://receiver:8000/alerts
EOF

docker network create "$network" >/dev/null
docker run --detach --name "$receiver" --network "$network" \
  --network-alias receiver \
  --volume "$repo_dir/scripts/alert-test-endpoint.py:/app/alert-test-endpoint.py:ro" \
  --volume "$tmp_dir:/tmp/task-space-alerts" \
  --env TASK_SPACE_ALERT_OUTPUT=/tmp/task-space-alerts/alerts.json \
  "$receiver_image" python3 /app/alert-test-endpoint.py >/dev/null

docker run --detach --name "$alertmanager" --network "$network" \
  --network-alias alertmanager \
  --volume "$tmp_dir/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro" \
  "$alertmanager_image" \
  --config.file=/etc/alertmanager/alertmanager.yml >/dev/null

docker run --detach --name "$prometheus" --network "$network" \
  --volume "$tmp_dir/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  --volume "$tmp_dir/alert-rules.yml:/etc/prometheus/alert-rules.yml:ro" \
  "$prometheus_image" \
  --config.file=/etc/prometheus/prometheus.yml >/dev/null

for _ in $(seq 1 45); do
  if docker exec "$receiver" test -s /tmp/task-space-alerts/alerts.json; then
    break
  fi
  sleep 1
done

docker cp "$receiver:/tmp/task-space-alerts/alerts.json" "$tmp_dir/alerts.json" >/dev/null
rg -q '"alertname":"TaskSpaceReadinessFailures"' "$tmp_dir/alerts.json"
rg -q '"owner":"database"' "$tmp_dir/alerts.json"
rg -q '"severity":"page"' "$tmp_dir/alerts.json"

printf 'Alert delivery acceptance passed: Prometheus fired TaskSpaceReadinessFailures and Alertmanager delivered it to the webhook receiver\n'
