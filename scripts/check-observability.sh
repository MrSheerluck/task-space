#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
prometheus_image=${PROMTOOL_IMAGE:-prom/prometheus:v2.55.1}
observability_tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/task-space-observability.XXXXXX")

cleanup() {
  rm -f -- \
    "$observability_tmp_dir/prometheus.yml" \
    "$observability_tmp_dir/task-space-alerts.yml" \
    "$observability_tmp_dir/task-space-alerts.test.yml" \
    "$observability_tmp_dir/grafana-task-space.json" \
    "$observability_tmp_dir/task-space-metrics-token"
  rmdir -- "$observability_tmp_dir"
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  printf '%s\n' 'docker is required to run the pinned Prometheus configuration check' >&2
  exit 1
fi

cp -- \
  "$repo_dir/deploy/observability/prometheus.yml" \
  "$repo_dir/deploy/observability/task-space-alerts.yml" \
  "$repo_dir/deploy/observability/task-space-alerts.test.yml" \
  "$repo_dir/deploy/observability/grafana-task-space.json" \
  "$observability_tmp_dir/"

python3 -m json.tool "$observability_tmp_dir/grafana-task-space.json" >/dev/null

# Prometheus validates credentials_file during config parsing. The real secret
# is intentionally never copied into this temporary validation mount.
touch -- "$observability_tmp_dir/task-space-metrics-token"

docker run --rm --network none \
  --entrypoint promtool \
  -v "$observability_tmp_dir:/etc/prometheus:ro" \
  "$prometheus_image" \
  check config /etc/prometheus/prometheus.yml

docker run --rm --network none \
  --entrypoint promtool \
  -v "$observability_tmp_dir:/etc/prometheus:ro" \
  "$prometheus_image" \
  check rules /etc/prometheus/task-space-alerts.yml

docker run --rm --network none \
  --entrypoint promtool \
  -v "$observability_tmp_dir:/etc/prometheus:ro" \
  "$prometheus_image" \
  test rules /etc/prometheus/task-space-alerts.test.yml
