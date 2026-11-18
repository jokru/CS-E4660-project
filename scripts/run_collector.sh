#!/usr/bin/env bash
# Run collectors from collectors/<name>/config.yaml in one OpenTelemetry Collector (Docker).
# Their configs are merged, so all named collectors receive the same traces and each writes
# out/<name>.jsonl. Use `all` for every collector (including `jaeger`, which needs Jaeger on host port 14317).
# Usage: bash scripts/run_collector.sh baseline_naive sampling anomalies_sampling
set -euo pipefail
[ "$#" -ge 1 ] || { echo "usage: run_collector.sh <name>... | all" >&2; exit 2; }
root="$(cd "$(dirname "$0")/.." && pwd)"
names=("$@")
if [ "${names[0]}" = all ]; then
  names=()
  for dir in "$root"/collectors/*/; do names+=("$(basename "$dir")"); done
fi
args=(--rm --name otelcol --user "$(id -u):$(id -g)" -p 4317:4317 -p 4318:4318 --add-host=host.docker.internal:host-gateway -v "$root/out:/out")
configs=()
for name in "${names[@]}"; do
  [ -f "$root/collectors/$name/config.yaml" ] || { echo "unknown collector: $name" >&2; exit 2; }
  args+=(-v "$root/collectors/$name/config.yaml:/etc/otelcol/$name.yaml:ro")
  configs+=("--config=/etc/otelcol/$name.yaml")
done
mkdir -p "$root/out"
exec docker run "${args[@]}" otel/opentelemetry-collector-contrib:latest "${configs[@]}"
