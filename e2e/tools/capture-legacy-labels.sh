#!/usr/bin/env bash
#
# capture-legacy-labels.sh — record the wire schema a Prometheus-compatible
# backend holds after ingestion from the legacy
# opennms-prometheus-remotewrite-plugin, as the empirical fixture that pins
# this plugin's `labels.profile = legacy` mapping (openspec change
# label-profiles; clean-room rule: observed wire output only, never the
# legacy plugin's AGPL source).
#
# Read-only: issues only /api/v1/labels, /api/v1/label/__name__/values and
# /api/v1/series queries.
#
# Usage:
#   capture-legacy-labels.sh <backend-base-url> <start-rfc3339> <end-rfc3339> [out.json]
# Example:
#   capture-legacy-labels.sh http://192.168.11.38:8428 \
#       2026-08-30T12:00:00Z 2026-08-30T21:00:00Z legacy-fixture.json
#
# The window MUST cover a period when ONLY the legacy plugin was writing,
# against a backend index that never held this plugin's output (label
# queries on e.g. VictoriaMetrics are approximate within an indexDB month,
# so a mixed index contaminates the capture).

set -euo pipefail

BASE="${1:?backend base url}"
START="${2:?start (RFC3339)}"
END="${3:?end (RFC3339)}"
OUT="${4:-legacy-fixture.json}"

# Representative resource shapes: node-level, interface, response-time, JMX.
MATCHERS=(
  '{__name__=~"ifHC.+"}'
  '{__name__=~"loadavg.+|CpuRawUser|memAvailReal"}'
  '{__name__=~"icmp.*|response.*"}'
  '{__name__=~".+", resourceId=~".*jvm.*|.*jmx.*"}'
)

q() { curl -sfG "$BASE$1" --data-urlencode "start=$START" --data-urlencode "end=$END" "${@:2}"; }

{
  echo '{'
  echo "  \"provenance\": {"
  echo "    \"description\": \"Observed wire output of opennms-prometheus-remotewrite-plugin (AGPL legacy plugin); captured via read-only backend queries. Pins labels.profile=legacy.\","
  echo "    \"backend\": \"$BASE\","
  echo "    \"window\": {\"start\": \"$START\", \"end\": \"$END\"},"
  echo "    \"captured_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
  echo "    \"captured_by\": \"e2e/tools/capture-legacy-labels.sh\""
  echo '  },'
  echo '  "label_names":'
  q /api/v1/labels
  echo '  ,"metric_names":'
  q "/api/v1/label/__name__/values" --data-urlencode "limit=500" || echo '{"status":"unsupported"}'
  echo '  ,"series_samples": ['
  first=1
  for m in "${MATCHERS[@]}"; do
    [ $first = 1 ] || echo ','
    first=0
    q /api/v1/series --data-urlencode "match[]=$m" --data-urlencode "limit=10" \
      || echo '{"status":"error","matcher":"'"$m"'"}'
  done
  echo '  ]'
  echo '}'
} > "$OUT"

python3 -m json.tool "$OUT" >/dev/null \
  && echo "fixture written: $OUT" \
  || { echo "ERROR: $OUT is not valid JSON — inspect and re-run" >&2; exit 1; }
