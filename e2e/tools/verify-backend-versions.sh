#!/usr/bin/env bash
#
# Copyright 2026 The OpenNMS Group, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Created by Ronny Trommer <ronny@opennms.com>
#
# verify-backend-versions.sh — fail when a backend version the docs quote
# disagrees with what the tests actually run.
#
# Five versions live in docs/src/docs/asciidoc/_attributes.adoc and are
# derived, never authoritative:
#   e2e-prometheus        <- image tag in e2e/compose.prometheus.yml
#                            (and e2e/compose.headers.yml, which must agree)
#   e2e-mimir             <- image tag in e2e/compose.mimir.yml
#   e2e-victoriametrics   <- image tag in e2e/compose.victoriametrics.yml
#   it-prometheus-v1      <- PrometheusImages.V1_REFERENCE in the test tree
#   it-prometheus-v2      <- PrometheusImages.V2_REFERENCE in the test tree
# Dependabot bumps the compose tags; the test references are deliberate.
# Either way a bump goes red here until the attribute follows, the same
# contract verify-horizon-badge.sh gives the README badge.
#
# Read-only: parses six files, writes nothing.
#
# Usage:
#   verify-backend-versions.sh [repo-root]

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
attrs="${root}/docs/src/docs/asciidoc/_attributes.adoc"
images="${root}/plugin/src/test/java/org/opennms/plugins/prometheus/remotewriter/PrometheusImages.java"
e2e="${root}/e2e"

for f in "${attrs}" "${images}" "${e2e}/compose.prometheus.yml" "${e2e}/compose.headers.yml" \
         "${e2e}/compose.mimir.yml" "${e2e}/compose.victoriametrics.yml"; do
  [[ -f "${f}" ]] || { echo "verify-backend-versions: no such file: ${f}" >&2; exit 2; }
done

# First image tag of the given repository in a compose file, leading "v"
# stripped, a digest suffix and YAML quotes tolerated.
compose_tag() {  # <file> <repository>
  sed -nE "s|^[[:space:]]*image:[[:space:]]*[\"']?$2:v?([^[:space:]@\"']+).*|\1|p" "$1" | head -1
}
# The version inside a PrometheusImages constant, leading "v" stripped.
java_ref() {     # <constant>
  sed -nE "s|.*$1[[:space:]]*=[[:space:]]*\"prom/prometheus:v?([^\"]+)\".*|\1|p" "${images}" | head -1
}
attr() {         # <attribute name>
  sed -nE "s|^:$1:[[:space:]]*([^[:space:]]+).*|\1|p" "${attrs}" | head -1
}

prom="$(compose_tag "${e2e}/compose.prometheus.yml" 'prom/prometheus')"
prom_headers="$(compose_tag "${e2e}/compose.headers.yml" 'prom/prometheus')"
mimir="$(compose_tag "${e2e}/compose.mimir.yml" 'grafana/mimir')"
vm="$(compose_tag "${e2e}/compose.victoriametrics.yml" 'victoriametrics/victoria-metrics')"
v1="$(java_ref V1_REFERENCE)"
v2="$(java_ref V2_REFERENCE)"

for pair in "prom:${prom}" "prom_headers:${prom_headers}" "mimir:${mimir}" "vm:${vm}" "v1:${v1}" "v2:${v2}"; do
  [[ -n "${pair#*:}" ]] || { echo "verify-backend-versions: could not read the ${pair%%:*} version from its source" >&2; exit 2; }
done

fail=0
check() {  # <attribute> <expected> <source description>
  local have; have="$(attr "$1")"
  if [[ "${have}" == "$2" ]]; then
    echo "verify-backend-versions: OK — :$1: ${have} (${3})"
  else
    echo "verify-backend-versions: DRIFT — :$1: is '${have:-<unset>}' but ${3} is ${2}" >&2
    echo "  set ':$1: ${2}' in docs/src/docs/asciidoc/_attributes.adoc" >&2
    fail=1
  fi
}
check e2e-prometheus      "${prom}"         "e2e/compose.prometheus.yml"
check e2e-prometheus      "${prom_headers}" "e2e/compose.headers.yml (must match compose.prometheus.yml)"
check e2e-mimir           "${mimir}" "e2e/compose.mimir.yml"
check e2e-victoriametrics "${vm}"    "e2e/compose.victoriametrics.yml"
check it-prometheus-v1    "${v1}"    "PrometheusImages.V1_REFERENCE"
check it-prometheus-v2    "${v2}"    "PrometheusImages.V2_REFERENCE"

exit "${fail}"
