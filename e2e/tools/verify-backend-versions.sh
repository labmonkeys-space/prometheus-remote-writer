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
# Two versions live in docs/src/docs/asciidoc/_attributes.adoc and are
# derived, never authoritative:
#   it-prometheus-v1      <- PrometheusImages.V1_REFERENCE in the test tree
#   it-prometheus-v2      <- PrometheusImages.V2_REFERENCE in the test tree
# The test references are deliberate, so a bump goes red here until the
# attribute follows, the same contract verify-horizon-badge.sh gives the
# README badge.
#
# The smoke backend versions need no check: `make docs` reads them from the
# compose pins (docs-backend-versions.sh). The docs quote one Prometheus
# version for the smoke, so e2e/compose.headers.yml must run the same
# Prometheus as e2e/compose.prometheus.yml.
#
# Read-only: parses four files, writes nothing.
#
# Usage:
#   verify-backend-versions.sh [repo-root]

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
attrs="${root}/docs/src/docs/asciidoc/_attributes.adoc"
images="${root}/plugin/src/test/java/org/opennms/plugins/prometheus/remotewriter/PrometheusImages.java"
e2e="${root}/e2e"

for f in "${attrs}" "${images}" "${e2e}/compose.prometheus.yml" "${e2e}/compose.headers.yml"; do
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
v1="$(java_ref V1_REFERENCE)"
v2="$(java_ref V2_REFERENCE)"

for pair in "prom:${prom}" "prom_headers:${prom_headers}" "v1:${v1}" "v2:${v2}"; do
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
if [[ "${prom_headers}" == "${prom}" ]]; then
  echo "verify-backend-versions: OK — e2e/compose.headers.yml runs Prometheus ${prom}, same as e2e/compose.prometheus.yml"
else
  echo "verify-backend-versions: DRIFT — e2e/compose.headers.yml runs Prometheus ${prom_headers} but e2e/compose.prometheus.yml runs ${prom}" >&2
  echo "  pin the same prom/prometheus tag in both files" >&2
  fail=1
fi
check it-prometheus-v1 "${v1}" "PrometheusImages.V1_REFERENCE"
check it-prometheus-v2 "${v2}" "PrometheusImages.V2_REFERENCE"

exit "${fail}"
