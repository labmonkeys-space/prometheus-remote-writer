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
# The smoke backend versions need no attribute: `make docs` reads them from
# the compose pins through docs-backend-versions.sh. This runs that script
# too, so a compose change it can no longer parse, or a headers stack on a
# different Prometheus, fails here instead of at docs publish time.
#
# Read-only: parses the attributes and the test tree, writes nothing.
#
# Usage:
#   verify-backend-versions.sh [repo-root]

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
attrs="${root}/docs/src/docs/asciidoc/_attributes.adoc"
images="${root}/plugin/src/test/java/org/opennms/plugins/prometheus/remotewriter/PrometheusImages.java"

for f in "${attrs}" "${images}"; do
  [[ -f "${f}" ]] || { echo "verify-backend-versions: no such file: ${f}" >&2; exit 2; }
done

# The version inside a PrometheusImages constant, leading "v" stripped.
java_ref() {     # <constant>
  sed -nE "s|.*$1[[:space:]]*=[[:space:]]*\"prom/prometheus:v?([^\"]+)\".*|\1|p" "${images}" | head -1
}
attr() {         # <attribute name>
  sed -nE "s|^:$1:[[:space:]]*([^[:space:]]+).*|\1|p" "${attrs}" | head -1
}

v1="$(java_ref V1_REFERENCE)"
v2="$(java_ref V2_REFERENCE)"

for pair in "v1:${v1}" "v2:${v2}"; do
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
if smoke="$("$(dirname "${BASH_SOURCE[0]}")/docs-backend-versions.sh" "${root}")"; then
  echo "verify-backend-versions: OK — smoke versions for the docs: ${smoke}"
else
  fail=1
fi
check it-prometheus-v1 "${v1}" "PrometheusImages.V1_REFERENCE"
check it-prometheus-v2 "${v2}" "PrometheusImages.V2_REFERENCE"

exit "${fail}"
