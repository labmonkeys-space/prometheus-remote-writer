#!/usr/bin/env bash
#
# Copyright 2026 The OpenNMS Group, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Created by Ronny Trommer <ronny@opennms.com>
#
# docs-backend-versions.sh — print the smoke backend versions as Maven
# -D flags for the asciidoctor <attributes> block in docs/pom.xml.
#
# The compose image tags are the only source for these versions. Dependabot
# bumps them, and `make docs` reads them here at render time, so a bump
# needs no docs edit:
#   e2e-prometheus        <- image tag in e2e/compose.prometheus.yml
#                            (and e2e/compose.headers.yml, which must agree)
#   e2e-mimir             <- image tag in e2e/compose.mimir.yml
#   e2e-victoriametrics   <- image tag in e2e/compose.victoriametrics.yml
# The docs quote one Prometheus version for the smoke, so a headers stack
# on a different Prometheus fails here rather than going unmentioned.
#
# verify-backend-versions.sh runs this on every PR, so a compose change
# that breaks the parsing fails CI, not the docs publish.
#
# Read-only: parses four files, writes nothing.
#
# Usage:
#   docs-backend-versions.sh [repo-root]

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
e2e="${root}/e2e"

# First image tag of the given repository in a compose file, leading "v"
# stripped, a digest suffix and YAML quotes tolerated.
compose_tag() {  # <file> <repository>
  sed -nE "s|^[[:space:]]*image:[[:space:]]*[\"']?$2:v?([^[:space:]@\"']+).*|\1|p" "$1" | head -1
}

flags=()
for spec in "e2e-prometheus:compose.prometheus.yml:prom/prometheus" \
            "e2e-mimir:compose.mimir.yml:grafana/mimir" \
            "e2e-victoriametrics:compose.victoriametrics.yml:victoriametrics/victoria-metrics"; do
  IFS=: read -r attr file repo <<< "${spec}"
  [[ -f "${e2e}/${file}" ]] || { echo "docs-backend-versions: no such file: ${e2e}/${file}" >&2; exit 2; }
  version="$(compose_tag "${e2e}/${file}" "${repo}")"
  [[ -n "${version}" ]] || { echo "docs-backend-versions: no ${repo} image tag in e2e/${file}" >&2; exit 2; }
  flags+=("-D${attr}=${version}")
  if [[ "${attr}" == e2e-prometheus ]]; then prom="${version}"; fi
done

[[ -f "${e2e}/compose.headers.yml" ]] || { echo "docs-backend-versions: no such file: ${e2e}/compose.headers.yml" >&2; exit 2; }
prom_headers="$(compose_tag "${e2e}/compose.headers.yml" prom/prometheus)"
if [[ "${prom_headers}" != "${prom}" ]]; then
  echo "docs-backend-versions: e2e/compose.headers.yml runs Prometheus '${prom_headers:-<none>}' but e2e/compose.prometheus.yml runs ${prom}" >&2
  echo "  pin the same prom/prometheus tag in both files" >&2
  exit 1
fi

echo "${flags[*]}"
