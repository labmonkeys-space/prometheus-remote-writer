#!/usr/bin/env bash
#
# Copyright 2026 The OpenNMS Group, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Created by Ronny Trommer <ronny@opennms.com>
#
# verify-compat-range.sh — fail when the three opennms-integration-api
# version declarations disagree.
#
# The floor is stated in three files that are edited independently and by
# different actors — a Dependabot bump touches only the build property, and
# nothing else notices. That drift is exactly what broke the KAR on Horizon
# 36.0.4 (#139): the build moved to 2.0.1 while the Karaf feature still
# demanded exactly 2.0.0.
#
#   pom.xml                 <opennms-integration-api.version>  what we build against
#   karaf-features/.../features.xml   <feature version="[x,y)">  what the KAR requires
#   plugin/pom.xml          Import-Package ...;version="[x,y)"   what the bundle wires
#
# Read-only: parses three files, writes nothing.
#
# Usage:
#   verify-compat-range.sh [repo-root]

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
parent_pom="${root}/pom.xml"
plugin_pom="${root}/plugin/pom.xml"
features="${root}/karaf-features/src/main/resources/features.xml"

for f in "${parent_pom}" "${plugin_pom}" "${features}"; do
  [[ -f "${f}" ]] || { echo "verify-compat-range: no such file: ${f}" >&2; exit 2; }
done

# What we build against.
build="$(sed -nE 's|.*<opennms-integration-api\.version>([^<]+)</opennms-integration-api\.version>.*|\1|p' "${parent_pom}" | head -1)"

# What the KAR requires: floor of the feature range.
feature_range="$(sed -nE 's|.*<feature version="([^"]+)">opennms-integration-api</feature>.*|\1|p' "${features}" | head -1)"
feature_floor="$(sed -nE 's|^\[([^,]+),.*|\1|p' <<<"${feature_range}")"

# What the bundle wires: floor of the Import-Package range.
import_range="$(sed -nE 's|.*org\.opennms\.integration\.api\.v1\.timeseries\.\*;version="([^"]+)".*|\1|p' "${plugin_pom}" | head -1)"
import_floor="$(sed -nE 's|^\[([^,]+),.*|\1|p' <<<"${import_range}")"

fail=0
[[ -n "${build}"         ]] || { echo "verify-compat-range: could not parse opennms-integration-api.version from ${parent_pom}" >&2; exit 2; }
[[ -n "${feature_floor}" ]] || { echo "verify-compat-range: feature version is not a range with an inclusive floor: '${feature_range:-<none>}'" >&2; fail=1; }
[[ -n "${import_floor}"  ]] || { echo "verify-compat-range: Import-Package has no explicit range for the timeseries packages" >&2; fail=1; }

if (( ! fail )); then
  [[ "${feature_floor}" == "${build}" ]] || fail=1
  [[ "${import_floor}"  == "${build}" ]] || fail=1
fi

if (( fail )); then
  cat >&2 <<EOF
verify-compat-range: opennms-integration-api floor drift

  pom.xml            build property   : ${build:-<unparsed>}
  features.xml       feature range    : ${feature_range:-<unparsed>}   (floor ${feature_floor:-<unparsed>})
  plugin/pom.xml     Import-Package   : ${import_range:-<unparsed>}   (floor ${import_floor:-<unparsed>})

All three must name the same floor. If this fired on a dependency bump, the
bump moved the build property alone — update the feature range and the
Import-Package range to match, and confirm which OpenNMS Horizon releases
still ship a satisfying version before widening support claims.
EOF
  exit 1
fi

echo "verify-compat-range: OK — opennms-integration-api floor ${build} in build property, feature range and Import-Package"
