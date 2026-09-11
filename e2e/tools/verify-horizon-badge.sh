#!/usr/bin/env bash
#
# Copyright 2026 The OpenNMS Group, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Created by Ronny Trommer <ronny@opennms.com>
#
# verify-horizon-badge.sh — fail when the README's "OpenNMS Horizon" badge
# disagrees with the opennms/horizon tag the e2e stack actually runs.
#
# The version is written in three independent places on one README line and
# in the compose file. This check makes the compose pin the source of truth
# and the other three derived, so a Dependabot bump that forgets the README
# goes red instead of going unnoticed.
#
# Read-only: parses two files, writes nothing.
#
# Usage:
#   verify-horizon-badge.sh [repo-root]

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
compose="${root}/e2e/compose.base.yml"
readme="${root}/README.md"

for f in "${compose}" "${readme}"; do
  [[ -f "${f}" ]] || { echo "verify-horizon-badge: no such file: ${f}" >&2; exit 2; }
done

# Source of truth: the core service's image tag.
pinned="$(sed -nE 's|^[[:space:]]*image:[[:space:]]*opennms/horizon:([^[:space:]]+).*|\1|p' "${compose}" | head -1)"
[[ -n "${pinned}" ]] || { echo "verify-horizon-badge: no opennms/horizon pin found in ${compose}" >&2; exit 2; }

badge_line="$(grep -n 'img.shields.io/badge/OpenNMS_Horizon-' "${readme}" | head -1)"
[[ -n "${badge_line}" ]] || { echo "verify-horizon-badge: no OpenNMS Horizon badge found in ${readme}" >&2; exit 2; }
lineno="${badge_line%%:*}"

# Two independent copies of the version live on that one line: the shields.io
# badge text and the upstream release link. Check both — verifying only one
# leaves the other free to rot.
shield="$(sed -nE 's|.*img\.shields\.io/badge/OpenNMS_Horizon-([^-]+)-.*|\1|p' <<<"${badge_line}")"
link="$(sed -nE 's|.*/releases/tag/opennms-([0-9][^)/]*)-[0-9]+.*|\1|p' <<<"${badge_line}")"

fail=0
[[ "${shield}" == "${pinned}" ]] || fail=1
[[ "${link}"   == "${pinned}" ]] || fail=1

if (( fail )); then
  cat >&2 <<EOF
verify-horizon-badge: OpenNMS Horizon version drift

  e2e/compose.base.yml  opennms/horizon tag : ${pinned:-<none>}
  README.md:${lineno}   shields.io badge    : ${shield:-<unparsed>}
  README.md:${lineno}   release link        : ${link:-<unparsed>}

The compose pin is the source of truth. Update the README badge line so both
the badge text and the release link (opennms-${pinned}-1) name ${pinned}.
EOF
  exit 1
fi

echo "verify-horizon-badge: OK — OpenNMS Horizon ${pinned} in compose, badge and release link"
