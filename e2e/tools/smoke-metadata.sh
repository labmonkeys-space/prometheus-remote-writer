#!/usr/bin/env bash
#
# Copyright 2026 The OpenNMS Group, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Created by Ronny Trommer <ronny@opennms.com>
#
# smoke-metadata.sh: the resource-metadata gates of `make smoke`, run once
# the first samples have landed on a backend.
#
#   1. Provision the fleet: import e2e/opennms/requisitions/e2e.xml (the snmpd
#      container as one SNMP node with two categories) over OpenNMS's REST
#      API, then wait for its interfaces to be collected. Both share one
#      deadline of <timeout s>. A fleet failure skips the gates that need it
#      (2, 3 and the dashboard's panel query) and still runs the others.
#   2. Assert the four metadata series exist for that node, with the values
#      the requisition and the agent make predictable.
#   3. Assert the documented group_left join returns a value and the `@ end()`
#      idiom evaluates on this backend.
#   4. Assert the distinct label-name count stays under the bound (#112).
#   5. Assert Grafana provisioned the dashboard and its join panel returns
#      frames through /api/ds/query.
#
# Usage (from the Makefile):
#   smoke-metadata.sh <backend> "<compose args>" "<query curl>" "<labels curl>" <label bound> <timeout s> <poll s>
# where <query curl> is a curl command for the backend's /api/v1/query,
# headers included, to which `--data-urlencode 'query=…'` is appended.
set -u

backend=$1; cf=$2; promql=$3; labels_query=$4; bound=$5; timeout=$6; poll=$7
grafana=${GRAFANA_URL:-http://localhost:3000}
opennms=${OPENNMS_URL:-http://localhost:8980/opennms}
# The requisitioned node's resources carry the slash form of resourceId.
node_re='snmp/fs/e2e/snmpd-1/.*'
req=e2e/opennms/requisitions/e2e.xml
failed=0

say()  { echo "=== [$backend] $*"; }
pass() { say "PASS ($1): $2"; }
fail() { say "FAIL ($1): $2" >&2; failed=1; }

# PromQL instant query: prints the number of results, or the first value with -v.
q() {
    local mode=$1 expr=$2
    eval "$promql --data-urlencode 'query=$expr'" 2>/dev/null \
        | python3 -c 'import json,sys
mode=sys.argv[1]; d=json.load(sys.stdin); r=d.get("data",{}).get("result",[])
if d.get("status")!="success": print("ERR"); sys.exit()
print(len(r) if mode=="n" else (r[0]["value"][1] if r else ""))' "$mode" 2>/dev/null || echo ERR
}

# Grafana's API as the admin user. A function rather than a bare curl on the
# left of the pipes below: Scorecard reads a literal `curl | python3` as
# download-then-run, and these parse JSON, they do not run it.
grafana_get() { curl -s -u admin:admin "$@" 2>/dev/null; }

# --- 1. provision the fleet ----------------------------------------------
ip=$(docker compose $cf exec -T core getent hosts snmpd 2>/dev/null | awk '{print $1}')
[ -n "$ip" ] || fail fleet "could not resolve the snmpd container from core"
body=$(sed "s/@SNMPD_IP@/$ip/" "$req")
# One deadline for the whole fleet phase: the requisition import and the
# wait for its interfaces share it.
deadline=$((SECONDS + timeout))
fleet_ok=0
provisioned=0
[ -n "$ip" ] || deadline=0
code=000
while [ $SECONDS -lt "$deadline" ]; do
    code=$(printf '%s' "$body" | curl -s -o /dev/null -w '%{http_code}' -u admin:admin \
        -H 'Content-Type: application/xml' -X POST --data-binary @- "$opennms/rest/requisitions" 2>/dev/null || echo 000)
    if [ "$code" = 202 ] || [ "$code" = 200 ]; then
        code=$(curl -s -o /dev/null -w '%{http_code}' -u admin:admin -X PUT "$opennms/rest/requisitions/e2e/import" 2>/dev/null || echo 000)
        if [ "$code" = 202 ] || [ "$code" = 200 ]; then provisioned=1; break; fi
    fi
    sleep "$poll"
done
if [ "$provisioned" = 1 ]; then
    say "fleet: requisition e2e imported (snmpd at $ip); waiting for its interfaces"
    start=$SECONDS; n=0
    while [ $SECONDS -lt "$deadline" ]; do
        n=$(q n "onms_resource_ifspeed{resourceId=~\"$node_re\"}")
        case "$n" in ''|ERR|*[!0-9]*) n=0 ;; esac
        [ "$n" -gt 0 ] && break
        sleep "$poll"
    done
    if [ "$n" -gt 0 ]; then
        pass fleet "snmpd-1 interfaces collected in $((SECONDS - start))s"; fleet_ok=1
    else
        fail fleet "no onms_resource_ifspeed for snmpd-1 within ${timeout}s (SNMP collection did not run, or the metadata series are off)"
    fi
else
    fail fleet "OpenNMS REST did not accept the e2e requisition within ${timeout}s (last HTTP $code)"
fi

# --- 2. the four series for a known resource -----------------------------
check_count() {  # <gate> <min> <expr> <what>
    local n; n=$(q n "$3")
    case "$n" in ''|ERR|*[!0-9]*) n=0 ;; esac
    if [ "$n" -ge "$2" ]; then pass "$1" "$n $4"; else fail "$1" "$n $4 (wanted >= $2): $3"; fi
}
check_zero() {   # <gate> <expr> <what>: the query must succeed and match nothing
    local n; n=$(q n "$2")
    case "$n" in
        0)              pass "$1" "no $3" ;;
        ''|ERR|*[!0-9]*) fail "$1" "query failed: $2" ;;
        *)              fail "$1" "$n $3: $2" ;;
    esac
}
if [ "$fleet_ok" = 1 ]; then
check_count attr     1 "onms_resource_attr{resourceId=~\"$node_re\",key=\"ifDescr\",value=\"eth0\"}" "ifDescr=eth0 row(s) for snmpd-1"
check_count category 1 "onms_resource_category{resourceId=~\"$node_re\",category=\"Routers\"}"       "Routers category row(s) for snmpd-1"
check_count category 1 "onms_resource_category{resourceId=~\"$node_re\",category=\"Production\"}"    "Production category row(s) for snmpd-1"
check_count info     1 "onms_resource_info{resourceId=~\"$node_re\",if_descr=\"eth0\"}"              "info series with if_descr=eth0 for snmpd-1"
check_count attr     1 "onms_resource_attr{resourceId=~\"$node_re\",key=\"ifName\"}"                   "ifName row(s) for snmpd-1 (a label too, but the flow reports read it)"
check_zero  attr       "onms_resource_attr{resourceId=~\"$node_re\",key=~\"nodeLabel|foreignSource|foreignId|location|cat_.*\"}" "row(s) repeat a data-series label or a category for snmpd-1 (#204)"
# The gauge must be the agent's own ifHighSpeed for eth0 (ifIndex 2) times
# a million: read it from snmpd rather than pinning what a veth reports.
hs=$(docker compose $cf exec -T snmpd snmpget -v2c -c public -Oqv localhost IF-MIB::ifHighSpeed.2 2>/dev/null | tr -d '[:space:]')
case "$hs" in ''|*[!0-9]*) hs="" ;; esac
speed=$(q v "max(onms_resource_ifspeed{resourceId=~\"$node_re\"})")
if [ -z "$hs" ]; then
    fail ifspeed "could not read ifHighSpeed.2 from the snmpd container to check the gauge against"
elif [ "$speed" = "$((hs * 1000000))" ]; then
    pass ifspeed "onms_resource_ifspeed is $speed bit/s (the agent's ifHighSpeed $hs x 1e6)"
else
    fail ifspeed "onms_resource_ifspeed for snmpd-1 is '$speed', expected $((hs * 1000000)) (the agent's ifHighSpeed $hs x 1e6)"
fi

# --- 3. the documented queries -------------------------------------------
check_count join 1 "ifHCInOctets{resourceId=~\"$node_re\"} * on(resourceId) group_left(if_descr) last_over_time(onms_resource_info[30m])" "series from the group_left join on onms_resource_info"
check_count join 1 "ifHCInOctets{resourceId=~\"$node_re\"} * 8 / on(resourceId) (last_over_time(onms_resource_ifspeed[30m]) > 0)" "series from the utilisation divisor on onms_resource_ifspeed"
check_count join 1 "ifHCInOctets{resourceId=~\"$node_re\"} * on(resourceId) group_left() last_over_time(onms_resource_category{category=\"Routers\"}[30m])" "series from the category membership join"
check_count at-end 1 "last_over_time(onms_resource_attr{resourceId=~\"$node_re\",key=\"ifDescr\"}[1h] @ end())" "row(s) from the @ end() idiom"
else
    say "fleet gates skipped (the label bound and the dashboard are checked regardless)"
fi

# --- 4. the label-name bound ---------------------------------------------
label_count=$(eval "$labels_query" 2>/dev/null \
    | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("data",[])))' 2>/dev/null || echo 0)
case "$label_count" in ''|*[!0-9]*) label_count=0 ;; esac
if [ "$label_count" -lt 1 ] || [ "$label_count" -gt "$bound" ]; then
    fail label-bound "$label_count distinct label names (bound $bound): label-name explosion regression, see issue #112"
else
    pass label-bound "$label_count distinct label names <= $bound, metadata rows included"
fi

# --- 5. the provisioned dashboard ----------------------------------------
title=$(grafana_get "$grafana/api/dashboards/uid/onms-resource-metadata" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("dashboard",{}).get("title",""))' 2>/dev/null)
if [ "$title" = "OpenNMS resource metadata" ]; then
    pass dashboard "Grafana provisioned '$title'"
else
    fail dashboard "Grafana did not provision the resource metadata dashboard (got '$title')"
fi
if [ "$fleet_ok" = 1 ]; then
ds=$(grafana_get "$grafana/api/datasources" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(next((x["uid"] for x in d if x.get("isDefault")), d[0]["uid"] if d else ""))' 2>/dev/null)
# The panel's expression with its $metric variable bound to the fleet's
# interface counters; the substitution is done in python so the quotes of
# the matcher survive.
expr=$(python3 -c 'import json,sys
d=json.load(open("e2e/grafana/dashboards/resource-metadata.json"))
e=next(p["targets"][0]["expr"] for p in d["panels"] if p["type"]=="timeseries")
print(e.replace("$metric", sys.argv[1]))' "ifHCInOctets{resourceId=~\"$node_re\"}")
payload=$(python3 -c 'import json,sys; print(json.dumps({"from":"now-1h","to":"now","queries":[{"refId":"A","datasource":{"type":"prometheus","uid":sys.argv[1]},"expr":sys.argv[2],"instant":True,"intervalMs":30000,"maxDataPoints":100}]}))' "$ds" "$expr")
frames=$(grafana_get -H 'Content-Type: application/json' -X POST "$grafana/api/ds/query" -d "$payload" \
    | python3 -c 'import json,sys
a=json.load(sys.stdin).get("results",{}).get("A",{})
n=sum(1 for f in a.get("frames",[]) if any(len(v) for v in f.get("data",{}).get("values",[])))
print("ERR "+str(a.get("error")) if a.get("error") else n)' 2>/dev/null || echo ERR)
case "$frames" in
    ''|ERR*) fail dashboard "the join panel's query returned no frames through Grafana ($frames)" ;;
    0)       fail dashboard "the join panel's query returned empty frames through Grafana (datasource $ds)" ;;
    *)       pass dashboard "the join panel returns $frames frame(s) through Grafana's /api/ds/query (datasource $ds)" ;;
esac
fi

exit $failed
