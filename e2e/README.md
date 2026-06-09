# End-to-end sandbox

A self-contained Docker Compose stack for manually exercising the standalone
**remote-write gateway** against real Prometheus-compatible backends.

## What it wires up

```
OpenNMS core ──(Kafka Producer: CollectionSetProtos)──▶ kafka ──▶ gateway ──(Remote Write)──▶ backend ──▶ grafana
```

- **core** — OpenNMS Horizon. Keeps its own time series store (`rrd`) and runs
  the `opennms-kafka-producer` feature with `forward.metrics=true`, publishing
  collected metrics to the Kafka `metrics` topic.
- **kafka** — single-node KRaft broker.
- **gateway** — this project, built from the repo `Dockerfile`. Consumes the
  `metrics` topic and writes Prometheus Remote Write to the backend. Exposes its
  own `/metrics`, `/healthz`, `/readyz` on `:9100`.
- **backend** — `prometheus`, `mimir`, or `victoriametrics` (one per compose file).
- **grafana** — points at the backend.

## Quick reference

```bash
# Bring up one backend (the gateway image is built on first up)
docker compose -f e2e/compose.prometheus.yml      up -d --build
docker compose -f e2e/compose.mimir.yml           up -d --build
docker compose -f e2e/compose.victoriametrics.yml up -d --build

# Smoke harness (Makefile-based): brings the stack up, provisions an ICMP node
# so collection produces metrics, then asserts onms_* series reach the backend.
make smoke                          # default backends: prometheus, mimir, victoriametrics
make smoke-prometheus               # one backend
make smoke BACKENDS="mimir victoriametrics"

# Tear down whatever's running
docker compose -f e2e/compose.<backend>.yml down -v --remove-orphans
```

## Endpoints

| Service | URL |
|---------|-----|
| OpenNMS | http://localhost:8980 (admin/admin) |
| Grafana | http://localhost:3000 (admin/admin, or anonymous Viewer) |
| Gateway | http://localhost:9100/metrics, /healthz, /readyz |
| Prometheus | http://localhost:9090 |
| Mimir | http://localhost:9009 (tenant `X-Scope-OrgID: e2e`) |
| VictoriaMetrics | http://localhost:8428 |

## How the smoke works vs. the full pipeline

There are two ways to get data flowing:

- **`make smoke` (deterministic, CI-grade):** brings up only `kafka` + the
  backend + `gateway`, then a host-run seeder
  (`cargo run -p gateway --example seed`) publishes synthetic
  `CollectionSetProtos` to the `metrics` topic via the host listener
  (`localhost:29092`). It asserts `onms_*` series reach the backend. This proves
  the **Kafka → gateway → backend** path without depending on OpenNMS collection
  timing. Tune with `SEED_COUNT`, `SMOKE_TIMEOUT`, `SMOKE_POLL`.

- **Full pipeline (manual):** `docker compose -f e2e/compose.<backend>.yml up -d
  --build` starts OpenNMS too. OpenNMS forwards *collected* metrics — so you
  need a `collectd` source (SNMP/JMX); provision a node pointing at an SNMP
  agent/simulator on the compose network. Pollerd/ICMP latency alone is **not**
  forwarded by the Kafka Producer.

## Notes

- **Gateway config** is supplied via `GATEWAY_*` environment variables in each
  compose file (see `compose.base.yml` for the shared defaults). Mimir
  additionally mounts `gateway/mimir.toml` to set the `X-Scope-OrgID` header,
  which can't be expressed as an environment variable.
- **Kafka topic**: `metrics` (OpenNMS `metricTopic` default). The gateway uses
  `auto.offset.reset=latest`, so start the stack before expecting data — it
  forwards metrics produced after the consumer joins.
- The `sentinel/` proof-of-concept predates this pivot and is not part of the
  gateway smoke flow.
