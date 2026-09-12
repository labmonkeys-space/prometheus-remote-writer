# Sentinel deployment spike — runnable stack

This directory contains the test stack for verifying that the
`prometheus-remote-writer` plugin runs on **OpenNMS Sentinel** in addition
to Horizon Core. It exists to drive the empirical work in the OpenSpec
change [`add-sentinel-deployment-spike`](../../openspec/changes/add-sentinel-deployment-spike/).

> ⚠️ **Spike phase.** Several configuration choices in this stack are
> **working hypotheses** documented in the OpenSpec design doc — confirm
> them before treating any of this as production guidance.

## Layout

```
e2e/sentinel/
├── compose.yml                       # full distributed stack
├── compose.override.yml              # KAR + etc-overlay mounts, port exposure
├── prometheus/
│   └── prometheus.yml                # remote-write receiver, no scrape jobs
└── container-fs/
    ├── core-etc-overlay/             # → /opt/opennms-etc-overlay
    │   ├── opennms.properties.d/
    │   │   ├── timeseries.properties # strategy = integration
    │   │   └── kafka-ipc.properties  # sink/rpc/twin via Kafka
    │   └── org.opennms.plugins.tss.prometheusremotewriter.cfg
    ├── minion-etc-overlay/           # → /opt/minion-etc-overlay
    │   ├── featuresBoot.d/
    │   │   └── sentinel-poc.boot     # additive Kafka IPC features
    │   ├── org.opennms.minion.controller.cfg
    │   ├── org.opennms.core.ipc.sink.kafka.cfg
    │   ├── org.opennms.core.ipc.rpc.kafka.cfg
    │   └── org.opennms.core.ipc.twin.kafka.cfg
    └── sentinel-etc-overlay/         # → /opt/sentinel-etc-overlay
        ├── featuresBoot.d/
        │   └── tss-plugin.boot       # sentinel-kafka, sentinel-telemetry, prometheus-remote-writer
        ├── custom.properties         # strategy switch + Kafka + DB + ES
        └── org.opennms.plugins.tss.prometheusremotewriter.cfg
```

The stack stands up Postgres, Kafka (KRaft), Elasticsearch, Prometheus,
Horizon Core, a Minion, and a Sentinel — all on a single Docker network.
The plugin's KAR is mounted from `../../assembly/kar/target/` into both
Core's and Sentinel's `deploy/` directories so each container picks it up
via Karaf hot-deploy.

## Prerequisites

- Docker 24+ with Compose v2.
- Build the KAR first so the volume mount has something to land:
  ```bash
  make -C ../.. kar
  ```

## Running the spike

```bash
make sentinel-poc              # build KAR + bring up the stack
make sentinel-poc-down         # tear down + remove volumes
```

Or directly:

```bash
docker compose up -d
docker compose down -v
```

First boot is slow — Core can take 5+ minutes on a fresh container
(database init + feature loading), Sentinel boots faster (~60s) but only
after the database is healthy. Tail logs while waiting:

```bash
docker compose logs -f sentinel
docker compose logs -f core
```

## Endpoints

| Service             | URL                                         | Default credentials |
|---------------------|---------------------------------------------|---------------------|
| OpenNMS Web UI      | <http://localhost:8980/opennms/>            | `admin` / `admin`   |
| Core Karaf SSH      | `ssh -p 8101 admin@localhost`               | `admin` / `admin`   |
| Minion Karaf SSH    | `ssh -p 8201 admin@localhost`               | `admin` / `admin`   |
| **Sentinel Karaf SSH** | `ssh -p 8301 admin@localhost`            | `admin` / `admin`   |
| Prometheus UI       | <http://localhost:9090/>                    | —                   |

## Verifying the spike (tasks 3.x / 4.x)

Run these from the Sentinel Karaf shell:

```bash
ssh -p 8301 admin@localhost
```

```
karaf@sentinel> system:property org.opennms.timeseries.strategy
karaf@sentinel> feature:list -i | grep prometheus-remote-writer
karaf@sentinel> feature:list -i | grep opennms-integration-api
karaf@sentinel> feature:list -i | grep sentinel-timeseries-api
karaf@sentinel> feature:list -i | grep sentinel-telemetry
karaf@sentinel> bundle:list | grep prometheus-remote-writer
karaf@sentinel> bundle:diag <plugin-bundle-id>
karaf@sentinel> log:tail
karaf@sentinel> opennms:prometheus-writer-stats
```

> **Karaf's built-in `grep` is not GNU grep** — no `-E` flag, no
> alternation. Run each narrower pattern separately, or run the
> command from the host shell where GNU grep is available:
>
> ```bash
> docker compose -f compose.yml exec sentinel \
>   /opt/sentinel/bin/client -- "feature:list -i" \
>   | grep -E "(prometheus-remote-writer|opennms-integration-api|sentinel-timeseries-api|sentinel-telemetry)"
> ```

Then drive samples (one of):

- **Streaming telemetry**: configure a telemetry adapter on Sentinel
  (sFlow, NX-OS, JTI, Graphite, BMP) and fire packets at it from a host.
  This is the path the spike most cares about — Sentinel's primary use
  case is scaling streaming telemetry processing.
- **Synthetic Karaf-shell injection**: invoke the plugin's `store`
  method directly via Karaf scripting on Sentinel. Faster to set up but
  bypasses the IPC fabric — only useful if the Sentinel-as-persister
  pipeline itself is suspected of bypassing OIA.

Once samples have been driven through, query Prometheus to confirm:

```bash
curl -s 'http://localhost:9090/api/v1/series?match%5B%5D={onms_instance_id="e2e-sentinel"}' | jq .
curl -s 'http://localhost:9090/api/v1/query?query={onms_instance_id="e2e-sentinel"}' | jq .
```

The `onms_instance_id` label disambiguates Sentinel-emitted samples from
any Core-emitted samples sharing the same Prometheus backend (Core uses
`onms_instance_id=e2e-sentinel-core` in this stack).

## Tear down

```bash
docker compose down -v --remove-orphans
```

`-v` removes the named volumes (postgres, opennms, prometheus). Drop it
to keep state across restarts.

## What's NOT (yet) exercised here

- **The disk overflow tier on Sentinel**: the plugin runs with
  `overflow.max-size-bytes=0` in this spike, so a full queue refuses
  instead of spilling. Once the basic sample path is verified, a follow-up
  scenario mounts a writable volume and turns the tier on.
- **Mimir / VictoriaMetrics / Cortex backends**: only Prometheus is
  wired up. Same plugin config shape applies; per-backend variants are
  follow-ups.
- **Multiple Sentinels / sharding**: single Sentinel only.
- **TLS / auth to Prometheus**: cleartext on the compose network.

## Iterating on the plugin

The KAR is mounted read-only from `../../assembly/kar/target/`. After a
rebuild:

```bash
make -C ../.. kar
docker compose restart core sentinel
```

Karaf hot-deploy *can* pick up the timestamp change without a restart,
but a restart is more reliable in containerised setups.
