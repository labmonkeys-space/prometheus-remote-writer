# OpenNMS Prometheus Remote Writer

[![CI](https://github.com/opennms-forge/prometheus-remote-writer/actions/workflows/ci.yml/badge.svg)](https://github.com/opennms-forge/prometheus-remote-writer/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A standalone **gateway** that forwards OpenNMS Horizon performance data to any
Prometheus-compatible Remote Write endpoint — Prometheus, Cortex, Grafana
Mimir, VictoriaMetrics, Thanos Receive. It consumes the `CollectionSetProtos`
metrics that the OpenNMS Kafka Producer publishes to Kafka, maps them to
Prometheus time series — surfacing OpenNMS resource context (node identity,
foreign-source qualification, interface descriptors, string attributes) as
native labels — and writes them out over Remote Write (v1 or v2). Query your
OpenNMS time-series data with PromQL directly from Grafana's Prometheus data
source.

This replaces the former OSGi/Karaf TSS plugin: the gateway runs as its own
container, decoupled from the OpenNMS JVM.

## How it works

```
OpenNMS core ──(Kafka Producer: CollectionSetProtos)──▶ kafka ──▶ gateway ──(Remote Write)──▶ backend
```

The gateway is a small async Rust service: it joins a Kafka consumer group on
the metrics topic, decodes each `CollectionSetProtos` record, maps it to
samples, batches them, and POSTs snappy-compressed Remote Write payloads.
Offsets commit only after a batch is resolved (written or deliberately
dropped), giving at-least-once delivery with crash-replay. It exposes its own
`/metrics`, `/healthz`, and `/readyz` on `:9100`.

## Requirements

| Component         | Required                                                           |
|-------------------|--------------------------------------------------------------------|
| OpenNMS Horizon   | the `opennms-kafka-producer` feature, `forward.metrics=true`       |
| Kafka             | a broker reachable by both OpenNMS and the gateway                 |
| Container runtime | Docker / Podman / Kubernetes (the gateway ships as an OCI image)   |
| Backend           | any Prometheus Remote Write receiver (v1; v2 needs Prometheus 3.0+) |

## Quick start

**1. Enable the OpenNMS Kafka Producer** so collected metrics are forwarded to
a Kafka topic (`/opt/opennms/etc/org.opennms.features.kafka.producer.cfg`):

```properties
metricTopic = metrics
forward.metrics = true
```

Point its client at your broker in
`org.opennms.features.kafka.producer.client.cfg` (`bootstrap.servers = …`),
install the feature (`feature:install opennms-kafka-producer`), and ensure a
`collectd` source (SNMP/JMX) is producing metrics — pollerd/ICMP latency is not
forwarded.

**2. Run the gateway**, pointed at the same broker and your Remote Write
endpoint:

```bash
docker run -d --name onms-remote-write-gateway -p 9100:9100 \
  -e GATEWAY_KAFKA__BROKERS=kafka:9092 \
  -e GATEWAY_KAFKA__TOPIC=metrics \
  -e GATEWAY_KAFKA__GROUP_ID=onms-remote-write-gateway \
  -e GATEWAY_REMOTE_WRITE__ENDPOINT=https://mimir.example.com/api/v1/push \
  ghcr.io/opennms-forge/prometheus-remote-writer:latest
```

Verify it is live and consuming: `curl localhost:9100/readyz` and look for
`gateway_records_consumed_total` / `gateway_samples_flushed_total` at
`localhost:9100/metrics`. Your `onms_*` series should appear in the backend.

Released images are also pinned by immutable digest
(`ghcr.io/opennms-forge/prometheus-remote-writer@sha256:…`) and carry a SLSA
build-provenance attestation — verify with
`gh attestation verify oci://… --repo opennms-forge/prometheus-remote-writer`.

## Configuration

Configuration loads from `GATEWAY_`-prefixed environment variables (nested keys
use `__`), optionally overlaid on a TOML file given by `CONFIG_FILE`. Validation
is fail-fast at startup.

| Env var                                       | Default        | Description                                          |
|-----------------------------------------------|----------------|------------------------------------------------------|
| `GATEWAY_KAFKA__BROKERS`                      | _(required)_   | Comma-separated bootstrap brokers (`host:port,…`)    |
| `GATEWAY_KAFKA__TOPIC`                        | _(required)_   | Topic carrying `CollectionSetProtos` (OpenNMS `metricTopic`) |
| `GATEWAY_KAFKA__GROUP_ID`                     | _(required)_   | Consumer group id (instances in a group share partitions) |
| `GATEWAY_REMOTE_WRITE__ENDPOINT`              | _(required)_   | Target Remote Write URL                              |
| `GATEWAY_REMOTE_WRITE__WIRE_VERSION`          | `1`            | Remote Write wire format: `1` or `2` (v2 ⇒ Prometheus 3.0+) |
| `GATEWAY_REMOTE_WRITE__BATCH_MAX_SAMPLES`     | `5000`         | Flush when this many samples accumulate              |
| `GATEWAY_REMOTE_WRITE__BATCH_MAX_INTERVAL_MS` | `1000`         | Flush at least this often                            |
| `GATEWAY_MAPPING__NAMESPACE`                  | `onms`         | Metric-name prefix                                   |
| `GATEWAY_RUNTIME__LISTEN`                     | `0.0.0.0:9100` | Address for `/metrics`, `/healthz`, `/readyz`        |
| `GATEWAY_RUNTIME__LOG_LEVEL`                  | `info`         | `error`\|`warn`\|`info`\|`debug`\|`trace`            |
| `GATEWAY_RUNTIME__SHUTDOWN_GRACE_MS`          | `10000`        | Max drain time on shutdown before exiting            |

Extra HTTP headers (e.g. Mimir's `X-Scope-OrgID` tenant header) can't be set via
environment variables because header names contain hyphens — supply them in a
`CONFIG_FILE` TOML instead:

```toml
[remote_write.headers]
"X-Scope-OrgID" = "my-tenant"
```

## Metric naming and labels

Metric names are assembled as `<namespace>_<group>_<name>` (default namespace
`onms`), with every segment sanitized to `[a-zA-Z0-9_]` and case preserved —
e.g. `onms_mib2_tcp_tcpCurrEstab`. Each numeric attribute becomes one sample at
the `CollectionSet` timestamp; GAUGE/COUNTER is carried as v2 metadata (no
`_total` suffix).

OpenNMS resource context is emitted as labels: `node_id`, `foreign_source`,
`foreign_id`, `node_label`, `location`, `if_index`, the resource instance as
`resource_instance` (never the reserved `instance`), and any string attributes.

## Build from source

The build is fronted by a `Makefile` over `cargo`; CI invokes `make` targets,
never raw cargo.

```bash
make help          # list every target
make build         # compile the workspace (debug)
make test          # unit tests
make verify        # fmt check + clippy + unit tests (CI entrypoint)
make integration   # Kafka integration tests via testcontainers (needs Docker)
make image         # build the OCI container image locally
make smoke         # e2e against all backends (needs Docker)
```

## End-to-end sandbox

[`e2e/`](e2e/README.md) is a self-contained Docker Compose stack
(OpenNMS + Kafka + the gateway + a Prometheus-compatible backend + Grafana) for
manually exercising the full pipeline, plus a deterministic `make smoke`
harness. See [`e2e/README.md`](e2e/README.md) for details.

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).

This is a **clean-room implementation**. It consumes the `CollectionSetProtos`
contract via the OpenNMS Kafka Producer and is written from public
specifications — the [Prometheus Remote Write spec](https://prometheus.io/docs/specs/prw/remote_write_spec/),
the Prometheus HTTP query API, and the upstream Apache-2.0 Prometheus protobuf
definitions — **not** derived from the AGPL-3.0
[`opennms-cortex-tss-plugin`](https://github.com/OpenNMS/opennms-cortex-tss-plugin).
PRs are reviewed for license hygiene; please flag any reference patterns you
suspect could be derivative.

## Links

- 📜 [`CHANGELOG.md`](CHANGELOG.md) — release history and unreleased changes
- 🚀 [`RELEASING.md`](RELEASING.md) — how releases are cut and published
- 🤖 [`CLAUDE.md`](CLAUDE.md) — project conventions for AI agents
- 🐛 [Issue tracker](https://github.com/opennms-forge/prometheus-remote-writer/issues)
