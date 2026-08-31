# OpenNMS Prometheus Remote Writer

[![CI](https://github.com/labmonkeys-space/prometheus-remote-writer/actions/workflows/ci.yml/badge.svg)](https://github.com/labmonkeys-space/prometheus-remote-writer/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![OpenNMS Horizon](https://img.shields.io/badge/OpenNMS_Horizon-36.0.3-4c9141.svg)](https://github.com/OpenNMS/opennms/releases/tag/opennms-36.0.3-1)

An OpenNMS Horizon plugin that pushes performance data to any
Prometheus-compatible Remote Write endpoint — Prometheus, Cortex, Grafana
Mimir, VictoriaMetrics, Thanos Receive — and surfaces OpenNMS resource
context (node identity, foreign-source qualification, surveillance
categories, interface descriptors) as native Prometheus labels. Query
your OpenNMS time-series data with PromQL directly from Grafana's
Prometheus data source — no OpenNMS REST round-trip at query time.

📖 **Documentation:** <https://labmonkeys-space.github.io/prometheus-remote-writer/>

## Compatibility

| Component             | Required                       |
|-----------------------|--------------------------------|
| OpenNMS Horizon Core  | 35+                            |
| JVM                   | Temurin / OpenJDK 17           |
| Apache Karaf          | 4.4.x                          |
| Integration API       | `opennms-integration-api` 2.0  |

## Install

```bash
# 1. Drop the KAR into Karaf's deploy/ directory.
curl -L -o /opt/opennms/deploy/prometheus-remote-writer.kar \
    https://github.com/labmonkeys-space/prometheus-remote-writer/releases/latest/download/prometheus-remote-writer.kar

# 2. Tell OpenNMS to use the integration TSS strategy.
echo 'org.opennms.timeseries.strategy = integration' \
    >> /opt/opennms/etc/opennms.properties.d/timeseries.properties

# 3. Point the plugin at a Remote Write endpoint.
cat > /opt/opennms/etc/org.opennms.plugins.tss.prometheusremotewriter.cfg <<EOF
write.url = https://mimir.example.com/api/v1/push
read.url  = https://mimir.example.com/prometheus
EOF

# 4. Restart OpenNMS. From the Karaf shell, verify with:
#    opennms:prometheus-writer-stats
```

The published [docs site](https://labmonkeys-space.github.io/prometheus-remote-writer/)
covers full configuration, wire-format selection (v1 / v2), the WAL,
label mapping, backend compatibility, operations, and troubleshooting.

## Build from source

```bash
make help          # list everything available
make build         # compile, run unit tests, install locally
make verify        # unit + integration tests (needs Docker)
make kar           # build assembly/kar/target/*.kar
make smoke         # e2e against all backends
make docs          # render single-page HTML to docs/target/generated-docs
```

The build is fronted by a `Makefile` over the Maven Wrapper (`mvnw`); CI
invokes `make` targets, never raw Maven. See [`Makefile`](Makefile) for
the full target list.

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).

This plugin is a **clean-room implementation**. It targets
`opennms-integration-api` v2.0 (Apache 2.0) and is written from public
specifications — not derived from the AGPL-3.0
[`opennms-cortex-tss-plugin`](https://github.com/OpenNMS/opennms-cortex-tss-plugin).
PRs are reviewed for license hygiene; please flag any reference patterns
you suspect could be derivative.

## Links

- 📖 [Docs site](https://labmonkeys-space.github.io/prometheus-remote-writer/) — full operator reference
- 📜 [`CHANGELOG.md`](CHANGELOG.md) — release history and unreleased changes
- 🚀 [`RELEASING.md`](RELEASING.md) — how releases are cut and published
- 🤖 [`CLAUDE.md`](CLAUDE.md) — project conventions for AI agents
- 🐛 [Issue tracker](https://github.com/labmonkeys-space/prometheus-remote-writer/issues)
