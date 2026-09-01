# End-to-end sandbox

A self-contained Docker Compose stack for manually exercising the
`prometheus-remote-writer` plugin against real Prometheus-compatible
backends.

📖 **Full documentation:** the "End-to-end sandbox" section of the
project docs site —
<https://labmonkeys-space.github.io/prometheus-remote-writer/#e2e-sandbox>

## Quick reference

```bash
# Build the KAR (the compose stacks mount it from assembly/kar/target)
make kar

# Bring up one backend
docker compose -f e2e/compose.prometheus.yml      up -d
docker compose -f e2e/compose.mimir.yml           up -d
docker compose -f e2e/compose.victoriametrics.yml up -d

# Custom HTTP headers behind an auth gate that 403s without them
docker compose -f e2e/compose.headers.yml           up -d

# Smoke harness (Makefile-based)
make smoke                          # default backends: prometheus, mimir, victoriametrics, headers
make smoke-prometheus               # one backend
make smoke-headers                  # http.headers.* end-to-end through the gate
make smoke BACKENDS="mimir victoriametrics"

# Sentinel deployment proof-of-concept — internal/iteration only,
# not yet functional end-to-end. See e2e/sentinel/README.md.
make sentinel-poc                   # interactive bring-up
make sentinel-poc-down              # teardown

# Tear down whatever's running
docker compose -f e2e/compose.<backend>.yml down -v --remove-orphans
```

For endpoint URLs, layout, plugin verification, backend queries, and the
list of things this sandbox does **not** exercise, see the docs link
above.

## The `headers` variant

`compose.headers.yml` is the Prometheus stack with an nginx gate in front of
the backend. The plugin's `write.url` and `read.url` point at the gate, which
rejects anything not carrying `X-Smoke-Token` — a value supplied only through
`http.headers.*` in `opennms/headers.cfg`.

That inversion is the point. Samples reach Prometheus if and only if the
custom header reached the wire, so the harness's ordinary "samples landed"
assertion becomes an end-to-end proof of the feature rather than a log-scrape.
The run additionally pins the gate itself (403 without the header, 200 with
it) so a misconfigured gate cannot let the check pass vacuously, and greps
`karaf.log` for the startup line that proves the prefix-scanned
`HttpHeadersConfig` bean activated on the same PID as the scalar
property-placeholder.

Ports are shifted (Prometheus on 9091, gate on 8081) so it can run alongside
the other stacks.
