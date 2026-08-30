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

# Smoke harness (Makefile-based)
make smoke                          # default backends: prometheus, mimir, victoriametrics
make smoke-prometheus               # one backend
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
