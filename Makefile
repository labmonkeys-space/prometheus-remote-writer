# ==============================================================================
# OpenNMS Prometheus Remote Writer — Build Facade
#
# Usage:
#   make build         Compile, run unit tests, install all modules locally
#   make test          Run unit tests only
#   make verify        Run unit + integration tests (requires Docker for IT)
#   make kar           Build the KAR (assembly/kar/target/*.kar)
#   make smoke         Build KAR and run e2e smoke against the default backends
#                      (prometheus mimir victoriametrics — sentinel is opt-in)
#   make smoke-<be>    Smoke against one backend: prometheus | mimir |
#                      victoriametrics | sentinel
#   make sentinel-poc         Bring up the Sentinel-as-TSS-persister proof-of-concept
#                             stack interactively (no auto-teardown)
#   make sentinel-poc-down    Tear down the proof-of-concept stack (removes volumes)
#   make docs          Render single-page HTML documentation
#   make sbom          Generate CycloneDX 1.6 aggregate SBOM (target/bom.json)
#   make clean         Remove all build artifacts
#
# Overridable variables:
#   MODULE             Maven module selector (e.g. :prometheus-remote-writer)
#   TEST               Test class name (suffix IT = integration test)
#   MAVEN_FLAGS        Extra Maven flags (default: -B --no-transfer-progress)
#   MAVEN_OPTS         JVM options for Maven
#   BACKENDS           Space-separated list for `make smoke` (default:
#                      prometheus mimir victoriametrics; sentinel is opt-in)
#   SMOKE_TIMEOUT      Per-backend deadline in seconds (default: 600)
#   SMOKE_POLL         Poll interval in seconds (default: 15)
# ==============================================================================

SHELL := /bin/bash

MODULE      ?=
TEST        ?=
MAVEN_FLAGS ?= -B --no-transfer-progress
MAVEN_OPTS  ?= -Xmx2g -XX:+TieredCompilation

MVN := ./mvnw

export MAVEN_OPTS

# ---- Smoke harness ----------------------------------------------------------
# Per project convention CI runs `make smoke` — never the underlying tooling
# directly. The whole local/CI command surface lives in this Makefile.
#
# Default backends explicitly EXCLUDE sentinel: the sentinel proof-of-concept
# stack brings the plugin up but has no sample driver wired in, so its series
# count stays at zero and the smoke check would fail by design. Run it
# explicitly via `make smoke-sentinel` or `BACKENDS=sentinel make smoke`.
SMOKE_TIMEOUT          ?= 600
SMOKE_POLL             ?= 15
SMOKE_DEFAULT_BACKENDS ?= prometheus mimir victoriametrics
BACKENDS               ?= $(SMOKE_DEFAULT_BACKENDS)

.PHONY: help build test verify kar smoke \
        smoke-prometheus smoke-mimir smoke-victoriametrics smoke-sentinel \
        sentinel-poc sentinel-poc-down docs sbom clean test-class

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Variables (override on command line):"
	@echo "  MODULE             Maven module selector                              (current: $(MODULE))"
	@echo "  TEST               Test class name (suffix IT = integration test)     (current: $(TEST))"
	@echo "  MAVEN_FLAGS        Extra Maven flags                                  (current: $(MAVEN_FLAGS))"
	@echo "  BACKENDS           Smoke backends to run                              (current: $(BACKENDS))"
	@echo "  SMOKE_TIMEOUT      Per-backend deadline (s)                           (current: $(SMOKE_TIMEOUT))"
	@echo "  SMOKE_POLL         Poll interval (s)                                  (current: $(SMOKE_POLL))"

build: ## Compile, run unit tests, install all modules locally
	$(MVN) $(MAVEN_FLAGS) install

test: ## Run unit tests only
	$(MVN) $(MAVEN_FLAGS) test

verify: ## Run unit + integration tests (needs Docker for testcontainers)
	$(MVN) $(MAVEN_FLAGS) verify

kar: ## Build the KAR artifact
	$(MVN) $(MAVEN_FLAGS) -DskipTests package

smoke: kar ## Run e2e smoke against BACKENDS (defaults exclude sentinel; SMOKE_TIMEOUT/SMOKE_POLL configurable)
	@set -o pipefail; \
	failed=""; passed=""; \
	current_file=""; current_backend=""; \
	cleanup() { \
	    if [ -n "$$current_file" ]; then \
	        echo "=== [$$current_backend] tearing down ==="; \
	        docker compose -f "$$current_file" down -v --remove-orphans >/dev/null 2>&1 || true; \
	    fi; \
	}; \
	trap 'cleanup; echo; echo "=== interrupted ==="; exit 130' INT TERM; \
	for backend in $(BACKENDS); do \
	    discovery_query=""; \
	    case "$$backend" in \
	        prometheus) \
	            file=e2e/compose.prometheus.yml; \
	            query="curl -sfG 'http://localhost:9090/api/v1/query' --data-urlencode 'query=count({__name__=~\".+\"})'"; \
	            discovery_query="curl -sfG 'http://localhost:9090/api/v1/label/resourceId/values'"; \
	            log_container=core; log_path=/opt/opennms/logs/karaf.log ;; \
	        mimir) \
	            file=e2e/compose.mimir.yml; \
	            query="curl -sfG 'http://localhost:9009/prometheus/api/v1/query' -H 'X-Scope-OrgID: e2e' --data-urlencode 'query=count({__name__=~\".+\"})'"; \
	            discovery_query="curl -sfG 'http://localhost:9009/prometheus/api/v1/label/resourceId/values' -H 'X-Scope-OrgID: e2e'"; \
	            log_container=core; log_path=/opt/opennms/logs/karaf.log ;; \
	        victoriametrics) \
	            file=e2e/compose.victoriametrics.yml; \
	            query="curl -sfG 'http://localhost:8428/api/v1/query' --data-urlencode 'query=count({__name__=~\".+\"})'"; \
	            discovery_query="curl -sfG 'http://localhost:8428/api/v1/label/resourceId/values'"; \
	            log_container=core; log_path=/opt/opennms/logs/karaf.log ;; \
	        sentinel) \
	            file=e2e/sentinel/compose.yml; \
	            query="curl -sfG 'http://localhost:9090/api/v1/query' --data-urlencode 'query=count({onms_instance_id=\"e2e-sentinel\"})'"; \
	            log_container=sentinel; log_path=/opt/sentinel/logs/karaf.log ;; \
	        *) \
	            echo "ERROR: unknown backend '$$backend' (known: prometheus mimir victoriametrics sentinel)" >&2; \
	            failed="$$failed $$backend"; continue ;; \
	    esac; \
	    current_file="$$file"; current_backend="$$backend"; \
	    echo; echo "=== [$$backend] starting stack ==="; \
	    docker compose -f "$$file" down -v --remove-orphans >/dev/null 2>&1 || true; \
	    docker compose -f "$$file" up -d >/dev/null; \
	    echo "=== [$$backend] waiting up to $(SMOKE_TIMEOUT)s for first samples ==="; \
	    start=$$SECONDS; \
	    ok=0; \
	    while [ $$((SECONDS - start)) -lt $(SMOKE_TIMEOUT) ]; do \
	        count=$$(eval "$$query" 2>/dev/null \
	            | python3 -c 'import json,sys; d=json.load(sys.stdin); r=d.get("data",{}).get("result",[]); print(int(r[0]["value"][1]) if r else 0)' \
	            2>/dev/null || echo 0); \
	        case "$$count" in ''|*[!0-9]*) count=0 ;; esac; \
	        if [ "$$count" -gt 0 ]; then \
	            echo "=== [$$backend] PASS: $$count series in $$((SECONDS - start))s ==="; \
	            ok=1; break; \
	        fi; \
	        sleep $(SMOKE_POLL); \
	    done; \
	    if [ "$$ok" = 1 ]; then \
	        if [ -n "$$discovery_query" ]; then \
	            echo "=== [$$backend] probing /api/v1/label/resourceId/values ==="; \
	            rid_count=$$(eval "$$discovery_query" 2>/dev/null \
	                | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get("data",[])))' \
	                2>/dev/null || echo 0); \
	            case "$$rid_count" in ''|*[!0-9]*) rid_count=0 ;; esac; \
	            if [ "$$rid_count" -gt 0 ]; then \
	                echo "=== [$$backend] PASS (label-values): $$rid_count resourceIds enumerated ==="; \
	                passed="$$passed $$backend"; \
	            else \
	                echo "=== [$$backend] FAIL: /api/v1/label/resourceId/values returned 0 values ===" >&2; \
	                echo "    (samples landed but the two-phase phase-1 endpoint is unusable)" >&2; \
	                failed="$$failed $$backend"; \
	            fi; \
	        else \
	            passed="$$passed $$backend"; \
	        fi; \
	    else \
	        echo "=== [$$backend] FAIL: no samples within $(SMOKE_TIMEOUT)s ===" >&2; \
	        echo "--- last 40 lines of $$log_container karaf.log ---" >&2; \
	        docker compose -f "$$file" exec -T "$$log_container" tail -n 40 "$$log_path" >&2 2>/dev/null || true; \
	        failed="$$failed $$backend"; \
	    fi; \
	    cleanup; \
	    current_file=""; current_backend=""; \
	done; \
	echo; echo "=== SUMMARY ==="; \
	for b in $$passed; do echo "  PASS  $$b"; done; \
	for b in $$failed; do echo "  FAIL  $$b"; done; \
	[ -z "$$failed" ]

# Convenience wrappers — each delegates to `smoke` with BACKENDS narrowed
# to one. Listed individually so `make help` shows each.
smoke-prometheus: ## Smoke against prometheus backend only
	@$(MAKE) --no-print-directory smoke BACKENDS=prometheus

smoke-mimir: ## Smoke against Grafana Mimir backend only
	@$(MAKE) --no-print-directory smoke BACKENDS=mimir

smoke-victoriametrics: ## Smoke against VictoriaMetrics backend only
	@$(MAKE) --no-print-directory smoke BACKENDS=victoriametrics

smoke-sentinel: ## Smoke against sentinel stack only (no sample driver — fails by design)
	@$(MAKE) --no-print-directory smoke BACKENDS=sentinel

sentinel-poc: kar ## Bring up Sentinel deployment proof-of-concept stack (Core + Minion + Sentinel + Kafka + Prom)
	cd e2e/sentinel && docker compose up -d

sentinel-poc-down: ## Tear down the Sentinel proof-of-concept stack and remove its volumes
	cd e2e/sentinel && docker compose down -v --remove-orphans

docs: ## Render single-page HTML documentation to docs/target/generated-docs
	$(MVN) $(MAVEN_FLAGS) -pl :prometheus-remote-writer-docs -am -DskipTests \
	  org.asciidoctor:asciidoctor-maven-plugin:process-asciidoc

sbom: ## Generate CycloneDX 1.6 aggregate SBOM (target/bom.json) — opt-in, gated by the sbom Maven profile
	$(MVN) $(MAVEN_FLAGS) -Psbom -DskipTests package

clean: ## Remove all build artifacts
	$(MVN) $(MAVEN_FLAGS) clean

test-class: ## Run a single test class; set MODULE and TEST
	@test -n "$(MODULE)" || (echo "ERROR: MODULE is required" && exit 1)
	@test -n "$(TEST)"   || (echo "ERROR: TEST is required" && exit 1)
	$(MVN) $(MAVEN_FLAGS) \
	  --projects $(MODULE) \
	  --also-make \
	  $(if $(filter %IT,$(TEST)),-Dit.test=$(TEST),-Dtest=$(TEST) -DskipTests=false) \
	  $(if $(filter %IT,$(TEST)),failsafe:integration-test failsafe:verify,test)
