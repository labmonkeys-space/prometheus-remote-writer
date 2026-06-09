# ==============================================================================
# OpenNMS Remote Write Gateway — Build Facade
#
# CI invokes these make targets, never cargo directly, so local and CI command
# surfaces stay in sync.
#
#   make build     Compile the workspace (debug)
#   make test      Run unit tests
#   make verify    fmt check + clippy + tests (what CI runs)
#   make fmt       Format the workspace
#   make clippy    Lint with warnings denied
#   make run       Run the gateway locally
#   make image     Build the OCI container image
#   make release   Compile the workspace (release)
#   make smoke     End-to-end smoke against the e2e stacks (needs Docker)
#   make clean     Remove build artifacts
# ==============================================================================

SHELL := /bin/bash

CARGO       ?= cargo
IMAGE       ?= onms-remote-write-gateway
IMAGE_TAG   ?= dev

# Smoke harness: per-backend deadline/poll and which backends to exercise.
SMOKE_DEFAULT_BACKENDS ?= prometheus mimir victoriametrics
BACKENDS               ?= $(SMOKE_DEFAULT_BACKENDS)
SMOKE_TIMEOUT          ?= 600
SMOKE_POLL             ?= 15

.PHONY: help build test verify fmt fmt-check clippy run image release \
        smoke smoke-prometheus smoke-mimir smoke-victoriametrics clean

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Compile the workspace (debug)
	$(CARGO) build --workspace

test: ## Run unit tests
	$(CARGO) test --workspace

verify: fmt-check clippy test ## fmt check + clippy + tests (CI entrypoint)

fmt: ## Format the workspace
	$(CARGO) fmt --all

fmt-check: ## Check formatting without modifying files
	$(CARGO) fmt --all --check

clippy: ## Lint with warnings denied
	$(CARGO) clippy --workspace --all-targets -- -D warnings

run: ## Run the gateway locally
	$(CARGO) run -p gateway

release: ## Compile the workspace (release)
	$(CARGO) build --workspace --release

image: ## Build the OCI container image ($(IMAGE):$(IMAGE_TAG))
	docker build -t $(IMAGE):$(IMAGE_TAG) .

smoke: ## E2E smoke for BACKENDS: OpenNMS -> Kafka -> gateway -> backend (needs Docker)
	@set -o pipefail; \
	failed=""; passed=""; cur=""; \
	cleanup() { [ -n "$$cur" ] && docker compose -f "e2e/compose.$$cur.yml" down -v --remove-orphans >/dev/null 2>&1 || true; }; \
	trap 'cleanup; echo; echo "=== interrupted ==="; exit 130' INT TERM; \
	for be in $(BACKENDS); do \
	    cur="$$be"; file="e2e/compose.$$be.yml"; \
	    case "$$be" in \
	        prometheus)      q="http://localhost:9090/api/v1/query"; hdr="" ;; \
	        mimir)           q="http://localhost:9009/prometheus/api/v1/query"; hdr="-HX-Scope-OrgID:e2e" ;; \
	        victoriametrics) q="http://localhost:8428/api/v1/query"; hdr="" ;; \
	        *) echo "ERROR: unknown backend '$$be'" >&2; failed="$$failed $$be"; continue ;; \
	    esac; \
	    echo; echo "=== [$$be] bringing up stack (building gateway) ==="; \
	    docker compose -f "$$file" down -v --remove-orphans >/dev/null 2>&1 || true; \
	    docker compose -f "$$file" up -d --build >/dev/null; \
	    echo "=== [$$be] waiting for the OpenNMS REST API ==="; \
	    for i in $$(seq 1 90); do \
	        curl -sf -u admin:admin http://localhost:8980/opennms/rest/info >/dev/null 2>&1 && break; \
	        sleep 5; \
	    done; \
	    echo "=== [$$be] provisioning an ICMP node so collection produces metrics ==="; \
	    curl -sf -u admin:admin -X POST http://localhost:8980/opennms/rest/requisitions \
	        -H 'Content-Type: application/xml' \
	        -d '<model-import foreign-source="e2e"><node foreign-id="onms-core" node-label="onms-core"><interface ip-addr="127.0.0.1" snmp-primary="N"><monitored-service service-name="ICMP"/></interface></node></model-import>' >/dev/null 2>&1 || true; \
	    curl -sf -u admin:admin -X PUT 'http://localhost:8980/opennms/rest/requisitions/e2e/import?rescanExisting=true' >/dev/null 2>&1 || true; \
	    echo "=== [$$be] waiting up to $(SMOKE_TIMEOUT)s for onms_* series at the backend ==="; \
	    start=$$SECONDS; ok=0; \
	    while [ $$((SECONDS - start)) -lt $(SMOKE_TIMEOUT) ]; do \
	        n=$$(curl -sfG $$hdr "$$q" --data-urlencode 'query=count({__name__=~"onms_.+"})' 2>/dev/null \
	            | python3 -c 'import json,sys; d=json.load(sys.stdin); r=d.get("data",{}).get("result",[]); print(int(float(r[0]["value"][1])) if r else 0)' \
	            2>/dev/null || echo 0); \
	        case "$$n" in ''|*[!0-9]*) n=0 ;; esac; \
	        if [ "$$n" -gt 0 ]; then \
	            echo "=== [$$be] PASS: $$n onms_* series after $$((SECONDS - start))s ==="; ok=1; break; \
	        fi; \
	        sleep $(SMOKE_POLL); \
	    done; \
	    if [ "$$ok" = 1 ]; then passed="$$passed $$be"; else \
	        echo "=== [$$be] FAIL: no onms_* series within $(SMOKE_TIMEOUT)s ===" >&2; \
	        echo "--- last 40 gateway log lines ---" >&2; \
	        docker compose -f "$$file" logs --tail 40 gateway >&2 2>/dev/null || true; \
	        failed="$$failed $$be"; \
	    fi; \
	    cleanup; cur=""; \
	done; \
	echo; echo "=== SUMMARY ==="; \
	for b in $$passed; do echo "  PASS  $$b"; done; \
	for b in $$failed; do echo "  FAIL  $$b"; done; \
	[ -z "$$failed" ]

smoke-prometheus: ## Smoke against the prometheus backend only
	@$(MAKE) --no-print-directory smoke BACKENDS=prometheus

smoke-mimir: ## Smoke against the Mimir backend only
	@$(MAKE) --no-print-directory smoke BACKENDS=mimir

smoke-victoriametrics: ## Smoke against the VictoriaMetrics backend only
	@$(MAKE) --no-print-directory smoke BACKENDS=victoriametrics

clean: ## Remove build artifacts
	$(CARGO) clean
