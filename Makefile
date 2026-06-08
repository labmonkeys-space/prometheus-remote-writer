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

.PHONY: help build test verify fmt fmt-check clippy run image release smoke clean

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

smoke: ## End-to-end smoke against the e2e stacks (needs Docker)
	@echo "e2e smoke harness is being rebuilt for the gateway (see openspec tasks group 7)"; exit 1

clean: ## Remove build artifacts
	$(CARGO) clean
