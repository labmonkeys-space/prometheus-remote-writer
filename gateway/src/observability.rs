/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! The gateway's own observability surface: a Prometheus `/metrics` endpoint
//! plus `/healthz` (liveness) and `/readyz` (readiness) HTTP checks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Install the Prometheus recorder and create the shared readiness flag.
pub fn init() -> Result<(PrometheusHandle, Arc<AtomicBool>)> {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .context("installing Prometheus metrics recorder")?;
    Ok((handle, Arc::new(AtomicBool::new(false))))
}

/// Serve `/metrics`, `/healthz`, and `/readyz` on `listen` until shut down.
pub async fn serve(listen: String, handle: PrometheusHandle, ready: Arc<AtomicBool>) -> Result<()> {
    let metrics_handle = handle.clone();
    let readiness = ready.clone();

    let app = Router::new()
        .route(
            "/metrics",
            get(move || {
                let h = metrics_handle.clone();
                async move { h.render() }
            }),
        )
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get(move || {
                let r = readiness.clone();
                async move {
                    if r.load(Ordering::Relaxed) {
                        (StatusCode::OK, "ready").into_response()
                    } else {
                        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
                    }
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("binding observability listener on {listen}"))?;
    axum::serve(listener, app)
        .await
        .context("observability server error")
}
