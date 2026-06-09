/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Standalone gateway entry point: load config, start observability, then run
//! the Kafka → Remote Write loop.

use anyhow::{Context, Result};
use figment::providers::{Env, Format, Toml};
use figment::Figment;
use gateway::sender::Sender;
use gateway::{ingest, observability};
use gateway_core::{Config, WireVersion};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config().context("loading configuration")?;
    init_tracing(&config.runtime.log_level);

    info!(
        topic = %config.kafka.topic,
        endpoint = %config.remote_write.endpoint,
        wire_version = config.remote_write.wire_version,
        namespace = %config.mapping.namespace,
        "starting OpenNMS Remote Write gateway"
    );

    let (handle, ready) = observability::init()?;
    let sender = Sender::new(
        config.remote_write.endpoint.clone(),
        WireVersion::from_u8(config.remote_write.wire_version),
        &config.remote_write.headers,
    )?;

    let obs = tokio::spawn(observability::serve(
        config.runtime.listen.clone(),
        handle,
        ready.clone(),
    ));

    // Readiness is flipped on by the ingest loop once the consumer subscribes,
    // and off again on a Kafka receive error — so /readyz reflects Kafka health.
    tokio::select! {
        result = ingest::run(&config, sender, ready.clone()) => result.context("ingestion loop")?,
        result = obs => result.context("observability task")?.context("observability server")?,
    }

    Ok(())
}

/// Load configuration from an optional TOML file (`CONFIG_FILE`) overlaid with
/// `GATEWAY_`-prefixed environment variables (nested keys use `__`, e.g.
/// `GATEWAY_KAFKA__BROKERS`).
fn load_config() -> Result<Config> {
    let mut figment = Figment::new();
    if let Ok(path) = std::env::var("CONFIG_FILE") {
        figment = figment.merge(Toml::file(path));
    }
    figment
        .merge(Env::prefixed("GATEWAY_").split("__"))
        .extract()
        .context("invalid or incomplete configuration")
}

fn init_tracing(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
