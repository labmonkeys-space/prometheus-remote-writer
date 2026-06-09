/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Gateway configuration model.
//!
//! Plain serde-deserializable structs shared by the core and the gateway
//! binary. The binary is responsible for loading these from the environment
//! and/or a file and for validation.

use serde::Deserialize;

/// Top-level gateway configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Kafka ingestion settings.
    pub kafka: KafkaConfig,
    /// Remote Write output settings.
    pub remote_write: RemoteWriteConfig,
    /// Metric mapping settings.
    #[serde(default)]
    pub mapping: MappingConfig,
    /// Process runtime settings.
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

/// Kafka consumer settings.
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    /// Comma-separated bootstrap brokers (`host:port,...`).
    pub brokers: String,
    /// Topic carrying `CollectionSetProtos` records.
    pub topic: String,
    /// Consumer group id.
    pub group_id: String,
}

/// Remote Write output settings.
#[derive(Debug, Clone, Deserialize)]
pub struct RemoteWriteConfig {
    /// Target Remote Write endpoint URL.
    pub endpoint: String,
    /// Wire format version: `1` (default) or `2`.
    #[serde(default = "default_wire_version")]
    pub wire_version: u8,
    /// Flush when this many samples have accumulated.
    #[serde(default = "default_batch_max_samples")]
    pub batch_max_samples: usize,
    /// Flush at least this often, in milliseconds.
    #[serde(default = "default_batch_max_interval_ms")]
    pub batch_max_interval_ms: u64,
    /// Extra HTTP headers (e.g. authorization, tenant id).
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
}

/// Metric mapping settings.
#[derive(Debug, Clone, Deserialize)]
pub struct MappingConfig {
    /// Metric-name namespace prefix.
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

/// Process runtime settings.
#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    /// `host:port` for the gateway's own `/metrics` and health endpoints.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Log level (`error`|`warn`|`info`|`debug`|`trace`).
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Max time, in milliseconds, to drain the in-flight batch on shutdown
    /// before exiting anyway (un-committed records replay on restart).
    #[serde(default = "default_shutdown_grace_ms")]
    pub shutdown_grace_ms: u64,
}

impl Default for MappingConfig {
    fn default() -> Self {
        Self {
            namespace: default_namespace(),
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            log_level: default_log_level(),
            shutdown_grace_ms: default_shutdown_grace_ms(),
        }
    }
}

fn default_wire_version() -> u8 {
    1
}
fn default_batch_max_samples() -> usize {
    5_000
}
fn default_batch_max_interval_ms() -> u64 {
    1_000
}
fn default_namespace() -> String {
    "onms".to_string()
}
fn default_listen() -> String {
    "0.0.0.0:9100".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_shutdown_grace_ms() -> u64 {
    10_000
}
