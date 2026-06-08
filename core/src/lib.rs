/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Core library for the OpenNMS Remote Write gateway: protobuf types, metric
//! mapping, and Prometheus Remote Write (v1/v2) encoding. Free of Kafka, HTTP,
//! and async concerns so the mapping and wire logic can be unit-tested in
//! isolation.

pub mod config;
pub mod mapping;
pub mod output;
pub mod proto;
pub mod sanitize;

pub use config::Config;
pub use mapping::{MappedSample, Mapper};
pub use output::{encode, Encoded, WireVersion};
