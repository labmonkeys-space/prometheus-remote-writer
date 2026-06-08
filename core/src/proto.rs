/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Generated protobuf types.
//!
//! - [`prometheus`]: Remote Write v1 (`WriteRequest`, `TimeSeries`, `Label`, `Sample`).
//! - [`io::prometheus::write::v2`]: Remote Write v2 (`Request` with interned `symbols`).
//! - Root items (`CollectionSet`, `CollectionSetResource`, `NumericAttribute`, …):
//!   the OpenNMS Kafka Producer `CollectionSetProtos` contract (empty package).

#![allow(clippy::all)]
#![allow(rustdoc::all)]

include!(concat!(env!("OUT_DIR"), "/_protos.rs"));
