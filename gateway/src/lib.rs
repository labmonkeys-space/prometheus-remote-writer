/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Gateway internals exposed as a library so the consume → map → batch → flush →
//! commit loop and the Remote Write sender can be driven from integration tests.
//! The `onms-remote-write-gateway` binary is a thin entry point over these modules.

pub mod ingest;
pub mod observability;
pub mod sender;
