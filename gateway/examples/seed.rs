/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! E2E smoke seeder: publish synthetic OpenNMS `CollectionSetProtos` records to
//! a Kafka topic so the gateway → backend path can be exercised deterministically
//! without driving live OpenNMS collection.
//!
//! Streams a fresh record roughly once a second (so the consumer picks it up
//! regardless of when it joins the group). Env:
//!   SEED_BROKERS (default localhost:29092), SEED_TOPIC (default metrics),
//!   SEED_COUNT (default 60).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gateway_core::proto::{
    collection_set_resource::Resource, numeric_attribute::Type, CollectionSet,
    CollectionSetResource, NodeLevelResource, NumericAttribute,
};
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let brokers = std::env::var("SEED_BROKERS").unwrap_or_else(|_| "localhost:29092".to_string());
    let topic = std::env::var("SEED_TOPIC").unwrap_or_else(|_| "metrics".to_string());
    let count: u32 = std::env::var("SEED_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()?;

    println!("seeding {count} CollectionSet record(s) to {brokers}/{topic}");
    for i in 0..count {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let cs = CollectionSet {
            timestamp: now_ms,
            resource: vec![CollectionSetResource {
                resource: Some(Resource::Node(NodeLevelResource {
                    node_id: 1,
                    foreign_source: "e2e".into(),
                    foreign_id: "seed".into(),
                    node_label: "seed-node".into(),
                    location: "Default".into(),
                })),
                resource_id: "node[1]".into(),
                resource_name: "seed-node".into(),
                resource_type_name: "node".into(),
                string: Vec::new(),
                numeric: vec![NumericAttribute {
                    group: "mib2-tcp".into(),
                    name: "tcpCurrEstab".into(),
                    value: 40.0 + i as f64,
                    r#type: Type::Gauge as i32,
                    metric_value: Some(40.0 + i as f64),
                }],
            }],
        };

        let payload = cs.encode_to_vec();
        producer
            .send(
                FutureRecord::to(&topic).payload(&payload).key("seed"),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("kafka send failed: {e}"))?;

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    println!("done seeding");
    Ok(())
}
