/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Kafka integration tests (task 5.5) against a containerized broker.
//!
//! These exercise the consume → map → batch → flush → commit loop end-to-end
//! through a real Kafka (testcontainers) with a wiremock Remote Write backend
//! standing in for Prometheus:
//!
//! - [`crash_replay_redelivers_uncommitted_records`] proves at-least-once /
//!   crash-replay (tasks 5.1–5.3): an offset committed during warmup, a backend
//!   stall (503) that prevents the marker's offset from committing, a hard
//!   abort (crash), then a restart of the same consumer group that replays the
//!   uncommitted marker from the last committed offset.
//! - [`batching_is_bounded_by_max_samples`] proves the structural backpressure
//!   bound (task 5.4): the in-flight batch never exceeds `batch_max_samples`,
//!   regardless of producer rate, because the loop flushes inline and does not
//!   poll Kafka during a flush.
//!
//! Both require Docker and are `#[ignore]`d so `make test`/`make verify` (no
//! Docker in CI) stay green. Run them with `make integration`.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gateway::ingest;
use gateway::sender::Sender;
use gateway_core::config::{Config, KafkaConfig, MappingConfig, RemoteWriteConfig, RuntimeConfig};
use gateway_core::proto::prometheus::WriteRequest;
use gateway_core::proto::{
    collection_set_resource::Resource, numeric_attribute::Type, CollectionSet,
    CollectionSetResource, NodeLevelResource, NumericAttribute,
};
use gateway_core::WireVersion;
use prost::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use testcontainers_modules::kafka::{Kafka, KAFKA_PORT};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// Start a single-broker Kafka container and return it (kept alive by the
/// caller) alongside its host bootstrap address.
async fn start_kafka() -> (ContainerAsync<Kafka>, String) {
    let node = Kafka::default()
        .start()
        .await
        .expect("starting Kafka container");
    let port = node
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("mapped Kafka port");
    (node, format!("127.0.0.1:{port}"))
}

/// Pre-create the topic so the consumer's `latest` reset has a partition to
/// attach to before the first produce, removing the auto-create race.
async fn create_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let _ = admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await;
}

fn producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer")
}

async fn produce(producer: &FutureProducer, topic: &str, payload: Vec<u8>) {
    producer
        .send(
            FutureRecord::to(topic).payload(&payload).key("k"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)
        .expect("produce");
}

/// A minimal single-numeric `CollectionSet`. The mapper renders the metric name
/// as `onms_<group>_<name>`.
fn collection_set(group: &str, name: &str, value: f64, ts_ms: i64) -> Vec<u8> {
    CollectionSet {
        timestamp: ts_ms,
        resource: vec![CollectionSetResource {
            resource: Some(Resource::Node(NodeLevelResource {
                node_id: 1,
                foreign_source: "it".into(),
                foreign_id: "x".into(),
                node_label: "n".into(),
                location: "Default".into(),
            })),
            resource_id: "node[1]".into(),
            resource_name: "n".into(),
            resource_type_name: "node".into(),
            string: Vec::new(),
            numeric: vec![NumericAttribute {
                group: group.into(),
                name: name.into(),
                value,
                r#type: Type::Gauge as i32,
                metric_value: Some(value),
            }],
        }],
    }
    .encode_to_vec()
}

fn config(
    brokers: &str,
    topic: &str,
    group: &str,
    endpoint: &str,
    batch_max_samples: usize,
    batch_max_interval_ms: u64,
) -> Config {
    Config {
        kafka: KafkaConfig {
            brokers: brokers.into(),
            topic: topic.into(),
            group_id: group.into(),
        },
        remote_write: RemoteWriteConfig {
            endpoint: endpoint.into(),
            wire_version: 1,
            batch_max_samples,
            batch_max_interval_ms,
            headers: BTreeMap::new(),
        },
        mapping: MappingConfig {
            namespace: "onms".into(),
        },
        runtime: RuntimeConfig::default(),
    }
}

/// Spawn the ingest loop as a task whose handle can be `.abort()`ed to simulate
/// a crash (drop without a graceful commit).
fn spawn_gateway(config: Config, sender: Sender) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let ready = Arc::new(AtomicBool::new(false));
        let _ = ingest::run(&config, sender, ready).await;
    })
}

fn decode_v1(req: &Request) -> Option<WriteRequest> {
    let raw = snap::raw::Decoder::new().decompress_vec(&req.body).ok()?;
    WriteRequest::decode(&raw[..]).ok()
}

/// True if any received Remote Write request carries a series named `metric`.
fn any_request_has_metric(reqs: &[Request], metric: &str) -> bool {
    reqs.iter().filter_map(decode_v1).any(|wr| {
        wr.timeseries.iter().any(|ts| {
            ts.labels
                .iter()
                .any(|l| l.name == "__name__" && l.value == metric)
        })
    })
}

/// Largest sample count carried by any single received request.
fn max_samples_per_request(reqs: &[Request]) -> usize {
    reqs.iter()
        .filter_map(decode_v1)
        .map(|wr| {
            wr.timeseries
                .iter()
                .map(|ts| ts.samples.len())
                .sum::<usize>()
        })
        .max()
        .unwrap_or(0)
}

#[ignore = "requires Docker (Kafka testcontainer); run via `make integration`"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_replay_redelivers_uncommitted_records() {
    let (_kafka, brokers) = start_kafka().await;
    let topic = "metrics";
    let group = "it-crash-replay";
    create_topic(&brokers, topic).await;
    let producer = producer(&brokers);

    let backend = MockServer::start().await;

    // --- Phase A: establish a committed offset. The consumer subscribes with
    // `latest`, so we stream warmup records until one is flushed (a request
    // reaches the backend), which proves a flush+commit happened.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&backend)
        .await;

    let cfg = config(&brokers, topic, group, &backend.uri(), 1_000, 500);
    let sender = Sender::new(backend.uri(), WireVersion::V1, &BTreeMap::new()).unwrap();
    let gw1 = spawn_gateway(cfg, sender);

    let mut ts = 1_700_000_000_000i64;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        produce(&producer, topic, collection_set("warmup", "beat", 1.0, ts)).await;
        ts += 1;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let n = backend.received_requests().await.unwrap_or_default().len();
        if n > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "warmup never committed an offset"
        );
    }

    // --- Phase B: stall the backend so the marker's offset can never commit.
    backend.reset().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&backend)
        .await;

    produce(
        &producer,
        topic,
        collection_set("replay", "marker", 42.0, ts + 1),
    )
    .await;
    // Give the loop time to consume the marker and get wedged retrying the 503.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // --- Crash: abort the task without a graceful commit. The marker's offset
    // is provably uncommitted (every flush in phase B 503'd), so it must replay.
    gw1.abort();
    let _ = gw1.await;

    // --- Phase C: a healthy backend and a fresh consumer in the SAME group.
    // It resumes from the last committed offset (before the marker) and redelivers.
    backend.reset().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&backend)
        .await;

    let cfg2 = config(&brokers, topic, group, &backend.uri(), 1_000, 500);
    let sender2 = Sender::new(backend.uri(), WireVersion::V1, &BTreeMap::new()).unwrap();
    let gw2 = spawn_gateway(cfg2, sender2);

    let deadline = Instant::now() + Duration::from_secs(90);
    let mut replayed = false;
    while Instant::now() < deadline {
        let reqs = backend.received_requests().await.unwrap_or_default();
        if any_request_has_metric(&reqs, "onms_replay_marker") {
            replayed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    gw2.abort();
    let _ = gw2.await;

    assert!(
        replayed,
        "marker uncommitted before the crash was not replayed after restart"
    );
}

#[ignore = "requires Docker (Kafka testcontainer); run via `make integration`"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batching_is_bounded_by_max_samples() {
    let (_kafka, brokers) = start_kafka().await;
    let topic = "metrics";
    let group = "it-backpressure";
    create_topic(&brokers, topic).await;
    let producer = producer(&brokers);

    let backend = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&backend)
        .await;

    // Small sample cap, long interval: flushes are driven by the size bound, so
    // the loop must flush at exactly the cap and never accumulate more — even
    // under a producer faster than the backend.
    const CAP: usize = 5;
    let cfg = config(&brokers, topic, group, &backend.uri(), CAP, 2_000);
    let sender = Sender::new(backend.uri(), WireVersion::V1, &BTreeMap::new()).unwrap();
    let gw = spawn_gateway(cfg, sender);

    // Stream well past the cap. Distinct timestamps keep each record a distinct
    // sample (no per-series dedup collapsing the count).
    let mut ts = 1_700_000_000_000i64;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        produce(&producer, topic, collection_set("bp", "v", 1.0, ts)).await;
        ts += 1;
        tokio::time::sleep(Duration::from_millis(40)).await;
        if backend.received_requests().await.unwrap_or_default().len() >= 5 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "backend never received batched requests"
        );
    }
    gw.abort();
    let _ = gw.await;

    let reqs = backend.received_requests().await.unwrap_or_default();
    // Guard against a vacuous pass: require requests that actually decode, so
    // `max` below reflects real payloads rather than `unwrap_or(0)` over an
    // empty decoded set.
    let decoded = reqs.iter().filter_map(decode_v1).count();
    assert!(
        decoded >= 5,
        "expected >=5 decodable flushes, only {decoded} decoded"
    );

    let max = max_samples_per_request(&reqs);
    assert!(
        max <= CAP,
        "a request carried {max} samples, exceeding the batch_max_samples bound of {CAP}"
    );
    // The cap must be the *active* flush trigger, not something smaller — at
    // least one flush reached exactly CAP. (One sample per record, fast
    // producer, long interval ⇒ size-triggered flushes of exactly CAP.)
    assert_eq!(
        max, CAP,
        "size cap should be the active flush trigger; largest request had {max} samples, expected {CAP}"
    );
}
