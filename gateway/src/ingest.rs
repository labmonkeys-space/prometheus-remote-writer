/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */

//! Kafka ingestion: consume `CollectionSetProtos`, map, batch, flush, commit.
//!
//! Offsets are committed only after a batch flushes successfully, giving
//! at-least-once delivery with crash-replay from the last committed offset.
//! Backpressure is structural: the consume loop performs each flush inline and
//! does not poll Kafka while a flush (which blocks until the write resolves) is
//! in flight, so the in-memory batch is bounded by `batch_max_samples`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use gateway_core::config::Config;
use gateway_core::proto::CollectionSet;
use gateway_core::{MappedSample, Mapper};
use prost::Message as _;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::{Offset, TopicPartitionList};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info, warn};

use crate::sender::Sender;

/// Run the consume → map → batch → flush → commit loop until cancelled.
pub async fn run(config: &Config, sender: Sender, ready: Arc<AtomicBool>) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka.brokers)
        .set("group.id", &config.kafka.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .create()
        .context("creating Kafka consumer")?;

    consumer
        .subscribe(&[config.kafka.topic.as_str()])
        .with_context(|| format!("subscribing to topic `{}`", config.kafka.topic))?;

    let mapper = Mapper::new(config.mapping.namespace.clone());
    let max_samples = config.remote_write.batch_max_samples;
    let interval = Duration::from_millis(config.remote_write.batch_max_interval_ms);

    // A persistent ticker decoupled from message arrival, so the max-interval
    // flush fires regardless of consume cadence. A missed tick is delayed (not
    // bursted) since flushing is the only action.
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.reset(); // skip the immediate first tick

    let mut batch: Vec<MappedSample> = Vec::with_capacity(max_samples);
    // Highest (offset + 1) seen per (topic, partition) since the last commit.
    let mut pending: HashMap<(String, i32), i64> = HashMap::new();

    let mut sigterm = signal(SignalKind::terminate()).context("installing SIGTERM handler")?;

    // Subscription succeeded; the consumer is live.
    ready.store(true, Ordering::Relaxed);
    info!(topic = %config.kafka.topic, group = %config.kafka.group_id, "consumer started");

    loop {
        tokio::select! {
            biased;

            _ = sigterm.recv() => {
                info!("SIGTERM received; draining and committing before exit");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("SIGINT received; draining and committing before exit");
                break;
            }

            received = consumer.recv() => {
                match received {
                    Ok(msg) => {
                        ready.store(true, Ordering::Relaxed);
                        let key = (msg.topic().to_string(), msg.partition());
                        let next_offset = msg.offset() + 1;
                        let payload = msg.payload().map(|p| p.to_vec());
                        drop(msg);

                        pending
                            .entry(key)
                            .and_modify(|o| *o = (*o).max(next_offset))
                            .or_insert(next_offset);

                        if let Some(bytes) = payload {
                            match CollectionSet::decode(&bytes[..]) {
                                Ok(cs) => {
                                    metrics::counter!("gateway_records_consumed_total").increment(1);
                                    let mut mapped = mapper.map(&cs);
                                    // Drop non-finite values and non-positive timestamps;
                                    // backends reject them and one would 4xx-drop the batch.
                                    let before = mapped.len();
                                    mapped.retain(|s| s.value.is_finite() && s.timestamp_ms > 0);
                                    let dropped = (before - mapped.len()) as u64;
                                    if dropped > 0 {
                                        metrics::counter!("gateway_samples_dropped_total")
                                            .increment(dropped);
                                    }
                                    metrics::counter!("gateway_samples_mapped_total")
                                        .increment(mapped.len() as u64);
                                    batch.extend(mapped);
                                }
                                Err(err) => {
                                    warn!(error = %err, "skipping malformed CollectionSet record");
                                    metrics::counter!("gateway_records_malformed_total").increment(1);
                                }
                            }
                        }

                        if batch.len() >= max_samples {
                            flush(&consumer, &sender, &mut batch, &mut pending).await;
                        }
                    }
                    Err(err) => {
                        ready.store(false, Ordering::Relaxed);
                        error!(error = %err, "Kafka receive error");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            _ = ticker.tick() => {
                flush(&consumer, &sender, &mut batch, &mut pending).await;
            }
        }
    }

    // Graceful shutdown: stop reporting ready, drain the in-flight batch, and
    // commit so we don't reprocess what was already written.
    ready.store(false, Ordering::Relaxed);
    flush(&consumer, &sender, &mut batch, &mut pending).await;
    info!("gateway stopped");
    Ok(())
}

/// Flush the current batch, then commit its offsets. No-op on an empty batch.
async fn flush(
    consumer: &StreamConsumer,
    sender: &Sender,
    batch: &mut Vec<MappedSample>,
    pending: &mut HashMap<(String, i32), i64>,
) {
    if batch.is_empty() {
        return;
    }

    // `send` resolves only after the write is accepted (2xx) or dropped (4xx);
    // both advance offsets. Persistent 5xx blocks here, which is the intended
    // backpressure.
    let outcome = sender.send(batch).await;
    let flushed = batch.len() as u64;

    let mut tpl = TopicPartitionList::new();
    for ((topic, partition), offset) in pending.iter() {
        let _ = tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset));
    }
    match consumer.commit(&tpl, CommitMode::Sync) {
        Ok(()) => {
            metrics::counter!("gateway_samples_flushed_total").increment(flushed);
            metrics::counter!("gateway_offset_commits_total").increment(1);
        }
        Err(err) => error!(error = %err, "offset commit failed; records may be reprocessed"),
    }

    tracing::debug!(?outcome, flushed, "batch flushed");
    batch.clear();
    pending.clear();
}
