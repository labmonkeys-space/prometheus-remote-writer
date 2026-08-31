/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;

/**
 * The sharded queue-mode write pipeline: N independent
 * ({@link SampleQueue}, {@link Flusher}) pairs, each owning a disjoint set
 * of series. Samples route to a shard by a hash of their full label map
 * (see {@link #shardFor}), every shard keeps at most one request in flight
 * (retries included), so the Remote Write in-order-per-series rule holds
 * structurally while shards flush in parallel — the spec permits parallel
 * requests over disjoint series sets.
 *
 * <p>With {@code shardCount == 1} this degenerates to exactly the classic
 * single-queue/single-flusher pipeline (router always returns shard 0).
 *
 * <p>Deliberately N independent queues rather than one shared queue with N
 * consumers: competing consumers could have two in-flight batches carrying
 * the same series, which breaks per-series ordering. Per-shard overflow
 * throws {@link StorageException} exactly like the classic full queue —
 * spilling to a sibling shard would likewise break ordering.
 */
public final class Shards {

    private final SampleQueue[] queues;
    private final Flusher[] flushers;

    public Shards(int shardCount,
                  int totalQueueCapacity,
                  RemoteWriteHttpClient httpClient,
                  int batchSize,
                  long flushIntervalMs,
                  PluginMetrics metrics,
                  Function<Collection<MappedSample>, BuildResult> builder) {
        if (shardCount < 1) throw new IllegalArgumentException("shardCount must be >= 1");
        Objects.requireNonNull(httpClient);
        Objects.requireNonNull(metrics);
        Objects.requireNonNull(builder);
        queues   = new SampleQueue[shardCount];
        flushers = new Flusher[shardCount];
        // Split the configured total capacity across shards without losing
        // slots to integer division (first `remainder` shards get one extra).
        // Config validation guarantees totalQueueCapacity / shardCount >=
        // batchSize, so every shard can fill a batch.
        int base = totalQueueCapacity / shardCount;
        int remainder = totalQueueCapacity % shardCount;
        for (int i = 0; i < shardCount; i++) {
            queues[i] = new SampleQueue(base + (i < remainder ? 1 : 0));
            String threadName = shardCount == 1
                    ? "prometheus-remote-writer-flusher"
                    : "prometheus-remote-writer-flusher-" + i;
            flushers[i] = new Flusher(queues[i], httpClient, batchSize, flushIntervalMs,
                    metrics, builder, threadName);
        }
    }

    /**
     * Series → shard assignment. Keyed on the sample's full label map: a
     * Prometheus series IS its complete label set, and {@link Map#hashCode()}
     * is content-based and iteration-order-independent by the {@code Map}
     * contract — so equal label sets hash identically regardless of how the
     * map was built. This is the same identity the wire builders group
     * {@code TimeSeries} entries by (map equality on the sorted label set),
     * which keeps "one series, one shard, one in-flight request" airtight.
     */
    static int shardFor(Map<String, String> labels, int shardCount) {
        if (shardCount == 1) return 0;
        return Math.floorMod(labels.hashCode(), shardCount);
    }

    public void enqueue(MappedSample sample) throws StorageException {
        queues[shardFor(sample.labels(), queues.length)].enqueue(sample);
    }

    public void start() {
        for (Flusher f : flushers) {
            f.start();
        }
    }

    /**
     * Stop all shards under ONE shared grace budget: signal every flusher
     * first (they begin their residual drains concurrently), then await each
     * with the remaining time. Sequential stop(grace) per shard would bound
     * shutdown at {@code shardCount × graceMs}.
     */
    public void stop(long graceMs) {
        for (Flusher f : flushers) {
            f.signalStop();
        }
        long deadline = System.nanoTime() + graceMs * 1_000_000L;
        for (Flusher f : flushers) {
            long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
            f.awaitStop(Math.max(1, remainingMs));
        }
    }

    public int shardCount() { return queues.length; }

    public int depth(int shard) { return queues[shard].depth(); }

    public int totalDepth() {
        int sum = 0;
        for (SampleQueue q : queues) sum += q.depth();
        return sum;
    }

    public long totalSamplesDroppedQueueFull() {
        long sum = 0;
        for (SampleQueue q : queues) sum += q.getSamplesDroppedQueueFull();
        return sum;
    }

    /**
     * Shard skew as a percentage: {@code max(depth) * 100 / mean(depth)}.
     * 100 = perfectly balanced; N×100 = everything on one shard. Returns
     * 100 when all queues are empty (no skew to report). Operators watch
     * this to spot hot series distributions that degrade a shard toward
     * single-flusher behavior.
     */
    public long skewPct() {
        int n = queues.length;
        long total = 0;
        long max = 0;
        for (SampleQueue q : queues) {
            int d = q.depth();
            total += d;
            if (d > max) max = d;
        }
        if (total == 0) return 100;
        // mean = total / n; skew = max / mean = max * n / total
        return max * 100L * n / total;
    }

    /** Package-private for tests. */
    List<SampleQueue> queuesForTesting() { return List.of(queues); }
}
