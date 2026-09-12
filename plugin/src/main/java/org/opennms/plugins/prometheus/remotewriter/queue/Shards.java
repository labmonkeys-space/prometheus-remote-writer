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
import java.util.concurrent.atomic.AtomicLong;
import java.util.Objects;
import java.util.function.Function;

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
 * the same series, which breaks per-series ordering. A full shard refuses
 * the sample ({@link #tryEnqueue} returns {@code false}) exactly like the
 * classic full queue; spilling to a sibling shard would likewise break
 * ordering.
 */
public final class Shards {

    private final SampleQueue[] queues;
    private final Flusher[] flushers;
    private final AtomicLong samplesDroppedQueueFull = new AtomicLong();
    /** Maximum total depth seen right after a successful offer: the peak
     *  the writer threads actually reached, not a later sample of it. */
    private final AtomicLong depthHighWater = new AtomicLong();

    public Shards(int shardCount,
                  int totalQueueCapacity,
                  RemoteWriteHttpClient httpClient,
                  int batchSize,
                  long flushIntervalMs,
                  long lingerMs,
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
            flushers[i] = new Flusher(queues[i], httpClient, batchSize, flushIntervalMs, lingerMs,
                    metrics, builder, threadName);
        }
    }

    /**
     * Series → shard assignment. Keyed on the sample's full label map: a
     * Prometheus series IS its complete label set, and {@link Map#hashCode()}
     * is content-based and iteration-order-independent by the {@code Map}
     * contract — so equal label sets hash identically regardless of how the
     * map was built. This is the same identity the wire builders group
     * {@code TimeSeries} entries by — they key on
     * {@link org.opennms.plugins.prometheus.remotewriter.wire.SeriesKey},
     * which is equal exactly when the label maps are — so "one series, one
     * shard, one in-flight request" stays airtight.
     */
    public static int shardFor(Map<String, String> labels, int shardCount) {
        if (shardCount == 1) return 0;
        return Math.floorMod(labels.hashCode(), shardCount);
    }

    /** Route {@code sample} to its shard and offer it; see {@link SampleQueue#tryEnqueue}. */
    public boolean tryEnqueue(MappedSample sample) {
        if (!queues[shardOf(sample)].tryEnqueue(sample)) return false;
        depthHighWater.accumulateAndGet(totalDepth(), Math::max);
        return true;
    }

    public long depthHighWater() { return depthHighWater.get(); }

    public int shardOf(MappedSample sample) {
        return shardFor(sample.labels(), queues.length);
    }

    public int remainingCapacity(int shard) { return queues[shard].remainingCapacity(); }

    /** Count {@code n} samples refused because a shard's queue was full.
     *  One counter for the whole pipeline because only the total is
     *  exported; the caller knows the refusing shard if per-shard drop
     *  gauges are ever wanted. */
    public void countDroppedQueueFull(long n) {
        if (n > 0) samplesDroppedQueueFull.addAndGet(n);
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

    public long totalSamplesDroppedQueueFull() { return samplesDroppedQueueFull.get(); }

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
