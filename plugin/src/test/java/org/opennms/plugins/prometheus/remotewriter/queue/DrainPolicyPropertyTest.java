/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders;

/**
 * The ordering property, over generated histories rather than hand-picked ones.
 *
 * <p>The interesting states of the tier are reached by interleaving: arrivals
 * that fit, arrivals that spill, drains that empty the bucket, drains that
 * fail and rewind. Hand-written cases cover the interleavings someone thought
 * of, and the two ordering bugs in this feature were both interleavings nobody
 * did. So this drives one shard through a pseudo-random history and asserts the
 * property that has to hold whatever the order of events.
 *
 * <p>Under {@code ordered}: for each series, the timestamps the backend
 * acknowledges are strictly increasing. Under {@code concurrent}: no such
 * guarantee — the assertion there is that both tiers make progress rather than
 * one starving.
 *
 * <p>Seeded and printed on failure, so a failing history is replayable.
 */
class DrainPolicyPropertyTest {

    private static final int MAX_PAYLOAD = 64 * 1024;
    private static final int QUEUE_CAPACITY = 8;
    private static final int BATCH = 3;
    private static final int SERIES = 4;

    /** A shard driven by hand: no threads, so the history is deterministic. */
    private record Harness(Shards shards, OverflowBucket bucket) {}

    private static Harness harness(Path dir, OverflowBucket.DrainPolicy drain) {
        OverflowBucket[] captured = new OverflowBucket[1];
        Shards shards = new Shards(1, QUEUE_CAPACITY, mock(RemoteWriteHttpClient.class),
                BATCH, 1_000, 0, new PluginMetrics(),
                RemoteWriteRequestBuilders.forVersion(1),
                shard -> {
                    try {
                        captured[0] = OverflowBucket.open(dir.resolve("shard-" + shard),
                                1L << 20, 1 << 17, OverflowBucket.FullPolicy.REFUSE,
                                FsyncPolicy.BATCH, MAX_PAYLOAD, "shard-" + shard);
                        return captured[0];
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }, drain);
        return new Harness(shards, captured[0]);
    }

    private static MappedSample sample(int series, long ts) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "m");
        labels.put("series", "s" + series);
        return new MappedSample(labels, ts, (double) ts);
    }

    /**
     * One generated history. Returns the timestamps acknowledged per series,
     * in the order the backend saw them.
     */
    private static Map<Integer, List<Long>> run(Path dir, OverflowBucket.DrainPolicy drain,
                                                long seed, StringBuilder log) throws IOException {
        Random rnd = new Random(seed);
        Map<Integer, List<Long>> acked = new HashMap<>();
        long ts = 0;
        int memoryDrains = 0;
        int diskDrains = 0;

        Harness h = harness(dir, drain);
        try (Shards shards = h.shards()) {
            OverflowBucket bucket = h.bucket();
            for (int step = 0; step < 400; step++) {
                int action = rnd.nextInt(100);
                if (action < 55) {
                    // Arrival. One series at a time keeps per-series order
                    // meaningful: the offered order is the timestamp order.
                    int series = rnd.nextInt(SERIES);
                    shards.accept(sample(series, ++ts));
                    log.append("a").append(series).append(' ');
                } else if (action < 85) {
                    // A drain that the backend accepts.
                    if (!bucket.isEmpty()) {
                        OverflowBucket.Batch b = bucket.nextBatch(BATCH);
                        if (!b.isEmpty()) {
                            record(acked, b.samples());
                            bucket.acknowledge(b.newOffset(), b.size());
                            diskDrains++;
                            log.append("D").append(b.size()).append(' ');
                            continue;
                        }
                    }
                    List<MappedSample> mem = shards.queuesForTesting().get(0).drain(BATCH);
                    if (!mem.isEmpty()) {
                        record(acked, mem);
                        memoryDrains++;
                        log.append("M").append(mem.size()).append(' ');
                    }
                } else {
                    // A drain the backend refuses: the disk reader rewinds and
                    // the same samples come back later.
                    if (!bucket.isEmpty()) {
                        OverflowBucket.Batch b = bucket.nextBatch(BATCH);
                        if (!b.isEmpty()) {
                            bucket.rewind("property-test-failure");
                            log.append("R").append(b.size()).append(' ');
                        }
                    }
                }
            }
            // Drain whatever is left, disk first so the ordered invariant is
            // given its best chance rather than being broken by the teardown.
            OverflowBucket.Batch b;
            while (!(b = bucket.nextBatch(BATCH)).isEmpty()) {
                record(acked, b.samples());
                bucket.acknowledge(b.newOffset(), b.size());
            }
            List<MappedSample> tail;
            while (!(tail = shards.queuesForTesting().get(0).drain(BATCH)).isEmpty()) {
                record(acked, tail);
            }
            log.append("| disk=").append(diskDrains).append(" mem=").append(memoryDrains);
        }
        return acked;
    }

    private static void record(Map<Integer, List<Long>> acked, List<MappedSample> samples) {
        for (MappedSample s : samples) {
            int series = Integer.parseInt(s.labels().get("series").substring(1));
            acked.computeIfAbsent(series, k -> new ArrayList<>()).add(s.timestampMs());
        }
    }

    @Test
    void ordered_acknowledges_each_series_in_increasing_timestamp_order(@TempDir Path root)
            throws IOException {
        for (long seed = 0; seed < 25; seed++) {
            Path dir = root.resolve("ordered-" + seed);
            StringBuilder log = new StringBuilder();
            Map<Integer, List<Long>> acked = run(dir, OverflowBucket.DrainPolicy.ORDERED, seed, log);

            assertThat(acked).as("seed %d produced no acknowledged samples: %s", seed, log)
                    .isNotEmpty();
            for (Map.Entry<Integer, List<Long>> e : acked.entrySet()) {
                assertThat(e.getValue())
                        .as("series %d out of order under ordered, seed %d%n  history: %s",
                                e.getKey(), seed, log)
                        .isSorted();
            }
        }
    }

    @Test
    void ordered_acknowledges_every_sample_exactly_once(@TempDir Path root) throws IOException {
        // A rewind must re-deliver, not duplicate, and an acknowledgement must
        // not skip. Both were real bugs in this feature.
        for (long seed = 100; seed < 115; seed++) {
            Path dir = root.resolve("once-" + seed);
            StringBuilder log = new StringBuilder();
            Map<Integer, List<Long>> acked = run(dir, OverflowBucket.DrainPolicy.ORDERED, seed, log);

            List<Long> all = new ArrayList<>();
            acked.values().forEach(all::addAll);
            assertThat(all).as("duplicate acknowledgement, seed %d%n  history: %s", seed, log)
                    .doesNotHaveDuplicates();
        }
    }

    @Test
    void concurrent_makes_progress_on_both_tiers(@TempDir Path root) throws IOException {
        // Ordering is explicitly given up here, so the property is liveness:
        // neither tier is starved by the other.
        for (long seed = 200; seed < 210; seed++) {
            Path dir = root.resolve("concurrent-" + seed);
            StringBuilder log = new StringBuilder();
            Map<Integer, List<Long>> acked =
                    run(dir, OverflowBucket.DrainPolicy.CONCURRENT, seed, log);

            assertThat(acked).as("seed %d: %s", seed, log).isNotEmpty();
            List<Long> all = new ArrayList<>();
            acked.values().forEach(all::addAll);
            assertThat(all).as("duplicate acknowledgement, seed %d%n  history: %s", seed, log)
                    .doesNotHaveDuplicates();
        }
    }
}
