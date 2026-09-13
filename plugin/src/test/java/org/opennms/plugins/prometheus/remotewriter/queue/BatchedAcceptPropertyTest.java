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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
 * Placing a {@code store()} call's samples per shard in one step changes
 * nothing about where each sample goes (#182).
 *
 * <p>The batched path takes a shard's accept lock once for all of a call's
 * samples bound to it, and encodes their frames before taking it. Both are
 * only worth having if the outcome is exactly the one-at-a-time outcome, so
 * this drives two identical pipelines through the same generated history,
 * one through {@link Shards#acceptAll} with whole calls and one a sample at a
 * time, and compares every observable after every step.
 *
 * <p>Seeded and printed on failure, so a failing history is replayable.
 */
class BatchedAcceptPropertyTest {

    private static final int MAX_PAYLOAD = 64 * 1024;
    private static final int SHARDS = 2;
    private static final int QUEUE_CAPACITY = 8;   // 4 a shard
    private static final int BATCH = 3;
    private static final int SERIES = 6;
    /** Kept small: every bucket drain fsyncs a checkpoint, and the small
     *  buckets rotate every few frames. 80 histories of 300 steps passed once
     *  when this was written; the default run is a sample of that. */
    private static final int STEPS = 150;
    private static final int SEEDS_PER_POLICY = 3;

    private static Shards shards(Path dir, long bucketBytes, OverflowBucket.FullPolicy full,
                                 OverflowBucket.DrainPolicy drain) {
        return new Shards(SHARDS, QUEUE_CAPACITY, mock(RemoteWriteHttpClient.class),
                BATCH, 1_000, 0, new PluginMetrics(), RemoteWriteRequestBuilders.forVersion(1),
                shard -> {
                    try {
                        Path d = Files.createDirectories(dir.resolve("shard-" + shard));
                        return OverflowBucket.open(d, bucketBytes, bucketBytes / 8, full,
                                FsyncPolicy.BATCH, MAX_PAYLOAD, "shard-" + shard);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }, drain);
    }

    private static MappedSample sample(int series, long ts) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "prop_metric");
        labels.put("series", "s" + series);
        return new MappedSample(labels, ts, (double) ts);
    }

    private static List<Long> timestamps(List<MappedSample> batch) {
        List<Long> out = new ArrayList<>(batch.size());
        for (MappedSample s : batch) out.add(s.timestampMs());
        return out;
    }

    /** Every observable of a pipeline that the placement decides. */
    private static String state(Shards s) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < SHARDS; i++) {
            b.append("shard ").append(i)
             .append(" depth=").append(s.depth(i))
             .append(" pending=").append(s.overflowPending(i))
             .append(" recovering=").append(s.isRecovering(i))
             .append(" bytes=").append(s.bucketForTesting(i).bytes()).append('\n');
        }
        b.append("spilled=").append(s.totalSamplesSpilled())
         .append(" droppedOverflowFull=").append(s.totalSamplesDroppedOverflowFull())
         .append(" evicted=").append(s.totalSamplesEvictedOverflow());
        return b.toString();
    }

    private static List<Long> drainMemory(Shards s, int shard) {
        Shards.InFlight token = s.pollMemoryBatch(shard, BATCH);
        if (token == null) return List.of();
        List<Long> ts = timestamps(token.batch());
        s.memoryBatchSettled(shard, token);
        return ts;
    }

    private static List<Long> drainBucket(Shards s, int shard, int max) throws IOException {
        OverflowBucket bucket = s.bucketForTesting(shard);
        OverflowBucket.Batch batch = bucket.nextBatch(max);
        if (batch.isEmpty()) return List.of();
        bucket.acknowledge(batch.newOffset(), batch.size());
        return timestamps(batch.samples());
    }

    private void run(Path dir, long seed, long bucketBytes, OverflowBucket.FullPolicy full,
                     OverflowBucket.DrainPolicy drain) throws IOException {
        Random rnd = new Random(seed);
        String ctx = "seed=" + seed + " full=" + full + " drain=" + drain;
        long[] nextTs = new long[SERIES];
        try (Shards batched = shards(dir.resolve("batched"), bucketBytes, full, drain);
             Shards single = shards(dir.resolve("single"), bucketBytes, full, drain)) {
            for (int step = 0; step < STEPS; step++) {
                String at = ctx + " step=" + step;
                int op = rnd.nextInt(10);
                if (op < 5) {
                    int n = 1 + rnd.nextInt(8);
                    List<MappedSample> call = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        int series = rnd.nextInt(SERIES);
                        // Same varint width for every timestamp, so every frame is the same size.
                        call.add(sample(series, 1_000_000L + (++nextTs[series])));
                    }
                    int refusedBatched = batched.acceptAll(call);
                    int refusedSingle = 0;
                    for (MappedSample m : call) {
                        if (single.accept(m) == Shards.Acceptance.REFUSED) refusedSingle++;
                    }
                    assertThat(refusedBatched).as(at + " refused").isEqualTo(refusedSingle);
                } else if (op < 7) {
                    int shard = rnd.nextInt(SHARDS);
                    assertThat(drainMemory(batched, shard)).as(at + " memory drain")
                            .isEqualTo(drainMemory(single, shard));
                } else {
                    int shard = rnd.nextInt(SHARDS);
                    int max = 1 + rnd.nextInt(BATCH);
                    assertThat(drainBucket(batched, shard, max)).as(at + " bucket drain")
                            .isEqualTo(drainBucket(single, shard, max));
                }
                assertThat(state(batched)).as(at).isEqualTo(state(single));
            }
            for (int shard = 0; shard < SHARDS; shard++) {
                List<Long> a = new ArrayList<>(), b = new ArrayList<>();
                List<Long> chunk;
                while (!(chunk = drainBucket(batched, shard, 100)).isEmpty()) a.addAll(chunk);
                while (!(chunk = drainBucket(single, shard, 100)).isEmpty()) b.addAll(chunk);
                assertThat(a).as(ctx + " final bucket " + shard).isEqualTo(b);
            }
        }
    }

    @Test
    void batched_placement_matches_one_at_a_time(@TempDir Path dir) throws IOException {
        long base = System.nanoTime();
        int run = 0;
        for (OverflowBucket.DrainPolicy drain : OverflowBucket.DrainPolicy.values()) {
            for (OverflowBucket.FullPolicy full : OverflowBucket.FullPolicy.values()) {
                for (int i = 0; i < SEEDS_PER_POLICY; i++) {
                    long seed = base + run;
                    // A small bucket so refusals and evictions happen; a roomy one
                    // so long spills and drains do.
                    long bytes = (i % 2 == 0) ? 2_400 : 64 * 1024;
                    run(dir.resolve("run-" + run++), seed, bytes, full, drain);
                }
            }
        }
    }

    @Test
    void a_call_that_crosses_into_spilling_keeps_offered_order(@TempDir Path dir) throws IOException {
        try (Shards shards = new Shards(1, 2, mock(RemoteWriteHttpClient.class), BATCH, 1_000, 0,
                new PluginMetrics(), RemoteWriteRequestBuilders.forVersion(1),
                s -> {
                    try {
                        return OverflowBucket.open(dir.resolve("b"), 1L << 20, 1L << 17,
                                OverflowBucket.FullPolicy.REFUSE, FsyncPolicy.BATCH, MAX_PAYLOAD, "b");
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })) {
            List<MappedSample> call = new ArrayList<>();
            for (long t = 1; t <= 5; t++) call.add(sample(0, 1_000_000L + t));
            assertThat(shards.acceptAll(call)).isZero();
            assertThat(shards.depth(0)).isZero();
            assertThat(drainBucket(shards, 0, 100))
                    .containsExactly(1_000_001L, 1_000_002L, 1_000_003L, 1_000_004L, 1_000_005L);
        }
    }

    @Test
    void a_bucket_refusal_partway_through_a_group(@TempDir Path dir) throws IOException {
        int frame = OverflowBucket.encodeFrame(sample(0, 1_000_001L)).remaining();
        // Room for exactly four frames; the first call puts two there.
        long cap = 4L * frame;
        try (Shards shards = new Shards(1, 1, mock(RemoteWriteHttpClient.class), BATCH, 1_000, 0,
                new PluginMetrics(), RemoteWriteRequestBuilders.forVersion(1),
                s -> {
                    try {
                        return OverflowBucket.open(dir.resolve("b"), cap, cap,
                                OverflowBucket.FullPolicy.REFUSE, FsyncPolicy.BATCH, MAX_PAYLOAD, "b");
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })) {
            assertThat(shards.acceptAll(List.of(sample(0, 1_000_001L), sample(0, 1_000_002L)))).isZero();
            assertThat(shards.overflowPending(0)).isEqualTo(2);

            List<MappedSample> call = new ArrayList<>();
            for (long t = 3; t <= 7; t++) call.add(sample(0, 1_000_000L + t));
            assertThat(shards.acceptAll(call)).isEqualTo(3);
            assertThat(drainBucket(shards, 0, 100))
                    .containsExactly(1_000_001L, 1_000_002L, 1_000_003L, 1_000_004L);
        }
    }
}
