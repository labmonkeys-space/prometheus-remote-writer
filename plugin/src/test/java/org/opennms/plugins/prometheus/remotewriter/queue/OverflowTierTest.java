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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders;

/**
 * The tiering rule at the {@link Shards} level: when a shard spills, what
 * moves, what stays, and in what order it comes back out.
 *
 * <p>These are the cases the ordering argument rests on. A queue spills
 * because it is <em>full</em>, so at the transition the memory tier holds the
 * older samples; if only the refused sample went to disk it would be sent
 * ahead of them, because disk drains first. The fix is that the transition
 * moves the whole backlog, and these tests pin that it does.
 *
 * <p>No flusher is started, so nothing drains: what a sample's tier is at the
 * end of the call is exactly what the accept path decided.
 */
class OverflowTierTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    /** One shard, a memory tier of {@code capacity}, a bucket with room. */
    private static Shards shards(Path dir, int capacity) {
        return shards(dir, capacity, 1, 1L << 20, OverflowBucket.FullPolicy.REFUSE);
    }

    private static Shards shards(Path dir, int totalCapacity, int shardCount,
                                 long bucketBytes, OverflowBucket.FullPolicy policy) {
        return new Shards(shardCount, totalCapacity, mock(RemoteWriteHttpClient.class),
                /* batchSize */ 100, /* flushIntervalMs */ 1_000, /* lingerMs */ 0,
                new PluginMetrics(), RemoteWriteRequestBuilders.forVersion(1),
                shard -> {
                    try {
                        return OverflowBucket.open(dir.resolve("shard-" + shard), bucketBytes,
                                bucketBytes / 8, policy, FsyncPolicy.BATCH, MAX_PAYLOAD,
                                "shard-" + shard);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    /** All samples of one series, so a single-shard setup routes them together. */
    private static MappedSample sample(long ts) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "test_metric");
        labels.put("instance", "ut");
        return new MappedSample(labels, ts, (double) ts);
    }

    private static List<Long> drainBucket(Shards shards, int shard) throws IOException {
        OverflowBucket bucket = shards.bucketForTesting(shard);
        List<Long> out = new ArrayList<>();
        OverflowBucket.Batch batch;
        while (!(batch = bucket.nextBatch(1_000)).isEmpty()) {
            batch.samples().forEach(s -> out.add(s.timestampMs()));
            bucket.acknowledge(batch.newOffset(), batch.size());
        }
        return out;
    }

    @Test
    void a_sample_goes_to_memory_while_there_is_room(@TempDir Path dir) {
        try (Shards shards = shards(dir, 4)) {
            assertThat(shards.accept(sample(1))).isEqualTo(Shards.Acceptance.MEMORY);
            assertThat(shards.totalDepth()).isEqualTo(1);
            assertThat(shards.totalSamplesSpilled()).isZero();
            assertThat(shards.totalOverflowPending()).isZero();
        }
    }

    @Test
    void the_transition_moves_the_whole_backlog_ahead_of_the_refused_sample(@TempDir Path dir)
            throws IOException {
        try (Shards shards = shards(dir, 3)) {
            // Fill memory exactly.
            for (long t = 1; t <= 3; t++) {
                assertThat(shards.accept(sample(t))).isEqualTo(Shards.Acceptance.MEMORY);
            }
            // The fourth has nowhere to go in memory — it spills, and takes
            // the three older ones with it.
            assertThat(shards.accept(sample(4))).isEqualTo(Shards.Acceptance.OVERFLOW);

            assertThat(shards.totalDepth()).as("memory is empty after the transition").isZero();
            assertThat(shards.totalOverflowPending()).isEqualTo(4);
            assertThat(shards.totalSamplesSpilled()).isEqualTo(4);
            assertThat(drainBucket(shards, 0))
                    .as("the backlog goes to disk in queue order, then the refused sample")
                    .containsExactly(1L, 2L, 3L, 4L);
        }
    }

    @Test
    void an_older_memory_sample_is_not_overtaken_by_a_newer_spilled_one(@TempDir Path dir)
            throws IOException {
        // The bug the transition rule exists to prevent. Without it the bucket
        // would hold only sample 2, and disk-before-memory would send it ahead
        // of sample 1.
        try (Shards shards = shards(dir, 1)) {
            shards.accept(sample(1));
            shards.accept(sample(2));

            assertThat(drainBucket(shards, 0)).containsExactly(1L, 2L);
        }
    }

    @Test
    void a_shard_keeps_spilling_while_its_bucket_is_non_empty(@TempDir Path dir) {
        // Sticky: memory has room again after the transition drained it, but
        // using it would put new samples ahead of what is still on disk.
        try (Shards shards = shards(dir, 3)) {
            for (long t = 1; t <= 4; t++) shards.accept(sample(t));
            assertThat(shards.totalDepth()).isZero();

            for (long t = 5; t <= 10; t++) {
                assertThat(shards.accept(sample(t)))
                        .as("sample %d must not go to memory while the bucket holds data", t)
                        .isEqualTo(Shards.Acceptance.OVERFLOW);
            }
            assertThat(shards.totalDepth()).as("memory stays empty while spilling").isZero();
            assertThat(shards.totalOverflowPending()).isEqualTo(10);
        }
    }

    @Test
    void memory_is_used_again_once_the_bucket_drains(@TempDir Path dir) throws IOException {
        try (Shards shards = shards(dir, 3)) {
            for (long t = 1; t <= 4; t++) shards.accept(sample(t));
            drainBucket(shards, 0);
            assertThat(shards.bucketForTesting(0).isEmpty()).isTrue();

            assertThat(shards.accept(sample(11))).isEqualTo(Shards.Acceptance.MEMORY);
            assertThat(shards.totalDepth()).isEqualTo(1);
            assertThat(shards.totalSamplesSpilled())
                    .as("the four spilled earlier, the fifth did not").isEqualTo(4);
        }
    }

    @Test
    void per_series_order_survives_fill_spill_and_drain(@TempDir Path dir) throws IOException {
        // One series across the whole cycle: memory, transition, disk, drain,
        // back to memory. Everything the backend sees must be in offered order.
        try (Shards shards = shards(dir, 3)) {
            List<Long> seen = new ArrayList<>();
            for (long t = 1; t <= 3; t++) shards.accept(sample(t));   // memory
            for (long t = 4; t <= 9; t++) shards.accept(sample(t));   // transition + disk

            seen.addAll(drainBucket(shards, 0));
            // Bucket empty: later samples go back to memory, behind everything
            // already drained.
            for (long t = 10; t <= 12; t++) shards.accept(sample(t));
            shards.queuesForTesting().get(0).drain(100)
                    .forEach(s -> seen.add(s.timestampMs()));

            assertThat(seen).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        }
    }

    @Test
    void concurrent_producers_leave_nothing_behind_in_memory(@TempDir Path dir) throws Exception {
        // The reason accept() is serialised per shard: without the lock a
        // producer can slip a sample into memory just after another drained
        // it, stranding an old sample behind a non-empty bucket.
        int threads = 8;
        int perThread = 200;
        try (Shards shards = shards(dir, 16)) {
            CountDownLatch go = new CountDownLatch(1);
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int base = t * perThread;
                Thread w = new Thread(() -> {
                    try {
                        go.await();
                        for (int i = 0; i < perThread; i++) shards.accept(sample(base + i));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                w.start();
                workers.add(w);
            }
            go.countDown();
            for (Thread w : workers) w.join(TimeUnit.SECONDS.toMillis(30));

            assertThat(shards.bucketForTesting(0).isEmpty()).isFalse();
            assertThat(shards.totalDepth())
                    .as("no sample may sit in memory while the bucket is non-empty")
                    .isZero();
            assertThat(shards.totalSamplesSpilled()).isEqualTo((long) threads * perThread);
        }
    }

    @Test
    void a_full_bucket_refuses_under_the_refuse_policy(@TempDir Path dir) {
        try (Shards shards = shards(dir, 2, 1, 8 * 1024, OverflowBucket.FullPolicy.REFUSE)) {
            boolean refused = false;
            for (long t = 1; t <= 10_000 && !refused; t++) {
                refused = shards.accept(sample(t)) == Shards.Acceptance.REFUSED;
            }
            assertThat(refused).as("both tiers full must refuse").isTrue();
        }
    }

    @Test
    void a_full_bucket_evicts_under_drop_oldest(@TempDir Path dir) {
        try (Shards shards = shards(dir, 2, 1, 8 * 1024, OverflowBucket.FullPolicy.DROP_OLDEST)) {
            for (long t = 1; t <= 500; t++) {
                assertThat(shards.accept(sample(t)))
                        .as("drop-oldest never refuses").isNotEqualTo(Shards.Acceptance.REFUSED);
            }
            assertThat(shards.totalSamplesEvictedOverflow()).isPositive();
            assertThat(shards.totalOverflowBytes()).isLessThanOrEqualTo(8 * 1024);
        }
    }

    @Test
    void each_shard_spills_into_its_own_bucket(@TempDir Path dir) {
        // Four shards, capacity 4 total so each gets one slot and spills on
        // its second sample. Series are chosen so every shard is hit.
        try (Shards shards = shards(dir, 4, 4, 1L << 20, OverflowBucket.FullPolicy.REFUSE)) {
            for (int i = 0; i < 200; i++) {
                Map<String, String> labels = new LinkedHashMap<>();
                labels.put("__name__", "m");
                labels.put("node", "n" + i);
                shards.accept(new MappedSample(labels, i, 1.0));
            }
            int withPending = 0;
            for (int s = 0; s < 4; s++) {
                if (shards.overflowPending(s) > 0) withPending++;
            }
            assertThat(withPending).as("every shard has its own bucket holding its own samples")
                    .isEqualTo(4);
            assertThat(shards.totalOverflowPending())
                    .isEqualTo(shards.overflowPending(0) + shards.overflowPending(1)
                            + shards.overflowPending(2) + shards.overflowPending(3));
        }
    }

    @Test
    void without_a_disk_tier_a_full_shard_still_refuses(@TempDir Path dir) {
        // overflow.max-size-bytes=0: the pre-0.8.0 contract, unchanged.
        try (Shards shards = new Shards(1, 2, mock(RemoteWriteHttpClient.class), 100, 1_000, 0,
                new PluginMetrics(), RemoteWriteRequestBuilders.forVersion(1))) {
            assertThat(shards.accept(sample(1))).isEqualTo(Shards.Acceptance.MEMORY);
            assertThat(shards.accept(sample(2))).isEqualTo(Shards.Acceptance.MEMORY);
            assertThat(shards.accept(sample(3))).isEqualTo(Shards.Acceptance.REFUSED);
            assertThat(shards.overflowEnabled()).isFalse();
            assertThat(shards.totalSamplesSpilled()).isZero();
        }
    }

    @Test
    void a_recovered_bucket_puts_new_samples_behind_what_it_holds(@TempDir Path dir)
            throws IOException {
        // Restart with a non-empty bucket: the shard starts out yielding, so a
        // sample offered after the restart cannot overtake the recovered ones.
        try (Shards first = shards(dir, 3)) {
            for (long t = 1; t <= 4; t++) first.accept(sample(t));
            assertThat(first.totalOverflowPending()).isEqualTo(4);
        }
        try (Shards restarted = shards(dir, 3)) {
            assertThat(restarted.accept(sample(99)))
                    .as("a recovered non-empty bucket keeps the shard on disk")
                    .isEqualTo(Shards.Acceptance.OVERFLOW);
            assertThat(restarted.totalDepth()).isZero();
            assertThat(drainBucket(restarted, 0)).containsExactly(1L, 2L, 3L, 4L, 99L);
        }
    }
}
