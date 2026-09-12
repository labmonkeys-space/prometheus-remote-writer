/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * One shard's disk tier in isolation.
 *
 * <p>The acknowledgement cases here are the ones that used to live in
 * {@code WalFlusherTest}, ported when the WAL pipeline folded into the tier:
 * counters may only tick once the checkpoint has persisted, a failed advance
 * rewinds instead of losing the batch, and a rewind re-delivers.
 */
class OverflowBucketTest {

    private static final int MAX_PAYLOAD = 64 * 1024;
    private static final long CAP = 1L << 20;
    private static final long SEGMENT = 64 * 1024;

    private static OverflowBucket open(Path dir) throws IOException {
        return open(dir, CAP, SEGMENT, OverflowBucket.FullPolicy.REFUSE);
    }

    private static OverflowBucket open(Path dir, long cap, long segment,
                                       OverflowBucket.FullPolicy policy) throws IOException {
        return OverflowBucket.open(dir, cap, segment, policy, FsyncPolicy.BATCH,
                MAX_PAYLOAD, "shard-0");
    }

    private static MappedSample sample(int i) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "test_metric");
        labels.put("instance", "ut-" + i);
        return new MappedSample(labels, 1_000L + i, (double) i);
    }

    @Test
    void a_fresh_bucket_is_empty(@TempDir Path dir) throws IOException {
        try (OverflowBucket b = open(dir)) {
            assertThat(b.isEmpty()).isTrue();
            assertThat(b.pendingSamples()).isZero();
            assertThat(b.nextBatch(10).isEmpty()).isTrue();
        }
    }

    @Test
    void appended_samples_read_back_in_order(@TempDir Path dir) throws IOException {
        try (OverflowBucket b = open(dir)) {
            for (int i = 0; i < 5; i++) {
                assertThat(b.append(sample(i)).accepted()).isTrue();
            }
            assertThat(b.isEmpty()).isFalse();
            assertThat(b.pendingSamples()).isEqualTo(5);

            OverflowBucket.Batch batch = b.nextBatch(10);
            assertThat(batch.size()).isEqualTo(5);
            assertThat(batch.samples()).extracting(MappedSample::timestampMs)
                    .containsExactly(1_000L, 1_001L, 1_002L, 1_003L, 1_004L);
        }
    }

    @Test
    void acknowledge_empties_the_bucket_and_reports_bytes(@TempDir Path dir) throws IOException {
        try (OverflowBucket b = open(dir)) {
            for (int i = 0; i < 3; i++) b.append(sample(i));
            OverflowBucket.Batch batch = b.nextBatch(10);

            long checkpointed = b.acknowledge(batch.newOffset(), batch.size());

            assertThat(checkpointed).isPositive();
            assertThat(b.isEmpty()).isTrue();
            assertThat(b.pendingSamples()).isZero();
        }
    }

    @Test
    void a_batch_is_not_gone_until_it_is_acknowledged(@TempDir Path dir) throws IOException {
        // The durability rule: reading does not remove. Until the checkpoint
        // advances, the samples are still the bucket's problem.
        try (OverflowBucket b = open(dir)) {
            for (int i = 0; i < 3; i++) b.append(sample(i));
            b.nextBatch(10);

            assertThat(b.isEmpty()).isFalse();
            assertThat(b.pendingSamples()).isEqualTo(3);
        }
    }

    @Test
    void rewind_re_delivers_the_same_samples(@TempDir Path dir) throws IOException {
        // What a 5xx or transport error does: the batch was not taken, so it
        // comes back on the next cycle rather than being dropped.
        try (OverflowBucket b = open(dir)) {
            for (int i = 0; i < 3; i++) b.append(sample(i));
            OverflowBucket.Batch first = b.nextBatch(10);
            assertThat(first.size()).isEqualTo(3);

            b.rewind("retry-after-5xx");

            OverflowBucket.Batch second = b.nextBatch(10);
            assertThat(second.samples()).extracting(MappedSample::timestampMs)
                    .containsExactly(1_000L, 1_001L, 1_002L);
            assertThat(b.pendingSamples()).isEqualTo(3);
        }
    }

    @Test
    void samples_survive_a_reopen(@TempDir Path dir) throws IOException {
        // Restart: the bucket comes back holding what was never acknowledged.
        try (OverflowBucket b = open(dir)) {
            for (int i = 0; i < 4; i++) b.append(sample(i));
        }
        try (OverflowBucket reopened = open(dir)) {
            assertThat(reopened.isEmpty()).isFalse();
            assertThat(reopened.pendingSamples()).isEqualTo(4);
            assertThat(reopened.nextBatch(10).samples())
                    .extracting(MappedSample::timestampMs)
                    .containsExactly(1_000L, 1_001L, 1_002L, 1_003L);
        }
    }

    @Test
    void an_acknowledged_batch_does_not_come_back_after_a_reopen(@TempDir Path dir)
            throws IOException {
        try (OverflowBucket b = open(dir)) {
            for (int i = 0; i < 4; i++) b.append(sample(i));
            OverflowBucket.Batch batch = b.nextBatch(10);
            b.acknowledge(batch.newOffset(), batch.size());
        }
        try (OverflowBucket reopened = open(dir)) {
            assertThat(reopened.isEmpty()).isTrue();
            assertThat(reopened.nextBatch(10).isEmpty()).isTrue();
        }
    }

    @Test
    void refuse_policy_refuses_once_the_bucket_is_at_its_bound(@TempDir Path dir)
            throws IOException {
        // 8 KiB of budget: a few dozen small samples, then no.
        try (OverflowBucket b = open(dir, 8 * 1024, 1024, OverflowBucket.FullPolicy.REFUSE)) {
            int accepted = 0;
            boolean refused = false;
            for (int i = 0; i < 10_000 && !refused; i++) {
                if (b.append(sample(i)).accepted()) accepted++;
                else refused = true;
            }
            assertThat(refused).as("the bucket must eventually refuse").isTrue();
            assertThat(accepted).isPositive();
            assertThat(b.bytes()).isLessThanOrEqualTo(8 * 1024);
        }
    }

    @Test
    void drop_oldest_policy_keeps_accepting_and_reports_what_it_evicted(@TempDir Path dir)
            throws IOException {
        try (OverflowBucket b = open(dir, 8 * 1024, 1024, OverflowBucket.FullPolicy.DROP_OLDEST)) {
            int evicted = 0;
            for (int i = 0; i < 500; i++) {
                OverflowBucket.AppendResult r = b.append(sample(i));
                assertThat(r.accepted()).as("drop-oldest always accepts").isTrue();
                evicted += r.evictedSamples();
            }
            assertThat(evicted).as("writing far past the cap must have evicted").isPositive();
            // The headline guarantee: the footprint stays inside the budget.
            assertThat(b.bytes()).isLessThanOrEqualTo(8 * 1024);
        }
    }

    @Test
    void drop_oldest_keeps_accepting_after_the_checkpoint_has_advanced(@TempDir Path dir)
            throws IOException {
        // The case the first version of this suite missed. With the checkpoint
        // still at 0 the eviction floor is 0 and eviction is unrestricted, so
        // drop-oldest looked fine. Once the checkpoint moves, a floor pinned to
        // it makes every surviving segment unevictable — and during an outage
        // the checkpoint is frozen, so the policy silently became `refuse`
        // exactly when it was needed.
        try (OverflowBucket b = open(dir, 8 * 1024, 1024, OverflowBucket.FullPolicy.DROP_OLDEST)) {
            for (int i = 0; i < 20; i++) b.append(sample(i));
            OverflowBucket.Batch first = b.nextBatch(5);
            assertThat(first.isEmpty()).isFalse();
            assertThat(b.acknowledge(first.newOffset(), first.size())).isNotNegative();
            assertThat(b.acknowledge(first.newOffset(), 0)).isNotNegative();

            int evicted = 0;
            for (int i = 20; i < 500; i++) {
                OverflowBucket.AppendResult r = b.append(sample(i));
                assertThat(r.accepted())
                        .as("drop-oldest must keep accepting once the checkpoint has moved")
                        .isTrue();
                evicted += r.evictedSamples();
            }
            assertThat(evicted).isPositive();
            assertThat(b.bytes()).isLessThanOrEqualTo(8 * 1024);
        }
    }

    @Test
    void a_bucket_evicted_out_from_under_its_reader_stays_readable(@TempDir Path dir)
            throws IOException {
        // Eviction under drop-oldest can delete frames the reader has not
        // reached. The checkpoint has to move past the hole, or the next scan
        // goes looking for segments that are gone.
        try (OverflowBucket b = open(dir, 8 * 1024, 1024, OverflowBucket.FullPolicy.DROP_OLDEST)) {
            for (int i = 0; i < 400; i++) b.append(sample(i));

            OverflowBucket.Batch batch = b.nextBatch(50);
            assertThat(batch.isEmpty()).as("the surviving tail must still be readable").isFalse();
            assertThat(b.acknowledge(batch.newOffset(), batch.size())).isNotNegative();
        }
    }

    @Test
    void buckets_in_different_directories_do_not_share_segments(@TempDir Path root)
            throws IOException {
        Path a = root.resolve("shard-0");
        Path b = root.resolve("shard-1");
        try (OverflowBucket ba = open(a); OverflowBucket bb = open(b)) {
            ba.append(sample(1));
            bb.append(sample(2));

            assertThat(ba.nextBatch(10).samples()).extracting(MappedSample::timestampMs)
                    .containsExactly(1_001L);
            assertThat(bb.nextBatch(10).samples()).extracting(MappedSample::timestampMs)
                    .containsExactly(1_002L);
        }
        // Each shard's segments live under its own directory, which is what
        // lets the buckets be independent ordered logs.
        assertThat(a.toFile().list()).anyMatch(n -> n.endsWith(".seg"));
        assertThat(b.toFile().list()).anyMatch(n -> n.endsWith(".seg"));
    }

    @Test
    void the_enqueue_stamp_survives_the_round_trip(@TempDir Path dir) throws IOException {
        // D3's stamp rides in the frame, so a replayed sample's latency still
        // counts from its original store() rather than from the replay.
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "test_metric");
        MappedSample original = new MappedSample(labels, 1_000L, 1.0, 123_456L);
        try (OverflowBucket b = open(dir)) {
            b.append(original);
            assertThat(b.nextBatch(1).samples().get(0).enqueuedEpochMs()).isEqualTo(123_456L);
        }
    }
}
