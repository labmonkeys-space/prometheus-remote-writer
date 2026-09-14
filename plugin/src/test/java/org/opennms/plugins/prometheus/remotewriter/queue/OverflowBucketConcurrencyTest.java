/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * The append thread and the flusher's threads share one bucket. Under
 * {@code drop-oldest} an append at the byte bound evicts segments the
 * reader may be inside; the reader must survive that, and the pending
 * count must come out exact once the dust settles (#214).
 *
 * <p>This is a race test: it pins the properties (no read fails on a closed
 * reader, the bucket drains to empty, no acknowledged or evicted frame is
 * still counted), not a schedule. It runs the two sides for a fixed number
 * of appends and drains what is left afterwards. The count's exactness is
 * pinned by the deterministic tests in {@link OverflowBucketTest} and
 * {@code CheckpointTest}: here evictions land inside in-flight batches and
 * half-acknowledged segments, where the count is an estimate by design.
 */
class OverflowBucketConcurrencyTest {

    private static final int APPENDS = 3_000;

    private static MappedSample sample(int i) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("__name__", "test_metric");
        labels.put("instance", "ut-" + i);
        return new MappedSample(labels, 1_000L + i, (double) i);
    }

    @Test
    void evicting_while_draining_neither_breaks_the_reader_nor_the_count(@TempDir Path dir) throws Exception {
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        AtomicInteger acknowledged = new AtomicInteger();
        AtomicBoolean appending = new AtomicBoolean(true);
        try (OverflowBucket b = OverflowBucket.open(dir, 8 * 1024, 1024,
                OverflowBucket.FullPolicy.DROP_OLDEST, FsyncPolicy.NEVER, 64 * 1024, "shard-0")) {

            // The append side: at an 8 KiB cap with 1 KiB segments nearly every
            // append past the first few evicts a segment.
            Thread appender = new Thread(() -> {
                try {
                    for (int i = 0; i < APPENDS; i++) {
                        assertThat(b.append(sample(i)).accepted()).isTrue();
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    appending.set(false);
                }
            }, "appender");

            // The flusher side: read and acknowledge whatever is there, the
            // way the builder and sender do, until the appends stop and the
            // bucket is empty.
            Thread drainer = new Thread(() -> {
                try {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (System.nanoTime() < deadline) {
                        OverflowBucket.Batch batch = b.nextBatch(20);
                        if (batch.isEmpty()) {
                            if (!appending.get() && b.isEmpty()) return;
                            Thread.sleep(1);
                            continue;
                        }
                        long r = b.acknowledge(batch.newOffset(), batch.size());
                        assertThat(r).as("an acknowledgement never fails on the read side").isNotNegative();
                        acknowledged.addAndGet(batch.size());
                    }
                    failures.add(new AssertionError("the bucket did not drain within 30 s; pending="
                            + b.pendingSamples()));
                } catch (Throwable t) {
                    failures.add(t);
                }
            }, "drainer");

            appender.start();
            drainer.start();
            appender.join(TimeUnit.SECONDS.toMillis(30));
            drainer.join(TimeUnit.SECONDS.toMillis(35));
            assertThat(appender.isAlive()).as("appender finished").isFalse();
            assertThat(drainer.isAlive()).as("drainer finished").isFalse();

            assertThat(failures).as("no exception escaped either side").isEmpty();
            assertThat(b.isEmpty()).as("drained to empty").isTrue();
            // The raw count, not the gauge: the gauge reads 0 for an empty
            // bucket whatever the count says.
            assertThat(b.pendingCount()).as("no acknowledged or evicted frame is still counted").isZero();
            // Something was delivered, and never more than was appended.
            assertThat(acknowledged.get()).isPositive().isLessThanOrEqualTo(APPENDS);
        }
    }
}
