/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy;

/**
 * What the disk accept path may no longer do, and what it must still do (#182).
 *
 * <p>A spilled sample holds its shard's accept lock for the whole append, so
 * every syscall and directory listing on this path is paid by every writer
 * thread queued behind it. These pin the cheap path: a pre-encoded frame
 * writes the same bytes as before, the byte count is exact without listing
 * the directory, and a flush does not hold the lock appends need.
 */
class WalWriterAccountingTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    private static byte[] payload(int i) {
        byte[] p = new byte[40 + (i % 7)];
        for (int j = 0; j < p.length; j++) p[j] = (byte) (i + j);
        return p;
    }

    /** Outcome of one append, comparable across the two entry points. */
    private record Outcome(boolean refused, long offsetAfter, long evictedBytes, int evictedFrames) {}

    private static Outcome viaPayload(WalWriter w, byte[] payload) throws IOException {
        try {
            WalWriter.AppendResult r = w.appendWithStats(payload);
            return new Outcome(false, r.offsetAfter(), r.evictedBytes(), r.evictedFrames());
        } catch (WalFullException full) {
            return new Outcome(true, -1, 0, full.evictedFramesBeforeFailure());
        }
    }

    private static Outcome viaFrame(WalWriter w, byte[] payload) throws IOException {
        try {
            WalWriter.AppendResult r = w.appendWithStats(Frame.encode(payload));
            return new Outcome(false, r.offsetAfter(), r.evictedBytes(), r.evictedFrames());
        } catch (WalFullException full) {
            return new Outcome(true, -1, 0, full.evictedFramesBeforeFailure());
        }
    }

    private static Map<String, byte[]> segments(Path dir) throws IOException {
        Map<String, byte[]> out = new TreeMap<>();
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                if (p.getFileName().toString().endsWith(WalSegment.SEG_EXT)) {
                    out.put(p.getFileName().toString(), Files.readAllBytes(p));
                }
            }
        }
        return out;
    }

    private static void assertSameSegments(Path a, Path b) throws IOException {
        Map<String, byte[]> sa = segments(a), sb = segments(b);
        assertThat(sb.keySet()).isEqualTo(sa.keySet());
        for (String name : sa.keySet()) {
            assertThat(sb.get(name)).as(name).isEqualTo(sa.get(name));
        }
    }

    private void compareEntryPoints(Path dir, OverflowPolicy policy, long segment, long max, int count)
            throws IOException {
        Path a = Files.createDirectories(dir.resolve("payload"));
        Path b = Files.createDirectories(dir.resolve("frame"));
        List<Outcome> oa = new ArrayList<>(), ob = new ArrayList<>();
        try (WalWriter wa = WalWriter.createNew(a, segment, max, policy, FsyncPolicy.BATCH, MAX_PAYLOAD);
             WalWriter wb = WalWriter.createNew(b, segment, max, policy, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < count; i++) {
                oa.add(viaPayload(wa, payload(i)));
                ob.add(viaFrame(wb, payload(i)));
            }
        }
        assertThat(ob).isEqualTo(oa);
        assertSameSegments(a, b);
    }

    @Test
    void a_pre_encoded_frame_writes_the_same_bytes_across_rotations(@TempDir Path dir) throws IOException {
        compareEntryPoints(dir, OverflowPolicy.BACKPRESSURE, 200, 1L << 20, 40);
    }

    @Test
    void a_pre_encoded_frame_is_refused_where_a_payload_is(@TempDir Path dir) throws IOException {
        compareEntryPoints(dir, OverflowPolicy.BACKPRESSURE, 200, 600, 40);
    }

    @Test
    void a_pre_encoded_frame_evicts_where_a_payload_does(@TempDir Path dir) throws IOException {
        compareEntryPoints(dir, OverflowPolicy.DROP_OLDEST, 200, 600, 40);
    }

    @Test
    void the_tracked_total_matches_the_files_through_rotation_gc_and_eviction(@TempDir Path dir)
            throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 200, 1_000, OverflowPolicy.DROP_OLDEST,
                FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.initTotalBytes();
            long offset = 0;
            for (int i = 0; i < 30; i++) {
                offset = w.appendWithStats(Frame.encode(payload(i))).offsetAfter();
                assertThat(w.totalBytes()).as("after append %d", i).isEqualTo(w.currentTotalBytes());
            }
            // Reclaim everything up to the middle, as an acknowledgement would.
            long reclaimed = Checkpoint.gcSegments(dir, offset / 2);
            w.reclaimed(reclaimed);
            assertThat(w.totalBytes()).isEqualTo(w.currentTotalBytes());
            // Evictions after a GC: a segment both reach is subtracted once.
            for (int i = 30; i < 60; i++) w.appendWithStats(Frame.encode(payload(i)));
            w.reclaimed(Checkpoint.gcSegments(dir, offset / 2));
            assertThat(w.totalBytes()).isEqualTo(w.currentTotalBytes());
        }
    }

    @Test
    void refusals_at_the_cap_do_not_list_the_directory(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 200, 600, OverflowPolicy.BACKPRESSURE,
                FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.initTotalBytes();
            int refused = 0;
            long listingsBefore = -1;
            for (int i = 0; i < 200; i++) {
                if (viaFrame(w, payload(i)).refused()) {
                    if (listingsBefore < 0) listingsBefore = w.listingsForTesting();
                    refused++;
                }
            }
            assertThat(refused).as("the cap was reached").isGreaterThan(100);
            assertThat(w.listingsForTesting()).isEqualTo(listingsBefore);
        }
    }

    @Test
    void an_append_does_not_wait_for_a_flush_in_progress(@TempDir Path dir) throws Exception {
        try (WalWriter w = WalWriter.createNew(dir, 1L << 20, 1L << 22, OverflowPolicy.BACKPRESSURE,
                FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            CountDownLatch inFlush = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            w.beforeForceForTesting = () -> {
                inFlush.countDown();
                try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            };
            CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> {
                try { w.flush(); } catch (IOException e) { throw new RuntimeException(e); }
            });
            try {
                assertThat(inFlush.await(5, TimeUnit.SECONDS)).as("flush reached the force").isTrue();
                CompletableFuture<WalWriter.AppendResult> append = CompletableFuture.supplyAsync(() -> {
                    try { return w.appendWithStats(Frame.encode(payload(1))); } catch (IOException e) { throw new RuntimeException(e); }
                });
                assertThat(append.get(2, TimeUnit.SECONDS).offsetAfter()).isPositive();
            } finally {
                release.countDown();
            }
            flush.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void a_flush_that_loses_a_race_with_rotation_completes(@TempDir Path dir) throws Exception {
        try (WalWriter w = WalWriter.createNew(dir, 200, 1L << 20, OverflowPolicy.BACKPRESSURE,
                FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            CountDownLatch inFlush = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            w.beforeForceForTesting = () -> {
                inFlush.countDown();
                try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            };
            CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> {
                try { w.flush(); } catch (IOException e) { throw new RuntimeException(e); }
            });
            assertThat(inFlush.await(5, TimeUnit.SECONDS)).isTrue();
            // Rotate the segment the flush captured, then let the force run on it.
            for (int i = 0; i < 10; i++) w.appendWithStats(Frame.encode(payload(i)));
            assertThat(segments(dir)).hasSizeGreaterThan(1);
            release.countDown();
            flush.get(5, TimeUnit.SECONDS);
        }
    }
}
