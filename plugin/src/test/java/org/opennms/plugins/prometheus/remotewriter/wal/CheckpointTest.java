/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy;

class CheckpointTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    @Test
    void load_in_empty_dir_creates_checkpoint_at_zero(@TempDir Path dir) throws IOException {
        Checkpoint cp = Checkpoint.loadOrCreate(dir);
        assertThat(cp.lastSentOffset()).isZero();
        assertThat(cp.lastSentAt()).isEqualTo(Instant.EPOCH);
        // No file is created until advance() is called.
        assertThat(Files.exists(dir.resolve(Checkpoint.FILE_NAME))).isFalse();
    }

    @Test
    void advance_persists_atomically(@TempDir Path dir) throws IOException {
        Checkpoint cp = Checkpoint.loadOrCreate(dir);
        cp.advance(1234);
        assertThat(cp.lastSentOffset()).isEqualTo(1234);
        assertThat(cp.lastSentAt()).isNotEqualTo(Instant.EPOCH);

        // The file on disk matches the in-memory state.
        Path file = dir.resolve(Checkpoint.FILE_NAME);
        assertThat(Files.exists(file)).isTrue();
        String content = Files.readString(file);
        assertThat(content).contains("\"last_sent_offset\":1234");
        assertThat(content).contains("\"last_sent_at\":\"");

        // The .tmp file is cleaned up after the atomic rename.
        assertThat(Files.exists(dir.resolve(Checkpoint.FILE_NAME + ".tmp"))).isFalse();
    }

    @Test
    void load_of_previously_persisted_checkpoint_round_trips(@TempDir Path dir)
            throws IOException {
        Checkpoint first = Checkpoint.loadOrCreate(dir);
        first.advance(8192);
        Instant firstAt = first.lastSentAt();

        Checkpoint second = Checkpoint.loadOrCreate(dir);
        assertThat(second.lastSentOffset()).isEqualTo(8192);
        assertThat(second.lastSentAt()).isEqualTo(firstAt);
    }

    @Test
    void advance_is_monotonic_and_rejects_backward_moves(@TempDir Path dir) throws IOException {
        Checkpoint cp = Checkpoint.loadOrCreate(dir);
        cp.advance(1000);
        assertThatThrownBy(() -> cp.advance(999))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("backwards");
        // No-op on same offset — does not throw.
        cp.advance(1000);
        assertThat(cp.lastSentOffset()).isEqualTo(1000);
    }

    @Test
    void advance_past_moves_only_when_behind_and_never_backwards(@TempDir Path dir) throws IOException {
        Checkpoint cp = Checkpoint.loadOrCreate(dir);
        assertThat(cp.advancePast(100L)).isEqualTo(0L);    // moved, from 0
        assertThat(cp.lastSentOffset()).isEqualTo(100L);
        // Already past: a no-op rather than the backward-move error, which is
        // what lets an eviction and an acknowledgement race without a throw.
        assertThat(cp.advancePast(40L)).isEqualTo(-1L);
        assertThat(cp.advancePast(100L)).isEqualTo(-1L);
        assertThat(cp.lastSentOffset()).isEqualTo(100L);
        assertThat(cp.advancePast(250L)).isEqualTo(100L);  // the delta's base, read under the lock
        assertThat(Checkpoint.loadOrCreate(dir).lastSentOffset()).isEqualTo(250L);
    }

    @Test
    void corrupt_checkpoint_file_surfaces_as_ioexception(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve(Checkpoint.FILE_NAME),
                "{\"last_sent_offset\":123,\"last_sent_at\":\"not-a-date\"}\n");
        assertThatThrownBy(() -> Checkpoint.loadOrCreate(dir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unparseable");
    }

    @Test
    void missing_field_in_checkpoint_file_surfaces_as_ioexception(@TempDir Path dir)
            throws IOException {
        Files.writeString(dir.resolve(Checkpoint.FILE_NAME), "{\"some_other_key\":42}\n");
        assertThatThrownBy(() -> Checkpoint.loadOrCreate(dir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("missing field");
    }

    @Test
    void gc_deletes_segments_whose_end_is_past_the_checkpoint(@TempDir Path dir)
            throws IOException {
        // Build a WAL with 3 segments via rotation.
        long segmentSize = 200;
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            // 2 frames per segment → 3 segments after 6 appends.
            for (int i = 0; i < 6; i++) w.append(new byte[100]);
            w.flush();
        }
        long[] starts = {
                WalSegment.parseStartOffset(dir.resolve("00000000000000000000.seg")),
        };
        assertThat(starts[0]).isZero();
        assertThat(Files.list(dir).count()).isGreaterThanOrEqualTo(6L); // .seg + .idx pairs

        // Checkpoint past the end of segment 0 and 1, but before end of segment 2.
        // Segment layout is 2 frames × ~108 bytes = 216 bytes per sealed segment
        // (rotation happens after threshold is crossed).
        long size0 = Files.size(WalSegment.segPathFor(dir, 0));
        long size1 = Files.size(WalSegment.segPathFor(dir, size0));
        long offsetPastSeg1 = size0 + size1;

        long reclaimed = Checkpoint.gcSegments(dir, offsetPastSeg1);
        assertThat(reclaimed).isEqualTo(size0 + size1);
        assertThat(Files.exists(WalSegment.segPathFor(dir, 0))).isFalse();
        assertThat(Files.exists(WalSegment.segPathFor(dir, size0))).isFalse();
    }

    @Test
    void gc_never_deletes_the_newest_segment(@TempDir Path dir) throws IOException {
        long segmentSize = 200;
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 6; i++) w.append(new byte[100]);
            w.flush();
        }
        // Checkpoint far beyond the end of the newest segment.
        long reclaimed = Checkpoint.gcSegments(dir, Long.MAX_VALUE / 2);
        // Exactly one segment remains (the newest).
        long segCount = Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(WalSegment.SEG_EXT)).count();
        assertThat(segCount).isEqualTo(1L);
        assertThat(reclaimed).isGreaterThan(0L);
    }

    @Test
    void gc_on_single_segment_directory_is_noop(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            w.append(new byte[100]);
            w.flush();
        }
        long reclaimed = Checkpoint.gcSegments(dir, Long.MAX_VALUE / 2);
        assertThat(reclaimed).isZero();
        assertThat(Files.exists(WalSegment.segPathFor(dir, 0))).isTrue();
    }

    @Test
    void gc_preserves_segment_whose_end_equals_checkpoint_minus_one(@TempDir Path dir)
            throws IOException {
        // Edge case: segment ends exactly at checkpoint-1. End-of-segment
        // offset is INCLUSIVE of the last frame, and checkpoint is
        // EXCLUSIVE (first unshipped offset). So a segment ending at
        // offset N with checkpoint at N means "entire segment shipped"
        // and it's eligible for deletion. This test pins the boundary
        // semantic.
        long segmentSize = 200;
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, 1L << 30, OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            for (int i = 0; i < 4; i++) w.append(new byte[100]);
            w.flush();
        }
        long size0 = Files.size(WalSegment.segPathFor(dir, 0));
        // Checkpoint at size0 - 1 (one byte shy of "segment 0 fully shipped").
        Checkpoint.gcSegments(dir, size0 - 1);
        assertThat(Files.exists(WalSegment.segPathFor(dir, 0))).isTrue();
        // Now at size0 exactly → seg 0 reclaimable (if not the newest).
        Checkpoint.gcSegments(dir, size0);
        assertThat(Files.exists(WalSegment.segPathFor(dir, 0))).isFalse();
    }

    @Test
    void advance_is_thread_safe_against_itself(@TempDir Path dir) throws Exception {
        Checkpoint cp = Checkpoint.loadOrCreate(dir);
        int threads = 4;
        int advancesPerThread = 250;
        Thread[] ts = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            ts[t] = new Thread(() -> {
                for (int i = 0; i < advancesPerThread; i++) {
                    long offset = 1L + tid * 1_000_000L + i * 100L;
                    try {
                        cp.advance(offset);
                    } catch (IllegalArgumentException expected) {
                        // Parallel advances can race to a non-monotonic
                        // order from any single thread's point of view;
                        // the invariant that matters is that the
                        // committed offset monotonically grows, which
                        // the class enforces.
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            ts[t].start();
        }
        for (Thread t : ts) t.join();
        // Final offset reflects at least one thread's maximum.
        assertThat(cp.lastSentOffset()).isGreaterThan(0L);
        // File on disk parses cleanly (atomic writes never left a torn
        // state visible).
        Checkpoint reloaded = Checkpoint.loadOrCreate(dir);
        assertThat(reloaded.lastSentOffset()).isEqualTo(cp.lastSentOffset());
    }

    @Test
    void evicted_segments_are_discarded_only_past_the_checkpoint(@TempDir Path dir) throws IOException {
        Checkpoint cp = Checkpoint.loadOrCreate(dir);
        cp.advance(1000);
        Checkpoint.EvictionAccount account = cp.advancePastEvicted(List.of(
                new WalWriter.EvictedSegment(0, 800, 800, 5),      // acknowledged in full: not discarded
                new WalWriter.EvictedSegment(800, 1000, 200, 3),   // ends at the checkpoint: not discarded
                new WalWriter.EvictedSegment(1000, 1500, 500, 7),  // starts at the checkpoint: discarded whole
                new WalWriter.EvictedSegment(1500, 1900, 400, 10)));
        assertThat(account.discardedFrames()).isEqualTo(17);
        assertThat(account.movedFrom()).isEqualTo(1000);
        assertThat(cp.lastSentOffset()).isEqualTo(1900);

        // A segment the checkpoint sits inside is apportioned by bytes:
        // three of its four quarters are past the checkpoint.
        cp.advance(2000);
        account = cp.advancePastEvicted(List.of(new WalWriter.EvictedSegment(1900, 2300, 400, 8)));
        assertThat(account.discardedFrames()).isEqualTo(6);
        assertThat(cp.lastSentOffset()).isEqualTo(2300);

        // Everything already covered: nothing discarded and the checkpoint stays.
        account = cp.advancePastEvicted(List.of(new WalWriter.EvictedSegment(0, 2300, 2300, 40)));
        assertThat(account.discardedFrames()).isZero();
        assertThat(account.movedFrom()).isEqualTo(-1L);
        assertThat(cp.lastSentOffset()).isEqualTo(2300);
    }
}
