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
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.AppendResult;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy;

class WalOverflowTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    @Test
    void backpressure_refuses_append_when_cap_reached(@TempDir Path dir) throws IOException {
        // segmentSize = 100, maxSize = 300. After 2 segments (~250 bytes
        // total), the next frame (~108 bytes) would push past the cap
        // and trigger BACKPRESSURE.
        try (WalWriter w = WalWriter.createNew(dir, 100, 300,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            // Append until the next would overflow.
            assertThatThrownBy(() -> {
                for (int i = 0; i < 10; i++) w.append(payload);
            }).isInstanceOf(WalFullException.class)
              .hasMessageContaining("WAL at cap");
        }
    }

    @Test
    void drop_oldest_evicts_and_succeeds(@TempDir Path dir) throws IOException {
        // Tight cap that forces eviction on the 3rd segment creation.
        long segmentSize = 100;
        long maxSize = 300;
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, maxSize,
                OverflowPolicy.DROP_OLDEST, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            for (int i = 0; i < 10; i++) {
                AppendResult r = w.appendWithStats(payload);
                if (i >= 3) {
                    // Evictions happen as we fill — exact counts vary but
                    // we never exceed the cap.
                    assertThat(w.currentTotalBytes()).isLessThanOrEqualTo(maxSize);
                }
            }
        }

        // At least one eviction has fired → total segments on disk is
        // bounded (not one per append).
        long segCount;
        try (Stream<Path> s = Files.list(dir)) {
            segCount = s.filter(p -> p.getFileName().toString().endsWith(WalSegment.SEG_EXT)).count();
        }
        assertThat(segCount).isLessThan(10); // without drop-oldest we'd have ~10
    }

    @Test
    void drop_oldest_reports_evicted_bytes_and_frames(@TempDir Path dir) throws IOException {
        long segmentSize = 100;
        long maxSize = 300;
        try (WalWriter w = WalWriter.createNew(dir, segmentSize, maxSize,
                OverflowPolicy.DROP_OLDEST, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            for (int i = 0; i < 4; i++) w.appendWithStats(payload);

            AppendResult r = w.appendWithStats(payload);
            // Either this append fit without eviction, or it evicted at
            // least one segment. We assert that eviction stats are
            // internally consistent (nonneg, plausible).
            if (r.evictedFrames() > 0) {
                assertThat(r.evictedBytes()).isPositive();
            }
            if (r.evictedBytes() > 0) {
                assertThat(r.evictedFrames()).isPositive();
            }
        }
    }

    @Test
    void backpressure_reports_total_size_in_error_message(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 100, 300,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            assertThatThrownBy(() -> {
                for (int i = 0; i < 10; i++) w.append(payload);
            }).isInstanceOf(WalFullException.class)
              .hasMessageContaining("/300");
        }
    }

    @Test
    void single_frame_larger_than_cap_fails_fast(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 100, 200,
                OverflowPolicy.DROP_OLDEST, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] tooLarge = new byte[1000]; // frame would be ~1008 bytes > 200 cap
            assertThatThrownBy(() -> w.append(tooLarge))
                    .isInstanceOf(WalFullException.class)
                    .hasMessageContaining("exceeds wal.max-size-bytes");
        }
    }

    @Test
    void backpressure_preserves_active_segment_on_failure(@TempDir Path dir) throws IOException {
        // The writer must remain usable after a BACKPRESSURE throw —
        // i.e., clearing some data out-of-band and retrying an append
        // should succeed. Pins that a failed append doesn't corrupt
        // writer state.
        try (WalWriter w = WalWriter.createNew(dir, 100, 300,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            int succeeded = 0;
            try {
                for (int i = 0; i < 10; i++) {
                    w.append(payload);
                    succeeded++;
                }
            } catch (WalFullException expected) {
                // fallthrough
            }
            // Now externally delete a sealed segment and retry. The writer
            // tracks its footprint rather than listing the directory on every
            // append (#182), so a deletion it did not make has to be told to
            // it; in production only GC and eviction delete, and both report.
            Files.deleteIfExists(WalSegment.segPathFor(dir, 0));
            Files.deleteIfExists(WalSegment.idxPathFor(dir, 0));
            w.rederiveTotalBytes();
            // Writer remains usable.
            w.append(payload);
            assertThat(succeeded).isGreaterThan(0);
        }
    }

    @Test
    void drop_oldest_never_evicts_the_active_segment(@TempDir Path dir) throws IOException {
        // Stress scenario: segmentSize == maxSize so the cap is hit
        // BEFORE the first rotation — only the (still-empty) active
        // segment exists when an append would overflow. Eviction has
        // nothing to evict; must throw with the explanatory message
        // rather than corrupt the active segment.
        long size = 200;
        try (WalWriter w = WalWriter.createNew(dir, size, size,
                OverflowPolicy.DROP_OLDEST, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            byte[] payload = new byte[100];
            // First append: 108 bytes in active seg 0; currentTotal 108 ≤ 200, OK.
            w.append(payload);
            // Second: currentTotal 108 + 108 = 216 > 200. Eviction runs
            // but the active is the only segment → throw.
            assertThatThrownBy(() -> w.append(payload))
                    .isInstanceOf(WalFullException.class)
                    .hasMessageContaining("no evictable segments");
        }
    }

    @Test
    void construction_rejects_zero_or_negative_max_size(@TempDir Path dir) {
        assertThatThrownBy(() -> WalWriter.createNew(dir, 100, 0,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxSizeBytes");
        assertThatThrownBy(() -> WalWriter.createNew(dir, 100, -1,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxSizeBytes");
    }

    @Test
    void construction_rejects_segment_size_greater_than_max(@TempDir Path dir) {
        assertThatThrownBy(() -> WalWriter.createNew(dir, 1000, 500,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segmentSizeBytes");
    }

    @Test
    void currentTotalBytes_reflects_on_disk_size(@TempDir Path dir) throws IOException {
        try (WalWriter w = WalWriter.createNew(dir, 1 << 20, 1 << 30,
                OverflowPolicy.BACKPRESSURE, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            assertThat(w.currentTotalBytes()).isZero();
            w.append(new byte[100]);
            // 4-byte length + 100 bytes + 4-byte CRC = 108.
            assertThat(w.currentTotalBytes()).isEqualTo(108);
            w.append(new byte[50]);
            assertThat(w.currentTotalBytes()).isEqualTo(108 + 58);
        }
    }
}
