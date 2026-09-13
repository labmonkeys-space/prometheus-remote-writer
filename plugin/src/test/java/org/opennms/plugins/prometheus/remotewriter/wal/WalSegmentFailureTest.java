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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy;

/**
 * How the append and flush paths fail, now that a segment tracks its own size
 * and a flush runs outside the writer's lock (#182).
 */
class WalSegmentFailureTest {

    private static final int MAX_PAYLOAD = 64 * 1024;

    /** A channel whose next positional write stores half the frame, then fails
     *  the way a full disk does. Everything else goes to the real channel. */
    private static final class TornWriteChannel extends FileChannel {
        private final FileChannel real;
        boolean failNext;

        TornWriteChannel(FileChannel real) { this.real = real; }

        @Override public int write(ByteBuffer src, long position) throws IOException {
            if (!failNext) return real.write(src, position);
            failNext = false;
            ByteBuffer half = src.slice(src.position(), src.remaining() / 2);
            real.write(half, position);
            throw new IOException("No space left on device");
        }
        @Override public int read(ByteBuffer dst) throws IOException { return real.read(dst); }
        @Override public long read(ByteBuffer[] d, int o, int l) throws IOException { return real.read(d, o, l); }
        @Override public int write(ByteBuffer src) throws IOException { return real.write(src); }
        @Override public long write(ByteBuffer[] s, int o, int l) throws IOException { return real.write(s, o, l); }
        @Override public long position() throws IOException { return real.position(); }
        @Override public FileChannel position(long p) throws IOException { real.position(p); return this; }
        @Override public long size() throws IOException { return real.size(); }
        @Override public FileChannel truncate(long s) throws IOException { real.truncate(s); return this; }
        @Override public void force(boolean m) throws IOException { real.force(m); }
        @Override public long transferTo(long p, long c, WritableByteChannel t) throws IOException { return real.transferTo(p, c, t); }
        @Override public long transferFrom(ReadableByteChannel s, long p, long c) throws IOException { return real.transferFrom(s, p, c); }
        @Override public int read(ByteBuffer dst, long p) throws IOException { return real.read(dst, p); }
        @Override public MappedByteBuffer map(MapMode m, long p, long s) throws IOException { return real.map(m, p, s); }
        @Override public FileLock lock(long p, long s, boolean sh) throws IOException { return real.lock(p, s, sh); }
        @Override public FileLock tryLock(long p, long s, boolean sh) throws IOException { return real.tryLock(p, s, sh); }
        @Override protected void implCloseChannel() throws IOException { real.close(); }
    }

    private static TornWriteChannel tearWritesOf(WalSegment segment) throws Exception {
        Field f = WalSegment.class.getDeclaredField("channel");
        f.setAccessible(true);
        TornWriteChannel torn = new TornWriteChannel((FileChannel) f.get(segment));
        f.set(segment, torn);
        return torn;
    }

    private static byte[] payload(int length, int fill) {
        byte[] p = new byte[length];
        java.util.Arrays.fill(p, (byte) fill);
        return p;
    }

    @Test
    void a_torn_write_is_cut_back_so_a_shorter_frame_and_a_rotation_stay_readable(@TempDir Path dir)
            throws Exception {
        List<byte[]> read = new ArrayList<>();
        long sealedEnd;
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            seg.append(payload(200, 1));
            long logicalEnd = seg.endOffset();
            TornWriteChannel torn = tearWritesOf(seg);

            torn.failNext = true;
            assertThatThrownBy(() -> seg.append(payload(200, 2))).hasMessageContaining("No space");
            assertThat(Files.size(seg.segPath())).as("file cut back to its logical end")
                    .isEqualTo(logicalEnd);

            // A shorter frame than the torn one: nothing of the torn bytes may survive it.
            seg.append(payload(20, 3));
            sealedEnd = seg.endOffset();
        }
        assertThat(Files.size(WalSegment.segPathFor(dir, 0))).isEqualTo(sealedEnd);
        try (WalSegment reader = WalSegment.openForRead(WalSegment.segPathFor(dir, 0), 0, MAX_PAYLOAD)) {
            assertThat(reader.scan(0, read::add)).isEqualTo(sealedEnd);
        }
        assertThat(read).hasSize(2);
        assertThat(read.get(0)).isEqualTo(payload(200, 1));
        assertThat(read.get(1)).isEqualTo(payload(20, 3));
    }

    @Test
    void a_flush_interrupted_mid_force_is_reported(@TempDir Path dir) throws IOException {
        try (WalSegment seg = WalSegment.create(dir, 0, FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            seg.append(payload(10, 1));
            // An interrupt closes a FileChannel under a force. Nothing sealed
            // the segment, so this is a real failure, not a lost race.
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(seg::flush).isInstanceOf(ClosedByInterruptException.class);
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void gc_reports_each_deletion_as_it_happens(@TempDir Path dir) throws IOException {
        List<Long> reported = new ArrayList<>();
        long offset;
        try (WalWriter w = WalWriter.createNew(dir, 200, 1L << 20, OverflowPolicy.BACKPRESSURE,
                FsyncPolicy.BATCH, MAX_PAYLOAD)) {
            offset = 0;
            for (int i = 0; i < 20; i++) offset = w.append(payload(60, i));
            long reclaimed = Checkpoint.gcSegments(dir, offset, reported::add);
            assertThat(reported).hasSizeGreaterThan(1);
            assertThat(reported.stream().mapToLong(Long::longValue).sum()).isEqualTo(reclaimed);
        }
    }
}
