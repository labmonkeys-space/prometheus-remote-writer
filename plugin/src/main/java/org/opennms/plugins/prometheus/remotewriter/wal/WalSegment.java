/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.function.Consumer;

/**
 * One WAL segment file on disk. A segment is a flat sequence of frames
 * (see {@link Frame}) covering a contiguous range of the global WAL
 * offset space. The {@link WalWriter} owns the active segment for
 * append, rotates to a new segment when this one crosses the configured
 * size threshold, and opens older segments read-only through the
 * {@link WalReader}.
 *
 * <p>Global offsets: the segment's {@link #startOffset} is the byte
 * position where its first frame sits in the WAL's global address space.
 * An offset N returned from {@link #append(byte[])} is the global offset
 * where the NEXT frame would land — i.e., an exclusive upper bound that
 * the checkpoint can safely record as "bytes 0..N-1 have been sent."
 *
 * <p>Fsync policy controls when {@link FileChannel#force(boolean)} fires
 * within {@link #append}:
 * <ul>
 *   <li>{@link FsyncPolicy#ALWAYS} — every append fsyncs. Highest
 *       durability; throughput floor on SSD ~10-30k appends/sec.</li>
 *   <li>{@link FsyncPolicy#BATCH} — {@link #append} does NOT fsync; caller
 *       must invoke {@link #flush()} explicitly, typically at the
 *       flush-interval boundary.</li>
 *   <li>{@link FsyncPolicy#NEVER} — neither {@link #append} nor
 *       {@link #flush()} forces to disk. Relies on OS page cache; loses
 *       pending writes on kernel panic or power loss.</li>
 * </ul>
 * {@link #close()} always fsyncs regardless of policy (best-effort final
 * durability on clean shutdown).
 *
 * <p>Not thread-safe — the writer enforces single-writer discipline
 * externally. The one exception is {@link #flush()}, which may run on another
 * thread than the appends so an fsync does not hold the writer's lock.
 */
public final class WalSegment implements Closeable {

    public enum FsyncPolicy { ALWAYS, BATCH, NEVER }

    /** File extensions — kept as constants so tests and recovery agree. */
    public static final String SEG_EXT = ".seg";
    public static final String IDX_EXT = ".idx";

    /** Segment filename grammar: 20-digit zero-padded start offset. */
    private static final String FILE_NAME_FORMAT = "%020d";

    private final Path segPath;
    private final Path idxPath;
    private final long startOffset;
    private final FileChannel channel;
    private final FsyncPolicy fsync;
    private final int maxPayload;
    private final Instant createdAt;

    private int sampleCount;
    private Status status;
    /** Volatile: a flush can run on another thread than the one closing. */
    private volatile boolean closed;
    /**
     * Bytes in the file, kept in step with the writes for a segment opened for
     * append, so the append path never asks the filesystem. -1 for a segment
     * opened for read, whose file grows underneath it and has to be queried.
     */
    private long size;

    /** Status mirrors the .idx file field. */
    public enum Status { OPEN, SEALED, TORN }

    private WalSegment(Path segPath, Path idxPath, long startOffset, FileChannel channel,
                       FsyncPolicy fsync, int maxPayload, Instant createdAt, int sampleCount,
                       Status status, long size) {
        this.segPath = segPath;
        this.idxPath = idxPath;
        this.startOffset = startOffset;
        this.channel = channel;
        this.fsync = fsync;
        this.maxPayload = maxPayload;
        this.createdAt = createdAt;
        this.sampleCount = sampleCount;
        this.status = status;
        this.size = size;
    }

    // --- Factories ---------------------------------------------------------

    /**
     * Create a brand-new segment file in the given directory. Fails if
     * the file already exists. The companion .idx is written with
     * {@link Status#OPEN}.
     */
    public static WalSegment create(Path dir, long startOffset, FsyncPolicy fsync,
                                    int maxPayload) throws IOException {
        Path segPath = segPathFor(dir, startOffset);
        Path idxPath = idxPathFor(dir, startOffset);
        FileChannel ch = FileChannel.open(segPath,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
        WalSegment seg = new WalSegment(segPath, idxPath, startOffset, ch, fsync, maxPayload,
                Instant.now(), 0, Status.OPEN, 0L);
        seg.writeIndex();
        // Fsync the directory so the new .seg dirent is durable. Without
        // this, a crash right after rotation can lose the "this segment
        // exists" signal even though the file's bytes are on disk.
        FsUtils.fsyncDirectory(dir);
        return seg;
    }

    /**
     * Re-open an existing segment for append. Used after recovery to
     * resume writing the latest segment. The channel is positioned at
     * end-of-file; {@code sampleCount} is seeded from the caller's
     * recovery scan (the segment does not re-scan on re-open).
     */
    public static WalSegment openForAppend(Path segPath, long startOffset, FsyncPolicy fsync,
                                           int maxPayload, int sampleCountFromRecovery)
            throws IOException {
        Path idxPath = idxPathFor(segPath.getParent(), startOffset);
        FileChannel ch = FileChannel.open(segPath,
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        long size = ch.size();
        ch.position(size);
        return new WalSegment(segPath, idxPath, startOffset, ch, fsync, maxPayload,
                Instant.now(), sampleCountFromRecovery, Status.OPEN, size);
    }

    /**
     * Open an existing segment read-only. Used by the reader for
     * already-sealed segments.
     */
    public static WalSegment openForRead(Path segPath, long startOffset, int maxPayload)
            throws IOException {
        Path idxPath = idxPathFor(segPath.getParent(), startOffset);
        FileChannel ch = FileChannel.open(segPath, StandardOpenOption.READ);
        return new WalSegment(segPath, idxPath, startOffset, ch, FsyncPolicy.NEVER, maxPayload,
                Instant.now(), 0, Status.SEALED, -1L);
    }

    // --- Ops ---------------------------------------------------------------

    /**
     * Append a framed payload. Returns the global offset immediately
     * past the written frame (exclusive upper bound) — this is the value
     * a checkpoint records as "sent through here."
     *
     * @throws IOException if the underlying write fails (including disk
     *                     full)
     */
    public long append(byte[] payload) throws IOException {
        return append(Frame.encode(payload));
    }

    /**
     * Append a frame already built by {@link Frame#encode}, so the caller can
     * do the encoding and the CRC before taking whatever lock serialises
     * appends. Consumes the buffer.
     *
     * <p>The write is positional, at the size this segment tracks, so the
     * append path makes one syscall: no {@code size()} to find the end and no
     * {@code position()} to seek to it. A write that fails partway, typically
     * a full disk, is cut back to the tracked size, so the file never holds
     * bytes past its logical end: a shorter frame after it, or a rotation
     * sealing it, would otherwise leave a torn tail inside a sealed segment
     * and send the reader into the middle of the next one.
     */
    public long append(ByteBuffer frame) throws IOException {
        ensureOpen();
        // Opened for read: no tracked size to write at. The channel would say
        // the same thing; a read-only segment is a read-only segment.
        if (size < 0) throw new java.nio.channels.NonWritableChannelException();
        // Reject payloads that Frame.decode would later reject as
        // "torn" on readback — writing them would create unreadable
        // bytes that recovery treats as a torn tail, potentially
        // losing subsequent good frames too. Encode/decode asymmetry
        // guard.
        int payloadLength = frame.remaining() - Frame.HEADER_BYTES;
        if (payloadLength > maxPayload) {
            throw new IOException(
                "payload size (" + payloadLength + ") exceeds this segment's "
                + "maxPayload (" + maxPayload + ") — frame would be unreadable");
        }
        long at = size;
        try {
            while (frame.hasRemaining()) at += channel.write(frame, at);
        } catch (IOException | RuntimeException e) {
            try {
                channel.truncate(size);
            } catch (IOException | RuntimeException truncate) {
                e.addSuppressed(truncate);
            }
            throw e;
        }
        size = at;
        if (fsync == FsyncPolicy.ALWAYS) channel.force(false);
        sampleCount++;
        return startOffset + size;
    }

    /**
     * Force fsync regardless of fsync policy. Called by the writer on
     * flush-interval boundary (under BATCH policy) and at segment
     * rotation. No-op under {@link FsyncPolicy#NEVER}.
     */
    public void flush() throws IOException {
        if (closed || fsync == FsyncPolicy.NEVER) return;
        try {
            channel.force(false);
        } catch (java.nio.channels.ClosedChannelException e) {
            // A rotation closed this segment while the flush was on its way to
            // it. close() marks the segment closed, forces, then seals, so the
            // bytes are on disk. Anything else, such as an interrupt closing
            // the channel under a force, is a real failure and propagates.
            if (!closed) throw e;
        }
    }

    /**
     * Scan frames forward from {@code fromGlobalOffset}, invoking the
     * consumer on each payload, up to {@code maxFrames} frames. Stops at
     * end-of-file, at a torn frame, or when {@code maxFrames} is
     * reached. Returns the global offset one past the last
     * successfully-decoded frame — the caller's new read position.
     *
     * <p>Pass {@link Integer#MAX_VALUE} for {@code maxFrames} to disable
     * the iteration bound (scan to end-of-segment).
     *
     * <p>The segment channel is repositioned by this method. Do not
     * interleave {@link #append} and {@link #scan} calls — the writer
     * should not be concurrently reading its active segment.
     */
    public long scan(long fromGlobalOffset, int maxFrames, Consumer<byte[]> consumer)
            throws IOException {
        ensureOpen();
        if (fromGlobalOffset < startOffset) {
            throw new IllegalArgumentException(
                "offset " + fromGlobalOffset + " is before segment start " + startOffset);
        }
        long fileOffset = fromGlobalOffset - startOffset;
        long fileSize = channel.size();
        if (fileOffset > fileSize) {
            throw new IllegalArgumentException(
                "offset " + fromGlobalOffset + " is past segment end " + (startOffset + fileSize));
        }
        channel.position(fileOffset);
        int seen = 0;
        while (channel.position() < fileSize && seen < maxFrames) {
            long beforeFrame = channel.position();
            byte[] payload = Frame.decode(channel, maxPayload);
            if (payload == null) {
                // Torn frame — stop. Channel is rewound to frame start by
                // Frame.decode.
                return startOffset + beforeFrame;
            }
            consumer.accept(payload);
            seen++;
        }
        return startOffset + channel.position();
    }

    /** Convenience overload: scan to end-of-segment without a bound. */
    public long scan(long fromGlobalOffset, Consumer<byte[]> consumer) throws IOException {
        return scan(fromGlobalOffset, Integer.MAX_VALUE, consumer);
    }

    /**
     * Recover the segment: scan from the beginning, count frames, detect
     * torn tail. On torn tail, truncate the file at the last good frame
     * and mark status as {@link Status#TORN}. Returns the recovered
     * {@link #endOffset()}.
     *
     * <p>Called during startup recovery; safe to call on a fresh or
     * already-sealed segment (no-op if no damage is found).
     */
    public long recover() throws IOException {
        ensureOpen();
        int framesSeen = 0;
        long lastGood = startOffset;
        long fileSize = channel.size();
        channel.position(0);
        while (channel.position() < fileSize) {
            byte[] payload = Frame.decode(channel, maxPayload);
            if (payload == null) {
                // Torn frame; truncate here.
                long truncateAt = channel.position();
                if (truncateAt < fileSize) {
                    channel.truncate(truncateAt);
                    channel.force(true);
                    status = Status.TORN;
                }
                lastGood = startOffset + truncateAt;
                break;
            }
            framesSeen++;
            lastGood = startOffset + channel.position();
        }
        if (status == Status.OPEN && channel.position() >= fileSize) {
            // Full scan completed cleanly — no torn tail. Leave status as OPEN
            // if still active; the writer (not recover) is responsible for
            // sealing.
            lastGood = startOffset + fileSize;
        }
        sampleCount = framesSeen;
        channel.position(channel.size());
        if (size >= 0) size = channel.size();
        return lastGood;
    }

    /**
     * Write / refresh the companion .idx file with current segment state.
     * Tiny footprint; called at create, at close, and explicitly after
     * recovery. The .idx is hand-rolled JSON rather than a library call
     * to avoid pulling another dependency into the plugin for a
     * five-field record.
     */
    public void writeIndex() throws IOException {
        long endOffset = startOffset + (channel.isOpen() ? channel.size() : Files.size(segPath));
        String line = String.format(
                "{\"start_offset\":%d,\"end_offset\":%d,\"sample_count\":%d,"
              + "\"created_at\":\"%s\",\"status\":\"%s\"}%n",
                startOffset, endOffset, sampleCount, createdAt, status.name().toLowerCase());
        Files.writeString(idxPath, line);
    }

    // --- Accessors ---------------------------------------------------------

    public long startOffset() { return startOffset; }

    public long endOffset() throws IOException {
        ensureOpen();
        return startOffset + (size >= 0 ? size : channel.size());
    }

    public int sampleCount() { return sampleCount; }

    public Path segPath() { return segPath; }

    public Path idxPath() { return idxPath; }

    public Status status() { return status; }

    // --- Lifecycle ---------------------------------------------------------

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            if (channel.isOpen()) channel.force(false);
            if (status == Status.OPEN) status = Status.SEALED;
            writeIndex();
        } finally {
            channel.close();
        }
    }

    // --- Helpers -----------------------------------------------------------

    private void ensureOpen() {
        if (closed) throw new IllegalStateException(
                "segment " + segPath.getFileName() + " is closed");
    }

    /** Resolve the .seg path for a given start offset in the directory. */
    public static Path segPathFor(Path dir, long startOffset) {
        return dir.resolve(String.format(FILE_NAME_FORMAT, startOffset) + SEG_EXT);
    }

    /** Resolve the .idx path for a given start offset in the directory. */
    public static Path idxPathFor(Path dir, long startOffset) {
        return dir.resolve(String.format(FILE_NAME_FORMAT, startOffset) + IDX_EXT);
    }

    /**
     * Parse the start offset out of a segment file name.
     * {@code 00000000000000000000.seg} → 0. Returns -1 if the name is not
     * a segment file.
     */
    public static long parseStartOffset(Path segPath) {
        String name = segPath.getFileName().toString();
        if (!name.endsWith(SEG_EXT)) return -1;
        String digits = name.substring(0, name.length() - SEG_EXT.length());
        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
