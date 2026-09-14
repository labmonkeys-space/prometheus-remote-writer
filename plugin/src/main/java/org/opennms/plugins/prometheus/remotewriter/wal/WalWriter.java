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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;

/**
 * Single-writer into the WAL directory. Owns the active
 * {@link WalSegment}, appends incoming payloads, and rotates to a new
 * segment once the active one exceeds the configured size threshold.
 *
 * <p>Thread-safety: {@code append} is synchronized; callers must still
 * coordinate externally for any invariant that spans more than one append
 * (there is none today). {@link #flush()} takes the lock only to find the
 * active segment and forces it after releasing it, so an fsync never holds
 * up an append. Concurrent reads via
 * {@link WalReader} against the same directory are safe — they use
 * independent {@link java.nio.channels.FileChannel}s per segment.
 *
 * <p>Does not manage overflow (that's handled by the caller — {@code
 * PrometheusRemoteWriterStorage} intercepts on the {@code store()} path);
 * does not manage recovery (use {@link WalRecovery} before constructing).
 */
public final class WalWriter implements Closeable {

    /** Overflow policy when the WAL hits {@code maxSizeBytes}. */
    public enum OverflowPolicy {
        /** Refuse the append with {@link WalFullException}. Caller handles. */
        BACKPRESSURE,
        /** Evict the oldest segment to make room; append proceeds. */
        DROP_OLDEST
    }

    private final Path dir;
    private final long segmentSizeBytes;
    private final long maxSizeBytes;
    private final OverflowPolicy overflow;
    private final FsyncPolicy fsync;
    private final int maxPayload;

    private WalSegment active;
    private boolean closed;

    /**
     * Floor offset below which the DROP_OLDEST path must not evict.
     * Set by the storage/flusher layer to the reader's current offset,
     * so eviction never deletes a segment the reader still needs. 0
     * (the default) means "no floor" — every sealed segment is
     * evictable — which matches test and standalone-library use cases.
     */
    private volatile long readerOffsetFloor;

    /**
     * The on-disk footprint, kept exact so neither the cap check on the append
     * path nor the {@code overflow_bytes} gauge ever lists the directory. Read
     * from the directory once ({@link #initTotalBytes()}), then every append
     * adds its frame, eviction subtracts what it deleted, and segment GC,
     * which deletes behind this class's back, reports what it deleted through
     * {@link #reclaimed(long)}. Eviction and GC each count only the files their
     * own delete removed, so a segment both reach is subtracted once.
     *
     * <p>Until 0.8.1 GC did not report, the value drifted high, and a breach
     * of the cap re-listed the directory on every append — including every
     * refused one, while the writer threads queued behind the lock (#182).
     *
     * <p>Negative means "not yet known"; the first use reads the directory.
     * Volatile so the gauge can read it without the lock.
     */
    private volatile long trackedTotalBytes = -1L;

    /** Directory listings taken for the byte count. Package-private for tests. */
    private final java.util.concurrent.atomic.AtomicLong listings =
            new java.util.concurrent.atomic.AtomicLong();

    /** Test-only: run by {@link #flush()} after it let go of the lock, before the force. */
    volatile Runnable beforeForceForTesting;

    public WalWriter(Path dir, WalSegment initialActiveSegment, long segmentSizeBytes,
                     long maxSizeBytes, OverflowPolicy overflow,
                     FsyncPolicy fsync, int maxPayload) {
        if (initialActiveSegment == null) {
            throw new IllegalArgumentException("initialActiveSegment must not be null — "
                + "use createNew() or resume() to obtain one");
        }
        if (maxSizeBytes <= 0) {
            throw new IllegalArgumentException("maxSizeBytes must be positive: " + maxSizeBytes);
        }
        if (segmentSizeBytes <= 0 || segmentSizeBytes > maxSizeBytes) {
            throw new IllegalArgumentException(
                "segmentSizeBytes must be positive and <= maxSizeBytes: segment="
                + segmentSizeBytes + ", max=" + maxSizeBytes);
        }
        this.dir = dir;
        this.active = initialActiveSegment;
        this.segmentSizeBytes = segmentSizeBytes;
        this.maxSizeBytes = maxSizeBytes;
        this.overflow = overflow;
        this.fsync = fsync;
        this.maxPayload = maxPayload;
    }

    /**
     * Bootstrap a brand-new WAL in an empty directory: opens segment 0
     * and returns a writer positioned at global offset 0.
     */
    public static WalWriter createNew(Path dir, long segmentSizeBytes, long maxSizeBytes,
                                      OverflowPolicy overflow, FsyncPolicy fsync,
                                      int maxPayload) throws IOException {
        // Validate args BEFORE creating the segment file so an invalid
        // config doesn't leave orphaned .seg files in the dir.
        if (maxSizeBytes <= 0) {
            throw new IllegalArgumentException("maxSizeBytes must be positive: " + maxSizeBytes);
        }
        if (segmentSizeBytes <= 0 || segmentSizeBytes > maxSizeBytes) {
            throw new IllegalArgumentException(
                "segmentSizeBytes must be positive and <= maxSizeBytes: segment="
                + segmentSizeBytes + ", max=" + maxSizeBytes);
        }
        WalSegment seg = WalSegment.create(dir, 0, fsync, maxPayload);
        return new WalWriter(dir, seg, segmentSizeBytes, maxSizeBytes, overflow, fsync, maxPayload);
    }

    /**
     * Resume into an existing, already-recovered segment. The segment is
     * expected to be open for append (see
     * {@link WalSegment#openForAppend}); rotation happens here when its
     * size exceeds the threshold.
     */
    public static WalWriter resume(Path dir, WalSegment activeSegment, long segmentSizeBytes,
                                   long maxSizeBytes, OverflowPolicy overflow,
                                   FsyncPolicy fsync, int maxPayload) {
        return new WalWriter(dir, activeSegment, segmentSizeBytes, maxSizeBytes,
                overflow, fsync, maxPayload);
    }

    /**
     * Append one encoded payload. Returns the global offset immediately
     * past the written frame (inclusive end; exclusive upper bound).
     *
     * <p>Applies the configured overflow policy if the projected WAL
     * size would exceed {@code maxSizeBytes} after this append:
     * {@link OverflowPolicy#BACKPRESSURE} throws {@link WalFullException}
     * (caller converts to operator-visible counter + StorageException);
     * {@link OverflowPolicy#DROP_OLDEST} evicts the oldest sealed
     * segment and retries, repeating until the append fits or no more
     * eviction is possible (a single frame larger than
     * {@code maxSizeBytes} is an unrecoverable configuration error —
     * {@link WalFullException} with an explanatory message).
     *
     * <p>Rotates to a new segment if the active one crosses
     * {@code segmentSizeBytes} after the append (post-append check).
     */
    public long append(byte[] payload) throws IOException {
        return appendWithStats(Frame.encode(payload)).offsetAfter();
    }

    /**
     * Like {@link #append(byte[])} but also reports eviction stats —
     * the number of frames and bytes dropped from the oldest segment
     * to make room (both zero under BACKPRESSURE or when no eviction
     * was needed). Used by the storage layer to bump
     * {@code samples_dropped_wal_full_total}.
     */
    public AppendResult appendWithStats(byte[] payload) throws IOException {
        return appendWithStats(Frame.encode(payload));
    }

    /**
     * {@link #appendWithStats(byte[])} for a frame already built by
     * {@link Frame#encode}, so the encoding and its CRC happen before the
     * caller takes the lock that serialises appends. Consumes the buffer.
     */
    public synchronized AppendResult appendWithStats(ByteBuffer frame) throws IOException {
        ensureOpen();
        long frameSize = frame.remaining();
        if (frameSize > maxSizeBytes) {
            throw new WalFullException(
                "single frame (" + frameSize + " bytes) exceeds wal.max-size-bytes ("
                + maxSizeBytes + "). Increase the cap or reduce the label set", 0, List.of());
        }
        long evictedBytes = 0L;
        int evictedFrames = 0;
        List<EvictedSegment> evictedSegments = new ArrayList<>();
        while (projectedTotalBytes(frameSize) > maxSizeBytes) {
            if (overflow == OverflowPolicy.BACKPRESSURE) {
                throw new WalFullException(
                    "WAL at cap (" + trackedTotalBytes + "/" + maxSizeBytes
                    + " bytes); refusing append under backpressure policy",
                    evictedFrames, evictedSegments);
            }
            EvictedSegment e = evictOldestSegment();
            if (e == null) {
                throw new WalFullException(
                    "WAL at cap with no evictable segments (single active segment "
                    + "exceeds cap)", evictedFrames, evictedSegments);
            }
            evictedBytes += e.bytes();
            evictedFrames += e.samples();
            evictedSegments.add(e);
            trackedTotalBytes -= e.bytes();
        }
        long offsetAfter = active.append(frame);
        trackedTotalBytes += frameSize;
        if (active.endOffset() - active.startOffset() >= segmentSizeBytes) {
            rotate();
        }
        return new AppendResult(offsetAfter, evictedBytes, evictedFrames, List.copyOf(evictedSegments));
    }

    /**
     * One segment a drop-oldest eviction deleted: the offsets it spanned, the
     * bytes it freed, and its sample count from the {@code .idx}. A caller
     * that keeps a pending count compares the offsets with its checkpoint to
     * tell acknowledged frames from discarded ones.
     */
    public record EvictedSegment(long startOffset, long endOffset, long bytes, int samples) {}

    /** The footprint this append would leave. Arithmetic, never a listing. */
    private long projectedTotalBytes(long frameSize) throws IOException {
        if (trackedTotalBytes < 0) trackedTotalBytes = currentTotalBytes();
        return trackedTotalBytes + frameSize;
    }

    /**
     * Read the footprint from the directory, once. Call before anything can
     * run segment GC concurrently: a listing that overlaps a GC could see its
     * deletions and then have {@link #reclaimed(long)} subtract them again.
     */
    public synchronized void initTotalBytes() throws IOException {
        if (trackedTotalBytes < 0) trackedTotalBytes = currentTotalBytes();
    }

    /** The on-disk footprint, without the lock once it is known. */
    public long totalBytes() throws IOException {
        long t = trackedTotalBytes;
        if (t >= 0) return t;
        initTotalBytes();
        return trackedTotalBytes;
    }

    /** Segment GC deleted {@code bytes} of this writer's segments. */
    public synchronized void reclaimed(long bytes) {
        if (trackedTotalBytes >= 0) trackedTotalBytes -= bytes;
    }

    /**
     * Re-read the footprint from the directory. For a GC that failed partway,
     * whose reclaimed count is therefore unknown. Call it from the thread that
     * ran the GC, once the GC has stopped.
     */
    public synchronized void rederiveTotalBytes() throws IOException {
        trackedTotalBytes = currentTotalBytes();
    }

    /** Package-private for tests. */
    long listingsForTesting() { return listings.get(); }

    /**
     * Computes the current total on-disk size of all segments.
     *
     * <p>Tolerates concurrent deletes: a segment file may vanish
     * between the directory listing and the {@link Files#size} call
     * (e.g., the Flusher's {@code Checkpoint.gcSegments} ran on another
     * thread). Such an entry contributes 0 to the total — its bytes
     * are no longer on disk, which is the correct accounting answer
     * for a sum that's about to feed an overflow check.
     */
    public synchronized long currentTotalBytes() throws IOException {
        listings.incrementAndGet();
        long total = 0;
        try (DirectoryStream<Path> s = Files.newDirectoryStream(dir, "*" + WalSegment.SEG_EXT)) {
            for (Path p : s) {
                total += FsUtils.sizeOrZero(p);
            }
        }
        return total;
    }

    /**
     * Set the reader-offset floor. The {@link OverflowPolicy#DROP_OLDEST}
     * path will not evict any segment whose {@code endOffset} exceeds
     * this floor — i.e., segments that still contain unread data from
     * the reader's perspective. Callable from any thread; reads and
     * writes are volatile.
     *
     * <p>In the standalone library (no reader), the default floor of 0
     * lets eviction proceed unrestricted — matching the behavior the
     * unit tests were written against.
     */
    public void setReaderOffsetFloor(long floor) {
        this.readerOffsetFloor = floor;
    }

    /**
     * Evict the oldest sealed segment and describe it, or return {@code null}
     * when there is nothing to evict (only the active segment exists, or the
     * oldest segment still holds data the reader has not drained; see
     * {@link #setReaderOffsetFloor(long)}).
     */
    private EvictedSegment evictOldestSegment() throws IOException {
        List<Long> starts = listSegmentStartOffsets();
        if (starts.size() <= 1) return null; // only the active one

        long oldest = starts.get(0);
        if (oldest == active.startOffset()) {
            // Shouldn't happen — the active is the newest, oldest is first.
            return null;
        }
        Path seg = WalSegment.segPathFor(dir, oldest);
        Path idx = WalSegment.idxPathFor(dir, oldest);
        long bytes = FsUtils.sizeOrZero(seg);
        long endOffset = oldest + bytes;

        // Protect the reader: if this segment still contains any bytes
        // the reader has not drained past, refuse to evict. The caller
        // (append's overflow loop) will see null and surface
        // WalFullException — correct: the WAL really is full from the
        // reader's point of view, and overwriting would corrupt its
        // state.
        long floor = readerOffsetFloor;
        if (floor > 0 && endOffset > floor) {
            return null;
        }

        // Read sample count from .idx without opening the .seg.
        String index = FsUtils.readStringOrNull(idx);
        long samples = index == null ? 0L : extractSampleCount(index);
        // Counted only if this delete is the one that removed it: segment GC
        // may reach the same file, and it reports its own deletions.
        boolean deleted = Files.deleteIfExists(seg);
        Files.deleteIfExists(idx);
        FsUtils.fsyncDirectory(dir);
        return deleted
                ? new EvictedSegment(oldest, endOffset, bytes, (int) samples)
                : new EvictedSegment(oldest, endOffset, 0L, 0);
    }

    private List<Long> listSegmentStartOffsets() throws IOException {
        List<Long> out = new ArrayList<>();
        try (DirectoryStream<Path> s = Files.newDirectoryStream(dir, "*" + WalSegment.SEG_EXT)) {
            for (Path p : s) {
                long start = WalSegment.parseStartOffset(p);
                if (start >= 0) out.add(start);
            }
        }
        Collections.sort(out);
        return out;
    }

    private static long extractSampleCount(String idxJson) {
        int i = idxJson.indexOf("\"sample_count\":");
        if (i < 0) return 0;
        i += "\"sample_count\":".length();
        int end = i;
        while (end < idxJson.length() && Character.isDigit(idxJson.charAt(end))) end++;
        try {
            return Long.parseLong(idxJson.substring(i, end));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Force the active segment's fsync policy to act now, regardless of
     * which policy is configured. No-op under
     * {@link FsyncPolicy#NEVER} (by policy). Called at flush-interval
     * boundaries from the flusher thread.
     */
    public void flush() throws IOException {
        WalSegment segment;
        synchronized (this) {
            ensureOpen();
            segment = active;
        }
        // Outside the lock: a force can take tens of milliseconds on a loaded
        // disk, and holding the lock would stall every append for that long.
        // FileChannel.force is safe alongside writes and covers every write
        // that completed before it. If a rotation seals the segment first,
        // close() has forced it already and the flush is a no-op.
        Runnable hook = beforeForceForTesting;
        if (hook != null) hook.run();
        segment.flush();
    }

    /**
     * The global offset where the next append will land. Equal to the
     * active segment's {@link WalSegment#endOffset()}.
     */
    public synchronized long currentOffset() throws IOException {
        ensureOpen();
        return active.endOffset();
    }

    /** The active segment's directory — exposed for reader construction. */
    public Path dir() { return dir; }

    /**
     * Rotate: seal the current segment, create a new one starting where
     * the old one ended. If creating the new segment fails (e.g., a
     * stale file from a prior crashed rotation already exists at the
     * expected path), reopen the just-closed segment for append so the
     * writer remains usable rather than wedged. The exception is still
     * propagated so the caller knows the rotation didn't land — they
     * may retry the append, which will try the check-and-rotate path
     * again.
     */
    private void rotate() throws IOException {
        long nextStart = active.endOffset();
        WalSegment old = active;
        old.close(); // seals and rewrites .idx with final state
        try {
            active = WalSegment.create(dir, nextStart, fsync, maxPayload);
        } catch (IOException e) {
            // New-segment creation failed. Reopen the just-closed
            // segment so the writer can continue appending; subsequent
            // appends will cross the size threshold again and re-enter
            // rotate(), giving the operator / filesystem a retry
            // opportunity. Rethrow so the current append sees the error.
            try {
                active = WalSegment.openForAppend(old.segPath(), old.startOffset(),
                        fsync, maxPayload, old.sampleCount());
            } catch (IOException reopen) {
                // Couldn't recover the old segment either — the writer
                // is genuinely stuck. Restore the closed reference so
                // ensureOpen still works; the next append will surface
                // "segment is closed" which is at least an honest error.
                active = old;
                IOException combined = new IOException(
                    "rotation failed and could not reopen previous segment", e);
                combined.addSuppressed(reopen);
                throw combined;
            }
            throw e;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        active.close();
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("writer is closed");
    }

    /**
     * Per-append observability: the offset past the written frame plus
     * the eviction stats from any drop-oldest policy application.
     *
     * @param offsetAfter    global offset past the written frame
     * @param evictedBytes   bytes reclaimed via eviction (0 unless
     *                       DROP_OLDEST fired during this append)
     * @param evictedFrames  sample count reclaimed (approximate — from
     *                       .idx rather than recomputed from .seg to
     *                       avoid a rescan on every eviction)
     * @param evictedSegments the evicted segments in eviction order, oldest
     *                       first, for a caller that splits the frames it
     *                       had acknowledged from the ones it discarded
     */
    public record AppendResult(long offsetAfter, long evictedBytes, int evictedFrames,
                               List<EvictedSegment> evictedSegments) {}
}
