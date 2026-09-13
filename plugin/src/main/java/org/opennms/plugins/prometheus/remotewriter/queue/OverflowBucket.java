/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.opennms.plugins.prometheus.remotewriter.wal.Checkpoint;
import org.opennms.plugins.prometheus.remotewriter.wal.WalEntryCodec;
import org.opennms.plugins.prometheus.remotewriter.wal.WalFullException;
import org.opennms.plugins.prometheus.remotewriter.wal.WalReader;
import org.opennms.plugins.prometheus.remotewriter.wal.WalReader.ReadResult;
import org.opennms.plugins.prometheus.remotewriter.wal.WalRecovery;
import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One shard's disk tier: the durable bucket that sits behind its
 * {@link SampleQueue}. A bucket is the existing WAL machinery
 * ({@link WalWriter}, {@link WalReader}, {@link Checkpoint},
 * {@link WalRecovery}) pointed at one directory, which is what lets every
 * shard own an independent ordered log instead of sharing one.
 *
 * <p>Durability rests on the acknowledgement rule inherited from the WAL:
 * {@link #acknowledge(long)} advances the checkpoint only after the backend
 * has taken the batch, and segments are released only once the checkpoint has
 * passed them. A batch whose write failed is {@linkplain #rewind(String)
 * rewound} and re-read on a later cycle, never dropped.
 *
 * <p>Not thread-safe for concurrent appends against concurrent reads by
 * design: {@link Shards} serialises appends per shard, and the reads all
 * happen on that shard's single builder thread.
 */
public final class OverflowBucket implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(OverflowBucket.class);

    /** What a shard does when its bucket is at its size bound. */
    public enum FullPolicy {
        /** Refuse the append; the caller counts it and throws. */
        REFUSE,
        /** Evict the oldest whole segment to make room, then append. */
        DROP_OLDEST
    }

    /**
     * How a shard divides its drain between its two tiers.
     *
     * <p>This is not only an arbiter setting. Under {@link #ORDERED} the
     * accept path keeps a shard's memory queue empty for as long as its bucket
     * is non-empty, which is what makes disk-before-memory equal offered
     * order; leave that in place and {@link #CONCURRENT} would have nothing to
     * alternate with. The policy therefore reaches both ends.
     */
    public enum DrainPolicy {
        /** Bucket drained to empty before any memory sample, memory unused
         *  while it holds anything. Per-series order survives the boundary, so
         *  this works on every backend. */
        ORDERED,
        /** Memory keeps being used while the bucket drains, and the tiers
         *  alternate. Fresh samples do not queue behind the backlog, at the
         *  cost of per-series order across the boundary — needs a backend that
         *  accepts out-of-order writes. */
        CONCURRENT
    }

    /** One drained batch: decoded samples, the offset past them, and any
     *  frames the reader had to skip as corrupted. */
    public record Batch(List<MappedSample> samples, long newOffset, int corruptedFramesSkipped) {
        public boolean isEmpty() { return samples.isEmpty(); }
        public int size()        { return samples.size(); }
    }

    /** Outcome of an append: whether it was taken, and how many samples a
     *  {@code drop-oldest} eviction discarded to make room for it. */
    public record AppendResult(boolean accepted, int evictedSamples, long bytesWritten) {
        static final AppendResult REFUSED = new AppendResult(false, 0, 0L);
    }

    private final Path dir;
    private final WalWriter writer;
    private final Checkpoint checkpoint;
    private final FullPolicy fullPolicy;
    private final long maxSizeBytes;
    private final int maxPayload;
    private final String name;

    private WalReader reader;

    /**
     * Samples on disk past the checkpoint. Incremented on append, reduced by
     * what an acknowledgement covered. A rewind does not change it — those
     * samples are still on disk, they will simply be read again. Volatile
     * because the gauge is read from a scrape thread.
     */
    private volatile int pending;

    /**
     * Offset immediately past the last appended frame, kept in step with the
     * writer so {@link #isEmpty()} is a pair of field reads. The accept path
     * consults it per sample, so it must not cost a syscall.
     */
    private volatile long writeOffset;

    /**
     * Wall-clock {@code store()} stamp of the oldest sample on disk that the
     * backend has not taken, or 0 when nothing is pending.
     *
     * <p>Set three ways, because each covers a window the others miss. The
     * first append into an empty bucket records the sample that is now the
     * oldest — without it a shard that has just spilled reads 0 until its
     * first batch comes back off disk. Every read refreshes it as the head
     * advances, which keeps it right through an outage since a rewind
     * re-reads the same head batch. And opening a recovered bucket peeks its
     * first frame, for the restart case where nothing in this process
     * appended. It returns to 0 when the bucket empties.
     */
    private volatile long oldestPendingStamp;

    private OverflowBucket(Path dir, WalWriter writer, Checkpoint checkpoint,
                           FullPolicy fullPolicy, long maxSizeBytes,
                           int maxPayload, String name,
                           int recoveredPending, long writeOffset) {
        this.dir             = dir;
        this.writer          = writer;
        this.checkpoint      = checkpoint;
        this.fullPolicy      = fullPolicy;
        this.maxSizeBytes    = maxSizeBytes;
        this.maxPayload      = maxPayload;
        this.name            = name;
        this.reader          = new WalReader(dir, checkpoint.lastSentOffset(), maxPayload);
        this.pending         = recoveredPending;
        this.writeOffset     = writeOffset;
        pinEvictionFloor();
        seedOldestPendingStamp();
    }

    /**
     * Where eviction is allowed to reach.
     *
     * <p>Under {@link FullPolicy#REFUSE} the floor is the checkpoint, so
     * nothing the reader still needs can be discarded — a full bucket refuses
     * instead.
     *
     * <p>Under {@link FullPolicy#DROP_OLDEST} there is no floor. The operator
     * has said newest-wins, and a floor at the checkpoint would make the
     * policy a no-op precisely when it is needed: during an outage the
     * checkpoint is frozen and every surviving segment sits above it, so
     * eviction would find nothing to take and the append would be refused.
     * Discarding acknowledged-but-unshipped data is the whole point; what has
     * to follow is moving the reader and checkpoint past the hole, which
     * {@link #repositionAfterEviction()} does.
     */
    private void pinEvictionFloor() {
        writer.setReaderOffsetFloor(
                fullPolicy == FullPolicy.DROP_OLDEST ? 0L : checkpoint.lastSentOffset());
    }

    /**
     * Open (and recover) the bucket rooted at {@code dir}, creating the
     * directory if it does not exist.
     *
     * @param name short identifier used in log lines, e.g. {@code shard-2}
     */
    public static OverflowBucket open(Path dir, long maxSizeBytes, long segmentSizeBytes,
                                      FullPolicy fullPolicy, FsyncPolicy fsync,
                                      int maxPayload, String name) throws IOException {
        Objects.requireNonNull(dir, "dir");
        WalRecovery.RecoveredWal recovered = WalRecovery.recover(dir, fsync, maxPayload);
        WalWriter writer = WalWriter.resume(dir, recovered.activeSegment(), segmentSizeBytes,
                maxSizeBytes,
                fullPolicy == FullPolicy.DROP_OLDEST
                        ? WalWriter.OverflowPolicy.DROP_OLDEST
                        : WalWriter.OverflowPolicy.BACKPRESSURE,
                fsync, maxPayload);
        // The one directory listing for the byte count, before any flusher
        // can run segment GC on this bucket. From here it is tracked.
        writer.initTotalBytes();
        return new OverflowBucket(dir, writer, recovered.checkpoint(), fullPolicy, maxSizeBytes,
                maxPayload, name, (int) recovered.pendingSampleCount(), writer.currentOffset());
    }

    /**
     * The frame {@link #append(MappedSample, ByteBuffer)} would write for
     * {@code sample}: the encoded entry, its length header and its CRC. Built
     * by the calling thread before it takes the shard's accept lock, so the
     * lock covers only the write.
     */
    public static ByteBuffer encodeFrame(MappedSample sample) {
        return org.opennms.plugins.prometheus.remotewriter.wal.Frame.encode(WalEntryCodec.encode(sample));
    }

    /**
     * Append one sample. Returns {@link AppendResult#accepted() accepted =
     * false} only under {@link FullPolicy#REFUSE} with the bucket at its
     * bound; under {@code drop-oldest} the append always succeeds and the
     * result carries how many samples were evicted for it.
     */
    public AppendResult append(MappedSample sample) throws IOException {
        return append(sample, null);
    }

    /**
     * {@link #append(MappedSample)} with the frame already built by
     * {@link #encodeFrame}, or null to build it here. Consumes the frame.
     */
    public AppendResult append(MappedSample sample, ByteBuffer frame) throws IOException {
        ByteBuffer f = frame != null ? frame : encodeFrame(sample);
        long frameBytes = f.remaining();
        try {
            WalWriter.AppendResult r = writer.appendWithStats(f);
            if (oldestPendingStamp <= 0L) oldestPendingStamp = sample.enqueuedEpochMs();
            writeOffset = r.offsetAfter();
            pending++;
            if (r.evictedFrames() > 0) {
                pending = Math.max(0, pending - r.evictedFrames());
                repositionAfterEviction();
            }
            return new AppendResult(true, r.evictedFrames(), frameBytes);
        } catch (WalFullException full) {
            // Under drop-oldest this only happens when a single frame cannot
            // fit the whole budget, which is a configuration error, not
            // backpressure. Either way the sample is refused; the caller
            // counts it. Any frames evicted before giving up are reported so
            // the eviction counter stays honest.
            if (full.evictedFramesBeforeFailure() > 0) {
                return new AppendResult(false, full.evictedFramesBeforeFailure(), 0L);
            }
            return AppendResult.REFUSED;
        }
    }

    /**
     * Move the checkpoint and reader past frames an eviction deleted.
     *
     * <p>Only reachable under {@code drop-oldest}, which can discard segments
     * the reader has not drained. Leaving the checkpoint behind the oldest
     * surviving segment would send the reader looking for frames that are no
     * longer there.
     */
    private void repositionAfterEviction() throws IOException {
        long oldest = oldestSegmentStart();
        if (oldest <= checkpoint.lastSentOffset()) return;
        try {
            checkpoint.advance(oldest);
        } catch (IOException | RuntimeException e) {
            LOG.warn("{}: could not move the checkpoint past evicted segments; the reader will "
                    + "skip the hole on its next scan", name, e);
        }
        pinEvictionFloor();
        rewind("after-eviction");
        // The evicted frames may include the one the stamp described. Leaving
        // it would have overflow_oldest_pending_age_ms — the documented
        // recovery alert — report the age of data that was deliberately
        // discarded, firing precisely in the mode where dropping is the point.
        oldestPendingStamp = 0L;
        seedOldestPendingStamp();
    }

    /** Lowest start offset still on disk, or the checkpoint when none remain. */
    private long oldestSegmentStart() throws IOException {
        long oldest = Long.MAX_VALUE;
        try (java.util.stream.Stream<Path> files = java.nio.file.Files.list(dir)) {
            for (Path p : (Iterable<Path>) files::iterator) {
                String n = p.getFileName().toString();
                if (!n.endsWith(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.SEG_EXT)) {
                    continue;
                }
                try {
                    oldest = Math.min(oldest, Long.parseLong(n.substring(0, n.length() - 4)));
                } catch (NumberFormatException ignored) {
                    // Not a segment we wrote; leave it alone.
                }
            }
        }
        return oldest == Long.MAX_VALUE ? checkpoint.lastSentOffset() : oldest;
    }

    /** True while the bucket is below its size bound. */
    public boolean hasRoom() {
        return bytes() < maxSizeBytes;
    }

    /** Read up to {@code maxSamples} unacknowledged samples, decoded. */
    public Batch nextBatch(int maxSamples) throws IOException {
        ReadResult read = reader.nextBatch(maxSamples);
        if (read.isEmpty()) {
            return new Batch(List.of(), read.newOffset(), read.corruptedFramesSkipped());
        }
        List<MappedSample> samples = new ArrayList<>(read.payloads().size());
        for (byte[] payload : read.payloads()) {
            samples.add(WalEntryCodec.decode(payload));
        }
        oldestPendingStamp = samples.get(0).enqueuedEpochMs();
        return new Batch(samples, read.newOffset(), read.corruptedFramesSkipped());
    }

    /**
     * Read the first unacknowledged frame without disturbing the reader, so
     * {@link #oldestPendingAgeMs()} is right before anything has been drained.
     * Best effort: a bucket that cannot be peeked simply reports 0 until its
     * first real read.
     */
    private void seedOldestPendingStamp() {
        if (isEmpty()) return;
        try (WalReader peek = new WalReader(dir, checkpoint.lastSentOffset(), maxPayload)) {
            ReadResult first = peek.nextBatch(1);
            if (!first.isEmpty()) {
                oldestPendingStamp = WalEntryCodec.decode(first.payloads().get(0)).enqueuedEpochMs();
            }
        } catch (IOException | RuntimeException e) {
            LOG.debug("{}: could not seed the oldest-pending stamp: {}", name, e.getMessage());
        }
    }

    /** Age of the oldest unacknowledged sample, or 0 when the bucket is empty. */
    public long oldestPendingAgeMs() {
        if (isEmpty()) return 0L;
        long stamp = oldestPendingStamp;
        if (stamp <= 0L) return 0L;
        return Math.max(0L, System.currentTimeMillis() - stamp);
    }

    /**
     * Advance the checkpoint past an acknowledged batch and release what it
     * covers. On failure the bucket rewinds to the last good checkpoint so
     * the batch re-ships, and returns {@code false} — the caller must not
     * tick any counter for a batch whose acknowledgement did not persist.
     *
     * @param samplesAcked how many samples the batch held, to take off the
     *                     pending depth
     * @return bytes newly checkpointed, or -1 when the advance failed
     */
    public long acknowledge(long newOffset, int samplesAcked) {
        long previousOffset = checkpoint.lastSentOffset();
        try {
            checkpoint.advance(newOffset);
        } catch (IOException | RuntimeException e) {
            // Includes the programming-bug backward move. Never silently
            // lose a batch: rewind and let the next cycle re-ship it.
            LOG.error("{}: checkpoint advance failed; resetting reader to last-good offset {} "
                    + "for retry", name, previousOffset, e);
            rewind("advance-fail");
            return -1L;
        }
        pending = Math.max(0, pending - samplesAcked);
        // The stamp still describes the batch just acknowledged. Clear it so
        // the next read sets it from what is actually oldest; until then the
        // gauge reports 0 rather than something too old, which is the safer
        // direction for an alert.
        oldestPendingStamp = 0L;
        if (!isEmpty()) seedOldestPendingStamp();

        // Best-effort: eviction floor and GC. A failure here only delays
        // reclaim; the next successful batch runs them again.
        try {
            pinEvictionFloor();
            // Each deletion reaches the writer's byte count as it happens.
            long reclaimed = Checkpoint.gcSegments(dir, newOffset, writer::reclaimed);
            if (reclaimed > 0) {
                LOG.debug("{}: reclaimed {} bytes at checkpoint {}", name, reclaimed, newOffset);
            }
        } catch (IOException e) {
            LOG.warn("{}: segment GC failed (non-fatal — checkpoint advanced cleanly)", name, e);
            // What the GC deleted before it failed is unknown, so the byte
            // count re-reads the directory. Safe here: this is the thread
            // that runs GC, and it has stopped.
            try {
                writer.rederiveTotalBytes();
            } catch (IOException again) {
                LOG.warn("{}: could not re-read the bucket size after a failed GC", name, again);
            }
        }
        return Math.max(0L, newOffset - previousOffset);
    }

    /**
     * Reset the reader to the last acknowledged offset, so the samples it had
     * read are read again on the next cycle. Pins the eviction floor before
     * constructing the new reader so a concurrent drop-oldest cannot discard
     * segments the reset reader is about to scan.
     */
    public void rewind(String reason) {
        long offset = checkpoint.lastSentOffset();
        pinEvictionFloor();
        try {
            reader.close();
        } catch (IOException e) {
            LOG.debug("{}: reader close during rewind ({}): {}", name, reason, e.getMessage());
        }
        reader = new WalReader(dir, offset, maxPayload);
    }

    /**
     * True when nothing on disk is waiting for acknowledgement. The accept
     * path reads this per sample to decide whether its shard is still
     * yielding to the bucket, so it is two volatile reads and no syscall:
     * the offset past the last append against the checkpoint, both kept in
     * step in memory.
     */
    public boolean isEmpty() {
        return writeOffset == checkpoint.lastSentOffset();
    }

    /** Count of samples on disk past the checkpoint. */
    public int pendingSamples() {
        if (isEmpty()) return 0;
        return Math.max(0, pending);
    }

    /**
     * Total bytes this bucket currently occupies on disk. Tracked, not listed:
     * this is read by every gauge scrape and every all-or-nothing room check,
     * and a listing would hold the writer's lock and stall this shard's
     * appends for as long as it took.
     */
    public long bytes() {
        try {
            return writer.totalBytes();
        } catch (IOException e) {
            return 0L;
        }
    }

    /** Force the configured fsync policy to the segment layer. */
    public void flush() {
        try {
            writer.flush();
        } catch (IOException e) {
            LOG.warn("{}: flush failed; samples since the last fsync may be lost on a "
                    + "kernel-level crash", name, e);
        }
    }

    public Path dir() { return dir; }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            LOG.debug("{}: reader close: {}", name, e.getMessage());
        }
        try {
            writer.close();
        } catch (IOException e) {
            LOG.debug("{}: writer close: {}", name, e.getMessage());
        }
    }
}
