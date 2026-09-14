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
import java.util.concurrent.atomic.AtomicInteger;
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
 * <p>Three threads share a bucket. {@link Shards} serialises appends per
 * shard, so {@link #append} never runs against itself. The flusher's builder
 * reads and its sender acknowledges and rewinds; the reader belongs to those
 * two threads and is used under {@link #readerLock}. The pending count and
 * the checkpoint are updated from all three sides: the count is atomic, and
 * the checkpoint decides under its own lock whether an acknowledgement or an
 * eviction moved it, which is what keeps the two from counting the same
 * frame twice.
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

    /**
     * Owned by the flusher's threads: the builder reads it, the sender
     * rewinds it, both under {@link #readerLock}. The append thread never
     * touches it; an eviction only raises {@link #repositionReader}, and the
     * builder repositions at its next read.
     */
    private WalReader reader;
    private final Object readerLock = new Object();
    /** Set by an eviction on the append thread; consumed by {@link #nextBatch}. */
    private volatile boolean repositionReader;

    /**
     * Samples on disk past the checkpoint. Incremented on append, reduced by
     * what an acknowledgement covered and by what an eviction discarded that
     * no acknowledgement had covered. A rewind does not change it. Those
     * samples are still on disk and will simply be read again. Atomic
     * because the append thread and the flusher's sender update it at the
     * same time, and the gauge reads it from a scrape thread. It is an
     * estimate while a bucket at its bound is being drained (see
     * {@link Checkpoint#advancePastEvicted}) and re-bases to 0 at the first
     * append into an empty bucket.
     */
    private final AtomicInteger pending = new AtomicInteger();

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
        this.pending.set(recoveredPending);
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
        // An empty bucket holds nothing pending, whatever rounding the
        // estimates below left behind: re-base before counting this frame.
        if (isEmpty()) pending.set(0);
        // Counted before the write: once the frame is on disk the builder can
        // read it and the sender acknowledge it, and that subtraction must
        // find the increment already there.
        pending.incrementAndGet();
        WalWriter.AppendResult r;
        try {
            r = writer.appendWithStats(f);
        } catch (WalFullException full) {
            pending.decrementAndGet();
            // Under drop-oldest this only happens when a single frame cannot
            // fit the whole budget, which is a configuration error, not
            // backpressure. Either way the sample is refused; the caller
            // counts it. Any frames evicted before giving up are reported so
            // the eviction counter stays honest, and accounted for the same
            // way as on the success path.
            if (full.evictedFramesBeforeFailure() > 0) {
                accountEvictions(full.evictedSegmentsBeforeFailure());
                return new AppendResult(false, full.evictedFramesBeforeFailure(), 0L);
            }
            return AppendResult.REFUSED;
        } catch (IOException | RuntimeException e) {
            pending.decrementAndGet();
            throw e;
        }
        if (oldestPendingStamp <= 0L) oldestPendingStamp = sample.enqueuedEpochMs();
        writeOffset = r.offsetAfter();
        if (r.evictedFrames() > 0) accountEvictions(r.evictedSegments());
        return new AppendResult(true, r.evictedFrames(), frameBytes);
    }

    /**
     * Take evicted frames off the pending count and move the checkpoint and
     * reader past them.
     *
     * <p>Only reachable under {@code drop-oldest}, which can discard segments
     * the reader has not drained. Leaving the checkpoint behind the oldest
     * surviving segment would send the reader looking for frames that are no
     * longer there. The checkpoint does the split between acknowledged and
     * discarded frames under its own lock, so an acknowledgement racing this
     * cannot count a frame the eviction counted too.
     */
    private void accountEvictions(List<WalWriter.EvictedSegment> evicted) {
        try {
            Checkpoint.EvictionAccount account = checkpoint.advancePastEvicted(evicted);
            subtractPending(account.discardedFrames());
            if (account.movedFrom() < 0) return;
        } catch (IOException | RuntimeException e) {
            LOG.warn("{}: could not move the checkpoint past evicted segments; the reader will "
                    + "skip the hole on its next scan", name, e);
        }
        pinEvictionFloor();
        // The reader belongs to the flusher's threads: flag it, the builder
        // repositions at its next read rather than this thread closing a
        // reader that may be mid-read.
        repositionReader = true;
        // The evicted frames may include the one the stamp described. Leaving
        // it would have overflow_oldest_pending_age_ms — the documented
        // recovery alert — report the age of data that was deliberately
        // discarded, firing precisely in the mode where dropping is the point.
        oldestPendingStamp = 0L;
        seedOldestPendingStamp();
    }

    /** True while the bucket is below its size bound. */
    public boolean hasRoom() {
        return bytes() < maxSizeBytes;
    }

    /** Read up to {@code maxSamples} unacknowledged samples, decoded. */
    public Batch nextBatch(int maxSamples) throws IOException {
        ReadResult read;
        synchronized (readerLock) {
            if (repositionReader) {
                repositionReader = false;
                rewindLocked("after-eviction");
            }
            read = reader.nextBatch(maxSamples);
        }
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
     *                     pending depth unless an eviction already did
     * @return bytes newly checkpointed (0 when a drop-oldest eviction had
     *         already moved the checkpoint past the batch), or -1 when the
     *         advance failed
     */
    public long acknowledge(long newOffset, int samplesAcked) {
        long previousOffset;
        try {
            // Under drop-oldest an eviction on the append thread may have
            // moved the checkpoint past this batch while it was in flight.
            // The backend took the batch all the same, so that is "already
            // covered", not an error: the checkpoint stays where it is and
            // nothing was newly checkpointed.
            previousOffset = checkpoint.advancePast(newOffset);
        } catch (IOException | RuntimeException e) {
            // Never silently lose a batch: rewind and let the next cycle
            // re-ship it.
            LOG.error("{}: checkpoint advance failed; resetting reader to last-good offset {} "
                    + "for retry", name, checkpoint.lastSentOffset(), e);
            rewind("advance-fail");
            return -1L;
        }
        // An eviction that moved the checkpoint past this batch discarded its
        // frames from the count already; subtracting them again would drift
        // the gauge low by a batch per eviction while draining at the bound.
        if (previousOffset >= 0) subtractPending(samplesAcked);
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
        return previousOffset < 0 ? 0L : newOffset - previousOffset;
    }

    /**
     * Reset the reader to the last acknowledged offset, so the samples it had
     * read are read again on the next cycle. Pins the eviction floor before
     * constructing the new reader so a concurrent drop-oldest cannot discard
     * segments the reset reader is about to scan.
     */
    public void rewind(String reason) {
        synchronized (readerLock) {
            rewindLocked(reason);
        }
    }

    private void rewindLocked(String reason) {
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

    /** Count of samples on disk past the checkpoint; see {@link #pending}. */
    public int pendingSamples() {
        if (isEmpty()) return 0;
        return Math.max(0, pending.get());
    }

    /** The raw count behind {@link #pendingSamples()}, for tests. */
    int pendingCount() {
        return pending.get();
    }

    private void subtractPending(int n) {
        if (n > 0) pending.updateAndGet(p -> Math.max(0, p - n));
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
        synchronized (readerLock) {
            try {
                reader.close();
            } catch (IOException e) {
                LOG.debug("{}: reader close: {}", name, e.getMessage());
            }
        }
        try {
            writer.close();
        } catch (IOException e) {
            LOG.debug("{}: writer close: {}", name, e.getMessage());
        }
    }
}
