/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import java.io.Closeable;
import java.io.IOException;
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

    private OverflowBucket(Path dir, WalWriter writer, Checkpoint checkpoint,
                           int maxPayload, String name,
                           int recoveredPending, long writeOffset) {
        this.dir             = dir;
        this.writer          = writer;
        this.checkpoint      = checkpoint;
        this.maxPayload      = maxPayload;
        this.name            = name;
        this.reader          = new WalReader(dir, checkpoint.lastSentOffset(), maxPayload);
        this.pending         = recoveredPending;
        this.writeOffset     = writeOffset;
        // Pin the eviction floor at the checkpoint so a drop-oldest cannot
        // discard a segment the reader is about to scan.
        writer.setReaderOffsetFloor(checkpoint.lastSentOffset());
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
        return new OverflowBucket(dir, writer, recovered.checkpoint(), maxPayload,
                name, (int) recovered.pendingSampleCount(), writer.currentOffset());
    }

    /**
     * Append one sample. Returns {@link AppendResult#accepted() accepted =
     * false} only under {@link FullPolicy#REFUSE} with the bucket at its
     * bound; under {@code drop-oldest} the append always succeeds and the
     * result carries how many samples were evicted for it.
     */
    public AppendResult append(MappedSample sample) throws IOException {
        byte[] encoded = WalEntryCodec.encode(sample);
        try {
            WalWriter.AppendResult r = writer.appendWithStats(encoded);
            writeOffset = r.offsetAfter();
            pending++;
            if (r.evictedFrames() > 0) pending = Math.max(0, pending - r.evictedFrames());
            return new AppendResult(true, r.evictedFrames(),
                    org.opennms.plugins.prometheus.remotewriter.wal.Frame.HEADER_BYTES
                            + encoded.length);
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
        return new Batch(samples, read.newOffset(), read.corruptedFramesSkipped());
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

        // Best-effort: eviction floor and GC. A failure here only delays
        // reclaim; the next successful batch runs them again.
        try {
            writer.setReaderOffsetFloor(newOffset);
            long reclaimed = Checkpoint.gcSegments(dir, newOffset);
            if (reclaimed > 0) {
                LOG.debug("{}: reclaimed {} bytes at checkpoint {}", name, reclaimed, newOffset);
            }
        } catch (IOException e) {
            LOG.warn("{}: segment GC failed (non-fatal — checkpoint advanced cleanly)", name, e);
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
        writer.setReaderOffsetFloor(offset);
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

    /** Total bytes this bucket currently occupies on disk. */
    public long bytes() {
        try {
            return writer.currentTotalBytes();
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
