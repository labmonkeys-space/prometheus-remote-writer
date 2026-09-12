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
import java.util.concurrent.TimeUnit;

import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteOutcome;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteResult;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wal.Checkpoint;
import org.opennms.plugins.prometheus.remotewriter.wal.WalEntryCodec;
import org.opennms.plugins.prometheus.remotewriter.wal.WalReader;
import org.opennms.plugins.prometheus.remotewriter.wal.WalReader.ReadResult;
import org.opennms.plugins.prometheus.remotewriter.wal.WalWriter;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import java.util.Collection;
import java.util.function.Function;

import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background flush thread for the WAL-enabled path. Drains up to
 * {@code batch.size} payloads from a {@link WalReader}, decodes them via
 * {@link WalEntryCodec}, builds a snappy-compressed RW v1 payload, and
 * POSTs through {@link RemoteWriteHttpClient}.
 *
 * <p>Outcome handling:
 * <ul>
 *   <li>SUCCESS → advance the {@link Checkpoint} past the batch, advance
 *       the {@link WalWriter}'s reader-floor so the next eviction cycle
 *       can reclaim the shipped bytes, and run a GC pass to delete
 *       fully-shipped segments.</li>
 *   <li>DROPPED_4XX → advance just like SUCCESS (matches v0.4 "4xx
 *       drops the batch permanently" semantics). The
 *       {@code wal_batches_dropped_4xx_total} counter ticks alongside
 *       the existing {@code samples_dropped_4xx_total}.</li>
 *   <li>DROPPED_5XX_EXHAUSTED / TRANSPORT_ERROR → checkpoint stays put.
 *       The reader is reset to {@code checkpoint.lastSentOffset}, so the
 *       next cycle re-reads and retries the same batch. The WAL holds
 *       the data durably across any outage length until the endpoint
 *       recovers or the cap is reached and overflow policy kicks in.</li>
 * </ul>
 *
 * <p>Unlike {@link Flusher}, shutdown does NOT attempt to drain residual
 * samples — the WAL is durable, so anything not shipped at stop()
 * replays on next start. The grace window controls only the in-flight
 * HTTP request deadline.
 */
public final class WalFlusher implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(WalFlusher.class);

    private final Path walDir;
    private final WalWriter writer;
    private final Checkpoint checkpoint;
    private final int maxPayload;
    private final RemoteWriteHttpClient httpClient;
    private final int batchSize;
    private final long flushIntervalMs;
    private final PluginMetrics metrics;
    private final Function<Collection<MappedSample>, BuildResult> builder;

    private WalReader reader;
    private volatile boolean running;
    private Thread thread;

    /**
     * Cumulative count of samples drained from the WAL but not yet
     * checkpointed. Bumped per-batch when nextBatch returns payloads;
     * cleared to zero on each successful advance and on each
     * reader-rewind (5xx/transport / advance-fail) so the count
     * never spans batches that haven't actually been acknowledged.
     *
     * <p>Volatile because {@link #pendingSampleCount()} reads it from
     * the bundle stop-thread before that thread invokes
     * {@link #stop(long)}; without the volatile the JMM would permit
     * stale reads. (32-bit int writes are atomic on the JVM, so torn
     * reads are not the concern — only visibility.)
     */
    private volatile int samplesDrainedSinceLastCheckpoint;

    /**
     * Test-only convenience constructor — hard-codes the v1 builder.
     *
     * <p><b>Do not use from production code.</b> The HTTP client selects
     * v1/v2 headers from {@link
     * org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig#getWireProtocolVersion()};
     * mixing this ctor with a {@code wire.protocol-version=2} config
     * would emit v1-shaped bytes under v2 headers, which the backend
     * rejects (or silently drops, on Prometheus 2.50–2.54). Production
     * call sites must thread {@link
     * RemoteWriteRequestBuilders#forVersion(int)
     * RemoteWriteRequestBuilders.forVersion(config.getWireProtocolVersion())}
     * into the explicit-builder constructor below.
     */
    public WalFlusher(Path walDir, WalWriter writer, Checkpoint checkpoint,
                      int maxPayload, RemoteWriteHttpClient httpClient,
                      int batchSize, long flushIntervalMs, PluginMetrics metrics) {
        this(walDir, writer, checkpoint, maxPayload, httpClient, batchSize,
                flushIntervalMs, metrics, RemoteWriteRequestBuilders.forVersion(1));
    }

    public WalFlusher(Path walDir, WalWriter writer, Checkpoint checkpoint,
                      int maxPayload, RemoteWriteHttpClient httpClient,
                      int batchSize, long flushIntervalMs, PluginMetrics metrics,
                      Function<Collection<MappedSample>, BuildResult> builder) {
        this.walDir          = Objects.requireNonNull(walDir);
        this.writer          = Objects.requireNonNull(writer);
        this.checkpoint      = Objects.requireNonNull(checkpoint);
        this.httpClient      = Objects.requireNonNull(httpClient);
        this.metrics         = Objects.requireNonNull(metrics);
        this.builder         = Objects.requireNonNull(builder);
        if (batchSize < 1)       throw new IllegalArgumentException("batchSize must be >= 1");
        if (flushIntervalMs < 1) throw new IllegalArgumentException("flushIntervalMs must be >= 1");
        this.maxPayload      = maxPayload;
        this.batchSize       = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.reader = new WalReader(walDir, checkpoint.lastSentOffset(), maxPayload);
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        thread = new Thread(this::run, "prometheus-remote-writer-wal-flusher");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Signal the flush loop to stop and wait up to {@code graceMs} for
     * any in-flight HTTP POST to complete. No WAL drain on shutdown —
     * anything unfinished is durable on disk and replays on next start.
     */
    public synchronized void stop(long graceMs) {
        // Close the reader regardless of whether the loop ever started.
        // The constructor eagerly creates `reader`, so a stop() called
        // after a failed start() (e.g., from rollbackStart) without a
        // matching start() must still release the FD — early-returning
        // on `!running` would leak it.
        try {
            if (running) {
                running = false;
                Thread t = thread;
                if (t != null) {
                    try {
                        t.join(Math.max(1, graceMs));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    if (t.isAlive()) {
                        LOG.warn("wal-flusher did not stop within {}ms, interrupting", graceMs);
                        t.interrupt();
                        try {
                            t.join(1_000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    thread = null;
                }
            }
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                LOG.debug("wal-reader close: {}", e.getMessage());
            }
        }
    }

    /**
     * Closeable contract — delegates to {@link #stop(long)} with a zero
     * grace, useful in tests using try-with-resources. Production code
     * uses the explicit {@link #stop(long)} with the operator's
     * configured grace.
     */
    @Override
    public void close() {
        stop(0);
    }

    /** Current reader offset (for shutdown logging). */
    public long readerOffset() {
        return reader.currentOffset();
    }

    /**
     * Approximate count of samples drained from the WAL but not yet
     * confirmed shipped (i.e., past the reader but not past the
     * checkpoint). Used for the shutdown INFO log "N samples pending"
     * which is a more operator-meaningful unit than raw bytes. Single
     * counter incremented per successful nextBatch payload list and
     * cleared (clamped to 0) per successful advance.
     */
    public int pendingSampleCount() {
        return Math.max(0, samplesDrainedSinceLastCheckpoint);
    }

    private void run() {
        LOG.info("wal-flusher started (batchSize={}, flushIntervalMs={}, resumeOffset={})",
                batchSize, flushIntervalMs, checkpoint.lastSentOffset());
        long lastFlushNanos = System.nanoTime();
        long flushIntervalNanos = TimeUnit.MILLISECONDS.toNanos(flushIntervalMs);
        while (running) {
            try {
                // Trigger writer.flush() on each flush-interval boundary
                // so wal.fsync=batch actually delivers the documented
                // "fsync at flush-interval" durability. WalSegment.flush
                // is a no-op under wal.fsync=never and a force(false)
                // under always|batch — the policy gate lives in the
                // segment, not here.
                long now = System.nanoTime();
                if (now - lastFlushNanos >= flushIntervalNanos) {
                    try {
                        writer.flush();
                    } catch (IOException e) {
                        LOG.warn("wal-writer flush failed; samples since last "
                                + "fsync may be lost on a kernel-level crash", e);
                    }
                    lastFlushNanos = now;
                }

                ReadResult batch = reader.nextBatch(batchSize);
                if (batch.isEmpty()) {
                    Thread.sleep(flushIntervalMs);
                    continue;
                }
                flushBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception unexpected) {
                LOG.error("wal-flusher caught unexpected exception", unexpected);
            }
        }
        LOG.info("wal-flusher stopped");
    }

    /** Package-private for unit tests — runs one flush iteration synchronously. */
    void flushBatch(ReadResult batch) {
        samplesDrainedSinceLastCheckpoint += batch.size();

        // Pass through any corruption-skip count from the reader so the
        // operator sees a counter tick (the reader logs WARN; this
        // surfaces it as a metric).
        if (batch.corruptedFramesSkipped() > 0) {
            metrics.walFramesDroppedCorrupted(batch.corruptedFramesSkipped());
        }

        // Decode each WAL-framed payload into a MappedSample. Decode
        // failures are surfaced as hard errors; the codec already
        // rejects malformed payloads with an actionable message, so
        // reaching here means the WAL is corrupted beyond what CRC +
        // schema validation can catch.
        List<MappedSample> samples = new ArrayList<>(batch.payloads().size());
        for (byte[] payload : batch.payloads()) {
            samples.add(WalEntryCodec.decode(payload));
        }

        BuildResult built = builder.apply(samples);
        metrics.samplesDroppedNonfinite(built.samplesDroppedNonfinite());
        metrics.samplesDroppedDuplicate(built.samplesDroppedDuplicate());
        if (!built.hasContent()) {
            // No payload to POST (every sample was dropped as non-finite
            // / duplicate). Still advance the checkpoint — the samples
            // were WAL-durable and are now accounted for. samplesWritten
            // is already 0 here, so the success-branch tick is a no-op.
            advanceAfterBatch(batch.newOffset(), BatchOutcome.success(0, 0L));
            return;
        }

        WriteResult result = httpClient.write(built.compressedPayload());
        switch (result.outcome()) {
            case SUCCESS -> {
                LOG.debug("wal-flushed {} samples in {} bytes on attempt {}",
                        built.samplesWritten(), built.compressedPayload().length, result.attemptsMade());
                // Counter ticks for SUCCESS/4xx are deferred to
                // advanceAfterBatch — they fire only when the on-disk
                // checkpoint actually persists. If advance fails the
                // reader rewinds and the batch re-ships next cycle,
                // and we don't want to double-count.
                advanceAfterBatch(batch.newOffset(),
                        BatchOutcome.success(built.samplesWritten(), built.enqueuedEpochMsSum()));
            }
            case DROPPED_4XX -> {
                LOG.warn("wal-dropped batch of {} samples after 4xx: status={}",
                        built.samplesWritten(), result.httpStatus());
                advanceAfterBatch(batch.newOffset(),
                        BatchOutcome.dropped4xx(built.samplesWritten()));
            }
            case DROPPED_5XX_EXHAUSTED, TRANSPORT_ERROR -> {
                if (result.outcome() == WriteOutcome.DROPPED_5XX_EXHAUSTED) {
                    LOG.warn("wal-retry pending for batch of {} samples after {} attempts: "
                            + "status={} — WAL holds the data; will retry next cycle",
                            built.samplesWritten(), result.attemptsMade(), result.httpStatus());
                } else {
                    LOG.warn("wal-retry pending for batch of {} samples after transport error: "
                            + "{} — WAL holds the data; will retry next cycle",
                            built.samplesWritten(), result.detail());
                }
                rewindReader(checkpoint.lastSentOffset(), "retry-after-fail");
            }
        }
    }

    /**
     * Carrier for "what counters need to tick when the checkpoint
     * advance succeeds." Ticking these BEFORE advance would over-count
     * on a transient advance failure that triggers a reader-rewind +
     * retry — same samples would re-ship, counters would tick again.
     */
    private record BatchOutcome(boolean success, int samplesWritten, int samplesDropped4xx, long enqueuedEpochMsSum) {
        static BatchOutcome success(int n, long enqueuedEpochMsSum) { return new BatchOutcome(true, n, 0, enqueuedEpochMsSum); }
        static BatchOutcome dropped4xx(int n) { return new BatchOutcome(false, 0, n, 0L); }
    }

    /**
     * Reset the reader to the given offset, releasing the current
     * channel. Pre-pins the writer's reader-offset floor BEFORE
     * constructing the new reader so a concurrent DROP_OLDEST eviction
     * can't trash the segments the reset reader is about to scan.
     * Also clears {@link #samplesDrainedSinceLastCheckpoint} — those
     * samples are about to be re-read.
     */
    private void rewindReader(long offset, String reason) {
        // Pin the floor first to prevent any eviction race with the
        // about-to-be-constructed reader.
        writer.setReaderOffsetFloor(offset);
        try {
            reader.close();
        } catch (IOException e) {
            LOG.debug("wal-reader close during rewind ({}): {}", reason, e.getMessage());
        }
        reader = new WalReader(walDir, offset, maxPayload);
        // Pending counter is in-memory only; on rewind those samples
        // will be re-read and re-counted in the next batch.
        samplesDrainedSinceLastCheckpoint = 0;
    }

    /**
     * Advance the on-disk checkpoint to {@code newOffset}. On success,
     * tick the carrier's deferred counters, advance the writer's
     * reader-floor, and run a GC pass for shipped segments. On
     * failure, rewind the reader to the previous checkpoint so the
     * batch re-ships next cycle (and counters never tick for the
     * un-acked attempt).
     */
    private void advanceAfterBatch(long newOffset, BatchOutcome outcome) {
        long previousOffset = checkpoint.lastSentOffset();
        try {
            checkpoint.advance(newOffset);
        } catch (IOException | RuntimeException e) {
            // Includes IllegalArgumentException (programming-bug
            // backward move) and any future runtime exception from
            // Checkpoint.advance — never silently lose a batch.
            LOG.error("wal-checkpoint advance failed; resetting reader to "
                    + "last-good offset {} for retry", previousOffset, e);
            rewindReader(previousOffset, "advance-fail");
            return;
        }

        // Advance succeeded — NOW it's safe to tick the deferred
        // counters and clear the pending-sample tracker.
        if (outcome.success()) {
            metrics.samplesWritten(outcome.samplesWritten());
            metrics.sampleLatency(outcome.samplesWritten(), outcome.enqueuedEpochMsSum());
        } else {
            metrics.samplesDropped4xx(outcome.samplesDropped4xx());
            metrics.walBatchesDropped4xx(1);
        }
        samplesDrainedSinceLastCheckpoint = 0;

        // Best-effort: reader-floor and GC. Failures here only delay GC
        // / floor updates; next successful batch re-runs them.
        try {
            writer.setReaderOffsetFloor(newOffset);
            long bytesPastCheckpoint = newOffset - previousOffset;
            if (bytesPastCheckpoint > 0) {
                metrics.walBytesCheckpointed(bytesPastCheckpoint);
            }
            long reclaimed = Checkpoint.gcSegments(walDir, newOffset);
            if (reclaimed > 0) {
                LOG.debug("wal-gc reclaimed {} bytes at checkpoint {}", reclaimed, newOffset);
            }
        } catch (IOException e) {
            LOG.warn("wal-gc failed (non-fatal — checkpoint advanced cleanly)", e);
        }
    }
}
