/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient.WriteResult;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import java.util.Collection;
import java.util.function.Function;

import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background flush thread for the {@link SampleQueue}. Each flush cycle
 * drains up to {@code batch.size} samples from the queue, builds a snappy-
 * compressed RW v1 payload via {@link RemoteWriteRequestBuilder}, and POSTs
 * it through {@link RemoteWriteHttpClient}.
 *
 * <p>The flush thread wakes on either of:
 * <ul>
 *   <li>A sample arriving in the queue — {@link SampleQueue#pollBatch(int, long, TimeUnit, long)}
 *       takes it, drains what is queued behind it and, when a linger is
 *       configured, waits up to {@code batch.linger-ms} for the batch to
 *       reach {@code batch.size}, returning early the moment it does.</li>
 *   <li>The flush interval elapsing with an empty queue — the poll returns
 *       an empty batch and the iteration becomes a no-op.</li>
 * </ul>
 */
public final class Flusher {

    private static final Logger LOG = LoggerFactory.getLogger(Flusher.class);

    private final SampleQueue queue;
    /** This shard's disk tier, or null when none is configured. */
    private final OverflowBucket bucket;
    /** The shard's memory-tier hooks, or null when there is no disk tier and
     *  so nothing to rescue a failed memory batch into. */
    private final MemoryTier memoryTier;

    /** How a {@link Flusher} talks to its shard about batches that have left
     *  the memory queue but not yet reached the backend. */
    public interface MemoryTier {
        /** Drain up to {@code maxBatch} and register it, as one step. Null
         *  when the queue was empty. */
        Shards.InFlight poll(int maxBatch);
        /** Grow a registered batch to at most {@code maxBatch}; false once a
         *  spill has taken it to disk. */
        boolean topUp(Shards.InFlight token, int maxBatch);
        /** Whether a spill already wrote this batch to disk. */
        boolean wasCarried(Shards.InFlight token);
        /** A batch was drawn from the queue and is on its way out. Returns a
         *  token identifying it, to be handed back when it settles. */
        Shards.InFlight inFlight(List<MappedSample> batch);
        /** The backend finished with this batch, one way or another. */
        void settled(Shards.InFlight token);
        /** Take a batch the backend refused onto disk; returns how many
         *  samples are accounted for there. */
        int returnToOverflow(Shards.InFlight token, List<MappedSample> batch);
    }
    private final RemoteWriteHttpClient httpClient;
    private final int batchSize;
    private final long flushIntervalMs;
    private final long lingerMs;
    private final PluginMetrics metrics;
    private final Function<Collection<MappedSample>, BuildResult> builder;
    private final String threadName;

    private volatile boolean running;
    private Thread thread;   // builder: drains a tier, builds payloads
    private Thread sender;   // sends payloads, one request in flight

    /** Which tier a batch came from. The two have different failure
     *  handling — a memory batch that exhausts its retries is dropped
     *  because there is nowhere to put it back, a disk batch rewinds and
     *  re-ships — so a batch is drawn from exactly one of them. */
    private enum Tier { MEMORY, OVERFLOW }

    /** A built payload on its way from the builder to the sender; {@code built == null} is the stop sentinel.
     *  {@code newOffset} is the bucket offset past the batch, meaningful for {@link Tier#OVERFLOW} only.
     *  {@code source} is the batch a memory payload was built from, retained only when a disk tier
     *  can take it back on failure — it costs one batch of references per shard in flight. */
    private record Prepared(BuildResult built, int sampleCount, Tier tier, long newOffset,
                            List<MappedSample> source, Shards.InFlight token,
                            int corruptedFramesSkipped) {}
    private static final Prepared STOP = new Prepared(null, 0, Tier.MEMORY, 0L, null, null, 0);
    /** Depth one: the builder may run at most one payload ahead of the sender,
     *  which is the whole benefit and bounds memory to one extra payload. */
    private final java.util.concurrent.ArrayBlockingQueue<Prepared> handoff = new java.util.concurrent.ArrayBlockingQueue<>(1);

    /**
     * Payloads handed to the sender but not yet dispatched.
     *
     * <p>The memory tier can be pipelined freely: a batch is off the queue for
     * good, and its outcome touches nothing the builder is about to read. The
     * disk tier cannot. Its reader position is shared state coupled to send
     * outcomes — a failure rewinds the reader, a success advances the
     * checkpoint — so a builder running ahead would read batch 2 while batch 1
     * is in flight, and batch 1's rewind would be undone by batch 2's
     * acknowledgement advancing the checkpoint straight past it. That loses
     * batch 1 and ships its series out of order. The old single-threaded
     * WalFlusher could not hit this; D2's pipelining can, so a disk batch
     * waits for the previous payload to settle.
     */
    private final java.util.concurrent.locks.ReentrantLock pipelineLock =
            new java.util.concurrent.locks.ReentrantLock();
    private final java.util.concurrent.locks.Condition settledCondition = pipelineLock.newCondition();
    private int outstandingPayloads;

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
    public Flusher(SampleQueue queue, RemoteWriteHttpClient httpClient,
                   int batchSize, long flushIntervalMs, PluginMetrics metrics) {
        this(queue, httpClient, batchSize, flushIntervalMs, metrics,
                RemoteWriteRequestBuilders.forVersion(1));
    }

    public Flusher(SampleQueue queue, RemoteWriteHttpClient httpClient,
                   int batchSize, long flushIntervalMs, PluginMetrics metrics,
                   Function<Collection<MappedSample>, BuildResult> builder) {
        this(queue, null, null, httpClient, batchSize, flushIntervalMs, 0L, metrics, builder,
                "prometheus-remote-writer-flusher");
    }

    /** Memory-only shard — no disk tier configured. */
    public Flusher(SampleQueue queue, RemoteWriteHttpClient httpClient,
                   int batchSize, long flushIntervalMs, long lingerMs, PluginMetrics metrics,
                   Function<Collection<MappedSample>, BuildResult> builder,
                   String threadName) {
        this(queue, null, null, httpClient, batchSize, flushIntervalMs, lingerMs, metrics, builder,
                threadName);
    }

    /** Full constructor — {@code threadName} keeps per-shard flushers
     *  distinguishable in thread dumps ({@code …-flusher-0}, {@code …-flusher-1}). */
    /** Full constructor. {@code lingerMs} is how long to wait for a batch
     *  to fill after a head sample arrived (batch.linger-ms; 0 = send on
     *  first arrival). See {@link SampleQueue#pollBatch(int, long, TimeUnit, long)}. */
    public Flusher(SampleQueue queue, OverflowBucket bucket,
                   MemoryTier memoryTier,
                   RemoteWriteHttpClient httpClient,
                   int batchSize, long flushIntervalMs, long lingerMs, PluginMetrics metrics,
                   Function<Collection<MappedSample>, BuildResult> builder,
                   String threadName) {
        this.queue            = Objects.requireNonNull(queue);
        this.bucket           = bucket;   // null = no disk tier
        this.memoryTier       = memoryTier;
        this.httpClient     = Objects.requireNonNull(httpClient);
        this.metrics        = Objects.requireNonNull(metrics);
        this.builder        = Objects.requireNonNull(builder);
        this.threadName     = Objects.requireNonNull(threadName);
        if (batchSize < 1)       throw new IllegalArgumentException("batchSize must be >= 1");
        if (flushIntervalMs < 1) throw new IllegalArgumentException("flushIntervalMs must be >= 1");
        this.batchSize       = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        if (lingerMs < 0) throw new IllegalArgumentException("lingerMs must be >= 0");
        this.lingerMs        = lingerMs;
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        handoff.clear();
        sender = new Thread(this::send, threadName + "-sender");
        sender.setDaemon(true);
        sender.start();
        thread = new Thread(this::run, threadName);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Signal the flush loop to stop without waiting. Used by the sharded
     * pipeline to signal every shard first, then await them under one
     * shared grace budget — signalling and joining sequentially would
     * multiply the worst-case shutdown time by the shard count.
     */
    public synchronized void signalStop() {
        running = false;
    }

    /**
     * Wait up to {@code graceMs} for the flush loop to finish its residual
     * drain, then interrupt. Safe to call after {@link #signalStop()};
     * no-op when the thread never started or already completed a stop.
     */
    public synchronized void awaitStop(long graceMs) {
        Thread b = thread, snd = sender;
        if (b == null && snd == null) return;
        long deadline = System.nanoTime() + Math.max(1, graceMs) * 1_000_000L;
        // The sender exits after the builder's stop sentinel, so it normally
        // finishes last; the second join closes the window in which the
        // sender has taken STOP but the builder has not yet left its finally.
        join(snd, deadline);
        join(b, deadline);
        boolean forced = false;
        for (Thread t : new Thread[] {b, snd}) {
            if (t != null && t.isAlive()) {
                LOG.warn("flusher thread {} did not stop within {}ms, interrupting", t.getName(), graceMs);
                t.interrupt();
                forced = true;
            }
        }
        if (forced) {
            for (Thread t : new Thread[] {b, snd}) {
                if (t != null) { try { t.join(1_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
            }
            // Whatever the sender never took cannot be sent now; account for it.
            Prepared orphan;
            while ((orphan = handoff.poll()) != null) dropAtShutdown(orphan);
        }
        thread = null;
        sender = null;
    }

    private void dropAtShutdown(Prepared p) {
        if (p == null || p == STOP || p.sampleCount() == 0) return;
        metrics.samplesDroppedShutdown(p.sampleCount());
        LOG.warn("forced shutdown: {} built sample(s) could not be sent", p.sampleCount());
    }

    private static void join(Thread t, long deadlineNanos) {
        if (t == null) return;
        long remainingMs = Math.max(1, (deadlineNanos - System.nanoTime()) / 1_000_000L);
        try { t.join(remainingMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /**
     * Signal the flush loop to stop, wait up to {@code graceMs} for it to
     * finish its last flush, then interrupt.
     */
    public synchronized void stop(long graceMs) {
        if (!running && thread == null) return;
        signalStop();
        awaitStop(graceMs);
    }

    /** Builder loop: poll, build, hand off. Never touches the network. */
    private void run() {
        LOG.info("flusher started (batchSize={}, flushIntervalMs={}, lingerMs={})", batchSize, flushIntervalMs, lingerMs);
        long lastFsyncNanos = System.nanoTime();
        long fsyncIntervalNanos = TimeUnit.MILLISECONDS.toNanos(flushIntervalMs);
        try {
            while (running) {
                try {
                    // What overflow.fsync=batch means: force the segment on
                    // each flush-interval boundary, so a kill -9 loses at most
                    // that window. The segment layer gates the policy — this is
                    // a no-op under `none` and redundant under `always`.
                    if (bucket != null) {
                        long now = System.nanoTime();
                        if (now - lastFsyncNanos >= fsyncIntervalNanos) {
                            bucket.flush();
                            lastFsyncNanos = now;
                        }
                    }
                    handOff(pollAndPrepare());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception unexpected) {
                    LOG.error("flusher caught unexpected exception", unexpected);
                }
            }
            // Residual drain so stop() with a non-zero grace gets everything
            // out: build each remaining batch and hand it to the sender, then
            // the sentinel. A stop-path interrupt is cleared first so the puts
            // are not short-circuited; if we are interrupted again (grace
            // expired) the batch in hand is accounted for and we leave. The
            // sentinel goes in the slot whenever it is free so the sender can
            // exit on its own.
            boolean wasInterrupted = Thread.interrupted();
            try {
                while (true) {
                    List<MappedSample> tail = queue.drain(batchSize);
                    if (tail.isEmpty()) break;
                    LOG.info("building {} residual sample(s) during shutdown", tail.size());
                    Prepared p;
                    try {
                        p = prepareMemory(tail);
                    } catch (RuntimeException e) {
                        LOG.error("could not build {} residual sample(s) during shutdown; dropping them", tail.size(), e);
                        metrics.samplesDroppedShutdown(tail.size());
                        continue;
                    }
                    try {
                        handoff.put(p);
                        payloadHandedOff();
                    } catch (InterruptedException e) {
                        wasInterrupted = true;
                        dropAtShutdown(p);
                        break;
                    }
                }
            } finally {
                try {
                    if (wasInterrupted) {
                        // Forced: the sender may be dead, so never block. If
                        // the slot is full, awaitStop accounts for the orphan.
                        if (!handoff.offer(STOP)) LOG.debug("handoff full at forced builder exit");
                    } else {
                        // Orderly: wait for the sender to free the slot, then
                        // hand it the sentinel so it exits on its own.
                        handoff.put(STOP);
                    }
                } catch (InterruptedException e) {
                    wasInterrupted = true;
                }
                if (wasInterrupted) Thread.currentThread().interrupt();
            }
        } finally {
            LOG.info("flusher builder stopped");
        }
    }

    /** One builder iteration: take a batch from whichever tier is active,
     *  book the waits, build it. Returns {@code null} when there was nothing
     *  to send. The polled list does not outlive this method, so a builder
     *  blocked on the handoff holds only the built payload.
     *
     *  <p>The disk tier goes first: while the bucket holds anything, the
     *  memory queue is empty by construction (see {@link Shards}), so this is
     *  a choice between a full tier and an empty one rather than a merge. */
    private Prepared pollAndPrepare() throws InterruptedException {
        if (bucket != null && !bucket.isEmpty()) {
            // One disk batch at a time: see the pipeline note on
            // outstandingPayloads.
            if (!awaitSenderIdle()) return null;
            Prepared fromDisk = pollOverflow();
            if (fromDisk != null) return fromDisk;
        }
        if (memoryTier != null) return pollTieredAndPrepare();
        long waitStarted = System.nanoTime();
        SampleQueue.Batch polled = queue.pollBatch(batchSize, flushIntervalMs, TimeUnit.MILLISECONDS, lingerMs);
        metrics.flusherIdleNanos(System.nanoTime() - waitStarted - polled.lingerNanos());
        metrics.flusherLingerNanos(polled.lingerNanos());
        if (polled.samples().isEmpty()) return null;
        return prepareMemory(polled.samples());
    }

    /**
     * The memory poll for a shard that has a disk tier. Unlike
     * {@link SampleQueue#pollBatch}, which removes samples as it waits, this
     * waits without taking anything and then drains under the shard's accept
     * lock, so taking the batch and registering it as in flight is one step.
     * A batch taken but not yet registered can be stranded by a concurrent
     * spill — it belongs on disk ahead of the backlog but arrives too late to
     * go there.
     */
    private Prepared pollTieredAndPrepare() throws InterruptedException {
        long waitStarted = System.nanoTime();
        Shards.InFlight token = memoryTier.poll(batchSize);
        if (token == null) {
            queue.awaitArrival(flushIntervalMs);
            token = memoryTier.poll(batchSize);
            if (token == null) {
                metrics.flusherIdleNanos(System.nanoTime() - waitStarted);
                return null;
            }
        }
        metrics.flusherIdleNanos(System.nanoTime() - waitStarted);

        long lingerNanos = 0L;
        if (lingerMs > 0 && token.size() < batchSize) {
            long lingerStarted = System.nanoTime();
            long deadline = lingerStarted + TimeUnit.MILLISECONDS.toNanos(lingerMs);
            while (token.size() < batchSize) {
                long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
                if (remainingMs <= 0) break;
                queue.awaitArrival(remainingMs);
                if (!memoryTier.topUp(token, batchSize)) break;   // spilled to disk
            }
            lingerNanos = System.nanoTime() - lingerStarted;
        }
        metrics.flusherLingerNanos(lingerNanos);

        if (memoryTier.wasCarried(token)) {
            // A spill took it while we were lingering; the disk copy ships.
            memoryTier.settled(token);
            return null;
        }
        BuildResult built = build(token.batch());
        return new Prepared(built, built.samplesWritten(), Tier.MEMORY, 0L,
                token.batch(), token, 0);
    }

    /** Read one batch off the disk tier. Null when the bucket turned out to
     *  have nothing readable, which lets the caller fall through to memory. */
    private Prepared pollOverflow() {
        OverflowBucket.Batch batch;
        try {
            batch = bucket.nextBatch(batchSize);
        } catch (java.io.IOException e) {
            LOG.error("{}: could not read the overflow bucket; rewinding and retrying next cycle",
                    threadName, e);
            bucket.rewind("read-fail");
            return null;
        }
        if (batch.isEmpty()) {
            // Nothing readable, but the scan may still have stepped over
            // corruption. Book it here: there is no acknowledgement coming to
            // defer it to, and without a batch there is nothing to re-scan.
            if (batch.corruptedFramesSkipped() > 0) {
                metrics.walFramesDroppedCorrupted(batch.corruptedFramesSkipped());
            }
            return null;
        }
        // Deferred to the acknowledgement, like the other disk counters. A
        // rewind re-scans the same segment, so counting on every read would
        // re-count the same bad frames once per retry cycle — unbounded during
        // a long outage over a bucket with one corrupt segment.
        Prepared p = prepare(batch.samples(), Tier.OVERFLOW, batch.newOffset());
        return new Prepared(p.built(), p.sampleCount(), p.tier(), p.newOffset(),
                p.source(), p.token(), batch.corruptedFramesSkipped());
    }

    private void handOff(Prepared p) throws InterruptedException {
        if (p == null) return;
        try {
            handoff.put(p);
            payloadHandedOff();
        } catch (InterruptedException e) {
            dropAtShutdown(p); // interrupted while blocked: this payload cannot be sent
            throw e;
        }
    }

    private void payloadHandedOff() {
        pipelineLock.lock();
        try {
            outstandingPayloads++;
        } finally {
            pipelineLock.unlock();
        }
    }

    private void payloadSettled() {
        pipelineLock.lock();
        try {
            if (outstandingPayloads > 0) outstandingPayloads--;
            settledCondition.signalAll();
        } finally {
            pipelineLock.unlock();
        }
    }

    /**
     * Wait until the sender has finished everything handed to it, so the next
     * disk read cannot be overtaken by the previous batch's outcome. Bounded
     * so a dead sender parks the builder for a cycle rather than forever.
     *
     * @return false when the wait timed out with work still outstanding
     */
    private boolean awaitSenderIdle() throws InterruptedException {
        pipelineLock.lock();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (outstandingPayloads > 0 && running) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return false;
                settledCondition.awaitNanos(remaining);
            }
            return outstandingPayloads == 0;
        } finally {
            pipelineLock.unlock();
        }
    }

    /** Sender loop: take a built payload, write it, account for it. One request in flight. */
    private void send() {
        try {
            while (true) {
                Prepared p = handoff.take();
                if (p == STOP) break;
                try {
                    dispatch(p);
                } catch (RuntimeException unexpected) {
                    // Keep the sender alive: a dead sender leaves the builder
                    // parked on a full handoff and the shard silently stops.
                    LOG.error("sender caught unexpected exception; {} sample(s) not sent", p.sampleCount(), unexpected);
                } finally {
                    payloadSettled();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            LOG.info("flusher sender stopped");
        }
    }

    /** Build one batch into a payload and account for what the build dropped. */
    /**
     * Build a memory batch, registering it with the shard first. From here
     * until it settles the batch is out of the queue but older than everything
     * left in it, so a spill transition has to carry it to disk with the
     * backlog — and the shutdown residual drain goes through here too, which
     * is what stops it from being silently discarded on failure.
     */
    private Prepared prepareMemory(List<MappedSample> batch) {
        Shards.InFlight token = memoryTier == null ? null : memoryTier.inFlight(batch);
        Prepared p = prepare(batch, Tier.MEMORY, 0L);
        return new Prepared(p.built(), p.sampleCount(), p.tier(), p.newOffset(), p.source(),
                token, 0);
    }

    /** Build one batch into a payload and account for what the build dropped. */
    private BuildResult build(List<MappedSample> batch) {
        long started = System.nanoTime();
        BuildResult built = builder.apply(batch);
        metrics.flusherBuildNanos(System.nanoTime() - started);
        metrics.samplesDroppedNonfinite(built.samplesDroppedNonfinite());
        metrics.samplesDroppedDuplicate(built.samplesDroppedDuplicate());
        return built;
    }

    private Prepared prepare(List<MappedSample> batch, Tier tier, long newOffset) {
        BuildResult built = build(batch);
        // A disk batch is acknowledged by sample count, and the build may have
        // dropped some as non-finite or duplicate; those are gone for good and
        // the checkpoint must still pass them, so the batch size rather than
        // the written count is what leaves the bucket.
        List<MappedSample> source = (tier == Tier.MEMORY && memoryTier != null) ? batch : null;
        return new Prepared(built, tier == Tier.OVERFLOW ? batch.size() : built.samplesWritten(),
                tier, newOffset, source, null, 0);
    }

    /** Package-private for unit tests. Builds and sends one batch synchronously. */
    void flushBatch(List<MappedSample> batch) {
        dispatch(prepareMemory(batch));
    }

    /** Write one built payload and account for the outcome. */
    private void dispatch(Prepared p) {
        try {
            dispatchInner(p);
        } finally {
            // Whatever happened, the shard no longer needs to carry this batch
            // through a spill: it either reached the backend, reached disk, or
            // was counted as dropped.
            if (memoryTier != null && p.tier() == Tier.MEMORY) {
                memoryTier.settled(p.token());
            }
        }
    }

    private void dispatchInner(Prepared p) {
        if (p.tier() == Tier.MEMORY && p.token() != null && memoryTier.wasCarried(p.token())) {
            // A spill took this batch to disk while it sat in the handoff.
            // Sending it now would put it on the wire ahead of the older
            // samples already queued on disk in front of it; the disk copy
            // ships instead, in order.
            LOG.debug("{}: skipping a memory batch a spill already wrote to disk", threadName);
            return;
        }
        BuildResult built = p.built();
        if (!built.hasContent()) {
            // Nothing to POST — every sample was dropped in the build. For a
            // disk batch the checkpoint must still advance past them, or the
            // same unsendable frames come back every cycle forever.
            if (p.tier() == Tier.OVERFLOW) acknowledgeOverflow(p, 0);
            return;
        }
        WriteResult result = httpClient.write(built.compressedPayload());
        switch (result.outcome()) {
            case SUCCESS -> {
                if (p.tier() == Tier.OVERFLOW) {
                    // Counters are deferred until the checkpoint persists: if
                    // the advance fails the batch re-ships, and it must not be
                    // counted twice.
                    if (acknowledgeOverflow(p, built.samplesWritten())) {
                        metrics.samplesWritten(built.samplesWritten());
                        metrics.sampleLatency(built.samplesWritten(), built.enqueuedEpochMsSum());
                        metrics.samplesDrainedFromOverflow(built.samplesWritten());
                    }
                } else {
                    metrics.samplesWritten(built.samplesWritten());
                    metrics.sampleLatency(built.samplesWritten(), built.enqueuedEpochMsSum());
                }
                LOG.debug("flushed {} samples in {} bytes on attempt {} from {}",
                        built.samplesWritten(), built.compressedPayload().length,
                        result.attemptsMade(), p.tier());
            }
            case DROPPED_4XX -> {
                // Permanent rejection: the backend will never take these, so a
                // disk batch advances past them rather than retrying forever.
                if (p.tier() == Tier.OVERFLOW) {
                    if (acknowledgeOverflow(p, built.samplesWritten())) {
                        metrics.samplesDropped4xx(built.samplesWritten());
                        metrics.walBatchesDropped4xx(1);
                    }
                } else {
                    metrics.samplesDropped4xx(built.samplesWritten());
                }
                LOG.warn("dropped batch of {} samples after 4xx: status={}",
                        built.samplesWritten(), result.httpStatus());
            }
            case DROPPED_5XX_EXHAUSTED -> {
                if (p.tier() == Tier.OVERFLOW) {
                    bucket.rewind("retry-after-5xx");
                    LOG.warn("batch of {} samples not accepted after {} attempts (status={}); "
                            + "the overflow bucket holds them and will retry",
                            built.samplesWritten(), result.attemptsMade(), result.httpStatus());
                } else {
                    int lost = rescueOrCount(p, built.samplesWritten());
                    if (lost > 0) {
                        metrics.samplesDropped5xx(lost);
                        LOG.warn("dropped {} of {} samples after {} attempts: status={}",
                                lost, built.samplesWritten(), result.attemptsMade(),
                                result.httpStatus());
                    }
                }
            }
            case TRANSPORT_ERROR -> {
                if (p.tier() == Tier.OVERFLOW) {
                    bucket.rewind("retry-after-transport-error");
                    LOG.warn("batch of {} samples not accepted after transport errors ({}); "
                            + "the overflow bucket holds them and will retry",
                            built.samplesWritten(), result.detail());
                } else {
                    int lost = rescueOrCount(p, built.samplesWritten());
                    if (lost > 0) {
                        metrics.samplesDroppedTransport(lost);
                        LOG.warn("dropped {} of {} samples after transport errors: {}",
                                lost, built.samplesWritten(), result.detail());
                    }
                }
            }
        }
    }

    /**
     * Try to hand a memory batch the backend would not take back to the disk
     * tier, so it is retried rather than lost.
     *
     * <p>Before 0.8.0 there was nowhere to put such a batch, so it was
     * dropped. With a tier configured there is, and leaving these samples to
     * die in memory while the bucket sat empty would make "zero loss while the
     * disk tier has room" false for exactly the samples that had not spilled
     * yet.
     *
     * @return how many samples could not be rescued and must still be counted
     *         as dropped by their original cause
     */
    private int rescueOrCount(Prepared p, int samplesInBatch) {
        if (memoryTier == null || p.source() == null) return samplesInBatch;
        int rescued = memoryTier.returnToOverflow(p.token(), p.source());
        // The rescue works front to back, so the remainder is the tail the
        // bucket had no room for.
        return Math.max(0, p.source().size() - rescued);
    }

    /**
     * Advance the bucket past a batch the backend has finished with, and log
     * the recovery when that empties it.
     *
     * @return true when the checkpoint persisted, so deferred counters may tick
     */
    private boolean acknowledgeOverflow(Prepared p, int samplesWritten) {
        long checkpointed = bucket.acknowledge(p.newOffset(), p.sampleCount());
        if (checkpointed < 0) return false;   // advance failed; bucket rewound
        if (checkpointed > 0) metrics.walBytesCheckpointed(checkpointed);
        // Now that the checkpoint has moved past them, these frames will not
        // be scanned again, so counting them here counts them once.
        if (p.corruptedFramesSkipped() > 0) {
            metrics.walFramesDroppedCorrupted(p.corruptedFramesSkipped());
        }
        if (bucket.isEmpty()) {
            LOG.info("{}: overflow bucket is empty again; new samples go back to memory "
                    + "({} sample(s) acknowledged in the last batch)", threadName, samplesWritten);
        }
        return true;
    }
}
