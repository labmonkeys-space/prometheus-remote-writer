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
    private final RemoteWriteHttpClient httpClient;
    private final int batchSize;
    private final long flushIntervalMs;
    private final long lingerMs;
    private final PluginMetrics metrics;
    private final Function<Collection<MappedSample>, BuildResult> builder;
    private final String threadName;

    private volatile boolean running;
    private Thread thread;   // builder: drains the queue, builds payloads
    private Thread sender;   // sends payloads, one request in flight

    /** A built payload on its way from the builder to the sender; {@code built == null} is the stop sentinel. */
    private record Prepared(BuildResult built, int sampleCount) {}
    private static final Prepared STOP = new Prepared(null, 0);
    /** Depth one: the builder may run at most one payload ahead of the sender,
     *  which is the whole benefit and bounds memory to one extra payload. */
    private final java.util.concurrent.ArrayBlockingQueue<Prepared> handoff = new java.util.concurrent.ArrayBlockingQueue<>(1);

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
        this(queue, httpClient, batchSize, flushIntervalMs, 0L, metrics, builder,
                "prometheus-remote-writer-flusher");
    }

    /** Full constructor — {@code threadName} keeps per-shard flushers
     *  distinguishable in thread dumps ({@code …-flusher-0}, {@code …-flusher-1}). */
    /** Full constructor. {@code lingerMs} is how long to wait for a batch
     *  to fill after a head sample arrived (batch.linger-ms; 0 = send on
     *  first arrival). See {@link SampleQueue#pollBatch(int, long, TimeUnit, long)}. */
    public Flusher(SampleQueue queue, RemoteWriteHttpClient httpClient,
                   int batchSize, long flushIntervalMs, long lingerMs, PluginMetrics metrics,
                   Function<Collection<MappedSample>, BuildResult> builder,
                   String threadName) {
        this.queue          = Objects.requireNonNull(queue);
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
        try {
            while (running) {
                try {
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
                        p = prepare(tail);
                    } catch (RuntimeException e) {
                        LOG.error("could not build {} residual sample(s) during shutdown; dropping them", tail.size(), e);
                        metrics.samplesDroppedShutdown(tail.size());
                        continue;
                    }
                    try {
                        handoff.put(p);
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

    /** One builder iteration: wait for a batch, book the waits, build it.
     *  Returns {@code null} when the poll timed out with nothing to send. The
     *  polled list does not outlive this method, so a builder blocked on the
     *  handoff holds only the built payload. */
    private Prepared pollAndPrepare() throws InterruptedException {
        long waitStarted = System.nanoTime();
        SampleQueue.Batch polled = queue.pollBatch(batchSize, flushIntervalMs, TimeUnit.MILLISECONDS, lingerMs);
        metrics.flusherIdleNanos(System.nanoTime() - waitStarted - polled.lingerNanos());
        metrics.flusherLingerNanos(polled.lingerNanos());
        if (polled.samples().isEmpty()) return null;
        return prepare(polled.samples());
    }

    private void handOff(Prepared p) throws InterruptedException {
        if (p == null) return;
        try {
            handoff.put(p);
        } catch (InterruptedException e) {
            dropAtShutdown(p); // interrupted while blocked: this payload cannot be sent
            throw e;
        }
    }

    /** Sender loop: take a built payload, write it, account for it. One request in flight. */
    private void send() {
        try {
            while (true) {
                Prepared p = handoff.take();
                if (p == STOP) break;
                try {
                    dispatch(p.built());
                } catch (RuntimeException unexpected) {
                    // Keep the sender alive: a dead sender leaves the builder
                    // parked on a full handoff and the shard silently stops.
                    LOG.error("sender caught unexpected exception; {} sample(s) not sent", p.sampleCount(), unexpected);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            LOG.info("flusher sender stopped");
        }
    }

    /** Build one batch into a payload and account for what the build dropped. */
    private Prepared prepare(List<MappedSample> batch) {
        long started = System.nanoTime();
        BuildResult built = builder.apply(batch);
        metrics.flusherBuildNanos(System.nanoTime() - started);
        metrics.samplesDroppedNonfinite(built.samplesDroppedNonfinite());
        metrics.samplesDroppedDuplicate(built.samplesDroppedDuplicate());
        return new Prepared(built, built.samplesWritten());
    }

    /** Package-private for unit tests. Builds and sends one batch synchronously. */
    void flushBatch(List<MappedSample> batch) {
        dispatch(prepare(batch).built());
    }

    /** Write one built payload and account for the outcome. */
    private void dispatch(BuildResult built) {
        if (!built.hasContent()) {
            return;
        }
        WriteResult result = httpClient.write(built.compressedPayload());
        switch (result.outcome()) {
            case SUCCESS -> {
                metrics.samplesWritten(built.samplesWritten());
                metrics.sampleLatency(built.samplesWritten(), built.enqueuedEpochMsSum());
                LOG.debug("flushed {} samples in {} bytes on attempt {}",
                        built.samplesWritten(), built.compressedPayload().length, result.attemptsMade());
            }
            case DROPPED_4XX -> {
                metrics.samplesDropped4xx(built.samplesWritten());
                LOG.warn("dropped batch of {} samples after 4xx: status={}",
                        built.samplesWritten(), result.httpStatus());
            }
            case DROPPED_5XX_EXHAUSTED -> {
                metrics.samplesDropped5xx(built.samplesWritten());
                LOG.warn("dropped batch of {} samples after {} attempts: status={}",
                        built.samplesWritten(), result.attemptsMade(), result.httpStatus());
            }
            case TRANSPORT_ERROR -> {
                metrics.samplesDroppedTransport(built.samplesWritten());
                LOG.warn("dropped batch of {} samples after transport errors: {}",
                        built.samplesWritten(), result.detail());
            }
        }
    }
}
