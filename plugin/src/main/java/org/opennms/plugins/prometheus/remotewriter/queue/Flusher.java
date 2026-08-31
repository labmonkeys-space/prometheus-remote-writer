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
 *   <li>A sample arriving in the queue — the {@link SampleQueue#pollBatch}
 *       call returns as soon as the first sample is available, then drains
 *       up to {@code batch.size - 1} more without blocking.</li>
 *   <li>The flush interval elapsing with an empty queue — the poll returns
 *       an empty list and the iteration becomes a no-op.</li>
 * </ul>
 * This gives us timer-driven flushing when idle and throughput-driven
 * flushing under load, without the complexity of a separate size-triggered
 * condition variable.
 */
public final class Flusher {

    private static final Logger LOG = LoggerFactory.getLogger(Flusher.class);

    private final SampleQueue queue;
    private final RemoteWriteHttpClient httpClient;
    private final int batchSize;
    private final long flushIntervalMs;
    private final PluginMetrics metrics;
    private final Function<Collection<MappedSample>, BuildResult> builder;
    private final String threadName;

    private volatile boolean running;
    private Thread thread;

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
        this(queue, httpClient, batchSize, flushIntervalMs, metrics, builder,
                "prometheus-remote-writer-flusher");
    }

    /** Full constructor — {@code threadName} keeps per-shard flushers
     *  distinguishable in thread dumps ({@code …-flusher-0}, {@code …-flusher-1}). */
    public Flusher(SampleQueue queue, RemoteWriteHttpClient httpClient,
                   int batchSize, long flushIntervalMs, PluginMetrics metrics,
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
    }

    public synchronized void start() {
        if (running) return;
        running = true;
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
        Thread t = thread;
        if (t == null) return;
        try {
            t.join(Math.max(1, graceMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            LOG.warn("flusher {} did not stop within {}ms, interrupting", threadName, graceMs);
            t.interrupt();
            try {
                t.join(1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        thread = null;
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

    private void run() {
        LOG.info("flusher started (batchSize={}, flushIntervalMs={})", batchSize, flushIntervalMs);
        while (running) {
            try {
                List<MappedSample> batch = queue.pollBatch(batchSize, flushIntervalMs, TimeUnit.MILLISECONDS);
                if (batch.isEmpty()) continue;
                flushBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception unexpected) {
                LOG.error("flusher caught unexpected exception", unexpected);
            }
        }

        // Drain all residual samples so stop() with a non-zero grace can get
        // them out — loop, not a single drain(batchSize), because the queue
        // may hold more than batchSize samples.
        //
        // Clear the interrupt flag first: a stop-path interrupt would
        // otherwise short-circuit Thread.sleep() inside the HTTP retry
        // backoff and abort the residual flushes. Shutdown is cooperative;
        // the caller's grace window bounds total time.
        boolean wasInterrupted = Thread.interrupted();
        try {
            while (true) {
                List<MappedSample> tail = queue.drain(batchSize);
                if (tail.isEmpty()) break;
                LOG.info("flushing {} residual sample(s) during shutdown", tail.size());
                flushBatch(tail);
            }
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("flusher stopped");
    }

    /** Package-private for unit tests — runs one flush iteration synchronously. */
    void flushBatch(List<MappedSample> batch) {
        BuildResult built = builder.apply(batch);
        metrics.samplesDroppedNonfinite(built.samplesDroppedNonfinite());
        metrics.samplesDroppedDuplicate(built.samplesDroppedDuplicate());
        if (!built.hasContent()) {
            return;
        }
        WriteResult result = httpClient.write(built.compressedPayload());
        switch (result.outcome()) {
            case SUCCESS -> {
                metrics.samplesWritten(built.samplesWritten());
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
