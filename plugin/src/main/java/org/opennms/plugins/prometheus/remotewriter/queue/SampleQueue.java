/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * Bounded in-memory queue of {@link MappedSample}s awaiting flush.
 *
 * <p>{@link #enqueue(MappedSample)} never blocks. If the queue has no capacity,
 * the method throws {@link StorageException}. The caller's {@code store()}
 * loop counts the refused sample together with the rest of its call in
 * {@link Shards#countDroppedQueueFull(long)}, because only the caller knows
 * how many samples the failure took down with it (issue #154). This is the
 * v0.1 backpressure contract: the plugin pushes the signal back to OpenNMS
 * rather than silently absorbing overruns.
 *
 * <p>This class backs the {@code wal.enabled=false} path. When the operator
 * enables the WAL ({@code wal.enabled=true}), the {@link WalFlusher} replaces
 * this queue + {@link Flusher} pair with a durable on-disk pipeline that
 * survives process restart and extended endpoint outages. See
 * {@link org.opennms.plugins.prometheus.remotewriter.wal} for the WAL
 * subsystem and the README for the operator trade-offs.
 */
public final class SampleQueue {

    private final ArrayBlockingQueue<MappedSample> queue;
    private final AtomicLong samplesEnqueued        = new AtomicLong();
    private final AtomicLong samplesDequeued        = new AtomicLong();

    public SampleQueue(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("queue capacity must be >= 1, got " + capacity);
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Enqueue a sample for later flush. Throws when the queue is full — the
     * caller (OpenNMS {@code store()}) sees the failure and can react.
     */
    public void enqueue(MappedSample sample) throws StorageException {
        if (sample == null) {
            // ArrayBlockingQueue.offer(null) NPEs; fail the same way every
            // other boundary fails — with a StorageException the caller can
            // react to.
            throw new StorageException("sample must not be null");
        }
        if (!queue.offer(sample)) {
            throw new StorageException(
                "prometheus-remote-writer queue full (depth=" + queue.size()
                    + ", capacity=" + (queue.size() + queue.remainingCapacity())
                    + "); dropping sample");
        }
        samplesEnqueued.incrementAndGet();
    }

    /**
     * Wait up to {@code timeout} for a sample; on arrival, additionally drain
     * up to {@code maxBatch - 1} more samples without blocking. Returns the
     * accumulated list, which is empty when the timeout elapses with an
     * empty queue.
     */
    public List<MappedSample> pollBatch(int maxBatch, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (maxBatch < 1) throw new IllegalArgumentException("maxBatch must be >= 1");
        MappedSample head = queue.poll(timeout, unit);
        if (head == null) return List.of();

        List<MappedSample> batch = new java.util.ArrayList<>(maxBatch);
        batch.add(head);
        queue.drainTo(batch, maxBatch - 1);
        samplesDequeued.addAndGet(batch.size());
        return batch;
    }

    /** Drain up to {@code maxBatch} samples without blocking. */
    public List<MappedSample> drain(int maxBatch) {
        List<MappedSample> batch = new java.util.ArrayList<>(maxBatch);
        queue.drainTo(batch, maxBatch);
        samplesDequeued.addAndGet(batch.size());
        return batch;
    }

    public int depth()    { return queue.size(); }
    public int capacity() { return queue.size() + queue.remainingCapacity(); }

    public long getSamplesEnqueued()         { return samplesEnqueued.get(); }
    public long getSamplesDequeued()         { return samplesDequeued.get(); }
}
