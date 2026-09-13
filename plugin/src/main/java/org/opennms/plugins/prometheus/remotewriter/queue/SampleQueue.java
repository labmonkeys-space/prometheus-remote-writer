/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;

/**
 * Bounded in-memory queue of {@link MappedSample}s awaiting flush.
 *
 * <p>{@link #tryEnqueue(MappedSample)} never blocks and returns {@code false}
 * when the queue has no capacity. The caller's {@code store()} loop counts
 * refusals in {@link Shards#countDroppedQueueFull(long)} and throws one
 * {@code StorageException} per call (issues #154, #156). This is the v0.1
 * backpressure contract: the plugin pushes the signal back to OpenNMS rather
 * than silently absorbing overruns.
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
     * Offer a sample for later flush. Never blocks; returns {@code false}
     * when the queue is full. The {@code store()} loop counts refusals and
     * throws once per call, so no exception is built per refused sample
     * (issue #156).
     */
    public boolean tryEnqueue(MappedSample sample) {
        if (!tryEnqueueQuietly(sample)) return false;
        signalArrival();
        return true;
    }

    /**
     * {@link #tryEnqueue} without waking the consumer. For a producer placing
     * several samples in one step, which then calls {@link #signalArrival()}
     * once rather than taking the arrival monitor per sample.
     */
    boolean tryEnqueueQuietly(MappedSample sample) {
        Objects.requireNonNull(sample, "sample");
        if (!queue.offer(sample)) return false;
        samplesEnqueued.incrementAndGet();
        return true;
    }

    /**
     * Monitor a consumer can wait on for a sample to show up without taking
     * one. {@link #pollBatch} removes as it waits, which is fine when the
     * consumer owns the queue outright; a shard with a disk tier cannot do
     * that, because a batch removed but not yet registered as in flight can be
     * stranded by a concurrent spill. Such a consumer waits here and then
     * drains under the shard's accept lock, so taking the samples and
     * registering them is one step.
     */
    private final Object arrival = new Object();

    void signalArrival() {
        synchronized (arrival) {
            arrival.notifyAll();
        }
    }

    /**
     * Wait up to {@code timeoutMs} for the queue to be non-empty, without
     * removing anything. Returns immediately when it already is. A spurious
     * return is harmless: the caller drains and finds nothing.
     */
    public void awaitArrival(long timeoutMs) throws InterruptedException {
        if (timeoutMs <= 0) return;
        synchronized (arrival) {
            // Checked inside the monitor, and tryEnqueue signals inside it
            // after the offer, so an arrival cannot slip between the two.
            if (queue.isEmpty()) arrival.wait(timeoutMs);
        }
    }

    /** What {@link #pollBatch(int, long, TimeUnit, long)} hands back: the
     *  samples and how long the call spent lingering for the batch to fill
     *  (zero when it returned on a full batch or with linger disabled). */
    public record Batch(List<MappedSample> samples, long lingerNanos) {
        static final Batch EMPTY = new Batch(List.of(), 0L);
    }

    /**
     * Wait up to {@code timeout} for a head sample, drain what is queued
     * behind it, then, once a head sample has arrived, wait up to
     * {@code lingerMs} for the batch to reach
     * {@code maxBatch}, draining at every arrival and returning early the
     * moment it is full. {@code lingerMs == 0} sends on the first arrival
     * as before. The linger wait is reported separately from the head wait
     * because it is time with something to send, by choice (#162).
     */
    public Batch pollBatch(int maxBatch, long timeout, TimeUnit unit, long lingerMs)
            throws InterruptedException {
        if (maxBatch < 1) throw new IllegalArgumentException("maxBatch must be >= 1");
        MappedSample head = queue.poll(timeout, unit);
        if (head == null) return Batch.EMPTY;

        List<MappedSample> batch = new java.util.ArrayList<>(maxBatch);
        batch.add(head);
        queue.drainTo(batch, maxBatch - 1);

        long lingerNanos = 0L;
        if (lingerMs > 0 && batch.size() < maxBatch) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(lingerMs);
            long started = System.nanoTime();
            try {
                while (batch.size() < maxBatch) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) break;
                    MappedSample next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                    if (next == null) break;          // linger elapsed
                    batch.add(next);
                    queue.drainTo(batch, maxBatch - batch.size());
                }
            } catch (InterruptedException e) {
                // Shutdown interrupt mid-linger: the samples already taken
                // off the queue must still be sent, so return the partial
                // batch and leave the flag set for the caller's next poll.
                Thread.currentThread().interrupt();
            }
            lingerNanos = System.nanoTime() - started;
        }
        samplesDequeued.addAndGet(batch.size());
        return new Batch(batch, lingerNanos);
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
    public int remainingCapacity() { return queue.remainingCapacity(); }

    public long getSamplesEnqueued()         { return samplesEnqueued.get(); }
    public long getSamplesDequeued()         { return samplesDequeued.get(); }
}
