/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.queue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Objects;
import java.util.function.Function;

import org.opennms.plugins.prometheus.remotewriter.http.RemoteWriteHttpClient;
import org.opennms.plugins.prometheus.remotewriter.metrics.PluginMetrics;
import org.opennms.plugins.prometheus.remotewriter.wire.MappedSample;
import org.opennms.plugins.prometheus.remotewriter.wire.RemoteWriteRequestBuilder.BuildResult;

/**
 * The sharded queue-mode write pipeline: N independent
 * ({@link SampleQueue}, {@link Flusher}) pairs, each owning a disjoint set
 * of series. Samples route to a shard by a hash of their full label map
 * (see {@link #shardFor}), every shard keeps at most one request in flight
 * (retries included), so the Remote Write in-order-per-series rule holds
 * structurally while shards flush in parallel — the spec permits parallel
 * requests over disjoint series sets.
 *
 * <p>With {@code shardCount == 1} this degenerates to exactly the classic
 * single-queue/single-flusher pipeline (router always returns shard 0).
 *
 * <p>Deliberately N independent queues rather than one shared queue with N
 * consumers: competing consumers could have two in-flight batches carrying
 * the same series, which breaks per-series ordering. Spilling to a sibling
 * shard would break it the same way, which is why a shard that runs out of
 * memory spills to its own {@link OverflowBucket} rather than anywhere else.
 *
 * <p><b>The tiering rule.</b> A queue spills because it is full, so at that
 * moment memory holds the <em>older</em> samples. Appending only the refused
 * one would put it on disk ahead of them, and disk drains first. So the
 * transition into spilling moves the shard's whole memory backlog into its
 * bucket, in order, before the refused sample; from then until the bucket is
 * empty every sample for that shard goes to the bucket. A shard is therefore
 * either memory-only or disk-only, never both, which is also what keeps a
 * batch from ever mixing tiers.
 *
 * <p>{@link #accept} is serialised per shard for that reason: without it one
 * producer can slip a sample into memory just after another drained it,
 * leaving an old sample stranded behind a non-empty bucket. The lock is not
 * new contention in kind — {@link java.util.concurrent.ArrayBlockingQueue}
 * takes one per offer regardless.
 */
public final class Shards implements java.io.Closeable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Shards.class);

    /** How a sample was taken, or that it was not. */
    public enum Acceptance {
        /** Enqueued in the shard's memory queue. */
        MEMORY,
        /** Appended to the shard's disk bucket. */
        OVERFLOW,
        /** Neither tier had room; the caller counts it and throws. */
        REFUSED
    }

    private final SampleQueue[] queues;
    private final Flusher[] flushers;
    /** Null when no disk tier is configured (overflow.max-size-bytes=0). */
    private final OverflowBucket[] buckets;
    private final java.util.concurrent.locks.ReentrantLock[] acceptLocks;
    /** Per shard, the memory batches drawn but not yet settled, oldest first.
     *  Guarded by that shard's accept lock. Null when there is no disk tier —
     *  nothing can be rescued, so nothing needs tracking. */
    private final java.util.ArrayDeque<InFlight>[] outstanding;

    /**
     * A memory batch that has left the queue but not yet reached the backend.
     *
     * <p>The flusher holds the token and hands it back, so whether a batch
     * already reached disk is <em>recorded</em> rather than inferred from its
     * absence — inferring it silently discarded any batch that was never
     * registered, such as the residual drain at shutdown.
     */
    public static final class InFlight {
        /** Mutable so a linger can keep topping it up under the shard's accept
         *  lock — a transition then always sees the current contents. */
        private final List<MappedSample> batch;
        /** Set when a spill transition wrote this batch to disk. Guarded by
         *  the owning shard's accept lock. */
        private boolean carried;

        private InFlight(List<MappedSample> batch) { this.batch = batch; }

        /** The samples, for the builder. Only read once topping up has
         *  stopped. */
        public List<MappedSample> batch() { return batch; }
        public int size() { return batch.size(); }
    }
    private final AtomicLong samplesDroppedQueueFull    = new AtomicLong();
    private final AtomicLong samplesSpilled             = new AtomicLong();
    private final AtomicLong samplesDroppedOverflowFull = new AtomicLong();
    private final AtomicLong samplesEvictedOverflow     = new AtomicLong();
    /** Maximum total depth seen right after a successful offer: the peak
     *  the writer threads actually reached, not a later sample of it. */
    private final AtomicLong depthHighWater = new AtomicLong();
    private final PluginMetrics metrics;
    private final OverflowBucket.DrainPolicy drain;

    /** No disk tier — the pre-0.8.0 pipeline, where a full shard refuses. */
    public Shards(int shardCount,
                  int totalQueueCapacity,
                  RemoteWriteHttpClient httpClient,
                  int batchSize,
                  long flushIntervalMs,
                  long lingerMs,
                  PluginMetrics metrics,
                  Function<Collection<MappedSample>, BuildResult> builder) {
        this(shardCount, totalQueueCapacity, httpClient, batchSize, flushIntervalMs, lingerMs,
                metrics, builder, null);
    }

    public Shards(int shardCount,
                  int totalQueueCapacity,
                  RemoteWriteHttpClient httpClient,
                  int batchSize,
                  long flushIntervalMs,
                  long lingerMs,
                  PluginMetrics metrics,
                  Function<Collection<MappedSample>, BuildResult> builder,
                  java.util.function.IntFunction<OverflowBucket> bucketFactory) {
        this(shardCount, totalQueueCapacity, httpClient, batchSize, flushIntervalMs, lingerMs,
                metrics, builder, bucketFactory, OverflowBucket.DrainPolicy.ORDERED);
    }

    /**
     * @param bucketFactory opens the bucket for a shard index, or null for no
     *                      disk tier. Taking a factory rather than a directory
     *                      keeps the filesystem out of this class and lets a
     *                      test hand in buckets over a temp dir.
     */
    public Shards(int shardCount,
                  int totalQueueCapacity,
                  RemoteWriteHttpClient httpClient,
                  int batchSize,
                  long flushIntervalMs,
                  long lingerMs,
                  PluginMetrics metrics,
                  Function<Collection<MappedSample>, BuildResult> builder,
                  java.util.function.IntFunction<OverflowBucket> bucketFactory,
                  OverflowBucket.DrainPolicy drain) {
        if (shardCount < 1) throw new IllegalArgumentException("shardCount must be >= 1");
        this.drain = drain == null ? OverflowBucket.DrainPolicy.ORDERED : drain;
        Objects.requireNonNull(httpClient);
        Objects.requireNonNull(metrics);
        Objects.requireNonNull(builder);
        queues      = new SampleQueue[shardCount];
        flushers    = new Flusher[shardCount];
        buckets     = bucketFactory == null ? null : new OverflowBucket[shardCount];
        acceptLocks = new java.util.concurrent.locks.ReentrantLock[shardCount];
        @SuppressWarnings("unchecked")
        java.util.ArrayDeque<InFlight>[] out =
                bucketFactory == null ? null : new java.util.ArrayDeque[shardCount];
        outstanding = out;
        // Split the configured total capacity across shards without losing
        // slots to integer division (first `remainder` shards get one extra).
        // Config validation guarantees totalQueueCapacity / shardCount >=
        // batchSize, so every shard can fill a batch.
        this.metrics = metrics;
        int base = totalQueueCapacity / shardCount;
        int remainder = totalQueueCapacity % shardCount;
        try {
        for (int i = 0; i < shardCount; i++) {
            queues[i] = new SampleQueue(base + (i < remainder ? 1 : 0));
            acceptLocks[i] = new java.util.concurrent.locks.ReentrantLock();
            if (outstanding != null) outstanding[i] = new java.util.ArrayDeque<>(2);
            if (buckets != null) buckets[i] = bucketFactory.apply(i);
            String threadName = shardCount == 1
                    ? "prometheus-remote-writer-flusher"
                    : "prometheus-remote-writer-flusher-" + i;
            final int shard = i;
            flushers[i] = new Flusher(queues[i], buckets == null ? null : buckets[i],
                    buckets == null ? null : new Flusher.MemoryTier() {
                        @Override public InFlight poll(int maxBatch) {
                            return pollMemoryBatch(shard, maxBatch);
                        }
                        @Override public boolean topUp(InFlight t, int maxBatch) {
                            return topUpMemoryBatch(shard, t, maxBatch);
                        }
                        @Override public boolean wasCarried(InFlight t) {
                            return Shards.this.wasCarried(shard, t);
                        }
                        @Override public InFlight inFlight(List<MappedSample> b) {
                            return memoryBatchInFlight(shard, b);
                        }
                        @Override public void settled(InFlight t) { memoryBatchSettled(shard, t); }
                        @Override public int returnToOverflow(InFlight t, List<MappedSample> b) {
                            return Shards.this.returnToOverflow(shard, t, b);
                        }
                    },
                    httpClient, batchSize, flushIntervalMs, lingerMs,
                    metrics, builder, threadName, this.drain);
        }
        } catch (RuntimeException e) {
            // A bucket that failed to open leaves the ones before it holding
            // segment file handles, and the half-built Shards is unreachable
            // for anyone to close. Blueprint retries start() on every config
            // reload, so leaking here leaks per reload.
            if (buckets != null) {
                for (OverflowBucket b : buckets) {
                    if (b == null) continue;
                    try { b.close(); } catch (RuntimeException ignored) { /* best effort */ }
                }
            }
            throw e;
        }
    }

    /**
     * Series → shard assignment. Keyed on the sample's full label map: a
     * Prometheus series IS its complete label set, and {@link Map#hashCode()}
     * is content-based and iteration-order-independent by the {@code Map}
     * contract — so equal label sets hash identically regardless of how the
     * map was built. This is the same identity the wire builders group
     * {@code TimeSeries} entries by — they key on
     * {@link org.opennms.plugins.prometheus.remotewriter.wire.SeriesKey},
     * which is equal exactly when the label maps are — so "one series, one
     * shard, one in-flight request" stays airtight.
     */
    public static int shardFor(Map<String, String> labels, int shardCount) {
        if (shardCount == 1) return 0;
        return Math.floorMod(labels.hashCode(), shardCount);
    }

    /**
     * Route {@code sample} to its shard and place it in whichever tier that
     * shard is using. See the tiering rule on the class doc: bucket if the
     * bucket is non-empty, memory otherwise, and on a memory refusal the
     * whole backlog moves to the bucket ahead of this sample.
     */
    public Acceptance accept(MappedSample sample) {
        int shard = shardOf(sample);
        if (buckets == null) {
            // No disk tier: the pre-0.8.0 contract, a full queue refuses.
            if (!queues[shard].tryEnqueue(sample)) return Acceptance.REFUSED;
            depthHighWater.accumulateAndGet(totalDepth(), Math::max);
            return Acceptance.MEMORY;
        }
        acceptLocks[shard].lock();
        try {
            OverflowBucket bucket = buckets[shard];
            // Under `concurrent` the memory tier stays in use whatever the
            // bucket holds: fresh samples are not made to queue behind the
            // backlog, which is the whole point of the policy. Under
            // `ordered` a non-empty bucket means the shard is yielding.
            boolean mayUseMemory = concurrent() || bucket.isEmpty();
            if (mayUseMemory) {
                if (queues[shard].tryEnqueue(sample)) {
                    depthHighWater.accumulateAndGet(totalDepth(), Math::max);
                    return Acceptance.MEMORY;
                }
                // Memory just filled. Under `ordered` everything in it was
                // offered before this sample, so it has to reach disk first or
                // this sample would overtake it. Under `concurrent` the
                // ordering that buys has already been given up, and moving the
                // backlog would be pure cost.
                if (!concurrent()) spillBacklog(shard, "memory queue full");
            }
            return appendToBucket(shard, sample);
        } finally {
            acceptLocks[shard].unlock();
        }
    }

    /**
     * Move a shard's whole memory queue into its bucket, in queue order.
     * Called under the shard's accept lock, so nothing can be enqueued behind
     * the drain; the queue is empty when this returns unless the bucket
     * refused, in which case the refused remainder is counted and dropped —
     * they cannot go back into memory ahead of what already reached disk.
     */
    private void spillBacklog(int shard, String reason) {
        // Outstanding batches first: they left the queue before anything still
        // in it, so they are the oldest samples the shard holds. Leaving them
        // out would make them unrescuable — by the time their send fails the
        // bucket is non-empty and they could only land behind newer data.
        int carried = 0;
        for (InFlight inFlight : outstanding[shard]) {
            if (inFlight.carried) continue;   // already on disk; do not write it twice
            int taken = appendAll(shard, inFlight.batch);
            carried += taken;
            int lost = inFlight.batch.size() - taken;
            if (lost > 0) {
                // The bucket refused partway. The tail is gone, and it has to
                // be counted here — marking the batch carried without counting
                // would let the rescue path report it fully rescued.
                samplesDroppedOverflowFull.addAndGet(lost);
                LOG.error("shard {}: overflow bucket refused {} sample(s) of an in-flight batch "
                        + "during spill; they are lost", shard, lost);
            }
            // Carried either way: the batch has been dealt with, so the rescue
            // path must not count it a second time.
            inFlight.carried = true;
        }
        List<MappedSample> backlog = queues[shard].drain(queues[shard].capacity());
        if (backlog.isEmpty() && carried == 0) return;
        LOG.warn("shard {} is spilling to disk ({}): moving {} queued sample(s) and {} in flight "
                + "to the overflow bucket. New samples go to disk until it drains.",
                shard, reason, backlog.size(), carried);
        for (int i = 0; i < backlog.size(); i++) {
            if (appendToBucket(shard, backlog.get(i)) == Acceptance.REFUSED) {
                int lost = backlog.size() - i;
                samplesDroppedOverflowFull.addAndGet(lost);
                LOG.error("shard {}: overflow bucket refused {} sample(s) of the memory backlog "
                        + "during spill; they are lost", shard, lost);
                return;
            }
        }
    }

    /**
     * Record that a batch has been drawn from a shard's memory queue and is on
     * its way to the backend. Called by the shard's builder thread.
     *
     * <p>An outstanding batch is out of the queue but not yet delivered, and it
     * is <em>older</em> than everything still in the queue. If the queue fills
     * while it is out — which is the normal way a shard enters spilling, since
     * the backend being slow or down is what causes both — the transition has
     * to carry it to disk ahead of the backlog, or those samples end up
     * unrescuable: by the time the send fails, the bucket is non-empty and
     * anything appended would land behind newer data.
     *
     * <p>At most two are outstanding per shard: one in the sender and one in
     * the depth-one handoff.
     */
    public InFlight memoryBatchInFlight(int shard, List<MappedSample> batch) {
        if (buckets == null || batch.isEmpty()) return null;
        acceptLocks[shard].lock();
        try {
            InFlight token = new InFlight(batch);
            outstanding[shard].addLast(token);
            return token;
        } finally {
            acceptLocks[shard].unlock();
        }
    }

    /**
     * Take up to {@code maxBatch} samples off a shard's memory queue and
     * register them as in flight, as one step.
     *
     * <p>Doing the two separately leaves a window: a batch drained while the
     * bucket is empty, with a spill transition landing before it is
     * registered, belongs on disk ahead of the backlog but arrives too late to
     * be put there — it is older than what the transition just wrote, so it
     * can only be dropped. In the degenerate one-slot configuration that
     * window is hit constantly, which is why the drain happens under the same
     * lock the transition takes.
     *
     * @return the registered batch, or null when the queue was empty
     */
    public InFlight pollMemoryBatch(int shard, int maxBatch) {
        if (buckets == null) throw new IllegalStateException("no disk tier on shard " + shard);
        acceptLocks[shard].lock();
        try {
            List<MappedSample> batch = queues[shard].drain(maxBatch);
            if (batch.isEmpty()) return null;
            InFlight token = new InFlight(batch);
            outstanding[shard].addLast(token);
            return token;
        } finally {
            acceptLocks[shard].unlock();
        }
    }

    /**
     * Add whatever has arrived since to a registered batch, up to
     * {@code maxBatch} in total. This is how {@code batch.linger-ms} fills a
     * batch on a shard with a disk tier: the batch grows in place under the
     * lock, so a transition always carries its current contents.
     *
     * @return false once a transition has taken the batch to disk, at which
     *         point the caller must stop lingering and drop it on the floor —
     *         the disk copy is what ships
     */
    public boolean topUpMemoryBatch(int shard, InFlight token, int maxBatch) {
        acceptLocks[shard].lock();
        try {
            if (token.carried) return false;
            int room = maxBatch - token.batch.size();
            if (room > 0) token.batch.addAll(queues[shard].drain(room));
            return true;
        } finally {
            acceptLocks[shard].unlock();
        }
    }

    /** Whether a transition has already written this batch to disk. */
    public boolean wasCarried(int shard, InFlight token) {
        acceptLocks[shard].lock();
        try {
            return token.carried;
        } finally {
            acceptLocks[shard].unlock();
        }
    }

    /** Forget an outstanding batch: the backend has finished with it, one way
     *  or another. */
    public void memoryBatchSettled(int shard, InFlight token) {
        if (buckets == null || token == null) return;
        acceptLocks[shard].lock();
        try {
            outstanding[shard].remove(token);
        } finally {
            acceptLocks[shard].unlock();
        }
    }

    /**
     * Hand a memory batch the backend would not take back to the disk tier, so
     * a sample is durable from the first failure rather than only once the
     * queue happens to fill.
     *
     * <p>Without this a memory batch that exhausts its retries is dropped —
     * there is nowhere to put it back — which would leave up to
     * {@code queue.capacity} samples in a tier that still loses data while the
     * bucket sits empty. That is not "zero loss while the disk tier has room".
     *
     * <p>Two outcomes both count as rescued. Either the batch is still
     * outstanding here, in which case the bucket is empty and it goes to the
     * front of it with the memory backlog behind — the accept-path transition,
     * driven from the other end. Or a transition already carried it to disk
     * while it was in flight, in which case there is nothing to do and it is
     * already queued for retry.
     *
     * <p>The second case can deliver a sample twice: the batch reached disk,
     * and then the very send that triggered the spill turned out to succeed.
     * Remote Write is idempotent for an identical series, timestamp and value,
     * so a duplicate costs a little bandwidth. Losing the batch instead would
     * cost data, at exactly the moment durability is the point.
     *
     * @return how many samples of {@code batch} are accounted for on disk
     */
    public int returnToOverflow(int shard, InFlight token, List<MappedSample> batch) {
        if (buckets == null || batch.isEmpty()) return 0;
        acceptLocks[shard].lock();
        try {
            if (token != null && token.carried) {
                // A transition already wrote it while the request was in
                // flight; it is on disk and will be retried from there.
                return batch.size();
            }
            if (!buckets[shard].isEmpty() && !concurrent()) {
                // The batch is older than what is already on disk, so it
                // cannot go behind it without reordering a series. Under
                // `concurrent` that is not a violation, so the rescue applies
                // there whatever the bucket holds.
                //
                // Unreachable for a batch that came through
                // pollMemoryBatch: draining and registering under this lock
                // means any transition that made the bucket non-empty carried
                // this batch too, and the carried check above caught it. Kept
                // as an honest fallback rather than an assertion.
                return 0;
            }
            int taken = appendAll(shard, batch);
            if (taken > 0) {
                // Mark and drop the token before spilling the backlog, or the
                // transition would find it still outstanding and write the
                // same samples to disk a second time.
                if (token != null) {
                    token.carried = true;
                    outstanding[shard].remove(token);
                }
                LOG.warn("shard {}: the backend would not take {} sample(s); returned them to the "
                        + "overflow bucket to retry instead of dropping them", shard, taken);
                spillBacklog(shard, "a batch came back from the backend");
            }
            return taken;
        } finally {
            acceptLocks[shard].unlock();
        }
    }

    /** Append every sample of {@code batch} in order; stops at the first
     *  refusal and returns how many made it. Caller holds the accept lock. */
    private int appendAll(int shard, List<MappedSample> batch) {
        int taken = 0;
        for (MappedSample s : batch) {
            if (appendToBucket(shard, s) == Acceptance.REFUSED) break;
            taken++;
        }
        return taken;
    }

    /** Append to a shard's bucket, booking spill and eviction counts. */
    private Acceptance appendToBucket(int shard, MappedSample sample) {
        try {
            OverflowBucket.AppendResult r = buckets[shard].append(sample);
            if (r.evictedSamples() > 0) samplesEvictedOverflow.addAndGet(r.evictedSamples());
            if (!r.accepted()) return Acceptance.REFUSED;
            metrics.walBytesWritten(r.bytesWritten());
            samplesSpilled.incrementAndGet();
            return Acceptance.OVERFLOW;
        } catch (java.io.IOException e) {
            // A failing filesystem is not a refusal we can paper over; the
            // caller turns it into a StorageException so OpenNMS backs off.
            throw new java.io.UncheckedIOException(
                    "overflow bucket append failed on shard " + shard, e);
        }
    }

    /**
     * Whether the shard routing {@code sample} could take one more sample in
     * either tier. Used by the all-or-nothing store policy, which has to know
     * before it places anything.
     */
    public boolean hasRoomFor(int shard, int samples) {
        if (buckets == null) return queues[shard].remainingCapacity() >= samples;
        // A shard that is spilling, or short of memory, depends on its bucket.
        // The bucket's bound is bytes rather than slots, so this cannot be
        // exact — but "is the bucket under its cap" is the question that
        // decides refusal, and answering it beats the previous unconditional
        // yes, which let all-or-nothing accept part of a call and then throw.
        if (!buckets[shard].isEmpty()) return buckets[shard].hasRoom();
        return queues[shard].remainingCapacity() >= samples || buckets[shard].hasRoom();
    }

    public long depthHighWater() { return depthHighWater.get(); }

    public int shardOf(MappedSample sample) {
        return shardFor(sample.labels(), queues.length);
    }

    public int remainingCapacity(int shard) { return queues[shard].remainingCapacity(); }

    /** Count {@code n} samples refused because a shard's queue was full.
     *  One counter for the whole pipeline because only the total is
     *  exported; the caller knows the refusing shard if per-shard drop
     *  gauges are ever wanted. */
    public void countDroppedQueueFull(long n) {
        if (n > 0) samplesDroppedQueueFull.addAndGet(n);
    }

    public void start() {
        for (Flusher f : flushers) {
            f.start();
        }
    }

    /**
     * Stop all shards under ONE shared grace budget: signal every flusher
     * first (they begin their residual drains concurrently), then await each
     * with the remaining time. Sequential stop(grace) per shard would bound
     * shutdown at {@code shardCount × graceMs}.
     */
    public void stop(long graceMs) {
        for (Flusher f : flushers) {
            f.signalStop();
        }
        long deadline = System.nanoTime() + graceMs * 1_000_000L;
        for (Flusher f : flushers) {
            long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
            f.awaitStop(Math.max(1, remainingMs));
        }
    }

    public int shardCount() { return queues.length; }

    public int depth(int shard) { return queues[shard].depth(); }

    public int totalDepth() {
        int sum = 0;
        for (SampleQueue q : queues) sum += q.depth();
        return sum;
    }

    public long totalSamplesDroppedQueueFull() { return samplesDroppedQueueFull.get(); }

    // --- overflow tier ------------------------------------------------------

    /** True when a disk tier is configured. */
    public boolean overflowEnabled() { return buckets != null; }

    public long totalSamplesSpilled()             { return samplesSpilled.get(); }
    public long totalSamplesDroppedOverflowFull() { return samplesDroppedOverflowFull.get(); }
    public long totalSamplesEvictedOverflow()     { return samplesEvictedOverflow.get(); }

    /** Count {@code n} samples refused because both tiers were full. */
    public void countDroppedOverflowFull(long n) {
        if (n > 0) samplesDroppedOverflowFull.addAndGet(n);
    }

    public int overflowPending(int shard) {
        return buckets == null ? 0 : buckets[shard].pendingSamples();
    }

    /** True when this shard's bucket holds unacknowledged samples: the
     *  {@code RECOVERING} half of the per-shard state, {@code NORMAL} being
     *  the other. Under {@code ordered} a recovering shard's memory queue is
     *  empty by construction; under {@code concurrent} both tiers can hold
     *  data at once. */
    public boolean isRecovering(int shard) {
        return buckets != null && !buckets[shard].isEmpty();
    }

    /** How many shards are draining a backlog right now. */
    public int recoveringShards() {
        if (buckets == null) return 0;
        int n = 0;
        for (int i = 0; i < buckets.length; i++) {
            if (!buckets[i].isEmpty()) n++;
        }
        return n;
    }

    /**
     * Milliseconds since the {@code store()} call of the oldest sample on disk
     * the backend has not taken, across shards; 0 when nothing is pending.
     * This is the recovery number in the unit the latency budget uses — depth
     * says how much is waiting, this says how far behind the tier is.
     */
    public long oldestPendingAgeMs() {
        if (buckets == null) return 0L;
        long oldest = 0L;
        for (OverflowBucket b : buckets) {
            oldest = Math.max(oldest, b.oldestPendingAgeMs());
        }
        return oldest;
    }

    boolean concurrent() { return drain == OverflowBucket.DrainPolicy.CONCURRENT; }

    public OverflowBucket.DrainPolicy drainPolicy() { return drain; }

    public int totalOverflowPending() {
        if (buckets == null) return 0;
        int sum = 0;
        for (OverflowBucket b : buckets) sum += b.pendingSamples();
        return sum;
    }

    public long totalOverflowBytes() {
        if (buckets == null) return 0L;
        long sum = 0;
        for (OverflowBucket b : buckets) sum += b.bytes();
        return sum;
    }

    /** Package-private for tests. */
    OverflowBucket bucketForTesting(int shard) { return buckets == null ? null : buckets[shard]; }

    @Override
    public void close() {
        if (buckets == null) return;
        for (OverflowBucket b : buckets) {
            try {
                b.close();
            } catch (RuntimeException e) {
                LOG.warn("closing overflow bucket {}: {}", b.dir(), e.getMessage(), e);
            }
        }
    }

    /**
     * Shard skew as a percentage: {@code max(depth) * 100 / mean(depth)}.
     * 100 = perfectly balanced; N×100 = everything on one shard. Returns
     * 100 when all queues are empty (no skew to report). Operators watch
     * this to spot hot series distributions that degrade a shard toward
     * single-flusher behavior.
     */
    public long skewPct() {
        int n = queues.length;
        long total = 0;
        long max = 0;
        for (SampleQueue q : queues) {
            int d = q.depth();
            total += d;
            if (d > max) max = d;
        }
        if (total == 0) return 100;
        // mean = total / n; skew = max / mean = max * n / total
        return max * 100L * n / total;
    }

    /** Package-private for tests. */
    List<SampleQueue> queuesForTesting() { return List.of(queues); }
}
