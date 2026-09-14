/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tracks the "last offset confirmed shipped" marker for a WAL
 * directory. Persisted as {@code checkpoint.json} in the WAL root; the
 * Flusher calls {@link #advance(long)} after each successful HTTP 2xx
 * (and each 4xx, which advances past a permanently-rejected batch —
 * matching v0.4 semantics).
 *
 * <p>Writes are atomic: the new content is written to
 * {@code checkpoint.json.tmp}, fsynced, then renamed over
 * {@code checkpoint.json}. Rename is atomic on POSIX file systems. A
 * crash mid-rename leaves the previous valid checkpoint intact;
 * recovery may replay a small tail of already-shipped samples (safe —
 * the endpoint accepts duplicates within a time window per the Remote
 * Write v1 spec).
 *
 * <p>The file format is hand-rolled JSON — same reasoning as .idx: two
 * fields, no schema evolution pressure, avoids pulling a JSON library
 * for a record that would fit on a business card.
 */
public final class Checkpoint {

    public static final String FILE_NAME = "checkpoint.json";
    private static final String TMP_SUFFIX = ".tmp";

    private final Path file;
    private final Path tmp;

    private long lastSentOffset;
    private Instant lastSentAt;

    private Checkpoint(Path file, long offset, Instant at) {
        this.file = file;
        this.tmp = file.resolveSibling(FILE_NAME + TMP_SUFFIX);
        this.lastSentOffset = offset;
        this.lastSentAt = at;
    }

    /**
     * Load the checkpoint from {@code dir/checkpoint.json} or create a
     * fresh one at offset 0 if the file does not exist. Throws if the
     * file exists but is unreadable — bad state is better surfaced loud
     * than papered over with "offset 0 fallback." Operators resetting
     * the WAL must delete the directory deliberately.
     */
    public static Checkpoint loadOrCreate(Path dir) throws IOException {
        Path file = dir.resolve(FILE_NAME);
        if (!Files.exists(file)) {
            return new Checkpoint(file, 0L, Instant.EPOCH);
        }
        String content = Files.readString(file);
        long offset = extractLong(content, "last_sent_offset");
        String tsString = extractString(content, "last_sent_at");
        Instant at;
        try {
            at = Instant.parse(tsString);
        } catch (RuntimeException e) {
            throw new IOException(
                "checkpoint.json has unparseable last_sent_at='" + tsString + "' — "
                + "file may be corrupt; remove the WAL directory to reset", e);
        }
        return new Checkpoint(file, offset, at);
    }

    /**
     * Advance the checkpoint to a new offset. Monotonic — rejects
     * backward moves as a programming error. Persists atomically
     * (write-tmp, fsync, rename) before returning. Updates
     * {@link #lastSentAt} to "now."
     */
    public synchronized void advance(long newOffset) throws IOException {
        if (newOffset < lastSentOffset) {
            throw new IllegalArgumentException(
                "checkpoint cannot move backwards: current=" + lastSentOffset
                + ", requested=" + newOffset);
        }
        if (newOffset == lastSentOffset) {
            return; // no-op; avoid the fsync churn
        }
        Instant now = Instant.now();
        String json = String.format(
                "{\"last_sent_offset\":%d,\"last_sent_at\":\"%s\"}%n",
                newOffset, now);
        Files.writeString(tmp, json);
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        // Fsync the parent directory so the rename's new dirent is
        // durable — without this, a crash after the rename can lose the
        // new name on ext4/XFS, leaving the old (or no) checkpoint.
        FsUtils.fsyncDirectory(file.getParent());
        lastSentOffset = newOffset;
        lastSentAt = now;
    }

    /**
     * Advance to {@code offset} if the checkpoint is still behind it, in one
     * step under the checkpoint's own lock, so a caller that would otherwise
     * read the offset and then advance cannot race another advancer in
     * between (an acknowledgement on the flusher thread against an eviction
     * on the append thread). The offset moved from is returned under the
     * same lock, so a caller's byte delta is exact.
     *
     * @return the offset the checkpoint moved from, or -1 when it was
     *         already at or past {@code offset} and did not move
     */
    public synchronized long advancePast(long offset) throws IOException {
        if (offset <= lastSentOffset) return -1L;
        long previous = lastSentOffset;
        advance(offset);
        return previous;
    }

    public synchronized long lastSentOffset() { return lastSentOffset; }
    public synchronized Instant lastSentAt() { return lastSentAt; }

    /**
     * Delete every sealed segment in {@code dir} whose endOffset
     * (startOffset + on-disk size) is less than or equal to
     * {@code checkpoint.lastSentOffset}. The newest segment (highest
     * startOffset) is never deleted — it is always either the writer's
     * active segment or the segment where the reader will resume.
     *
     * <p>Safe against races with the writer: deletions target segments
     * strictly below the newest one, so the writer's active segment is
     * never a candidate.
     *
     * @return the cumulative bytes reclaimed
     */
    public static long gcSegments(Path dir, long checkpointOffset) throws IOException {
        return gcSegments(dir, checkpointOffset, bytes -> { });
    }

    /**
     * {@link #gcSegments(Path, long)}, telling {@code onDeleted} the bytes of
     * each segment as soon as it is gone. A writer that tracks its footprint
     * learns of the room at once, rather than after the whole pass and its
     * directory fsync, during which a full bucket would still refuse.
     */
    public static long gcSegments(Path dir, long checkpointOffset,
                                  java.util.function.LongConsumer onDeleted) throws IOException {
        List<Long> starts = listSegmentStartOffsets(dir);
        if (starts.size() <= 1) return 0L; // nothing to GC; keep the newest
        long bytesReclaimed = 0L;
        long newest = starts.get(starts.size() - 1);
        for (int i = 0; i < starts.size() - 1; i++) {
            long s = starts.get(i);
            if (s >= newest) continue; // belt-and-braces
            Path segPath = WalSegment.segPathFor(dir, s);
            Path idxPath = WalSegment.idxPathFor(dir, s);
            long size = Files.exists(segPath) ? Files.size(segPath) : 0L;
            long endOffset = s + size;
            if (endOffset <= checkpointOffset) {
                // Counted only if this delete removed it: a drop-oldest
                // eviction may reach the same segment and counts its own.
                if (Files.deleteIfExists(segPath)) {
                    bytesReclaimed += size;
                    onDeleted.accept(size);
                }
                Files.deleteIfExists(idxPath);
            }
        }
        if (bytesReclaimed > 0) {
            // Make the deletions durable — otherwise a crash can resurrect
            // the "deleted" segment on next mount.
            FsUtils.fsyncDirectory(dir);
        }
        return bytesReclaimed;
    }

    // --- minimal JSON extraction -------------------------------------------
    //
    // The file is always produced by this class, so we don't need a
    // general-purpose JSON parser. These helpers assume the exact
    // format written by advance(): no whitespace, exactly those two
    // fields, in that order. If the file is anything else, IOException.

    private static long extractLong(String content, String key) throws IOException {
        int i = content.indexOf("\"" + key + "\":");
        if (i < 0) throw new IOException("checkpoint.json missing field " + key);
        i += key.length() + 3; // "key":
        int end = i;
        while (end < content.length()
                && (Character.isDigit(content.charAt(end)) || content.charAt(end) == '-')) {
            end++;
        }
        try {
            return Long.parseLong(content.substring(i, end));
        } catch (NumberFormatException e) {
            throw new IOException("checkpoint.json field " + key + " is not an integer", e);
        }
    }

    private static String extractString(String content, String key) throws IOException {
        int i = content.indexOf("\"" + key + "\":\"");
        if (i < 0) throw new IOException("checkpoint.json missing field " + key);
        i += key.length() + 4; // "key":"
        int end = content.indexOf('"', i);
        if (end < 0) throw new IOException("checkpoint.json field " + key + " is not closed");
        return content.substring(i, end);
    }

    private static List<Long> listSegmentStartOffsets(Path dir) throws IOException {
        List<Long> out = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + WalSegment.SEG_EXT)) {
            for (Path p : stream) {
                long s = WalSegment.parseStartOffset(p);
                if (s >= 0) out.add(s);
            }
        }
        Collections.sort(out);
        return out;
    }
}
