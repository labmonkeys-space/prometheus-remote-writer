/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy;

/**
 * Startup-time recovery for a WAL directory. Enumerates segments,
 * loads the checkpoint (or creates a fresh one at offset 0), opens the
 * newest segment for append — running torn-tail truncation if its last
 * frame was written incompletely — and reports the pending-sample
 * count so the metrics layer can emit {@code wal_replay_samples_total}
 * at startup.
 *
 * <p>Must-succeed semantics: any failure (unreadable directory,
 * corrupt checkpoint, unreadable segment, recovery error) is surfaced
 * as an IOException. The storage layer refuses to start on IOException
 * rather than silently falling back to offset 0 (which would replay
 * the entire WAL on every startup after a corruption).
 *
 * <p>Operators reset by removing the WAL directory — documented in
 * CHANGELOG / README. Never auto-delete here.
 */
public final class WalRecovery {

    private WalRecovery() { /* utility */ }

    /**
     * Scan {@code dir} and return the state a {@link WalWriter} needs
     * to resume. Creates the directory if missing. Creates segment 0
     * if no segments exist. Torn-tail-truncates the newest segment if
     * its last frame is incomplete.
     *
     * @param dir         the WAL directory (created if missing)
     * @param fsync       fsync policy for the returned active segment
     * @param maxPayload  max frame payload size (for sanity-bounded
     *                    frame decode during recovery)
     */
    public static RecoveredWal recover(Path dir, FsyncPolicy fsync, int maxPayload)
            throws IOException {
        ensureDirectoryWritable(dir);
        // Sweep stale .tmp files left behind by a crash mid-rename.
        // Typical offender is checkpoint.json.tmp (see Checkpoint.advance).
        // Left unchecked, they linger forever and can complicate
        // operator triage.
        sweepStaleTempFiles(dir);
        Checkpoint checkpoint = Checkpoint.loadOrCreate(dir);

        List<Long> starts = listSegmentStartOffsets(dir);
        if (starts.isEmpty()) {
            // Fresh WAL — create segment 0.
            WalSegment seg = WalSegment.create(dir, 0, fsync, maxPayload);
            return new RecoveredWal(seg, checkpoint, 0, 0);
        }

        // Open the newest segment for append and recover it (torn-tail
        // truncation). The newest segment is the only one the writer
        // could have been actively appending to at crash time.
        long newestStart = starts.get(starts.size() - 1);
        Path newestPath = WalSegment.segPathFor(dir, newestStart);
        WalSegment activeSegment = WalSegment.openForAppend(newestPath, newestStart,
                fsync, maxPayload, 0);
        // Any error between here and the return must close activeSegment
        // so a failed startup recovery does not leak file descriptors.
        try {
            activeSegment.recover();
            // Persist the updated .idx (recovery may have flipped status to
            // TORN or updated sample_count).
            activeSegment.writeIndex();

            // Pending-sample count: sum of sample_count across every segment
            // whose endOffset is past the checkpoint offset. Approximate for
            // segments that span the checkpoint (over-counts the shipped
            // prefix) but good enough for the startup INFO line and the
            // wal_replay_samples_total gauge — Flusher will precisely
            // account for what actually flushes.
            long pending = 0;
            long totalBytes = 0;
            long checkpointOffset = checkpoint.lastSentOffset();
            for (long start : starts) {
                Path segPath = WalSegment.segPathFor(dir, start);
                long size = Files.size(segPath);
                totalBytes += size;
                long endOffset = start + size;
                if (endOffset > checkpointOffset) {
                    Path idxPath = WalSegment.idxPathFor(dir, start);
                    int samples;
                    if (start == newestStart) {
                        // Use the live recovery count — .idx may be stale if
                        // recovery just truncated.
                        samples = activeSegment.sampleCount();
                    } else if (Files.exists(idxPath)) {
                        samples = extractSampleCount(Files.readString(idxPath));
                    } else {
                        samples = 0;
                    }
                    pending += samples;
                }
            }

            // Sanity: checkpoint must not be past the newest segment's end.
            // If it is, the WAL is inconsistent (likely a misconfigured
            // restore). Fail loud rather than silently reset.
            long newestEnd = newestStart + Files.size(newestPath);
            if (checkpointOffset > newestEnd) {
                throw new IOException(
                    "checkpoint.last_sent_offset (" + checkpointOffset + ") is past "
                    + "the newest segment's end (" + newestEnd + "). The WAL "
                    + "directory is inconsistent — remove it to reset.");
            }

            return new RecoveredWal(activeSegment, checkpoint, pending, totalBytes);
        } catch (IOException | RuntimeException e) {
            try {
                activeSegment.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private static void sweepStaleTempFiles(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.tmp")) {
            for (Path p : stream) {
                Files.deleteIfExists(p);
            }
        }
    }

    private static void ensureDirectoryWritable(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }
        if (!Files.isDirectory(dir)) {
            throw new IOException("overflow.dir is not a directory: " + dir);
        }
        if (!Files.isWritable(dir)) {
            throw new IOException(
                "overflow.dir is not writable: " + dir + ". Check filesystem "
                + "permissions or pick a different overflow.dir");
        }
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

    private static int extractSampleCount(String idxJson) {
        int i = idxJson.indexOf("\"sample_count\":");
        if (i < 0) return 0;
        i += "\"sample_count\":".length();
        int end = i;
        while (end < idxJson.length() && Character.isDigit(idxJson.charAt(end))) end++;
        try {
            return Integer.parseInt(idxJson.substring(i, end));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Result of a successful recovery.
     *
     * @param activeSegment        open for append, torn-tail-truncated
     *                             if the last frame was incomplete
     * @param checkpoint           loaded from disk or fresh at offset 0
     * @param pendingSampleCount   samples at or past the checkpoint
     *                             (approximate; for wal_replay_samples_total
     *                             and startup INFO logging)
     * @param totalBytesOnDisk     sum of .seg sizes (for
     *                             wal_disk_usage_bytes gauge init)
     */
    public record RecoveredWal(WalSegment activeSegment, Checkpoint checkpoint,
                               long pendingSampleCount, long totalBytesOnDisk) {}
}
