/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.fuzz;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.opennms.plugins.prometheus.remotewriter.wal.Frame;

/**
 * {@link Frame#decode} reads a length, a payload and a CRC from a segment a
 * crash may have torn mid-write, so its input is whatever survived. The
 * contract it has to keep, on any bytes at all:
 *
 * <ul>
 *   <li>it returns a payload or null, and throws nothing but {@link IOException};</li>
 *   <li>a null leaves the channel exactly where the frame started, because the
 *       caller truncates there and a drifted position would truncate good data
 *       or leave a torn tail behind;</li>
 *   <li>a payload is no longer than the cap it was given, which is what stops
 *       a rotted length prefix from becoming an enormous allocation;</li>
 *   <li>a payload consumes exactly its own framing, so the next frame starts
 *       where this one ended.</li>
 * </ul>
 *
 * <p>Entry point is the plain {@code byte[]} form, so the target compiles and
 * runs with no Jazzer on the classpath: {@code make test} drives it through
 * {@link FuzzTargetsSeedTest} and ClusterFuzzLite drives it with a mutator.
 */
public final class FrameFuzzer {

    private static final int MAX_PAYLOAD = 64 * 1024;

    /** One reused file: a fuzzer runs this millions of times. */
    private static Path file;

    private FrameFuzzer() { }

    public static synchronized void fuzzerTestOneInput(byte[] data) {
        try {
            if (file == null) {
                file = Files.createTempFile("frame-fuzz", ".seg");
                file.toFile().deleteOnExit();
            }
            try (FileChannel ch = FileChannel.open(file,
                    StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING)) {
                ch.write(ByteBuffer.wrap(data));
                ch.position(0);

                byte[] payload = Frame.decode(ch, MAX_PAYLOAD);

                if (payload == null) {
                    if (ch.position() != 0) {
                        throw new AssertionError("a torn frame must leave the channel at the frame start, not "
                                + ch.position());
                    }
                    return;
                }
                if (payload.length > MAX_PAYLOAD) {
                    throw new AssertionError("decoded a payload past the cap: " + payload.length);
                }
                long consumed = ch.position();
                if (consumed != Frame.HEADER_BYTES + (long) payload.length) {
                    throw new AssertionError("consumed " + consumed + " bytes for a payload of "
                            + payload.length + "; the next frame would start in the wrong place");
                }
                // What it read has to be what encode() would write, or a
                // replayed segment and a fresh one disagree.
                ByteBuffer again = Frame.encode(payload);
                byte[] expected = new byte[again.remaining()];
                again.get(expected);
                for (int i = 0; i < expected.length; i++) {
                    if (expected[i] != data[i]) {
                        throw new AssertionError("re-encoding the payload differs at byte " + i);
                    }
                }
            }
        } catch (IOException e) {
            // The only exception the decoder is allowed to raise.
            throw new java.io.UncheckedIOException(e);
        }
    }
}
