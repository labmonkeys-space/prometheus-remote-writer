/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.fuzz;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opennms.plugins.prometheus.remotewriter.wal.Frame;

/**
 * The fuzz targets, driven without a fuzzer.
 *
 * <p>Fuzzing lives in CI and finds new inputs; this keeps the targets honest
 * in every {@code make test}. It feeds them a seed corpus of the shapes that
 * matter, the torn frame, the flipped CRC, the rotted length prefix, plus a
 * deterministic pseudo-random sweep, so a target that stops compiling or an
 * invariant that stops holding fails here rather than in a nightly run.
 *
 * <p>Seeded from a constant, so a failure names an input that reproduces.
 */
class FuzzTargetsSeedTest {

    private static final int SWEEP = 2_000;

    /** The shapes a torn or rotted segment actually produces. */
    private static List<byte[]> frameSeeds() {
        List<byte[]> seeds = new ArrayList<>();
        seeds.add(new byte[0]);
        seeds.add(new byte[] {0});
        seeds.add(new byte[] {0, 0, 0});                       // short of a length prefix
        seeds.add(new byte[] {0, 0, 0, 1});                    // length, then nothing
        seeds.add(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});  // negative length
        seeds.add(new byte[] {0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});         // length past the cap

        byte[] good = bytesOf(Frame.encode("hello".getBytes(StandardCharsets.UTF_8)));
        seeds.add(good);

        byte[] tornTail = new byte[good.length - 1];           // lost its last CRC byte
        System.arraycopy(good, 0, tornTail, 0, tornTail.length);
        seeds.add(tornTail);

        byte[] badCrc = good.clone();
        badCrc[badCrc.length - 1] ^= 0x01;                     // one bit of rot
        seeds.add(badCrc);

        byte[] two = new byte[good.length * 2];                // a frame followed by another
        System.arraycopy(good, 0, two, 0, good.length);
        System.arraycopy(good, 0, two, good.length, good.length);
        seeds.add(two);

        seeds.add(bytesOf(Frame.encode(new byte[0])));         // an empty payload is legal
        return seeds;
    }

    /**
     * Strings that have broken sanitisers before: other scripts, emoji,
     * separators, and lengths either side of the label-value cap.
     */
    private static List<byte[]> textSeeds() {
        List<String> text = List.of(
                "", "a", "0", "0leading", "ifHCInOctets", "GigabitEthernet0/0",
                "ICMP/10.0.0.1", "a:b:c", "a b c", "unicode punctuation — dash",
                "ünïcödé", "日本語のラベル",
                "emoji 😀 pair", " ", "-dash-", "__dunder__",
                "x".repeat(2047), "x".repeat(2048), "x".repeat(2049),
                "ü".repeat(1500), "😀".repeat(600));
        List<byte[]> seeds = new ArrayList<>();
        for (String s : text) {
            seeds.add(s.getBytes(StandardCharsets.UTF_8));
        }
        // Bytes that are not valid UTF-8 at all.
        seeds.add(new byte[] {(byte) 0xC3});
        seeds.add(new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80});               // a surrogate half
        seeds.add(new byte[] {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80});  // past U+10FFFF
        return seeds;
    }

    private static byte[] bytesOf(ByteBuffer buf) {
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }

    private static void drive(String what, Consumer<byte[]> target, List<byte[]> seeds) {
        for (byte[] seed : seeds) {
            assertThatCode(() -> target.accept(seed))
                    .as("%s on a %d-byte seed", what, seed.length)
                    .doesNotThrowAnyException();
        }
        Random random = new Random(20260915L);
        for (int i = 0; i < SWEEP; i++) {
            byte[] data = new byte[random.nextInt(96)];
            random.nextBytes(data);
            // Half the sweep mutates one bit of a seed, which is where the
            // near-misses live: a good frame with a wrong CRC, a length one
            // byte too long.
            if (i % 2 == 0) {
                byte[] base = seeds.get(random.nextInt(seeds.size()));
                if (base.length > 0) {
                    data = base.clone();
                    data[random.nextInt(data.length)] ^= (byte) (1 << random.nextInt(8));
                }
            }
            byte[] input = data;
            assertThatCode(() -> target.accept(input))
                    .as("%s on sweep iteration %d", what, i)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    @DisplayName("Frame.decode holds its contract on torn, rotted and truncated segments")
    void frame_decode() {
        drive("FrameFuzzer", FrameFuzzer::fuzzerTestOneInput, frameSeeds());
    }

    @Test
    @DisplayName("Sanitizer produces grammatical names and whole codepoints")
    void sanitizer() {
        drive("SanitizerFuzzer", SanitizerFuzzer::fuzzerTestOneInput, textSeeds());
    }

    @Test
    @DisplayName("WalEntryCodec.decode answers with a sample or IllegalStateException, nothing else")
    void wal_entry_codec() {
        drive("WalEntryCodecFuzzer", WalEntryCodecFuzzer::fuzzerTestOneInput, frameSeeds());
    }
}
