/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.fuzz;

import java.nio.charset.StandardCharsets;

import org.opennms.plugins.prometheus.remotewriter.sanitize.Sanitizer;

/**
 * Every label name and value on the wire passes through {@link Sanitizer},
 * and its input is whatever OpenNMS collected: an interface description
 * somebody typed, a JMX bean path, a hostname in an alphabet nobody planned
 * for. A name it lets through ungrammatical is a series the backend rejects;
 * a value it truncates mid-codepoint is a series with a broken label.
 *
 * <p>The invariants, for any input:
 *
 * <ul>
 *   <li>a sanitised name is grammatical for Prometheus and never starts with
 *       a digit;</li>
 *   <li>sanitising is idempotent, so a value that survived one pass is not
 *       changed by a second;</li>
 *   <li>a sanitised name keeps the input's length, since every substitution
 *       is one character for one character;</li>
 *   <li>a sanitised value fits the byte cap and is still valid UTF-8, which
 *       means the truncation stopped on a codepoint boundary.</li>
 * </ul>
 */
public final class SanitizerFuzzer {

    private SanitizerFuzzer() { }

    public static void fuzzerTestOneInput(byte[] data) {
        String in = new String(data, StandardCharsets.UTF_8);

        String metric = Sanitizer.metricName(in);
        String label  = Sanitizer.labelName(in);
        checkName(metric, true, in);
        checkName(label, false, in);

        if (!Sanitizer.metricName(metric).equals(metric)) {
            throw new AssertionError("metricName is not idempotent for: " + describe(in));
        }
        if (!Sanitizer.labelName(label).equals(label)) {
            throw new AssertionError("labelName is not idempotent for: " + describe(in));
        }

        String value = Sanitizer.labelValue(in);
        byte[] out = value.getBytes(StandardCharsets.UTF_8);
        if (out.length > Sanitizer.MAX_LABEL_VALUE_BYTES) {
            throw new AssertionError("label value of " + out.length + " bytes is past the cap");
        }
        // A truncation that split a codepoint would not survive the round
        // trip: the trailing bytes would decode to a replacement character
        // the value did not have.
        if (!new String(out, StandardCharsets.UTF_8).equals(value)) {
            throw new AssertionError("truncation left bytes that are not valid UTF-8");
        }
        if (!in.startsWith(value) && !value.isEmpty()) {
            throw new AssertionError("a truncated value is no longer a prefix of its input");
        }
        if (!Sanitizer.labelValue(value).equals(value)) {
            throw new AssertionError("labelValue is not idempotent");
        }
    }

    private static void checkName(String name, boolean metricName, String in) {
        if (name == null || name.isEmpty()) return;
        if (name.length() != in.length()) {
            throw new AssertionError("sanitising changed the length of " + describe(in));
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean letter = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
            boolean digit  = c >= '0' && c <= '9';
            boolean ok = letter || c == '_' || (digit && i > 0) || (metricName && c == ':');
            if (!ok) {
                throw new AssertionError("character '" + c + "' at " + i
                        + " is not legal in a sanitised " + (metricName ? "metric" : "label")
                        + " name: " + describe(in));
            }
        }
    }

    private static String describe(String in) {
        return in.length() <= 64 ? in : in.substring(0, 64) + "… (" + in.length() + " chars)";
    }
}
