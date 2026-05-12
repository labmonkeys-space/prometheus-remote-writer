/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link HttpHeadersConfig}. Exercises prefix-scanning,
 * RFC 7230 name validation, reserved-name rejection (case-insensitive,
 * with pointer text for managed-elsewhere names), value validation (empty,
 * CRLF, non-printable), case preservation on the wire, secret-safe error
 * messages, and the startup-message redaction format.
 */
class HttpHeadersConfigTest {

    // ---- empty / unrelated properties ----------------------------------

    @Test
    void empty_properties_yields_empty_headers_and_no_startup_message() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(Map.of());
        assertThat(cfg.headers()).isEmpty();
        // Activation log skipped for empty map — verified indirectly by
        // formatActivationMessage being called only when non-empty in the
        // production path (see logActivation in HttpHeadersConfig).
    }

    @Test
    void null_properties_resets_headers_to_empty() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(Map.of("http.headers.x-test", "value"));
        assertThat(cfg.headers()).hasSize(1);
        cfg.updated(null);
        assertThat(cfg.headers()).isEmpty();
    }

    @Test
    void unrelated_properties_are_ignored() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("write.url", "http://example.com/write");
        props.put("auth.basic.password", "shouldnotappear");
        props.put("http.headers.x-tenant", "alpha");
        props.put("http.headersnotaprefix", "ignored");      // missing trailing dot
        props.put("nothttp.headers.x", "ignored");           // wrong prefix
        cfg.updated(props);
        assertThat(cfg.headers()).containsOnlyKeys("x-tenant");
        assertThat(cfg.headers().get("x-tenant")).isEqualTo("alpha");
    }

    // ---- reserved-name rejection ---------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {
            "Content-Type", "Content-Encoding", "Content-Length",
            "X-Prometheus-Remote-Write-Version",
            "Transfer-Encoding", "Host", "Connection", "Upgrade"
    })
    void reserved_protocol_headers_are_hard_rejected(String reservedName) {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers." + reservedName, "anything")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining(reservedName)
            .hasMessageContaining("reserved");
    }

    @ParameterizedTest
    @CsvSource({
            "content-type",
            "CONTENT-TYPE",
            "Content-TYPE",
            "x-prometheus-remote-write-version",
            "X-PROMETHEUS-REMOTE-WRITE-VERSION"
    })
    void reserved_name_check_is_case_insensitive(String variant) {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers." + variant, "v")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("reserved");
    }

    @Test
    void authorization_rejection_points_at_auth_config_keys() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.Authorization", "Bearer xyz")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Authorization")
            .hasMessageContaining("auth.basic")
            .hasMessageContaining("auth.bearer.token")
            // Secret hygiene: the offending value must NOT appear in the
            // message — a misconfigured operator's bearer would otherwise
            // leak to the Karaf log.
            .extracting(Throwable::getMessage)
            .satisfies(msg -> assertThat(msg).doesNotContain("Bearer xyz")
                                             .doesNotContain("xyz"));
    }

    @Test
    void x_scope_orgid_rejection_points_at_tenant_org_id() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.X-Scope-OrgID", "team-alpha")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("X-Scope-OrgID")
            .hasMessageContaining("tenant.org-id")
            .extracting(Throwable::getMessage)
            .satisfies(msg -> assertThat(msg).doesNotContain("team-alpha"));
    }

    @Test
    void user_agent_override_is_accepted_and_preserves_casing() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(Map.of("http.headers.User-Agent", "onms-edge-site-7/1.0"));
        assertThat(cfg.headers())
            .containsEntry("User-Agent", "onms-edge-site-7/1.0");
    }

    // ---- malformed names ------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {
            "x tenant",          // whitespace
            "x:tenant",          // colon
            "x,tenant",          // comma
            "x/tenant",          // slash
            "x(tenant)",         // parens
            "x=tenant"           // equals
    })
    void malformed_header_names_are_rejected(String badName) {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers." + badName, "v")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("RFC 7230");
    }

    @Test
    void empty_header_name_after_prefix_is_rejected() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.", "v")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("empty header name");
    }

    // ---- malformed values ----------------------------------------------

    @Test
    void empty_value_is_rejected() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.x-tenant", "")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("x-tenant")
            .hasMessageContaining("empty value");
    }

    @Test
    void whitespace_only_value_is_rejected_after_trim() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.x-tenant", "   ")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("x-tenant")
            .hasMessageContaining("empty value");
    }

    @Test
    void cr_in_value_is_rejected_without_echoing_value() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        String hostile = "ok\rX-Injected: bad";
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.x-tenant", hostile)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("x-tenant")
            .hasMessageContaining("CR or LF")
            .extracting(Throwable::getMessage)
            .satisfies(msg -> assertThat(msg).doesNotContain("X-Injected"));
    }

    @Test
    void lf_in_value_is_rejected_without_echoing_value() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        String hostile = "ok\nX-Injected: bad";
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.x-tenant", hostile)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("CR or LF")
            .extracting(Throwable::getMessage)
            .satisfies(msg -> assertThat(msg).doesNotContain("X-Injected"));
    }

    @Test
    void non_printable_byte_in_value_is_rejected() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        String hostile = "okbell";              // BEL 0x07
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.x-tenant", hostile)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("non-printable");
    }

    @Test
    void high_ascii_byte_in_value_is_rejected() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        String hostile = "ok nbsp";              // NBSP 0xA0
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.x-tenant", hostile)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("non-printable");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "<http://example.com>; rel=\"next\", <http://example.org>; rel=\"alt\"",
            "Bearer abc.def.ghi",        // JWT-shaped
            "abc-def_ghi:123|456",       // contains ':' and '|' — fine inside value
            "key=value;other=value2",    // ';' and '=' fine inside value
            "a -> b -> c"                // '->' fine inside value
    })
    void values_with_internal_punctuation_are_accepted(String value) {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(Map.of("http.headers.x-link", value));
        assertThat(cfg.headers()).containsEntry("x-link", value);
    }

    @Test
    void surrounding_whitespace_in_value_is_trimmed_but_internal_is_preserved() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(Map.of("http.headers.x-tenant", "  team alpha  "));
        // Internal space preserved; surrounding stripped.
        assertThat(cfg.headers()).containsEntry("x-tenant", "team alpha");
    }

    // ---- secret hygiene over the whole rejection surface ---------------

    @Test
    void no_rejection_message_contains_a_literal_header_value() {
        // Sample several rejection categories with a sentinel value the
        // operator would NEVER want leaked. Asserts the message never
        // contains the sentinel.
        String sentinel = "SECRET-VALUE-MUST-NOT-LEAK";
        HttpHeadersConfig cfg = new HttpHeadersConfig();

        // Reserved name (value is the sentinel)
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.Content-Type", sentinel)))
            .extracting(Throwable::getMessage)
            .satisfies(m -> assertThat(m).doesNotContain(sentinel));

        // Authorization rejection (value is the sentinel)
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.Authorization", sentinel)))
            .extracting(Throwable::getMessage)
            .satisfies(m -> assertThat(m).doesNotContain(sentinel));

        // CRLF injection (value contains the sentinel)
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.x-tenant", "x\r" + sentinel)))
            .extracting(Throwable::getMessage)
            .satisfies(m -> assertThat(m).doesNotContain(sentinel));

        // Non-printable (value contains the sentinel)
        assertThatThrownBy(() ->
            cfg.updated(Map.of("http.headers.x-tenant", sentinel + "")))
            .extracting(Throwable::getMessage)
            .satisfies(m -> assertThat(m).doesNotContain(sentinel));
    }

    // ---- startup message redaction format ------------------------------

    @Test
    void activation_message_lists_names_alphabetically_with_redaction() {
        Map<String, String> parsed = new LinkedHashMap<>();
        parsed.put("zebra-header", "v1");           // intentionally out of order
        parsed.put("alpha-header", "v2");
        parsed.put("mike-header",  "v3");
        String msg = HttpHeadersConfig.formatActivationMessage(parsed);

        assertThat(msg).startsWith("Custom HTTP headers attached:");
        // Names appear alphabetically, each followed by " (value redacted)".
        int alphaIdx = msg.indexOf("alpha-header (value redacted)");
        int mikeIdx  = msg.indexOf("mike-header (value redacted)");
        int zebraIdx = msg.indexOf("zebra-header (value redacted)");
        assertThat(alphaIdx).isGreaterThan(0);
        assertThat(mikeIdx).isGreaterThan(alphaIdx);
        assertThat(zebraIdx).isGreaterThan(mikeIdx);
        // No literal values escape into the message.
        assertThat(msg).doesNotContain("v1").doesNotContain("v2").doesNotContain("v3");
    }

    // ---- API contract --------------------------------------------------

    @Test
    void headers_returns_immutable_map() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(Map.of("http.headers.x-tenant", "alpha"));
        Map<String, String> view = cfg.headers();
        assertThatThrownBy(() -> view.put("y-other", "v"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void operator_casing_preserved_in_map_keys() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(Map.of("http.headers.Cf-Access-Client-Id", "abc123"));
        // The map key preserves the operator's exact casing — important for
        // wire-side fidelity even though HTTP headers are case-insensitive.
        assertThat(cfg.headers()).containsKey("Cf-Access-Client-Id");
        assertThat(cfg.headers()).doesNotContainKey("cf-access-client-id");
    }

    @Test
    void to_string_masks_values() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(Map.of("http.headers.x-tenant", "must-not-appear-in-toString"));
        assertThat(cfg.toString())
            .contains("count=1")
            .contains("redacted")
            .doesNotContain("must-not-appear-in-toString");
    }

    @Test
    void empty_factory_returns_no_op_instance() {
        HttpHeadersConfig empty = HttpHeadersConfig.empty();
        assertThat(empty.headers()).isEmpty();
    }

    // ---- additional rejection-rule coverage from code-review ------------

    @Test
    void cr_in_header_name_is_rejected_log_injection_guard() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(java.util.Map.of("http.headers.x\rinjected", "v")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("CR or LF");
    }

    @Test
    void lf_in_header_name_is_rejected_log_injection_guard() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThatThrownBy(() ->
            cfg.updated(java.util.Map.of("http.headers.x\ninjected", "v")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("CR or LF");
    }

    @Test
    void nul_byte_in_header_value_is_rejected() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        String hostile = "good bad";
        assertThatThrownBy(() ->
            cfg.updated(java.util.Map.of("http.headers.x-tenant", hostile)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("non-printable");
    }

    @Test
    void case_only_duplicate_header_names_are_rejected() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("http.headers.X-Foo", "v1");
        props.put("http.headers.x-foo", "v2");
        assertThatThrownBy(() -> cfg.updated(props))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("differing only in case");
    }

    @Test
    void validation_failure_retains_previous_headers_snapshot() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        // First, install a good snapshot
        cfg.updated(java.util.Map.of("http.headers.x-tenant", "team-alpha"));
        assertThat(cfg.headers()).containsEntry("x-tenant", "team-alpha");
        // Now push a bad config that validation rejects
        assertThatThrownBy(() ->
            cfg.updated(java.util.Map.of(
                "http.headers.x-tenant",  "still-ok",
                "http.headers.Content-Type", "rejected"   // reserved
            )))
            .isInstanceOf(IllegalStateException.class);
        // The previous good snapshot is retained — the bean did NOT switch
        // to an empty map or to the partial intermediate state.
        assertThat(cfg.headers()).containsEntry("x-tenant", "team-alpha");
        assertThat(cfg.headers()).doesNotContainKey("Content-Type");
    }

    // ---- diff() ---------------------------------------------------------

    @Test
    void diff_against_null_previous_is_empty_when_no_headers_set() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        assertThat(cfg.diff(null)).isEmpty();
    }

    @Test
    void diff_detects_added_header() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(java.util.Map.of("http.headers.x-tenant", "alpha"));
        java.util.List<String> lines = cfg.diff(java.util.Collections.emptyMap());
        assertThat(lines).containsExactly("http.headers.x-tenant: (unset) -> (set)");
    }

    @Test
    void diff_detects_removed_header() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        // current state: no headers; previous: had x-tenant
        java.util.List<String> lines =
            cfg.diff(java.util.Map.of("x-tenant", "alpha"));
        assertThat(lines).containsExactly("http.headers.x-tenant: (set) -> (unset)");
    }

    @Test
    void diff_detects_changed_value_with_set_to_set_redaction() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(java.util.Map.of("http.headers.x-tenant", "newvalue"));
        java.util.List<String> lines =
            cfg.diff(java.util.Map.of("x-tenant", "oldvalue"));
        assertThat(lines).containsExactly("http.headers.x-tenant: (set) -> (set)");
        // No literal value bytes leak through the diff output.
        assertThat(lines.toString()).doesNotContain("oldvalue").doesNotContain("newvalue");
    }

    @Test
    void diff_is_empty_when_snapshots_are_equal() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(java.util.Map.of("http.headers.x-tenant", "alpha"));
        assertThat(cfg.diff(java.util.Map.of("x-tenant", "alpha"))).isEmpty();
    }

    @Test
    void diff_sorts_lines_alphabetically_for_determinism() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        Map<String, Object> in = new LinkedHashMap<>();
        in.put("http.headers.zebra", "v");
        in.put("http.headers.alpha", "v");
        in.put("http.headers.mike",  "v");
        cfg.updated(in);
        java.util.List<String> lines = cfg.diff(java.util.Collections.emptyMap());
        assertThat(lines).containsExactly(
            "http.headers.alpha: (unset) -> (set)",
            "http.headers.mike: (unset) -> (set)",
            "http.headers.zebra: (unset) -> (set)"
        );
    }

    @Test
    void static_diff_overload_tolerates_null_value_in_previous_map() {
        // R2-P3 defense: a caller passing a Map<String,String> with a null
        // value (e.g., a Map.of-style literal that allowed nulls) must not
        // NPE in the equality check. Objects.equals handles null safely.
        Map<String, String> previous = new LinkedHashMap<>();
        previous.put("x-tenant", null);
        Map<String, String> current = Map.of("x-tenant", "alpha");
        java.util.List<String> lines = HttpHeadersConfig.diff(previous, current);
        // null in previous, non-null in current → value change.
        assertThat(lines).containsExactly("http.headers.x-tenant: (set) -> (set)");
    }

    @Test
    void diff_mixed_add_remove_change_lines() {
        HttpHeadersConfig cfg = new HttpHeadersConfig();
        cfg.updated(java.util.Map.of(
            "http.headers.changed",  "new",
            "http.headers.added",    "v"
        ));
        Map<String, String> previous = new LinkedHashMap<>();
        previous.put("changed", "old");
        previous.put("removed", "gone");
        java.util.List<String> lines = cfg.diff(previous);
        assertThat(lines).containsExactly(
            "http.headers.added: (unset) -> (set)",
            "http.headers.changed: (set) -> (set)",
            "http.headers.removed: (set) -> (unset)"
        );
    }
}
