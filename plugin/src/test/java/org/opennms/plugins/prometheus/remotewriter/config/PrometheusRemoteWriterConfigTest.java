/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PrometheusRemoteWriterConfigTest {

    // ---------- defaults ----------------------------------------------------

    @Test
    void minimal_valid_config_accepts_defaults() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatCode(c::validate).doesNotThrowAnyException();

        assertThat(c.getQueueCapacity()).isEqualTo(10_000);
        assertThat(c.getBatchSize()).isEqualTo(1_000);
        assertThat(c.getFlushIntervalMs()).isEqualTo(1_000L);
        assertThat(c.getRetryMaxAttempts()).isEqualTo(5);
        assertThat(c.getHttpMaxConnections()).isEqualTo(16);
        assertThat(c.getShutdownGracePeriodMs()).isEqualTo(10_000L);
        assertThat(c.getMetadataLabelPrefix()).isEqualTo("onms_meta_");
        assertThat(c.getMetadataCase()).isEqualTo(PrometheusRemoteWriterConfig.MetadataCase.PRESERVE);
        assertThat(c.isMetadataEnabled()).isFalse();
    }

    // ---------- blank/null normalisation ------------------------------------

    @Test
    void blank_string_properties_are_normalized_to_null() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("   ");
        c.setBearerToken("");
        c.setLabelsInclude("   ");
        c.setMetricPrefix("");

        assertThat(c.getBasicUsername()).isNull();
        assertThat(c.getBearerToken()).isNull();
        assertThat(c.getLabelsInclude()).isNull();
        assertThat(c.getMetricPrefix()).isNull();
    }

    // ---------- instance.id -------------------------------------------------

    @Test
    void instance_id_is_parsed_and_normalised() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("opennms-us-east");
        assertThat(c.getInstanceId()).isEqualTo("opennms-us-east");
    }

    @Test
    void instance_id_whitespace_normalises_to_null() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("   ");
        assertThat(c.getInstanceId()).isNull();
        c.setInstanceId("");
        assertThat(c.getInstanceId()).isNull();
        c.setInstanceId(null);
        assertThat(c.getInstanceId()).isNull();
    }

    @Test
    void instance_id_unicode_whitespace_normalises_to_null() {
        // String.trim() only strips <= U+0020; strip() is Unicode-aware and
        // catches NBSP / ideographic space / other Unicode whitespace. Without
        // this, invisible-character-only values would pass through as valid.
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("\u00a0");          // NBSP
        assertThat(c.getInstanceId()).isNull();
        c.setInstanceId("\u3000");          // ideographic space
        assertThat(c.getInstanceId()).isNull();
        c.setInstanceId("\u00a0 \u3000 ");  // mixed whitespace
        assertThat(c.getInstanceId()).isNull();
    }

    @Test
    void instance_id_with_control_characters_is_rejected_at_validate() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("foo\nbar");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("instance.id")
            .hasMessageContaining("control characters");
    }

    @Test
    void instance_id_with_null_byte_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("foo\0bar");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("control characters");
    }

    @Test
    void instance_id_longer_than_2048_bytes_is_rejected_at_validate() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("a".repeat(2049));
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("instance.id")
            .hasMessageContaining("2048");
    }

    @Test
    void instance_id_at_2048_bytes_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("a".repeat(2048));
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    // ---------- job.name ----------------------------------------------------

    @Test
    void job_name_is_unset_by_default() {
        assertThat(minimal().getJobName()).isNull();
    }

    @Test
    void job_name_blank_normalises_to_null() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setJobName("   ");
        assertThat(c.getJobName()).isNull();
        c.setJobName("");
        assertThat(c.getJobName()).isNull();
    }

    @Test
    void job_name_non_blank_is_preserved() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setJobName("opennms-prod");
        assertThat(c.getJobName()).isEqualTo("opennms-prod");
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void job_name_with_control_characters_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setJobName("ops\nteam");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("job.name")
            .hasMessageContaining("control characters");
    }

    @Test
    void job_name_longer_than_2048_bytes_is_rejected_at_validate() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setJobName("j".repeat(2049));
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("job.name")
            .hasMessageContaining("2048");
    }

    @Test
    void instance_id_is_trimmed() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setInstanceId("  opennms-us-east  ");
        assertThat(c.getInstanceId()).isEqualTo("opennms-us-east");
    }

    @Test
    void instance_id_is_reported_in_diff() {
        PrometheusRemoteWriterConfig before = minimal();
        PrometheusRemoteWriterConfig after  = minimal();
        after.setInstanceId("opennms-us-east");
        assertThat(after.diff(before))
            .anyMatch(l -> l.startsWith("instance.id: (unset) -> \"opennms-us-east\""));
    }

    // ---------- required endpoints ------------------------------------------

    @Test
    void missing_write_url_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWriteUrl(null);
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("write.url is required");
    }

    @Test
    void missing_read_url_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setReadUrl(null);
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("read.url is required");
    }

    // ---------- auth exclusivity --------------------------------------------

    @Test
    void basic_and_bearer_are_mutually_exclusive() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("u");
        c.setBasicPassword("p");
        c.setBearerToken("tok");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("mutually exclusive");
    }

    @Test
    void basic_requires_both_username_and_password() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBasicUsername("u");
        // password intentionally missing
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Basic auth requires both");
    }

    @Test
    void bearer_alone_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setBearerToken("tok");
        assertThatCode(c::validate).doesNotThrowAnyException();
        assertThat(c.hasBearerAuth()).isTrue();
        assertThat(c.hasBasicAuth()).isFalse();
    }

    // ---------- numeric bounds ----------------------------------------------

    @Test
    void batch_larger_than_queue_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setQueueCapacity(100);
        c.setBatchSize(500);
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("batch.size")
            .hasMessageContaining("queue.capacity");
    }

    @Test
    void backoff_max_must_be_at_least_initial() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setRetryInitialBackoffMs(1_000);
        c.setRetryMaxBackoffMs(500);
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("retry");
    }

    // ---------- labels.rename parser ----------------------------------------

    @Test
    void rename_map_parses_valid_entries() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo->bar, baz -> qux ,  ");
        Map<String, String> m = c.labelsRenameMap();
        assertThat(m).containsExactly(Map.entry("foo", "bar"), Map.entry("baz", "qux"));
    }

    @Test
    void rename_map_tolerates_internal_and_trailing_empty_entries() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("a->b,,c->d,");
        Map<String, String> m = c.labelsRenameMap();
        assertThat(m).containsExactly(Map.entry("a", "b"), Map.entry("c", "d"));
    }

    @Test
    void rename_map_rejects_malformed_entry() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("oops");
        assertThatThrownBy(c::labelsRenameMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("from->to");
    }

    // ---------- glob CSV parser ---------------------------------------------

    @Test
    void include_glob_csv_trims_and_drops_blanks() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsInclude("ifAlias, , sysDescr ");
        assertThat(c.labelsIncludeGlobs()).containsExactly("ifAlias", "sysDescr");
    }

    // ---------- metadata.case parser ----------------------------------------

    @Test
    void metadata_label_prefix_is_sanitized_to_valid_label_name_grammar() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setMetadataLabelPrefix("my-prefix.");
        // Forbidden chars replaced with '_' per Prometheus label-name rules.
        assertThat(c.getMetadataLabelPrefix()).isEqualTo("my_prefix_");
    }

    @Test
    void metadata_label_prefix_default_when_blank() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setMetadataLabelPrefix("   ");
        assertThat(c.getMetadataLabelPrefix()).isEqualTo("onms_meta_");
    }

    @Test
    void metadata_case_accepts_preserve_and_snake_case_spellings() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setMetadataCase("preserve");
        assertThat(c.getMetadataCase()).isEqualTo(PrometheusRemoteWriterConfig.MetadataCase.PRESERVE);
        c.setMetadataCase("snake_case");
        assertThat(c.getMetadataCase()).isEqualTo(PrometheusRemoteWriterConfig.MetadataCase.SNAKE_CASE);
        c.setMetadataCase("snake-case"); // dash variant normalised
        assertThat(c.getMetadataCase()).isEqualTo(PrometheusRemoteWriterConfig.MetadataCase.SNAKE_CASE);
    }

    @Test
    void metadata_case_rejects_unknown_value() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setMetadataCase("camelCase"))
            .isInstanceOf(IllegalStateException.class);
    }

    // ---------- labels.rename reserved-target validation --------------------

    @Test
    void labels_rename_target_matching_default_label_name_is_rejected() {
        // Parameterised-by-hand: every exact reserved name must be rejected.
        String[] reserved = {
            "__name__", "resourceId", "node",
            "foreign_source", "foreign_id", "node_label", "location",
            "resource_type", "resource_instance",
            "if_name", "if_descr", "if_speed",
            "onms_instance_id",
            "mtype"
        };
        for (String name : reserved) {
            PrometheusRemoteWriterConfig c = minimal();
            c.setLabelsRename("foo -> " + name);
            assertThatThrownBy(c::validate)
                .as("rename target '%s' should be rejected", name)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("labels.rename")
                .hasMessageContaining("'" + name + "'");
        }
    }

    @Test
    void labels_rename_target_matching_onms_cat_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> onms_cat_Router");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("onms_cat_")
            .hasMessageContaining("surveillance categories");
    }

    @Test
    void labels_rename_target_matching_onms_meta_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> onms_meta_custom");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("onms_meta_")
            .hasMessageContaining("metadata passthrough");
    }

    @Test
    void labels_rename_target_matching_onms_attr_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> onms_attr_name");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("onms_attr_")
            .hasMessageContaining("meta partition");
    }

    @Test
    void labels_rename_target_matching_onms_extattr_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> onms_extattr_name");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("onms_extattr_")
            .hasMessageContaining("external partition");
    }

    @Test
    void labels_rename_two_entries_with_same_target_are_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foreign_source -> cluster, location -> cluster");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("same target")
            .hasMessageContaining("'cluster'");
    }

    @Test
    void labels_rename_target_onms_instance_id_is_rejected_even_when_instance_id_is_unset() {
        // Reserved-unconditionally guarantee: the operator cannot set a
        // latent-unsafe rename now and have it silently break after a
        // hot-reload enables instance.id.
        PrometheusRemoteWriterConfig c = minimal();
        assertThat(c.getInstanceId()).isNull();
        c.setLabelsRename("node -> onms_instance_id");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("onms_instance_id");
    }

    @Test
    void labels_rename_to_non_reserved_name_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foreign_source -> hostname, location -> region");
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void labels_rename_cortex_compat_recipe_validates_and_parses() {
        // Pin the published "Migration from opennms-cortex-tss-plugin" recipe
        // (docs/src/docs/asciidoc/sections/_label-mapping.adoc) so a future
        // change to RESERVED_LABEL_NAMES, the rename parser, or the sanitizer
        // that would silently break the recipe fails CI instead of shipping
        // a doc that no longer works. If this test changes, the doc section
        // must change in lockstep.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename(
            "node_label -> nodeLabel, "
            + "foreign_source -> foreignSource, "
            + "foreign_id -> foreignId, "
            + "if_name -> ifName, "
            + "if_descr -> ifDescr, "
            + "if_speed -> ifSpeed");
        assertThatCode(c::validate).doesNotThrowAnyException();
        assertThat(c.labelsRenameMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
            "node_label",     "nodeLabel",
            "foreign_source", "foreignSource",
            "foreign_id",     "foreignId",
            "if_name",        "ifName",
            "if_descr",       "ifDescr",
            "if_speed",       "ifSpeed"));
    }

    @Test
    void labels_rename_multiple_errors_are_accumulated_into_one_exception() {
        // foo -> __name__     -> reserved exact
        // bar -> onms_cat_x   -> reserved prefix
        // baz -> cluster      -> ok (first time)
        // qux -> cluster      -> duplicate target of baz
        // Expect 3 distinct errors reported in one IllegalStateException.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> __name__, bar -> onms_cat_x, baz -> cluster, qux -> cluster");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename has 3 errors")
            .hasMessageContaining("__name__")
            .hasMessageContaining("onms_cat_")
            .hasMessageContaining("same target");
    }

    @Test
    void labels_rename_single_error_uses_singular_phrasing() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> __name__");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename has 1 error:")
            .hasMessageContaining("__name__");
    }

    @Test
    void labels_rename_duplicate_from_entries_are_rejected_at_parse() {
        // 'a -> cluster, a -> tenant' parses to {a=tenant} under a plain
        // LinkedHashMap put; silently dropping the first entry is a
        // copy-paste-error footgun. Reject at labelsRenameMap() — surfaces
        // during validate() too since validate() calls labelsRenameMap().
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("a -> cluster, a -> tenant");
        assertThatThrownBy(c::labelsRenameMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("same 'from'")
            .hasMessageContaining("'a'");
    }

    @Test
    void labels_rename_duplicate_from_entries_are_rejected_at_validate() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("a -> cluster, a -> tenant");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("same 'from'")
            .hasMessageContaining("'a'");
    }

    // ---------- diff --------------------------------------------------------

    @Test
    void diff_against_null_returns_placeholder() {
        List<String> lines = minimal().diff(null);
        assertThat(lines).containsExactly("(no prior config)");
    }

    @Test
    void diff_reports_changed_fields() {
        PrometheusRemoteWriterConfig before = minimal();
        PrometheusRemoteWriterConfig after  = minimal();
        after.setBatchSize(500);
        after.setMetadataEnabled(true);

        List<String> lines = after.diff(before);
        assertThat(lines).hasSize(2);
        assertThat(lines)
            .anyMatch(l -> l.startsWith("batch.size: 1000 -> 500"))
            .anyMatch(l -> l.startsWith("metadata.enabled: false -> true"));
    }

    @Test
    void diff_masks_credentials() {
        PrometheusRemoteWriterConfig before = minimal();
        PrometheusRemoteWriterConfig after  = minimal();
        after.setBasicUsername("u");
        after.setBasicPassword("hunter2");
        after.setBearerToken("dont-leak-me");

        List<String> lines = after.diff(before);
        assertThat(lines).anyMatch(l -> l.contains("auth.basic.password"));
        assertThat(lines).noneMatch(l -> l.contains("hunter2"));
        assertThat(lines).noneMatch(l -> l.contains("dont-leak-me"));
    }

    @Test
    void diff_is_empty_when_configs_match() {
        assertThat(minimal().diff(minimal())).isEmpty();
    }

    // ---------- labels.copy -------------------------------------------------

    @Test
    void copy_map_is_empty_when_unset() {
        assertThat(minimal().labelsCopyMap()).isEmpty();
    }

    @Test
    void copy_map_parses_single_entry() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> cluster");
        assertThat(c.labelsCopyMap()).containsExactly(
            Map.entry("node", List.of("cluster")));
    }

    @Test
    void copy_map_parses_multiple_entries_with_whitespace() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node->cluster, foreign_source -> tenant ,  ");
        assertThat(c.labelsCopyMap()).containsExactly(
            Map.entry("node", List.of("cluster")),
            Map.entry("foreign_source", List.of("tenant")));
    }

    @Test
    void copy_map_preserves_multiple_targets_from_same_source() {
        // Unlike labels.rename, two copies with the same 'from' are allowed:
        // `node -> cluster, node -> host` emits both cluster and host. (Non-
        // reserved target names picked so this test stays meaningful if a
        // future refactor pipes validation into parsing.)
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> cluster, node -> host");
        assertThat(c.labelsCopyMap()).containsExactly(
            Map.entry("node", List.of("cluster", "host")));
    }

    @Test
    void copy_map_rejects_malformed_entry() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("oops");
        assertThatThrownBy(c::labelsCopyMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("from->to");
    }

    @Test
    void copy_map_rejects_trailing_arrow() {
        // `a->b->` used to split to length 2 because Java's default split
        // drops trailing empty strings, silently parsing as `a -> b`. With
        // the -1 limit, the trailing arrow produces a third empty part and
        // the entry is rejected as malformed.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("a->b->");
        assertThatThrownBy(c::labelsCopyMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("from->to");
    }

    @Test
    void rename_map_rejects_trailing_arrow() {
        // Same silent-truncation bug lived in labels.rename too; same fix.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("a->b->");
        assertThatThrownBy(c::labelsRenameMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("from->to");
    }

    @Test
    void copy_target_with_invalid_prometheus_grammar_is_rejected() {
        // `foreign-source` is not a valid Prometheus label name; at emit time
        // the Sanitizer would rewrite it to `foreign_source`, silently
        // clobbering the default label. Reject at startup with an explicit
        // "not a valid Prometheus label name" message so operators fix the
        // source of the problem rather than seeing a downstream reserved-name
        // error after renaming.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("x -> foreign-source");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("'foreign-source'")
            .hasMessageContaining("not a valid Prometheus label name")
            .hasMessageContaining("'foreign_source'");   // sanitized form shown as hint
    }

    @Test
    void rename_target_with_invalid_prometheus_grammar_is_rejected() {
        // Same sanitary guard applies to labels.rename — previously the raw
        // string could pass the reserved-name check even if its sanitized
        // form collided.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("node -> foreign-source");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("'foreign-source'")
            .hasMessageContaining("not a valid Prometheus label name");
    }

    // ---------- parse-map caching ------------------------------------------

    @Test
    void rename_map_is_cached_across_repeated_calls() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> bar");
        Map<String, String> first = c.labelsRenameMap();
        Map<String, String> second = c.labelsRenameMap();
        // Reference-equal: same cached instance. If caching breaks (e.g.,
        // someone removes the memoization), this becomes a simple
        // content-equality comparison and passes spuriously — use isSameAs
        // to lock the identity invariant.
        assertThat(second).isSameAs(first);
    }

    @Test
    void copy_map_is_cached_across_repeated_calls() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> cluster");
        Map<String, List<String>> first = c.labelsCopyMap();
        Map<String, List<String>> second = c.labelsCopyMap();
        assertThat(second).isSameAs(first);
    }

    @Test
    void rename_map_cache_is_invalidated_on_setter_call() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> bar");
        Map<String, String> before = c.labelsRenameMap();
        c.setLabelsRename("baz -> qux");
        Map<String, String> after = c.labelsRenameMap();
        assertThat(after).isNotSameAs(before);
        assertThat(after).containsExactly(Map.entry("baz", "qux"));
    }

    @Test
    void copy_map_cache_is_invalidated_on_setter_call() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> cluster");
        Map<String, List<String>> before = c.labelsCopyMap();
        c.setLabelsCopy("foreign_source -> tenant");
        Map<String, List<String>> after = c.labelsCopyMap();
        assertThat(after).isNotSameAs(before);
        assertThat(after).containsExactly(Map.entry("foreign_source", List.of("tenant")));
    }

    @Test
    void blank_rename_setter_invalidates_and_caches_empty_map() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("a -> b");
        assertThat(c.labelsRenameMap()).hasSize(1);
        c.setLabelsRename("   ");   // blank — normalises to null
        Map<String, String> first = c.labelsRenameMap();
        assertThat(first).isEmpty();
        // Reference identity must also hold for the blank case — both calls
        // return the SAME cached instance, not a fresh `Collections.emptyMap()`
        // each time. Without this assertion, the invariant would pass
        // accidentally via the `emptyMap()` singleton rather than via the
        // memoization path.
        assertThat(c.labelsRenameMap()).isSameAs(first);
    }

    @Test
    void blank_copy_setter_invalidates_and_caches_empty_map() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> cluster");
        assertThat(c.labelsCopyMap()).hasSize(1);
        c.setLabelsCopy("   ");
        Map<String, List<String>> first = c.labelsCopyMap();
        assertThat(first).isEmpty();
        assertThat(c.labelsCopyMap()).isSameAs(first);
    }

    @Test
    void rename_parse_error_is_idempotent_across_repeated_calls() {
        // A malformed directive must throw on every call with the same
        // underlying string — no stale cached null, no silently-succeeding
        // second attempt. Parse errors are deliberately NOT memoised; this
        // test pins the "re-parse, re-throw" contract AND that the error
        // message itself is stable across calls (a substring match alone
        // would let the parser return different messages between calls
        // without the test noticing).
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("oops");
        Throwable first = catchThrowable(c::labelsRenameMap);
        Throwable second = catchThrowable(c::labelsRenameMap);
        assertThat(first).isInstanceOf(IllegalStateException.class)
                         .hasMessageContaining("from->to");
        assertThat(second).isInstanceOf(IllegalStateException.class);
        assertThat(second.getMessage()).isEqualTo(first.getMessage());
    }

    @Test
    void copy_parse_error_is_idempotent_across_repeated_calls() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("oops");
        Throwable first = catchThrowable(c::labelsCopyMap);
        Throwable second = catchThrowable(c::labelsCopyMap);
        assertThat(first).isInstanceOf(IllegalStateException.class)
                         .hasMessageContaining("from->to");
        assertThat(second).isInstanceOf(IllegalStateException.class);
        assertThat(second.getMessage()).isEqualTo(first.getMessage());
    }

    @Test
    void validate_returns_cached_maps_to_all_three_subvalidators() {
        // validate() invokes labelsRenameMap() via validateRenameTargets and
        // validateCrossPrimitiveTargets, and labelsCopyMap() via
        // validateCopyTargets and validateCrossPrimitiveTargets. The cache
        // means each accessor returns the same instance to every caller —
        // this test pins that "first call primes the cache, subsequent calls
        // hit it" invariant across the validate() boundary. Name preferred
        // over "exactly once" since this test asserts identity of returned
        // maps, not parse-invocation count.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("a -> b");
        c.setLabelsCopy("foreign_source -> tenant");
        // Prime the cache by calling both once.
        Map<String, String> renameA = c.labelsRenameMap();
        Map<String, List<String>> copyA = c.labelsCopyMap();
        c.validate();
        // Post-validate, calls still hit the same cached instance.
        assertThat(c.labelsRenameMap()).isSameAs(renameA);
        assertThat(c.labelsCopyMap()).isSameAs(copyA);
    }

    // ---------- copy-target sanity guards ----------------------------------

    @Test
    void copy_two_raw_targets_that_sanitize_to_same_name_are_rejected_individually() {
        // `foo-bar` and `foo_bar` both sanitize to `foo_bar`. Previously both
        // would pass the raw-string duplicate-target check and the second
        // would silently overwrite the first at emit. Now each hits the
        // grammar rejection on its own line (foo-bar has the hyphen,
        // foo_bar is valid but is caught as duplicate only after one of
        // them is corrected).
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("a -> foo-bar, b -> foo_bar");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("'foo-bar'")
            .hasMessageContaining("not a valid Prometheus label name");
    }

    @Test
    void copy_map_rejects_empty_sides() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("-> instance");
        assertThatThrownBy(c::labelsCopyMap)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("empty side");
    }

    @Test
    void copy_to_non_reserved_name_validates() {
        // `cluster` and `tenant` are unreserved; validate() accepts them.
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> cluster, foreign_source -> tenant");
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void rename_target_instance_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> instance");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("'instance'")
            .hasMessageContaining("default label");
    }

    @Test
    void rename_target_job_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("foo -> job");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.rename")
            .hasMessageContaining("'job'")
            .hasMessageContaining("default label");
    }

    @Test
    void copy_target_instance_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("foo -> instance");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("'instance'")
            .hasMessageContaining("default label");
    }

    @Test
    void copy_target_job_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("foo -> job");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("'job'")
            .hasMessageContaining("default label");
    }

    @Test
    void copy_target_collides_with_default_label_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> foreign_source");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("foreign_source")
            .hasMessageContaining("default label");
    }

    @Test
    void copy_target_with_reserved_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("foo -> onms_cat_router");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("onms_cat_")
            .hasMessageContaining("surveillance categories");
    }

    @Test
    void copy_target_matching_onms_attr_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> onms_attr_name");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("onms_attr_")
            .hasMessageContaining("meta partition");
    }

    @Test
    void copy_target_matching_onms_extattr_prefix_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> onms_extattr_name");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("onms_extattr_")
            .hasMessageContaining("external partition");
    }

    @Test
    void metadata_label_prefix_colliding_with_reserved_namespace_is_rejected() {
        // The metadata processor's prefix may not collide with another
        // emitter's reserved namespace (onms_cat_ for category expansion,
        // onms_attr_ for resource string attributes). The default
        // onms_meta_ is intentionally exempt — it's this emitter's own home.
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setMetadataLabelPrefix("onms_attr_"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("metadata.label-prefix")
            .hasMessageContaining("onms_attr_");

        assertThatThrownBy(() -> c.setMetadataLabelPrefix("onms_cat_"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("metadata.label-prefix")
            .hasMessageContaining("onms_cat_");

        // Subsumption: a prefix that *starts with* a reserved prefix is also
        // rejected (an operator-set "onms_attr_extra_" would still write into
        // the reserved namespace).
        assertThatThrownBy(() -> c.setMetadataLabelPrefix("onms_attr_extra_"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("onms_attr_");
    }

    @Test
    void metadata_label_prefix_default_onms_meta_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        // Setting the default explicitly must not trip the reserved-prefix
        // guard — onms_meta_ is the metadata processor's own canonical home.
        assertThatCode(() -> c.setMetadataLabelPrefix("onms_meta_"))
            .doesNotThrowAnyException();
        assertThat(c.getMetadataLabelPrefix()).isEqualTo("onms_meta_");
    }

    @Test
    void metadata_label_prefix_unrelated_value_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatCode(() -> c.setMetadataLabelPrefix("custom_"))
            .doesNotThrowAnyException();
        assertThat(c.getMetadataLabelPrefix()).isEqualTo("custom_");
    }

    @Test
    void metadata_label_prefix_uppercase_reserved_form_is_rejected() {
        // The rejection's intent is "namespace ownership", not "byte
        // equality on the wire". An uppercase ONMS_ATTR_ is bytes-distinct
        // from onms_attr_ (Prometheus preserves case in label names) but
        // semantically claims the same namespace, so it must be caught.
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setMetadataLabelPrefix("ONMS_ATTR_"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("onms_attr_");
    }

    @Test
    void metadata_label_prefix_shorter_subsuming_value_is_rejected() {
        // A shorter operator prefix that every reserved namespace starts
        // with (e.g. onms_) would emit metadata under the same wire shape
        // as onms_cat_*, onms_attr_*, and onms_meta_*. The setter must
        // reject this both ways: sanitized.startsWith(reserved) covers the
        // longer-side case; reserved.startsWith(sanitized) covers this one.
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setMetadataLabelPrefix("onms_"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("metadata.label-prefix");
    }

    @Test
    void metadata_label_prefix_colliding_with_onms_extattr_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setMetadataLabelPrefix("onms_extattr_"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("metadata.label-prefix")
            .hasMessageContaining("onms_extattr_");
    }

    @Test
    void metadata_label_prefix_subsumed_by_onms_extattr_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setMetadataLabelPrefix("onms_extattr_extra_"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("onms_extattr_");
    }

    @Test
    void metadata_label_prefix_unrelated_mixed_case_value_is_accepted() {
        // Tightening for case must not over-trigger: a benign mixed-case
        // prefix that doesn't collide with any reserved namespace stays
        // accepted.
        PrometheusRemoteWriterConfig c = minimal();
        assertThatCode(() -> c.setMetadataLabelPrefix("Custom_"))
            .doesNotThrowAnyException();
        // Sanitizer.labelName preserves case; the stored value is the
        // sanitized (case-preserved) form, not the lowercased comparison form.
        assertThat(c.getMetadataLabelPrefix()).isEqualTo("Custom_");
    }

    @Test
    void copy_duplicate_target_across_entries_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("node -> cluster, foreign_source -> cluster");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("'cluster'")
            .hasMessageContaining("same target");
    }

    @Test
    void copy_target_collides_with_rename_target_is_rejected() {
        // rename: node -> cluster (cluster isn't reserved)
        // copy:   foreign_source -> cluster (cross-primitive collision on cluster)
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsRename("node -> cluster");
        c.setLabelsCopy("foreign_source -> cluster");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy")
            .hasMessageContaining("'cluster'")
            .hasMessageContaining("labels.rename");
    }

    @Test
    void copy_multiple_errors_are_accumulated_into_one_message() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setLabelsCopy("a -> node, b -> onms_cat_x, c -> cluster, d -> cluster");
        assertThatThrownBy(c::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("labels.copy has 3 error")
            .hasMessageContaining("'node'")
            .hasMessageContaining("onms_cat_")
            .hasMessageContaining("'cluster'");
    }

    // ---------- wal.* -------------------------------------------------------

    @Test
    void wal_defaults_match_the_design_contract() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThat(c.isWalEnabled()).isFalse();
        assertThat(c.getWalPath()).isEmpty();
        assertThat(c.getWalMaxSizeBytes()).isEqualTo(536_870_912L); // 512 MB
        assertThat(c.getWalSegmentSizeBytes()).isEqualTo(67_108_864L); // 64 MB
        assertThat(c.getWalFsync())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.BATCH);
        assertThat(c.getWalOverflow())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.BACKPRESSURE);
    }

    @Test
    void wal_disabled_skips_wal_validation_entirely() {
        // When wal.enabled=false, invalid wal.* values are NOT rejected —
        // they are simply ignored. Prevents operator frustration with
        // "I turned WAL off but it still complains."
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalEnabled(false);
        c.setWalMaxSizeBytes(-1);       // would be invalid if enabled
        c.setWalSegmentSizeBytes(0);    // would be invalid if enabled
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void wal_enabled_with_minimal_extra_config_validates_cleanly() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalEnabled(true);
        c.setWalPath("/tmp/test-wal-path"); // any non-blank path; resolveWalPath returns it
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void wal_fsync_parses_case_insensitively() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalFsync("always");
        assertThat(c.getWalFsync())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.ALWAYS);
        c.setWalFsync("NEVER");
        assertThat(c.getWalFsync())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.NEVER);
        c.setWalFsync("Batch");
        assertThat(c.getWalFsync())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.BATCH);
    }

    @Test
    void wal_fsync_blank_defaults_to_batch() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalFsync("");
        assertThat(c.getWalFsync())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.BATCH);
    }

    @Test
    void wal_fsync_rejects_unknown_value() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setWalFsync("sometimes"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wal.fsync")
                .hasMessageContaining("sometimes");
    }

    @Test
    void wal_overflow_accepts_hyphen_and_underscore_forms() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalOverflow("drop-oldest");
        assertThat(c.getWalOverflow())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.DROP_OLDEST);
        c.setWalOverflow("DROP_OLDEST");
        assertThat(c.getWalOverflow())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.DROP_OLDEST);
        c.setWalOverflow("backpressure");
        assertThat(c.getWalOverflow())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.BACKPRESSURE);
    }

    @Test
    void wal_overflow_rejects_unknown_value() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setWalOverflow("lose-them-all"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wal.overflow");
    }

    @Test
    void validate_rejects_zero_or_negative_wal_max_size_when_enabled() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalEnabled(true);
        c.setWalMaxSizeBytes(0);
        assertThatThrownBy(c::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wal.max-size-bytes");
    }

    @Test
    void validate_rejects_segment_size_exceeding_max_size() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalEnabled(true);
        c.setWalMaxSizeBytes(1000);
        c.setWalSegmentSizeBytes(2000);
        assertThatThrownBy(c::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wal.segment-size-bytes")
                .hasMessageContaining("wal.max-size-bytes");
    }

    @Test
    void resolveWalPath_returns_explicit_path_when_set() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalPath("/var/lib/opennms/wal");
        assertThat(c.resolveWalPath()).isEqualTo("/var/lib/opennms/wal");
    }

    @Test
    void resolveWalPath_uses_karaf_data_system_property_when_blank() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalPath("");
        String original = System.getProperty("karaf.data");
        try {
            System.setProperty("karaf.data", "/opt/opennms/data");
            assertThat(c.resolveWalPath())
                    .isEqualTo("/opt/opennms/data/prometheus-remote-writer/wal");
        } finally {
            if (original == null) System.clearProperty("karaf.data");
            else System.setProperty("karaf.data", original);
        }
    }

    @Test
    void resolveWalPath_throws_when_blank_and_no_karaf_data() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalPath("");
        String original = System.getProperty("karaf.data");
        try {
            System.clearProperty("karaf.data");
            assertThatThrownBy(c::resolveWalPath)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("karaf.data");
        } finally {
            if (original != null) System.setProperty("karaf.data", original);
        }
    }

    // ---------- wire.protocol-version --------------------------------------

    @Test
    void wire_protocol_version_default_is_one() {
        assertThat(minimal().getWireProtocolVersion()).isEqualTo(1);
    }

    @Test
    void wire_protocol_version_accepts_one_and_two() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWireProtocolVersion("1");
        assertThat(c.getWireProtocolVersion()).isEqualTo(1);
        c.setWireProtocolVersion("2");
        assertThat(c.getWireProtocolVersion()).isEqualTo(2);
    }

    @Test
    void wire_protocol_version_blank_defaults_to_one() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWireProtocolVersion("2");
        assertThat(c.getWireProtocolVersion()).isEqualTo(2);
        // Blank explicitly resets to default — same convention as wal.fsync.
        c.setWireProtocolVersion("");
        assertThat(c.getWireProtocolVersion()).isEqualTo(1);
        c.setWireProtocolVersion("   ");
        assertThat(c.getWireProtocolVersion()).isEqualTo(1);
    }

    @Test
    void wire_protocol_version_rejects_unknown_values() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setWireProtocolVersion("3"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wire.protocol-version")
                .hasMessageContaining("3");
        assertThatThrownBy(() -> c.setWireProtocolVersion("v2"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wire.protocol-version");
        assertThatThrownBy(() -> c.setWireProtocolVersion("nonsense"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void wire_protocol_version_is_reported_in_diff() {
        PrometheusRemoteWriterConfig before = minimal();
        PrometheusRemoteWriterConfig after  = minimal();
        after.setWireProtocolVersion("2");
        assertThat(after.diff(before))
            .anyMatch(l -> l.startsWith("wire.protocol-version: 1 -> 2"));
    }

    // The int overload exists so Aries Blueprint's PropertyDescriptor
    // validation finds a setter matching getWireProtocolVersion()'s int
    // return type. Sentinel's blueprint enforces this strictly; without
    // the overload the plugin bundle fails to start. See setMetadataCase
    // for the analogous fix.
    @Test
    void wire_protocol_version_int_setter_accepts_one_and_two() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWireProtocolVersion(1);
        assertThat(c.getWireProtocolVersion()).isEqualTo(1);
        c.setWireProtocolVersion(2);
        assertThat(c.getWireProtocolVersion()).isEqualTo(2);
    }

    @Test
    void wire_protocol_version_int_setter_rejects_unknown_values() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatThrownBy(() -> c.setWireProtocolVersion(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wire.protocol-version");
        assertThatThrownBy(() -> c.setWireProtocolVersion(3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wire.protocol-version");
    }

    // The enum-typed overloads exist for the same Aries Blueprint
    // PropertyDescriptor reason as setWireProtocolVersion(int) — without
    // them, Sentinel's blueprint refuses to instantiate the bean because
    // no setter matches the FsyncPolicy / OverflowPolicy getter return
    // type. See setMetadataCase for the original instance of the pattern.

    @Test
    void wal_fsync_enum_setter_accepts_each_policy() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalFsync(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.ALWAYS);
        assertThat(c.getWalFsync())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.ALWAYS);
        c.setWalFsync(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.NEVER);
        assertThat(c.getWalFsync())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.NEVER);
        c.setWalFsync(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.BATCH);
        assertThat(c.getWalFsync())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.BATCH);
    }

    @Test
    void wal_fsync_enum_setter_null_defaults_to_batch() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalFsync(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.ALWAYS);
        c.setWalFsync((org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy) null);
        assertThat(c.getWalFsync())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalSegment.FsyncPolicy.BATCH);
    }

    @Test
    void wal_overflow_enum_setter_accepts_each_policy() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalOverflow(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.DROP_OLDEST);
        assertThat(c.getWalOverflow())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.DROP_OLDEST);
        c.setWalOverflow(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.BACKPRESSURE);
        assertThat(c.getWalOverflow())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.BACKPRESSURE);
    }

    @Test
    void wal_overflow_enum_setter_null_defaults_to_backpressure() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setWalOverflow(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.DROP_OLDEST);
        c.setWalOverflow((org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy) null);
        assertThat(c.getWalOverflow())
                .isEqualTo(org.opennms.plugins.prometheus.remotewriter.wal.WalWriter.OverflowPolicy.BACKPRESSURE);
    }

    // ---------- two-phase resource discovery -------------------------------

    @Test
    void discovery_strategy_default_is_single_pass() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThatCode(c::validate).doesNotThrowAnyException();
        assertThat(c.getDiscoveryStrategy())
                .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
    }

    @Test
    void discovery_strategy_label_values_first_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryStrategy("label-values-first");
        assertThatCode(c::validate).doesNotThrowAnyException();
        assertThat(c.getDiscoveryStrategy())
                .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.LABEL_VALUES_FIRST);
    }

    @Test
    void discovery_strategy_is_case_insensitive_and_trimmed() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryStrategy("  Label-Values-First  ");
        assertThat(c.getDiscoveryStrategy())
                .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.LABEL_VALUES_FIRST);

        c.setDiscoveryStrategy("SINGLE-PASS");
        assertThat(c.getDiscoveryStrategy())
                .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
    }

    @Test
    void discovery_strategy_blank_or_null_falls_back_to_default() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryStrategy("label-values-first");
        c.setDiscoveryStrategy("   ");
        assertThat(c.getDiscoveryStrategy())
                .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
        c.setDiscoveryStrategy("label-values-first");
        c.setDiscoveryStrategy((String) null);
        assertThat(c.getDiscoveryStrategy())
                .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
    }

    @Test
    void discovery_strategy_unknown_value_is_rejected_with_accepted_forms_listed() {
        PrometheusRemoteWriterConfig c = minimal();
        Throwable t = catchThrowable(() -> c.setDiscoveryStrategy("fancy-mode"));
        assertThat(t)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("read.discovery-strategy")
                .hasMessageContaining("single-pass")
                .hasMessageContaining("label-values-first")
                .hasMessageContaining("fancy-mode");
    }

    @Test
    void discovery_strategy_enum_setter_null_defaults_to_single_pass() {
        // Aries Blueprint setter overload contract — see setDiscoveryStrategy(DiscoveryStrategy).
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryStrategy(PrometheusRemoteWriterConfig.DiscoveryStrategy.LABEL_VALUES_FIRST);
        c.setDiscoveryStrategy((PrometheusRemoteWriterConfig.DiscoveryStrategy) null);
        assertThat(c.getDiscoveryStrategy())
                .isEqualTo(PrometheusRemoteWriterConfig.DiscoveryStrategy.SINGLE_PASS);
    }

    @Test
    void discovery_batch_size_default_is_50() {
        PrometheusRemoteWriterConfig c = minimal();
        assertThat(c.getDiscoveryBatchSize()).isEqualTo(50);
    }

    @Test
    void discovery_batch_size_zero_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryBatchSize(0);
        assertThatThrownBy(c::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("read.discovery-batch-size")
                .hasMessageContaining("[1, 200]");
    }

    @Test
    void discovery_batch_size_negative_is_rejected() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryBatchSize(-1);
        assertThatThrownBy(c::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("[1, 200]");
    }

    @Test
    void discovery_batch_size_above_200_is_rejected() {
        // Upper bound tightened from 1000 to 200 in code-review round 1
        // (D2). At batch-size 200 with typical OpenNMS resourceIds the
        // request line is ~30 KiB — fits cloud LB defaults but is the
        // safe ceiling above default nginx / Mimir 8 KiB limits.
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryBatchSize(201);
        assertThatThrownBy(c::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("[1, 200]");
    }

    @Test
    void discovery_batch_size_at_lower_bound_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryStrategy("label-values-first");
        c.setDiscoveryBatchSize(1);
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void discovery_batch_size_at_upper_bound_is_accepted() {
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryStrategy("label-values-first");
        c.setDiscoveryBatchSize(200);
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    @Test
    void discovery_batch_size_set_non_default_with_single_pass_strategy_validates_without_error() {
        // Validates without rejection; the WARN log is observable but not
        // asserted here — the project has no SLF4J binding on the test
        // classpath, so a programmatic Logback ListAppender would require
        // a new test dependency. Code-review round 1 reclassified P4 from
        // PATCH → DEFER pending a logger-injection refactor (recorded in
        // _bmad-output/implementation-artifacts/deferred-work.md).
        PrometheusRemoteWriterConfig c = minimal();
        c.setDiscoveryBatchSize(75); // non-default
        assertThatCode(c::validate).doesNotThrowAnyException();
    }

    // ---------- helpers -----------------------------------------------------

    private static PrometheusRemoteWriterConfig minimal() {
        PrometheusRemoteWriterConfig c = new PrometheusRemoteWriterConfig();
        c.setWriteUrl("https://example.com/api/v1/push");
        c.setReadUrl("https://example.com/prometheus");
        return c;
    }
}
