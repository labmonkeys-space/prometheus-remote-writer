/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * The {@code metadata.info-columns} grammar: {@code column=key} entries,
 * the column a label name on the wire and the key an OpenNMS attribute.
 */
class InfoColumnsTest {

    @Test
    void the_default_covers_the_shipped_datacollection_one_column_per_key() {
        Map<String, String> d = InfoColumns.parse(InfoColumns.DEFAULT_SPEC);
        assertThat(d).containsExactly(
                Map.entry("if_alias", "ifAlias"),
                Map.entry("if_descr", "ifDescr"),
                Map.entry("resource_name", "name"),
                Map.entry("hr_storage_descr", "hrStorageDescr"),
                Map.entry("dsk_path", "dskPath"),
                Map.entry("datname", "datname"),
                Map.entry("spcname", "spcname"));
    }

    @Test
    void entries_are_trimmed_and_ordered_as_written() {
        assertThat(InfoColumns.parse("  b = beta ,a=alpha  ")).containsExactly(
                Map.entry("b", "beta"), Map.entry("a", "alpha"));
    }

    @Test
    void blank_means_no_columns() {
        assertThat(InfoColumns.parse("")).isEmpty();
        assertThat(InfoColumns.parse(null)).isEmpty();
        assertThat(InfoColumns.parse(" , ")).isEmpty();
    }

    @Test
    void an_entry_without_a_key_is_rejected() {
        assertThatThrownBy(() -> InfoColumns.parse("if_alias"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("column=key");
        assertThatThrownBy(() -> InfoColumns.parse("if_alias="))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("if_alias");
        assertThatThrownBy(() -> InfoColumns.parse("=ifAlias"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ifAlias");
    }

    @Test
    void a_column_must_be_a_label_name() {
        assertThatThrownBy(() -> InfoColumns.parse("if-alias=ifAlias"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("if-alias");
        assertThatThrownBy(() -> InfoColumns.parse("1st=ifAlias"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1st");
    }

    @Test
    void a_column_the_info_series_already_owns_is_rejected() {
        assertThat(InfoColumns.RESERVED).contains("resourceId", "__name__", "onms_instance_id", "name");
        for (String reserved : InfoColumns.RESERVED) {
            assertThatThrownBy(() -> InfoColumns.parse(reserved + "=ifAlias"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(reserved);
        }
    }

    @Test
    void a_name_column_points_at_resource_name() {
        assertThatThrownBy(() -> InfoColumns.parse("name=name"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("resource_name=name");
    }

    @Test
    void a_key_the_registry_never_records_is_rejected() {
        for (String key : new String[] {"ifSpeed", "ifHighSpeed", "categories", "mtype", "requisition:location", "snmp-community"}) {
            assertThatThrownBy(() -> InfoColumns.parse("c=" + key))
                    .as(key)
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(key);
        }
    }

    @Test
    void a_key_the_data_series_carry_as_a_label_is_rejected_naming_the_label() {
        assertThatThrownBy(() -> InfoColumns.parse("node=nodeLabel"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("node_label");
        assertThatThrownBy(() -> InfoColumns.parse("c=cat_Routers"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("onms_resource_category");
        // With the label excluded from the wire the key is a row again, and a column may read it.
        assertThat(InfoColumns.parse("node=nodeLabel", java.util.Set.of())).containsEntry("node", "nodeLabel");
    }

    @Test
    void a_duplicate_column_is_rejected() {
        assertThatThrownBy(() -> InfoColumns.parse("alias=ifAlias, alias=ifDescr"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("alias");
    }

    @Test
    void two_columns_may_read_the_same_key() {
        assertThat(InfoColumns.parse("a=ifAlias, b=ifAlias")).hasSize(2);
    }

    @Test
    void a_column_is_judged_with_the_operator_globs() {
        java.util.Set<String> rowless = MetadataRegistry.LABEL_KEYS.keySet();
        // Without the globs the validation contradicts the registry in both
        // directions: it refuses a key attr-include admits, and accepts one
        // attr-exclude drops, whose column could only ever be empty.
        assertThatThrownBy(() -> InfoColumns.parse("dsk_path=disk.path", rowless))
                .hasMessageContaining("not shaped like an attribute key");
        assertThat(InfoColumns.parse("dsk_path=disk.path", rowless, java.util.List.of("disk.*"), java.util.List.of()))
                .containsEntry("dsk_path", "disk.path");
        assertThatThrownBy(() -> InfoColumns.parse("if_alias=ifAlias", rowless,
                        java.util.List.of(), java.util.List.of("ifAlias")))
                .hasMessageContaining("metadata.attr-exclude drops it");
    }

}
