/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Structural test that catches the class of bug v0.3 shipped: a Java setter
 * was added to {@link PrometheusRemoteWriterConfig} for {@code labels.copy},
 * but the corresponding {@code <property name="labelsCopy" value="${labels.copy}"/>}
 * binding was missing from {@code blueprint.xml} and the {@code <cm:property>}
 * default was missing from the property-placeholder. Every unit test passed
 * because they called the Java setter directly, bypassing the XML path.
 * Operators configuring {@code labels.copy} via their {@code .cfg} file got a
 * silent no-op.
 *
 * <p>This test parses the actual {@code blueprint.xml} and asserts three
 * invariants:
 * <ol>
 *   <li>Every {@code <property name="X" value="${...}"/>} binding on the
 *       {@code config} bean has a corresponding {@code setX(...)} method on
 *       {@link PrometheusRemoteWriterConfig} with at least one overload whose
 *       argument type Blueprint can coerce from a placeholder String
 *       (String, primitives, primitive wrappers, or an enum).</li>
 *   <li>Every {@code <cm:property name="Y">} entry in the property-placeholder
 *       defaults has a matching {@code <property value="${Y}">} binding on the
 *       {@code config} bean (and vice versa) — catches the "added on one side
 *       but not the other" failure mode.</li>
 *   <li>Every public Java setter on {@link PrometheusRemoteWriterConfig}
 *       (except those explicitly exempted) has a matching
 *       {@code <property name="X">} binding on the {@code config} bean.
 *       This is the direction that catches the v0.3 {@code labels.copy}
 *       regression shape: setter added, blueprint binding and cm:property
 *       both missing — the two other checks above would have stayed silent
 *       because nothing in the XML mentioned {@code labels.copy}.</li>
 * </ol>
 *
 * <p>A maintainer who adds a new knob without wiring every layer will see a
 * named failure from this test rather than a silent no-op in production.
 */
class BlueprintWiringTest {

    private static final String BLUEPRINT_CLASSPATH = "/OSGI-INF/blueprint/blueprint.xml";

    /** Matches {@code ${foo}} / {@code ${foo.bar}} inside a Blueprint value. */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");

    /**
     * Setter names on {@link PrometheusRemoteWriterConfig} that are
     * intentionally NOT exposed as Blueprint {@code <property>} bindings.
     * Empty today — every setter is blueprint-bound. A maintainer adding a
     * setter that must not be blueprint-bound (e.g., a test-only hook or a
     * programmatic-only configuration knob) should add the property name here
     * with a one-line comment explaining why.
     */
    private static final Set<String> SETTERS_NOT_BLUEPRINT_BOUND_BY_DESIGN = Set.of();

    @Test
    void every_config_bean_property_has_a_matching_java_setter() throws Exception {
        Document doc = loadBlueprint();
        Set<String> beanProps = collectConfigBeanPropertyNames(doc);
        assertThat(beanProps)
            .as("config bean must have at least one <property> binding")
            .isNotEmpty();

        Map<String, List<Class<?>>> setters = collectSetters(PrometheusRemoteWriterConfig.class);

        for (String prop : beanProps) {
            List<Class<?>> overloads = setters.get(prop);
            assertThat(overloads)
                .as("blueprint.xml binds <property name=\"%s\"> but "
                    + "PrometheusRemoteWriterConfig has no set%s(...) method — "
                    + "add the setter or remove the binding",
                    prop, capitalize(prop))
                .isNotNull();

            // Blueprint populates bean properties from String placeholder
            // values. At least one setter overload must take a type Blueprint
            // can coerce from a String, or the container rejects the bean at
            // activation. This also guards against a refactor that drops the
            // String-compatible overload from an overloaded pair (e.g.,
            // removing setMetadataCase(String) and leaving only the enum form
            // would break Blueprint binding).
            assertThat(overloads.stream().anyMatch(BlueprintWiringTest::isBlueprintCoercibleFromString))
                .as("blueprint.xml binds <property name=\"%s\"> but no set%s(...) "
                    + "overload accepts a Blueprint-coercible type (String, "
                    + "primitive, primitive wrapper, or enum). Existing overloads: %s",
                    prop, capitalize(prop), overloads)
                .isTrue();
        }
    }

    @Test
    void every_java_setter_has_a_matching_blueprint_property_binding() throws Exception {
        // This is the direction that catches the v0.3 labels.copy regression
        // shape: the Java setter was added, but the blueprint binding AND
        // the cm:property default were BOTH missing. Neither other test fires
        // in that scenario — the XML makes no mention of labels.copy, so
        // there's nothing to compare against a setter or a placeholder.
        //
        // The flip-side check here enumerates every public setter and asserts
        // the blueprint binds it. A new operator-facing knob therefore MUST
        // wire up the binding or be explicitly exempted.
        Document doc = loadBlueprint();
        Set<String> beanProps = collectConfigBeanPropertyNames(doc);
        Map<String, List<Class<?>>> setters = collectSetters(PrometheusRemoteWriterConfig.class);

        Set<String> unboundSetters = new LinkedHashSet<>(setters.keySet());
        unboundSetters.removeAll(beanProps);
        unboundSetters.removeAll(SETTERS_NOT_BLUEPRINT_BOUND_BY_DESIGN);

        assertThat(unboundSetters)
            .as("PrometheusRemoteWriterConfig has public setter(s) with no "
                + "corresponding <property name=\"X\"> binding in blueprint.xml — "
                + "operators configuring these via .cfg will get a silent no-op. "
                + "Add the binding, or (if intentional) add the property name to "
                + "SETTERS_NOT_BLUEPRINT_BOUND_BY_DESIGN with a comment explaining "
                + "why.")
            .isEmpty();
    }

    @Test
    void every_cm_default_is_bound_on_the_config_bean() throws Exception {
        // This is the v0.3-specific failure mode: the Java setter existed but
        // the <cm:property> default AND the <property value="${...}"> binding
        // were both missing. We check both directions so the test fails with
        // a precise name on either side.
        Document doc = loadBlueprint();
        Set<String> cmDefaults = collectCmPropertyNames(doc);
        Set<String> placeholderRefs = collectPlaceholderRefs(doc);

        // Direction 1: every cm:property default should be referenced by a
        // <property value="${…}"> binding.
        Set<String> defaultsMissingBinding = new LinkedHashSet<>(cmDefaults);
        defaultsMissingBinding.removeAll(placeholderRefs);
        assertThat(defaultsMissingBinding)
            .as("<cm:property> default exists but no <property value=\"${X}\"> "
                + "binding references it — operator-facing knob will silently "
                + "no-op. Add the binding on the config bean.")
            .isEmpty();

        // Direction 2: every <property value="${…}"> reference should have a
        // cm:property default (so the placeholder resolves even without a cfg
        // file entry).
        Set<String> refsMissingDefault = new LinkedHashSet<>(placeholderRefs);
        refsMissingDefault.removeAll(cmDefaults);
        assertThat(refsMissingDefault)
            .as("<property value=\"${X}\"> references a placeholder with no "
                + "<cm:property> default — config-placeholder resolution will "
                + "fail at bundle activation unless the operator's cfg file "
                + "defines the key. Add the default.")
            .isEmpty();
    }

    // ---- helpers -----------------------------------------------------------

    private static Document loadBlueprint() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        try (InputStream in = BlueprintWiringTest.class.getResourceAsStream(BLUEPRINT_CLASSPATH)) {
            if (in == null) {
                throw new IllegalStateException(
                    "Could not locate " + BLUEPRINT_CLASSPATH + " on the test classpath. "
                    + "Verify the plugin's resources are compiled and published to the test "
                    + "classpath (Maven should do this automatically).");
            }
            return builder.parse(in);
        }
    }

    /** Collect {@code name} attributes from every {@code <property>} element
     *  (blueprint namespace) nested inside the {@code config} bean. */
    private static Set<String> collectConfigBeanPropertyNames(Document doc) {
        Set<String> out = new LinkedHashSet<>();
        Element configBean = findBeanById(doc, "config");
        NodeList children = configBean.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) continue;
            Element el = (Element) n;
            if ("property".equals(el.getLocalName())) {
                out.add(el.getAttribute("name"));
            }
        }
        return out;
    }

    private static Element findBeanById(Document doc, String id) {
        NodeList beans = doc.getElementsByTagNameNS("*", "bean");
        for (int i = 0; i < beans.getLength(); i++) {
            Element e = (Element) beans.item(i);
            if (id.equals(e.getAttribute("id"))) return e;
        }
        throw new IllegalStateException("No <bean id=\"" + id + "\"> found in blueprint.xml");
    }

    /** Collect {@code name} attributes from every {@code <cm:property>}
     *  element (cm namespace) under the property-placeholder defaults. */
    private static Set<String> collectCmPropertyNames(Document doc) {
        Set<String> out = new LinkedHashSet<>();
        // Namespace-scoped — cm:property lives in the Aries CM namespace, not
        // the default blueprint namespace. `*` wildcard matches both.
        NodeList props = doc.getElementsByTagNameNS("*", "property");
        for (int i = 0; i < props.getLength(); i++) {
            Element el = (Element) props.item(i);
            String ns = el.getNamespaceURI();
            // Only cm:property entries live inside <cm:default-properties>.
            // Filter by namespace to avoid confusion with the bean-level
            // <property> elements the other test counts.
            if (ns != null && ns.contains("blueprint-cm")) {
                String name = el.getAttribute("name");
                if (!name.isEmpty()) out.add(name);
            }
        }
        return out;
    }

    /** Collect every {@code ${X}} referenced by {@code <property value="...">}
     *  on the {@code config} bean. */
    private static Set<String> collectPlaceholderRefs(Document doc) {
        Set<String> out = new LinkedHashSet<>();
        Element configBean = findBeanById(doc, "config");
        NodeList children = configBean.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) continue;
            Element el = (Element) n;
            if (!"property".equals(el.getLocalName())) continue;
            String value = el.getAttribute("value");
            Matcher m = PLACEHOLDER.matcher(value);
            while (m.find()) {
                out.add(m.group(1));
            }
        }
        return out;
    }

    /**
     * Collect every public 1-arg setter as a map keyed by the JavaBean
     * property name (i.e. {@code setWriteUrl} → {@code writeUrl}), with the
     * parameter types of all overloads. This preserves information the
     * previous name-only implementation erased: overloaded setters like
     * {@code setMetadataCase(String)} plus {@code setMetadataCase(MetadataCase)}
     * collapse to one map entry with two list elements — each test can inspect
     * the overload list to assert what it needs.
     */
    private static Map<String, List<Class<?>>> collectSetters(Class<?> clazz) {
        Map<String, List<Class<?>>> out = new LinkedHashMap<>();
        for (Method m : clazz.getMethods()) {
            if (!m.getName().startsWith("set") || m.getName().length() <= 3) continue;
            if (m.getParameterCount() != 1) continue;
            String prop = Character.toLowerCase(m.getName().charAt(3))
                        + m.getName().substring(4);
            out.computeIfAbsent(prop, k -> new ArrayList<>()).add(m.getParameterTypes()[0]);
        }
        return out;
    }

    private static boolean isBlueprintCoercibleFromString(Class<?> type) {
        if (type == String.class) return true;
        if (type.isPrimitive()) return true;
        if (type == Boolean.class || type == Integer.class || type == Long.class
                || type == Short.class || type == Byte.class || type == Double.class
                || type == Float.class || type == Character.class) return true;
        // Aries Blueprint coerces String → enum via Enum.valueOf.
        if (type.isEnum()) return true;
        return false;
    }

    private static String capitalize(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    /**
     * The storage bean MUST stay registered under BOTH names, as two
     * separate {@code <service>} elements (#113):
     *
     * <ul>
     *   <li>the OIA interface with {@code registration.export=true} —
     *       what OpenNMS core's TSS discovery consumes; it must remain
     *       single-objectClass because the export bridge skips a service
     *       carrying any objectClass core cannot load;</li>
     *   <li>the concrete class WITHOUT the export property — what the
     *       shell command's {@code @Reference} resolves. Deleting this
     *       "redundant-looking" registration breaks only the stats
     *       command, and only at runtime.</li>
     * </ul>
     */
    @Test
    void storage_bean_keeps_both_service_registrations() throws Exception {
        Document doc = loadBlueprint();
        org.w3c.dom.NodeList services = doc.getElementsByTagNameNS("*", "service");
        // List, not Set: cardinality matters — a duplicate registration of
        // either interface is exactly the stale-KAR hazard the blueprint
        // comment documents, and must fail here, not in an operator's shell.
        java.util.List<String> interfaces = new java.util.ArrayList<>();
        java.util.Map<String, Boolean> exported = new java.util.HashMap<>();
        for (int i = 0; i < services.getLength(); i++) {
            org.w3c.dom.Element svc = (org.w3c.dom.Element) services.item(i);
            if (!"prometheusRemoteWriterStorage".equals(svc.getAttribute("ref"))) continue;
            String iface = svc.getAttribute("interface");
            interfaces.add(iface);
            boolean hasExport = false;
            org.w3c.dom.NodeList entries = svc.getElementsByTagNameNS("*", "entry");
            for (int j = 0; j < entries.getLength(); j++) {
                org.w3c.dom.Element entry = (org.w3c.dom.Element) entries.item(j);
                if ("registration.export".equals(entry.getAttribute("key"))
                        && "true".equals(entry.getAttribute("value"))) {
                    hasExport = true;
                }
            }
            exported.put(iface, hasExport);
        }
        assertThat(interfaces)
                .as("blueprint.xml must register the storage bean as exactly TWO separate "
                    + "single-interface <service> elements — OIA interface (exported) + "
                    + "concrete class (Karaf-only, feeds the stats command's @Reference). "
                    + "Do NOT merge them into one multi-objectClass registration and do NOT "
                    + "use auto-export: the OpenNMS export bridge skips a service carrying "
                    + "any objectClass core cannot load (#113).")
                .containsExactlyInAnyOrder(
                        "org.opennms.integration.api.v1.timeseries.TimeSeriesStorage",
                        "org.opennms.plugins.prometheus.remotewriter.PrometheusRemoteWriterStorage");
        // The asymmetry IS the contract: exported OIA interface, non-exported
        // concrete class. Flipping either side breaks core discovery or
        // reintroduces the multi-objectClass export bug.
        assertThat(exported.get("org.opennms.integration.api.v1.timeseries.TimeSeriesStorage"))
                .as("the OIA registration must carry registration.export=true")
                .isTrue();
        assertThat(exported.get("org.opennms.plugins.prometheus.remotewriter.PrometheusRemoteWriterStorage"))
                .as("the concrete-class registration must NOT be exported")
                .isFalse();
    }
}
