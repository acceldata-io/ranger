/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.airflow.ranger;

import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps the service-def and the mapper from drifting apart.
 *
 * <p>Two things are stated twice in this project: which Ranger access types make
 * sense for a given Airflow resource. {@link AccessMapper} knows it because it
 * decides what to emit; {@code ranger-servicedef-airflow.json} states it in each
 * resource's {@code accessTypeRestrictions}, which is what filters the checkboxes
 * an admin sees in the Ranger Admin policy form.
 *
 * <p>Rather than maintain the JSON list by hand, this test derives the answer from
 * the mapper -- walk every (resource_type, method, access_entity) combination and
 * collect whatever comes back MAPPED -- and asserts the JSON agrees. Extend the
 * mapper in M2 and this test tells you exactly which JSON line to update.
 *
 * <p>An unrestricted resource ({@code accessTypeRestrictions} absent or empty)
 * means "all access types allowed" to Ranger, per
 * {@code RangerBaseService.getAllowedAccesses}. That is almost never what we want
 * here, so it is treated as a failure rather than tolerated.
 */
class ServiceDefConsistencyTest {

    private static RangerServiceDef serviceDef;

    @BeforeAll
    static void loadServiceDef() throws Exception {
        try (InputStream in = RangerServiceDef.class.getResourceAsStream(
                "/service-defs/ranger-servicedef-airflow.json")) {
            assertThat(in).as("airflow service-def on classpath").isNotNull();
            serviceDef = JsonUtils.jsonToObject(
                    new InputStreamReader(in, StandardCharsets.UTF_8), RangerServiceDef.class);
        }
        assertThat(serviceDef).isNotNull();
    }

    @Test
    @DisplayName("every access type the mapper emits is declared in the service-def")
    void mapperEmitsOnlyDeclaredAccessTypes() {
        Set<String> declared = serviceDef.getAccessTypes().stream()
                .map(RangerAccessTypeDef::getName)
                .collect(Collectors.toSet());

        Set<String> emitted = new TreeSet<>();
        reachableByResource().values().forEach(emitted::addAll);

        assertThat(declared)
                .as("access types in the service-def, which is what an admin can grant")
                .containsAll(emitted);
    }

    @Test
    @DisplayName("mapper and service-def agree on the resource types")
    void resourceTypesMatch() {
        Set<String> inServiceDef = serviceDef.getResources().stream()
                .map(RangerResourceDef::getName)
                .collect(Collectors.toCollection(TreeSet::new));

        assertThat(new TreeSet<>(AccessMapper.resourceTypes()))
                .as("a resource added to one side only is a silent hole")
                .isEqualTo(inServiceDef);
    }

    @Test
    @DisplayName("accessTypeRestrictions match what the mapper can reach for each resource")
    void restrictionsMatchMapper() {
        Map<String, Set<String>> reachable = reachableByResource();
        List<String> problems = new ArrayList<>();

        for (RangerResourceDef def : serviceDef.getResources()) {
            Set<String> expected = reachable.getOrDefault(def.getName(), Set.of());
            Set<String> actual = def.getAccessTypeRestrictions() == null
                    ? Set.of()
                    : new TreeSet<>(def.getAccessTypeRestrictions());

            if (actual.isEmpty()) {
                problems.add(def.getName() + ": no accessTypeRestrictions, so Ranger shows all "
                        + serviceDef.getAccessTypes().size() + " access types. Expected "
                        + new TreeSet<>(expected));
                continue;
            }

            Set<String> missing = new TreeSet<>(expected);
            missing.removeAll(actual);
            Set<String> unreachable = new TreeSet<>(actual);
            unreachable.removeAll(expected);

            if (!missing.isEmpty()) {
                problems.add(def.getName() + ": mapper can emit " + missing
                        + " but the service-def does not offer them, so no admin can grant them");
            }
            if (!unreachable.isEmpty()) {
                problems.add(def.getName() + ": service-def offers " + unreachable
                        + " but the mapper never emits them, so ticking the box does nothing");
            }
        }

        assertThat(problems).as("service-def / mapper drift").isEmpty();
    }

    @Test
    @DisplayName("dag offers every access type except create")
    void dagIsFullyCoveredExceptCreate() {
        // Pinned deliberately. DAGs are not created through the API -- a DAG appears
        // because a file appeared in the DAG bundle -- so `create` is unreachable for
        // dag and must not be offered. If this ever changes, it is a design decision
        // and not an incidental edit.
        Set<String> declared = serviceDef.getAccessTypes().stream()
                .map(RangerAccessTypeDef::getName)
                .collect(Collectors.toCollection(TreeSet::new));
        Set<String> expected = new TreeSet<>(declared);
        expected.remove("create");

        assertThat(reachableByResource().get("dag")).isEqualTo(expected);
    }

    /**
     * Walks the mapper's whole input space and records, per resource type, every
     * Ranger access type that comes back MAPPED. {@code null} is included in the
     * access_entity dimension because most resources take no entity at all.
     */
    private static Map<String, Set<String>> reachableByResource() {
        Map<String, Set<String>> ret = new HashMap<>();

        Set<String> entities = new HashSet<>(AccessMapper.accessEntities());
        entities.add(null);

        for (String resourceType : AccessMapper.resourceTypes()) {
            Set<String> accessTypes = new TreeSet<>();
            for (String method : AccessMapper.methods()) {
                for (String entity : entities) {
                    AccessMapper.Result result = AccessMapper.map(resourceType, method, entity);
                    if (result.status == AccessMapper.Status.MAPPED) {
                        accessTypes.add(result.accessType);
                    }
                }
            }
            ret.put(resourceType, accessTypes);
        }
        return ret;
    }
}
