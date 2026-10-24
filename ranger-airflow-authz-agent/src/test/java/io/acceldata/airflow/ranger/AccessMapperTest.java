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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AccessMapperTest {

    @Test
    @DisplayName("DAG trigger maps POST+RUN to Ranger trigger")
    void dagTrigger() {
        AccessMapper.Result r = AccessMapper.map("dag", "POST", "RUN");
        assertThat(r.status).isEqualTo(AccessMapper.Status.MAPPED);
        assertThat(r.resourceType).isEqualTo("dag");
        assertThat(r.accessType).isEqualTo("trigger");
    }

    @Test
    @DisplayName("DAG object GET maps to read")
    void dagRead() {
        AccessMapper.Result r = AccessMapper.map("dag", "GET", null);
        assertThat(r.status).isEqualTo(AccessMapper.Status.MAPPED);
        assertThat(r.accessType).isEqualTo("read");
    }

    @Test
    @DisplayName("connection CRUD maps 1:1")
    void connectionCrud() {
        assertThat(AccessMapper.map("connection", "GET", null).accessType).isEqualTo("read");
        assertThat(AccessMapper.map("connection", "POST", null).accessType).isEqualTo("create");
        assertThat(AccessMapper.map("connection", "PUT", null).accessType).isEqualTo("edit");
        assertThat(AccessMapper.map("connection", "DELETE", null).accessType).isEqualTo("delete");
    }

    @Test
    @DisplayName("POST on config is known vocabulary but unmapped")
    void configPostUnmapped() {
        AccessMapper.Result r = AccessMapper.map("config", "POST", null);
        assertThat(r.status).isEqualTo(AccessMapper.Status.UNMAPPED);
    }

    @Test
    @DisplayName("unknown resource_type is 422 vocabulary, not unmapped")
    void unknownResourceType() {
        AccessMapper.Result r = AccessMapper.map("backfill", "GET", null);
        assertThat(r.status).isEqualTo(AccessMapper.Status.UNKNOWN_VOCABULARY);
    }

    @Test
    @DisplayName("unknown access_entity is 422")
    void unknownAccessEntity() {
        AccessMapper.Result r = AccessMapper.map("dag", "GET", "NOT_A_THING");
        assertThat(r.status).isEqualTo(AccessMapper.Status.UNKNOWN_VOCABULARY);
    }

    @Test
    @DisplayName("method is case-insensitive")
    void methodCase() {
        assertThat(AccessMapper.map("dag", "post", "RUN").accessType).isEqualTo("trigger");
    }

    @Test
    @DisplayName("TASK_INSTANCE PUT and DELETE both clear_task")
    void clearTask() {
        assertThat(AccessMapper.map("dag", "PUT", "TASK_INSTANCE").accessType).isEqualTo("clear_task");
        assertThat(AccessMapper.map("dag", "DELETE", "TASK_INSTANCE").accessType).isEqualTo("clear_task");
    }
}
