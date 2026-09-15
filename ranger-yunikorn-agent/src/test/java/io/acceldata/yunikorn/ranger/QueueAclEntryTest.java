/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.acceldata.yunikorn.ranger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueueAclEntryTest {

    @Test
    void rejectsNullPath() {
        assertThatThrownBy(() -> new QueueAclEntry(null, "a", "b"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankPath() {
        assertThatThrownBy(() -> new QueueAclEntry("   ", "a", "b"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void allowsNullAcls() {
        QueueAclEntry e = new QueueAclEntry("root.x", null, null);
        assertThat(e.submitAcl()).isNull();
        assertThat(e.adminAcl()).isNull();
        assertThat(e.isEmpty()).isTrue();
    }

    @Test
    void equalityByAllFields() {
        QueueAclEntry a = new QueueAclEntry("root.x", "alice", "admin");
        QueueAclEntry b = new QueueAclEntry("root.x", "alice", "admin");
        QueueAclEntry c = new QueueAclEntry("root.x", "bob",   "admin");

        assertThat(a).isEqualTo(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void toStringMasksNullVsEmpty() {
        QueueAclEntry withNull  = new QueueAclEntry("root.x", null, "admin");
        QueueAclEntry withEmpty = new QueueAclEntry("root.x", "",   "admin");
        assertThat(withNull.toString()).contains("submitAcl=null");
        assertThat(withEmpty.toString()).contains("submitAcl=''");
    }
}
