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

import java.util.Objects;

/**
 * The unit of "Ranger has a policy for queue X" that flows from the converter
 * to the splicer.
 *
 * <p>Both ACL strings are {@code null} when the corresponding access type has
 * no Ranger policy granting it. Distinguishing "no policy" from "empty ACL"
 * matters: the splicer treats {@code null} as "do not write this field" while
 * an empty string would be a legitimate no-allow ACL value.
 *
 * <p>ACL strings follow YuniKorn's native format:
 * {@code "user1,user2 group1,group2"} (users space-separated from groups,
 * comma-separated within each side). See
 * {@code yunikorn-core/pkg/common/security/acl.go}.
 *
 * <p>Value type. Equality is by all three fields. Immutable.
 */
public final class QueueAclEntry {

    private final String queuePath;
    private final String submitAcl;
    private final String adminAcl;

    /**
     * @param queuePath full dotted queue path, e.g. {@code "root.research.nlp"}.
     *                  Must not be null or blank.
     * @param submitAcl YuniKorn-format submit ACL, or {@code null} if Ranger has
     *                  no submit policy for this queue.
     * @param adminAcl  YuniKorn-format admin ACL, or {@code null} if Ranger has
     *                  no admin policy for this queue.
     */
    public QueueAclEntry(String queuePath, String submitAcl, String adminAcl) {
        if (queuePath == null || queuePath.isBlank()) {
            throw new IllegalArgumentException("queuePath must not be null or blank");
        }
        this.queuePath = queuePath;
        this.submitAcl = submitAcl;
        this.adminAcl  = adminAcl;
    }

    public String queuePath() {
        return queuePath;
    }

    public String submitAcl() {
        return submitAcl;
    }

    public String adminAcl() {
        return adminAcl;
    }

    /** True when neither ACL is set — the entry is effectively a no-op. */
    public boolean isEmpty() {
        return submitAcl == null && adminAcl == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueueAclEntry)) return false;
        QueueAclEntry that = (QueueAclEntry) o;
        return queuePath.equals(that.queuePath)
                && Objects.equals(submitAcl, that.submitAcl)
                && Objects.equals(adminAcl,  that.adminAcl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queuePath, submitAcl, adminAcl);
    }

    @Override
    public String toString() {
        return "QueueAclEntry{" +
                "queuePath='" + queuePath + '\'' +
                ", submitAcl=" + (submitAcl == null ? "null" : "'" + submitAcl + "'") +
                ", adminAcl="  + (adminAcl  == null ? "null" : "'" + adminAcl  + "'") +
                '}';
    }
}
