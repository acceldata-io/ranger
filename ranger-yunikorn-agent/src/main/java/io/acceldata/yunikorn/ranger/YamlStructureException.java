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

/**
 * Thrown when {@link AclSplicer} encounters a {@code queues.yaml} document
 * whose structure does not match YuniKorn's schema.
 *
 * <p>Examples: missing top-level {@code partitions}, a queue node lacking a
 * {@code name}, or a {@code queues} field that isn't a list. These conditions
 * indicate a malformed ConfigMap and the caller should refuse to write back
 * any modified version.
 *
 * <p>Note: a partition that legitimately has no {@code queues} key, or a leaf
 * queue with no {@code queues} children, is NOT a structural error. The
 * splicer skips these cases without raising.
 */
public class YamlStructureException extends RuntimeException {

    public YamlStructureException(String message) {
        super(message);
    }

    public YamlStructureException(String message, Throwable cause) {
        super(message, cause);
    }
}
