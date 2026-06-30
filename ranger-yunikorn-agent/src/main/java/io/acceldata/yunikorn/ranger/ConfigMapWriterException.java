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
 * Thrown by {@link ConfigMapWriter} when it cannot complete an applyAcls
 * operation — fetch failures, missing ConfigMap or key, or exhausted CAS
 * retries.
 *
 * <p>The sync service catches this, logs it, increments a failure metric,
 * and tries again on the next polling cycle. It is NOT fatal to the agent.
 */
public class ConfigMapWriterException extends RuntimeException {

    public ConfigMapWriterException(String message) {
        super(message);
    }

    public ConfigMapWriterException(String message, Throwable cause) {
        super(message, cause);
    }
}
