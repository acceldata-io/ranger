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
 * Thrown when the candidate {@code queues.yaml} fails YuniKorn's
 * {@code /ws/v1/validate-conf} preflight check — either because YuniKorn
 * actively rejected the document ({@code allowed=false}) or because the
 * validate endpoint could not be reached.
 *
 * <p><b>Fail-closed.</b> Both cases prevent the ConfigMap write. We never
 * persist a config we couldn't prove valid: a rejection means the document
 * is broken; an unreachable validator means we can't be sure it isn't.
 *
 * <p>The sync service catches this, logs it, increments the failure metric,
 * and retries on the next polling cycle. It is NOT fatal to the agent —
 * a transient YuniKorn REST outage self-heals once the endpoint returns.
 */
public class PreflightException extends RuntimeException {

    public PreflightException(String message) {
        super(message);
    }

    public PreflightException(String message, Throwable cause) {
        super(message, cause);
    }
}
