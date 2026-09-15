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
 * Validates a candidate {@code queues.yaml} document before it is persisted
 * to the YuniKorn ConfigMap.
 *
 * <p>Extracted as an interface so {@link ConfigMapWriter} can run without a
 * validator (when preflight is disabled or no YuniKorn REST URL is
 * configured) and so tests can inject a fake without standing up an HTTP
 * server. The production implementation is {@link YuniKornPreflightClient}.
 */
public interface PreflightValidator {

    /**
     * Validate {@code queuesYaml}. Returns normally if the document is
     * acceptable to YuniKorn.
     *
     * @throws PreflightException if YuniKorn rejected the document or the
     *                            validation endpoint could not be reached.
     */
    void validate(String queuesYaml) throws PreflightException;
}
