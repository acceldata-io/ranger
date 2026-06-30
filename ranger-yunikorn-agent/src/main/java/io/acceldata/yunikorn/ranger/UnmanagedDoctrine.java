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
 * How the splicer treats queues that have no Ranger policy.
 *
 * <p>This is a deployment-time choice with real operational consequences:
 *
 * <ul>
 *   <li>{@link #LENIENT}: leave the queue's existing {@code submitacl} and
 *       {@code adminacl} fields untouched. Use this during rollout when not
 *       all queues are managed by Ranger yet, or when operators need an
 *       emergency-override channel via direct ConfigMap edits.</li>
 *
 *   <li>{@link #STRICT}: clear {@code submitacl} and {@code adminacl} on
 *       any queue not represented in the Ranger policy set. Ranger becomes
 *       the sole authority for ACLs; anything not authored in Ranger is
 *       effectively default-denied.</li>
 * </ul>
 *
 * <p>Default is {@link #LENIENT} for safer initial rollout. Migrate to
 * {@link #STRICT} once Ranger covers all queues that should be authorised.
 */
public enum UnmanagedDoctrine {
    LENIENT,
    STRICT
}
