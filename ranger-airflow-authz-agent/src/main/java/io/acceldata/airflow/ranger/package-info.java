/*
 * Copyright 2026 Acceldata Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Ranger Airflow authorization agent.
 *
 * <p>A colocated policy decision point that embeds {@code RangerBasePlugin}
 * and answers authorization questions from {@code odp-airflow-ranger-auth-manager}
 * over loopback HTTP. Binds to 127.0.0.1 only; authenticates the local client
 * with a shared secret.
 */
package io.acceldata.airflow.ranger;
