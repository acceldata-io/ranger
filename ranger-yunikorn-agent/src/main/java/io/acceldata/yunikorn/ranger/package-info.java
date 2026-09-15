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
 * Ranger YuniKorn Agent.
 *
 * <p>Polls Apache Ranger Admin for policy changes against a {@code yunikorn}
 * service, translates allow-rules into YuniKorn-format ACL strings, and
 * patches the YuniKorn ConfigMap so the live scheduler enforces them.
 *
 * <p>Designed to run as a sidecar Deployment in the same Kubernetes cluster
 * as YuniKorn. Authenticates to Ranger Admin via basic auth or Kerberos/SPNEGO
 * (see {@link io.acceldata.yunikorn.ranger.AgentConfig.AuthMode}); uses the
 * pod's ServiceAccount for ConfigMap access.
 */
package io.acceldata.yunikorn.ranger;
