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

import org.apache.ranger.admin.client.RangerAdminClient;

/**
 * Builds the {@link RangerAdminClient} the agent uses to talk to Ranger Admin.
 *
 * <p>Always returns a {@link RangerHttpClient} — our own thin HTTP client.
 * Both auth modes are handled inside that one class:
 *
 * <ul>
 *   <li>{@code basic} — adds an {@code Authorization: Basic} header on each
 *       request</li>
 *   <li>{@code kerberos} — performs a JAAS keytab login at startup, then
 *       builds {@code Authorization: Negotiate} headers per request via
 *       direct GSSAPI calls (see {@link SpnegoAuthenticator})</li>
 * </ul>
 *
 * <p><b>Why not the official {@code RangerAdminRESTClient}?</b> We tried.
 * In our shaded-jar deployment topology, the official client either:
 * <ul>
 *   <li>routes basic-auth requests through Hadoop UGI in {@code SIMPLE} mode
 *       and ends up on the plain {@code /service/plugins/policies/download/...}
 *       path which Ranger Admin's auth filter rejects with a misleading 400, or</li>
 *   <li>logs in via Kerberos but doesn't actually perform the second leg of the
 *       SPNEGO handshake when challenged — even with
 *       {@code useSubjectCredsOnly=false}, a JAAS config file pointing at the
 *       keytab, and {@code policy.rest.client.use.kerberos=true} all set.</li>
 * </ul>
 *
 * <p>Rather than keep chasing those, we do what curl does: build the URL
 * ourselves, sign the headers ourselves, parse the response. This trades the
 * official client's delta protocol + on-disk cache + HA URL list for a
 * 200-line agent that actually works against the deployment topology we ship in.
 *
 * <p>This mirrors the same exit Gravitino took when their original
 * {@code XDPOAuth2} client approach didn't work — they switched to
 * forwarding headers directly (see commit {@code XDP-937}).
 */
public final class RangerAdminClientFactory {

    public static RangerAdminClient create(AgentConfig config) {
        return new RangerHttpClient(config);
    }

    private RangerAdminClientFactory() {
        // utility
    }
}
