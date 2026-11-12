/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.acceldata.mlflow.ranger;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

/**
 * Generates {@code Authorization: Negotiate <token>} headers using Kerberos/SPNEGO.
 */
final class SpnegoAuthenticator {

    private static final String SPNEGO_OID       = "1.3.6.1.5.5.2";
    private static final String JAAS_ENTRY_NAME  = "com.sun.security.jgss.krb5.initiate";

    private final Subject subject;

    SpnegoAuthenticator() throws LoginException {
        LoginContext lc = new LoginContext(JAAS_ENTRY_NAME);
        lc.login();
        this.subject = lc.getSubject();
    }

    String buildAuthorizationHeader(URI uri) throws Exception {
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("URI has no host component: " + uri);
        }
        try {
            byte[] token = Subject.doAs(subject, (PrivilegedExceptionAction<byte[]>) () -> {
                GSSManager manager = GSSManager.getInstance();
                Oid spnegoOid = new Oid(SPNEGO_OID);
                GSSName serverName = manager.createName(
                        "HTTP@" + host, GSSName.NT_HOSTBASED_SERVICE);
                GSSContext context = manager.createContext(
                        serverName.canonicalize(spnegoOid),
                        spnegoOid,
                        null,
                        GSSContext.DEFAULT_LIFETIME);
                try {
                    context.requestMutualAuth(false);
                    context.requestCredDeleg(false);
                    return context.initSecContext(new byte[0], 0, 0);
                } finally {
                    context.dispose();
                }
            });
            return "Negotiate " + Base64.getEncoder().encodeToString(token);
        } catch (PrivilegedActionException pae) {
            throw pae.getException();
        }
    }
}
