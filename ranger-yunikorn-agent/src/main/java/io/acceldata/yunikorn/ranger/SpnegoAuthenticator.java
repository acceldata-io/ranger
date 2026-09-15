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

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Date;

/**
 * Builds {@code Authorization: Negotiate <token>} headers using SPNEGO,
 * driven directly by the JDK's GSSAPI ({@code org.ietf.jgss}) rather than
 * the Ranger admin client's HTTP plumbing.
 *
 * <p><b>Why this exists:</b> the official {@code RangerAdminRESTClient} in
 * our shaded JAR doesn't reliably perform the second leg of SPNEGO when
 * Ranger Admin returns {@code 401 + WWW-Authenticate: Negotiate}. After a
 * full debug cycle (DNS verified, JAAS file generated, {@code
 * useSubjectCredsOnly=false} set, {@code policy.rest.client.use.kerberos=true}
 * set, krb5.conf correct, principal valid, curl with the same ticket works
 * fine) we still saw the agent send unauthenticated requests. Rather than
 * chase the remaining unknown in the Ranger client's internals, this class
 * does what curl does: log in via JAAS, get a service ticket via GSS, send
 * the token in the {@code Authorization} header.
 *
 * <p>This mirrors the exit Gravitino took when their original
 * {@code XDPOAuth2} approach didn't work — they switched to forwarding
 * headers directly (commit {@code XDP-937}).
 *
 * <h2>How SPNEGO works (annotated)</h2>
 * <ol>
 *   <li>Login: {@code LoginContext("com.sun.security.jgss.krb5.initiate")}
 *       finds the JAAS section we generate at startup, runs the
 *       {@code Krb5LoginModule} with {@code keyTab=...} and
 *       {@code principal=...}, populates a {@link Subject} with our
 *       Kerberos credentials.</li>
 *   <li>Server principal: SPNEGO requires the target service principal,
 *       conventionally {@code HTTP/<host>@<REALM>}. We derive
 *       {@code <host>} from the URL and let the KDC use its default realm
 *       (configured in krb5.conf).</li>
 *   <li>Token: inside {@code Subject.doAs(...)}, we ask the GSSManager
 *       for a SPNEGO context targeted at that service principal, call
 *       {@code initSecContext(emptyToken)}, and the JDK does the KDC
 *       round-trip (TGS-REQ for the service ticket) and produces a
 *       SPNEGO-wrapped Kerberos AP-REQ token.</li>
 *   <li>Header: base64-encode the token, prefix with {@code "Negotiate "},
 *       slap it on the request as {@code Authorization}.</li>
 * </ol>
 *
 * <p>One {@link SpnegoAuthenticator} instance is reusable across requests:
 * the JAAS login populates a {@link Subject} that is reused for service-ticket
 * acquisition (one fast KDC round-trip per call).
 *
 * <h2>TGT renewal (keytab re-login)</h2>
 * The TGT obtained at login has a finite lifetime (commonly 10–24h). Without
 * refresh, every Ranger fetch would start failing with {@code "No valid
 * credentials provided"} once it expires, and stay broken until the pod is
 * restarted. Because we authenticate from a <b>keytab</b> (not a ticket
 * cache), the robust fix is to <b>re-login from the keytab</b> — re-read it
 * and obtain a fresh TGT — rather than renew the existing ticket (renewal
 * only works within the renewable window and not at all once fully expired).
 * {@link #reloginFromKeytabIfNeeded()} runs before each header build and
 * re-logs in once the current TGT has burned through {@link #RELOGIN_RATIO}
 * of its lifetime, mirroring Hadoop's {@code UserGroupInformation}
 * relogin heuristic. The check is cheap (a timestamp comparison); an actual
 * re-login only happens near expiry.
 */
public class SpnegoAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(SpnegoAuthenticator.class);

    /** SPNEGO mechanism OID, RFC 4178. */
    private static final Oid SPNEGO_OID;

    static {
        try {
            SPNEGO_OID = new Oid("1.3.6.1.5.5.2");
        } catch (GSSException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * JAAS section name where the JDK looks for the {@code Krb5LoginModule}
     * config. Matches what we write in
     * {@link RangerYuniKornAgent#writeJaasConfigForSpnego(String, String)}.
     */
    private static final String JAAS_SECTION = "com.sun.security.jgss.krb5.initiate";

    /**
     * Re-login from the keytab once the current TGT has burned through this
     * fraction of its validity window. 0.8 mirrors Hadoop's
     * {@code UserGroupInformation} relogin heuristic — refresh with comfortable
     * margin before the KDC stops accepting the ticket, never on the boundary.
     */
    private static final double RELOGIN_RATIO = 0.8d;

    /** Fallback margin before expiry used when the ticket carries no start time. */
    private static final long RELOGIN_FALLBACK_BUFFER_MS = 10 * 60 * 1000L;

    /**
     * The active LoginContext. On re-login we build a <em>fresh</em> context
     * (not logout+login on this one) and swap both fields only after the new
     * login succeeds, so a failed re-login can never empty the live Subject and
     * an in-flight {@code doAs} keeps using a never-mutated instance.
     */
    private volatile LoginContext loginContext;
    /** Replaced on every successful re-login; read via a local in {@link #buildAuthorizationHeader}. */
    private volatile Subject subject;
    private final String  realm;
    private final Object  reloginLock = new Object();

    /**
     * Performs the JAAS login eagerly so any keytab/principal misconfiguration
     * fails fast at startup instead of on the first sync cycle.
     */
    public SpnegoAuthenticator() throws LoginException {
        this.loginContext = new LoginContext(JAAS_SECTION);
        loginContext.login();
        this.subject = loginContext.getSubject();
        this.realm   = extractRealmFromSubject(subject);
        KerberosTicket tgt = findTgt(subject);
        LOG.info("SPNEGO authenticator ready. Subject principals: {}, realm={}, TGT valid until {}",
                subject.getPrincipals(), realm, tgt != null ? tgt.getEndTime() : "<unknown>");
    }

    /**
     * Pull the realm out of the logged-in subject's Kerberos principal. We
     * need this to construct fully-qualified service principal names later;
     * relying on the JDK's default-realm canonicalisation has proven brittle.
     */
    private static String extractRealmFromSubject(Subject subject) {
        for (java.security.Principal p : subject.getPrincipals()) {
            String name = p.getName();          // e.g. "admin/admin@ADSRE.COM"
            int at = name.lastIndexOf('@');
            if (at > 0 && at < name.length() - 1) {
                return name.substring(at + 1);
            }
        }
        throw new IllegalStateException(
                "JAAS login produced no Kerberos principal with a realm. " +
                "Check kerberos.principal in agent.properties.");
    }

    /**
     * Build the {@code Authorization: Negotiate <token>} header value for a
     * given target URL. Returns the full header value (including the
     * {@code Negotiate } prefix), ready to drop into
     * {@code HttpURLConnection.setRequestProperty("Authorization", ...)}.
     *
     * <p><b>Hostname canonicalisation:</b> Kerberos service principals are
     * keyed on hostnames (e.g. {@code HTTP/ranger.example.com@REALM}), not
     * IP addresses. If the URL uses an IP, the JDK by default asks the KDC
     * for {@code HTTP/<ip>@REALM} which won't exist, and the request fails
     * with {@code "Server not found in Kerberos database"}. We mimic what
     * curl's {@code --negotiate} does: when the URL's host is an IP, do a
     * reverse-DNS lookup and use the resulting hostname for the SPN.
     *
     * @throws GSSException              if the JDK can't acquire a service
     *                                   ticket (KDC unreachable, principal
     *                                   unknown, clock skew, etc.)
     * @throws PrivilegedActionException if the doAs block raises
     */
    public String buildAuthorizationHeader(URI targetUrl)
            throws GSSException, PrivilegedActionException {
        // Refresh the TGT from the keytab if it's expired or close to it, so a
        // long-running agent never falls off the end of its ticket lifetime.
        reloginFromKeytabIfNeeded();
        Subject current = this.subject;

        String host = targetUrl.getHost();
        String resolvedHost = canonicalHostForSpn(host);

        // Build the FULL principal name including the realm, so we can pass
        // it via NT_USER_NAME and skip the JDK's NT_HOSTBASED_SERVICE
        // canonicalisation. With NT_USER_NAME, the JDK asks the KDC for
        // exactly the SPN string we provide — same as what `kvno` does — so
        // there's no chance of forward/reverse DNS round-trips changing
        // the hostname into something the KDC doesn't have registered.
        String spn = "HTTP/" + resolvedHost + "@" + realm;
        if (!resolvedHost.equals(host)) {
            LOG.debug("SPN hostname canonicalised from URL host '{}' to '{}'", host, resolvedHost);
        }
        LOG.debug("Requesting service ticket for SPN: {}", spn);
        return Subject.doAs(current,
                (PrivilegedExceptionAction<String>) () -> generateNegotiateToken(spn));
    }

    /**
     * Re-acquire a TGT from the keytab when the current one is expired or has
     * burned through {@link #RELOGIN_RATIO} of its lifetime. Cheap on the
     * common path (a single timestamp comparison); only performs the
     * logout/login round-trip near expiry.
     *
     * <p>A failed re-login is logged and swallowed: the in-flight request will
     * proceed with the stale subject (and likely fail, to be retried next
     * cycle) rather than throwing here. This keeps a transient KDC blip from
     * being indistinguishable from a fatal misconfiguration.
     */
    private void reloginFromKeytabIfNeeded() {
        if (!needsRelogin(findTgt(this.subject))) {
            return;
        }
        synchronized (reloginLock) {
            // Re-check under the lock: another caller may have just refreshed it.
            if (!needsRelogin(findTgt(this.subject))) {
                return;
            }
            LoginContext previous = this.loginContext;
            try {
                LOG.info("Kerberos TGT expired or nearing expiry; re-logging in from keytab...");
                // Log into a FRESH context/Subject. Only swap the live fields
                // after login() succeeds, so a failure leaves the existing
                // Subject (and any still-valid ticket) untouched, and an
                // in-flight doAs never observes a half-cleared Subject.
                LoginContext fresh = new LoginContext(JAAS_SECTION);
                fresh.login();
                this.subject      = fresh.getSubject();
                this.loginContext = fresh;
                KerberosTicket tgt = findTgt(this.subject);
                LOG.info("Kerberos re-login from keytab succeeded; new TGT valid until {}",
                        tgt != null ? tgt.getEndTime() : "<unknown>");
                // Release the old context's credentials/service tickets, best
                // effort — after the swap, so a hiccup here can't affect us.
                try {
                    previous.logout();
                } catch (LoginException ignored) {
                    // old creds will be GC'd with the old Subject anyway
                }
            } catch (LoginException e) {
                LOG.error("Kerberos re-login from keytab FAILED; continuing with the existing " +
                        "ticket. Ranger fetches will fail once it expires, until this recovers. " +
                        "Check keytab validity (kvno after any password rotation) and KDC " +
                        "reachability/clock sync.", e);
            }
        }
    }

    /** True if the TGT is missing, destroyed, expired, or past its refresh threshold. Visible for testing. */
    static boolean needsRelogin(KerberosTicket tgt) {
        if (tgt == null || tgt.isDestroyed()) {
            return true;
        }
        Date end = tgt.getEndTime();
        if (end == null) {
            return true;
        }
        long now = System.currentTimeMillis();
        Date start = tgt.getStartTime();
        long threshold = (start == null)
                ? end.getTime() - RELOGIN_FALLBACK_BUFFER_MS
                : start.getTime() + (long) ((end.getTime() - start.getTime()) * RELOGIN_RATIO);
        return now >= threshold;
    }

    /**
     * Find the ticket-granting ticket in the subject's private credentials.
     * The TGT is the {@link KerberosTicket} whose server principal is
     * {@code krbtgt/REALM@REALM}; service tickets (e.g. {@code HTTP/...}) are
     * skipped so we measure lifetime against the credential that actually
     * gates re-login.
     */
    private static KerberosTicket findTgt(Subject subject) {
        if (subject == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        KerberosTicket best = null;
        for (KerberosTicket t : subject.getPrivateCredentials(KerberosTicket.class)) {
            // TGT only (krbtgt/REALM@REALM), never a service ticket.
            if (t.getServer() == null || !t.getServer().getName().startsWith("krbtgt/")) {
                continue;
            }
            // Skip destroyed/expired tickets so a stale one left behind after a
            // renewal can't be mistaken for the live TGT (which would otherwise
            // make every call trigger a needless re-login). Among valid TGTs,
            // keep the one that lives longest.
            Date end = t.getEndTime();
            if (t.isDestroyed() || end == null || end.getTime() <= now) {
                continue;
            }
            if (best == null || end.after(best.getEndTime())) {
                best = t;
            }
        }
        return best;
    }

    /**
     * If {@code host} looks like a numeric IP address, perform a reverse-DNS
     * lookup and return the canonical hostname. Otherwise return {@code host}
     * unchanged.
     *
     * <p>If the reverse lookup fails, we fall back to the original IP — the
     * caller will then get a clear "Server not found in Kerberos database"
     * error from the KDC, which is what we want over silently failing.
     */
    static String canonicalHostForSpn(String host) {
        if (host == null || host.isEmpty()) return host;
        // Cheap check: is it numeric? (works for IPv4; IPv6 already has colons
        // which won't appear in URL hosts unless bracketed.)
        if (!Character.isDigit(host.charAt(0))) {
            return host;
        }
        try {
            java.net.InetAddress addr = java.net.InetAddress.getByName(host);
            String fqdn = addr.getCanonicalHostName();
            // Some JDKs return the IP back when reverse DNS fails. Detect that
            // and fall through to the original input.
            if (fqdn != null && !fqdn.equals(host)) {
                return fqdn;
            }
        } catch (Throwable t) {
            LOG.warn("Reverse DNS lookup for '{}' failed; using host as-is. " +
                    "If Kerberos auth fails with 'Server not found in Kerberos " +
                    "database', use the FQDN in ranger.admin.url instead of an IP.", host, t);
        }
        return host;
    }

    private String generateNegotiateToken(String fullServicePrincipal) throws GSSException {
        GSSManager manager = GSSManager.getInstance();

        // NT_USER_NAME with the full "service/host@REALM" string tells the
        // JDK: "ask the KDC for *exactly* this principal, don't canonicalise."
        // This sidesteps the JDK's NT_HOSTBASED_SERVICE machinery, which
        // does its own forward/reverse DNS lookups and can produce an SPN
        // that doesn't match what's registered in the KDC. We confirmed
        // empirically (via `kvno HTTP/<full-host>@REALM`) that the KDC
        // accepts the literal SPN; this matches that exactly.
        GSSName serverName = manager.createName(fullServicePrincipal,
                GSSName.NT_USER_NAME);

        GSSContext context = manager.createContext(
                serverName,
                SPNEGO_OID,
                /* GSSCredential — null = use the Subject's creds */ null,
                GSSContext.DEFAULT_LIFETIME);

        try {
            context.requestMutualAuth(true);
            context.requestCredDeleg(false);

            // Drive the handshake. For SPNEGO/Kerberos, this completes in
            // one shot — initSecContext returns the wrapped AP-REQ token.
            byte[] outgoing = context.initSecContext(new byte[0], 0, 0);
            if (outgoing == null) {
                throw new GSSException(GSSException.FAILURE, 0,
                        "GSS produced an empty SPNEGO token for SPN=" + fullServicePrincipal);
            }
            String b64 = Base64.getEncoder().encodeToString(outgoing);
            return "Negotiate " + b64;
        } finally {
            try { context.dispose(); } catch (GSSException ignored) {}
        }
    }
}
