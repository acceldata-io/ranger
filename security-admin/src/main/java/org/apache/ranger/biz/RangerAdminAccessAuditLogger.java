/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.biz;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAccessAudit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Access-audit writer for Ranger Admin's access check endpoint.
 *
 * <p>Where events go depends on {@code ranger.audit.source.type}:
 * <ul>
 *   <li>{@code db}: events are queued and written to the Ranger Admin audit table ({@link XXAccessAudit})
 *       on a background thread.</li>
 *   <li>anything else (default {@code solr}): events are handed to {@link AuditProviderFactory}, the same
 *       audit pipeline Ranger plugins use. The destinations (Solr URL, collection, Kerberos and so on) come
 *       from the {@code xasecure.audit.*} properties in <b>Ranger Admin's own</b> site configuration, and
 *       {@code xasecure.audit.is.enabled} must be {@code true}. If it is not, events are dropped and a
 *       warning is logged at startup.</li>
 * </ul>
 */
@Component
public class RangerAdminAccessAuditLogger {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAdminAccessAuditLogger.class);

    private static final int  MAX_QUEUE_SIZE = 10_000;
    private static final int  MAX_BATCH_SIZE = 100;
    private static final long POLL_WAIT_MS   = 500L;

    @Autowired(required = false)
    RangerDaoManager daoManager;

    private final BlockingQueue<AuthzAuditEvent> queue   = new LinkedBlockingQueue<AuthzAuditEvent>(MAX_QUEUE_SIZE);
    private final AtomicLong                     seqGen  = new AtomicLong(0L);
    private final AtomicBoolean                  running = new AtomicBoolean(false);
    private final String                         localHostName;

    private volatile boolean                    dbMode;
    private volatile AuditHandler               externalAuditHandler;
    private volatile Consumer<AuthzAuditEvent>  testSink;
    private Thread                              workerThread;

    public RangerAdminAccessAuditLogger() {
        String host = "ranger-admin";
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception ignored) {
            // keep default fallback
        }
        this.localHostName = host;
    }

    @PostConstruct
    public void init() {
        String auditSource = PropertiesUtil.getProperty("ranger.audit.source.type", "solr")
                .trim().toLowerCase(Locale.ROOT);

        if ("db".equals(auditSource)) {
            dbMode = true;
            startDbWriter();
            return;
        }

        Properties props = PropertiesUtil.getProps();
        if (props != null && "true".equalsIgnoreCase(props.getProperty(AuditProviderFactory.AUDIT_IS_ENABLED_PROP))) {
            try {
                AuditProviderFactory factory = AuditProviderFactory.getInstance();
                if (!factory.isInitDone()) {
                    factory.init(props, "ranger-access-check");
                }
                externalAuditHandler = factory.getAuditProvider();
            } catch (Exception e) {
                LOG.error("Unable to initialize AuditProviderFactory for Ranger access check audit", e);
            }
        }

        if (externalAuditHandler == null) {
            LOG.warn("Access check audit events will be dropped: ranger.audit.source.type is '{}' and no audit provider is available. "
                    + "Set xasecure.audit.is.enabled=true and the xasecure.audit.destination.* properties in Ranger Admin's site "
                    + "configuration, or set ranger.audit.source.type=db.", auditSource);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            if (workerThread != null) {
                workerThread.interrupt();
                try {
                    workerThread.join(2_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            drainAndFlush();
        }
    }

    public boolean log(AuthzAuditEvent event) {
        if (event == null) {
            return false;
        }
        if (StringUtils.isBlank(event.getEventId())) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getEventTime() == null) {
            event.setEventTime(new Date());
        }
        if (StringUtils.isBlank(event.getAgentHostname())) {
            event.setAgentHostname(localHostName);
        }
        if (StringUtils.isBlank(event.getAgentId())) {
            event.setAgentId("ranger-admin-access-check");
        }
        if (StringUtils.isBlank(event.getAclEnforcer())) {
            event.setAclEnforcer("ranger-acl");
        }
        event.setSeqNum(seqGen.incrementAndGet());

        Consumer<AuthzAuditEvent> sink = this.testSink;
        if (sink != null) {
            sink.accept(event);
            return true;
        }

        if (dbMode) {
            return logToDb(event);
        }

        AuditHandler handler = externalAuditHandler;
        return handler != null && handler.log(event);
    }

    void setTestSink(Consumer<AuthzAuditEvent> sink) {
        this.testSink = sink;
    }

    private boolean logToDb(AuthzAuditEvent event) {
        if (!running.get()) {
            flushBatch(Collections.singletonList(event));
            return true;
        }

        boolean offered = queue.offer(event);
        if (!offered) {
            LOG.warn("Ranger Admin access audit queue full ({}); dropping audit event for repo=[{}], user=[{}]",
                    MAX_QUEUE_SIZE, event.getRepositoryName(), event.getUser());
        }
        return offered;
    }

    private void startDbWriter() {
        if (running.compareAndSet(false, true)) {
            workerThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    runFlushLoop();
                }
            }, "ranger-admin-access-audit-flusher");
            workerThread.setDaemon(true);
            workerThread.start();
        }
    }

    private void runFlushLoop() {
        while (running.get()) {
            try {
                AuthzAuditEvent first = queue.poll(POLL_WAIT_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                List<AuthzAuditEvent> batch = new ArrayList<AuthzAuditEvent>(MAX_BATCH_SIZE);
                batch.add(first);
                queue.drainTo(batch, MAX_BATCH_SIZE - 1);
                flushBatch(batch);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                LOG.error("Unexpected error in RangerAdminAccessAuditLogger flush loop", t);
            }
        }
    }

    private void drainAndFlush() {
        List<AuthzAuditEvent> remaining = new ArrayList<AuthzAuditEvent>();
        queue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            flushBatch(remaining);
        }
    }

    void flushBatch(List<AuthzAuditEvent> batch) {
        if (CollectionUtils.isEmpty(batch)) {
            return;
        }
        try {
            writeToDb(batch);
        } catch (Exception e) {
            LOG.error("Failed to persist {} access audit event(s) to the Ranger Admin audit DB", batch.size(), e);
        }
    }

    private void writeToDb(Collection<AuthzAuditEvent> batch) {
        if (daoManager == null || daoManager.getXXAccessAudit() == null) {
            return;
        }
        for (AuthzAuditEvent evt : batch) {
            XXAccessAudit dbRow = new XXAccessAudit();
            dbRow.setAuditType(evt.getRepositoryType());
            dbRow.setAccessResult(evt.getAccessResult());
            dbRow.setAccessType(evt.getAccessType());
            dbRow.setAclEnforcer(evt.getAclEnforcer());
            dbRow.setAgentId(evt.getAgentId());
            dbRow.setClientIP(evt.getClientIP());
            dbRow.setClientType(evt.getClientType());
            dbRow.setPolicyId(evt.getPolicyId());
            dbRow.setRepoName(evt.getRepositoryName());
            dbRow.setRepoType(evt.getRepositoryType());
            dbRow.setResultReason(evt.getResultReason());
            dbRow.setSessionId(evt.getSessionId());
            dbRow.setEventTime(evt.getEventTime());
            dbRow.setRequestUser(evt.getUser());
            dbRow.setAction(evt.getAction());
            dbRow.setRequestData(evt.getRequestData());
            dbRow.setResourcePath(evt.getResourcePath());
            dbRow.setResourceType(evt.getResourceType());
            dbRow.setSequenceNumber(evt.getSeqNum());
            dbRow.setEventCount(evt.getEventCount());
            dbRow.setEventDuration(evt.getEventDurationMS());
            // XXAccessAudit has no columns for cluster, zone, agent host, policy version or event id,
            // so those fields are not stored when the audit source is the DB.
            if (CollectionUtils.isNotEmpty(evt.getTags())) {
                dbRow.setTags("[" + StringUtils.join(evt.getTags(), ",") + "]");
            }
            daoManager.getXXAccessAudit().create(dbRow);
        }
    }
}
