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
package org.apache.ranger.authorization.pinot;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Decides Pinot requests with the policies of a Ranger service of type {@code pinot}, and supplies Ranger row filters.
 *
 * <p>Resources: {@code cluster} for requests that name no table or database, {@code database} for database requests, {@code database} +
 * {@code table} for table requests (raw table name, {@code default} database when the table has none). The access
 * type is the one Pinot assigns to the request's action. Users and groups come from Pinot's authentication method;
 * Ranger can add its own groups ({@code ranger.plugin.pinot.use.rangerGroups}).
 *
 * <p>Configuration: only the usual Ranger plug-in files on the class path - {@code ranger-pinot-security.xml}
 * ({@code ranger.plugin.pinot.service.name}, {@code .policy.rest.url}, {@code .policy.cache.dir},
 * {@code .policy.pollIntervalMs}, {@code .access.cluster.name} (value of the cluster resource, default {@code pinot};
 * policies normally use {@code *}), {@code .ugi.*} (Kerberos, see {@link #enableKerberos}),
 * {@code .decision.cache.ttl.ms} (how long a decision is reused for the same user, groups, resource and access type;
 * default 5000, 0 disables; reused decisions are not audited again), {@code .decision.cache.max.entries} (default
 * 100000)), {@code ranger-pinot-audit.xml} and {@code ranger-policymgr-ssl.xml}, written when the plug-in is enabled
 * for Pinot. Pinot passes no settings.
 *
 * <p>Fails closed: until policies are downloaded (and with no policy cache from an earlier run) every request is
 * denied. When Ranger Admin is unreachable the plug-in keeps enforcing the policies it has, from memory or from its
 * cache directory. Thread-safe after {@link #init}.
 */
public class RangerPinotAccessChecker implements PinotAccessChecker {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPinotAccessChecker.class);

    public static final String SERVICE_TYPE      = "pinot";
    public static final String RESOURCE_CLUSTER  = "cluster";
    public static final String RESOURCE_DB       = "database";
    public static final String RESOURCE_TBL      = "table";
    public static final String ACCESS_QUERY      = "query";
    public static final String DEFAULT_DATABASE  = "default";

    /** Suffixes of the decision cache settings under ranger.plugin.pinot (ranger-pinot-security.xml). */
    public static final String DECISION_CACHE_TTL_MS      = ".decision.cache.ttl.ms";
    public static final String DECISION_CACHE_MAX_ENTRIES = ".decision.cache.max.entries";

    private RangerBasePlugin            plugin;
    private RangerAccessResultProcessor auditHandler;
    private String                      clusterName;
    private long                        cacheTtlMs;
    private int                         cacheMaxEntries;

    private final ConcurrentHashMap<String, CachedDecision> cache = new ConcurrentHashMap<>();

    public RangerPinotAccessChecker() {
    }

    /** For tests: use an already initialised plug-in. */
    RangerPinotAccessChecker(RangerBasePlugin plugin, RangerAccessResultProcessor auditHandler, String clusterName, long cacheTtlMs) {
        this.plugin          = plugin;
        this.auditHandler    = auditHandler;
        this.clusterName     = clusterName;
        this.cacheTtlMs      = cacheTtlMs;
        this.cacheMaxEntries = 100_000;
    }

    @Override
    public void init(Map<String, String> settings) {
        // Everything comes from the Ranger plug-in files (ranger-pinot-security.xml, ranger-pinot-audit.xml,
        // ranger-policymgr-ssl.xml) on the class path, written when the plug-in is enabled; Pinot only selects the mode
        RangerPluginConfig pluginConfig = new RangerPluginConfig(SERVICE_TYPE, null, SERVICE_TYPE, null, null, null);
        String             prefix       = pluginConfig.getPropertyPrefix();
        String             policyUrl    = pluginConfig.get(prefix + ".policy.rest.url");

        if (StringUtils.isBlank(pluginConfig.getServiceName())) {
            throw new IllegalStateException("No Ranger service name: ranger-pinot-security.xml with " + prefix + ".service.name is not on the class path");
        }

        if (StringUtils.isBlank(policyUrl)) {
            throw new IllegalStateException("No Ranger Admin URL: set " + prefix + ".policy.rest.url in ranger-pinot-security.xml");
        }

        clusterName     = StringUtils.defaultIfBlank(pluginConfig.getClusterName(), "pinot").trim();
        cacheTtlMs      = pluginConfig.getLong(prefix + DECISION_CACHE_TTL_MS, 5000L);
        cacheMaxEntries = pluginConfig.getInt(prefix + DECISION_CACHE_MAX_ENTRIES, 100_000);

        enableKerberos(pluginConfig);

        RangerBasePlugin basePlugin = new RangerBasePlugin(pluginConfig);

        basePlugin.init();

        plugin       = basePlugin;
        auditHandler = new RangerDefaultAuditHandler(pluginConfig);

        LOG.info("Ranger authorization for Pinot ready: service={}, policy URL={}, cluster resource={}, decision cache={} ms", pluginConfig.getServiceName(), policyUrl, clusterName, cacheTtlMs);
    }

    /**
     * On a Kerberos cluster Ranger Admin refuses unauthenticated policy downloads. The standard Ranger settings
     * {@code ranger.plugin.pinot.ugi.initialize=true}, {@code .ugi.login.type=keytab}, {@code .ugi.keytab.principal}
     * and {@code .ugi.keytab.file} make the base plug-in log in from the keytab; that login only takes effect when
     * Hadoop security is on, which Pinot (unlike Hadoop services) has no core-site.xml for, so it is switched on here.
     * This only affects the plug-in's own Hadoop classes, which its class loader keeps apart from Pinot's.
     */
    static void enableKerberos(Configuration pluginConfig) {
        String prefix = "ranger.plugin." + SERVICE_TYPE + ".ugi";

        if (!pluginConfig.getBoolean(prefix + ".initialize", false) || !"keytab".equalsIgnoreCase(pluginConfig.get(prefix + ".login.type"))) {
            return;
        }

        String keytab = StringUtils.trimToNull(pluginConfig.get(prefix + ".keytab.file"));

        if (keytab == null || StringUtils.isBlank(pluginConfig.get(prefix + ".keytab.principal"))) {
            throw new IllegalArgumentException("Set " + prefix + ".keytab.principal and " + prefix + ".keytab.file in ranger-pinot-security.xml");
        }

        if (!new File(keytab).canRead()) {
            throw new IllegalStateException("Kerberos keytab " + keytab + " is missing or not readable by the Pinot user");
        }

        Configuration conf = new Configuration();

        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        UserGroupInformation.setConfiguration(conf);
    }

    @Override
    public Map.Entry<Boolean, String> checkAccess(String user, Set<String> groups, String resourceType, String database, String table, String accessType, String action) {
        if (StringUtils.isEmpty(accessType)) {
            return decision(false, "no Ranger access type for action " + action);
        }

        RangerAccessResourceImpl resource = resource(resourceType, database, table);
        String                   key      = cacheTtlMs > 0 ? cacheKey(user, groups, resource, accessType) : null;

        if (key != null) {
            CachedDecision cached = cache.get(key);

            if (cached != null && cached.expiresAt > System.currentTimeMillis()) {
                return cached.decision;
            }
        }

        RangerAccessResult         result = plugin.isAccessAllowed(request(user, groups, resource, accessType, action), auditHandler);
        Map.Entry<Boolean, String> ret;

        if (result != null && result.getIsAllowed()) {
            ret = decision(true, "Ranger policy " + result.getPolicyId());
        } else {
            ret = decision(false, "no Ranger policy allows " + accessType + " on " + resource.getAsString());
        }

        if (key != null) {
            if (cache.size() >= cacheMaxEntries) {
                cache.clear();
            }

            cache.put(key, new CachedDecision(ret, System.currentTimeMillis() + cacheTtlMs));
        }

        return ret;
    }

    @Override
    public List<String> getRowFilters(String user, Set<String> groups, String database, String table) {
        RangerAccessResourceImpl resource = resource(RESOURCE_TABLE, database, table);
        RangerAccessResult       result   = plugin.evalRowFilterPolicies(request(user, groups, resource, ACCESS_QUERY, ACCESS_QUERY), auditHandler);

        if (result != null && result.isRowFilterEnabled() && StringUtils.isNotBlank(result.getFilterExpr())) {
            return Collections.singletonList(result.getFilterExpr());
        }

        return Collections.emptyList();
    }

    @Override
    public void close() {
        cache.clear();

        if (plugin != null) {
            plugin.cleanup();
        }
    }

    RangerAccessResourceImpl resource(String resourceType, String database, String table) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        String                   db       = StringUtils.defaultIfBlank(database, DEFAULT_DATABASE);

        if (RESOURCE_TABLE.equals(resourceType)) {
            resource.setValue(RESOURCE_DB, db);
            resource.setValue(RESOURCE_TBL, table);
        } else if (RESOURCE_DATABASE.equals(resourceType)) {
            resource.setValue(RESOURCE_DB, db);
        } else {
            resource.setValue(RESOURCE_CLUSTER, clusterName);
        }

        return resource;
    }

    private RangerAccessRequestImpl request(String user, Set<String> groups, RangerAccessResourceImpl resource, String accessType, String action) {
        RangerAccessRequestImpl ret = new RangerAccessRequestImpl(resource, accessType, user, groups, null);

        ret.setAction(action != null ? action : accessType);
        ret.setAccessTime(new Date());
        ret.setClusterName(plugin.getClusterName());

        return ret;
    }

    private static Map.Entry<Boolean, String> decision(boolean allowed, String reason) {
        return new AbstractMap.SimpleImmutableEntry<>(allowed, reason);
    }

    private static String cacheKey(String user, Set<String> groups, RangerAccessResourceImpl resource, String accessType) {
        Set<String> sorted = groups != null ? new TreeSet<>(groups) : Collections.<String>emptySet();

        return user + '\u0000' + sorted + '\u0000' + resource.getAsString() + '\u0000' + accessType;
    }


    private static final class CachedDecision {
        final Map.Entry<Boolean, String> decision;
        final long                       expiresAt;

        CachedDecision(Map.Entry<Boolean, String> decision, long expiresAt) {
            this.decision  = decision;
            this.expiresAt = expiresAt;
        }
    }
}
