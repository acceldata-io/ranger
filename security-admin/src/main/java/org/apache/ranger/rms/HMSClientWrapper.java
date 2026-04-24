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

package org.apache.ranger.rms;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * HMSClientWrapper provides a reflection-based wrapper for Hive Metastore
 * using raw Thrift transport with support for plain, Kerberos/SASL, and SSL connections.
 *
 * Transport modes:
 * - PLAIN:    raw TSocket -> TFramedTransport -> TBinaryProtocol  (default, non-secure)
 * - SASL:     raw TSocket -> TSaslClientTransport (GSSAPI) -> TBinaryProtocol  (Kerberos)
 * - SSL:      TSSLTransportFactory.getClientSocket -> TBinaryProtocol  (SSL/TLS)
 * - SSL+SASL: TSSLTransportFactory.getClientSocket -> TSaslClientTransport -> TBinaryProtocol
 *
 * At runtime, ensure these JARs are in the classpath:
 * - hive-standalone-metastore-common (contains ThriftHiveMetastore API)
 * - libthrift (Thrift transport/protocol)
 * - libfb303 (Thrift fb303 base)
 */
public class HMSClientWrapper implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HMSClientWrapper.class);

    private static final String THRIFT_CLIENT_CLASS = "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client";
    private static final String TSOCKET_CLASS = "org.apache.thrift.transport.TSocket";
    private static final String TTRANSPORT_CLASS = "org.apache.thrift.transport.TTransport";
    private static final String TBINARY_PROTOCOL_CLASS = "org.apache.thrift.protocol.TBinaryProtocol";
    private static final String TPROTOCOL_CLASS = "org.apache.thrift.protocol.TProtocol";
    private static final String TFRAMED_TRANSPORT_CLASS = "org.apache.thrift.transport.TFramedTransport";
    private static final String TBUFFERED_TRANSPORT_CLASS = "org.apache.thrift.transport.TBufferedTransport";
    private static final String TSASL_CLIENT_TRANSPORT_CLASS = "org.apache.thrift.transport.TSaslClientTransport";
    private static final String TSSL_TRANSPORT_FACTORY_CLASS = "org.apache.thrift.transport.TSSLTransportFactory";
    private static final String TSSL_TRANSPORT_PARAMS_CLASS = "org.apache.thrift.transport.TSSLTransportFactory$TSSLTransportParameters";

    private static final int DEFAULT_SOCKET_TIMEOUT_MS = 30000;

    private Object thriftClient;
    private Object transport;
    private boolean connected = false;

    private boolean saslEnabled = false;
    private String kerberosServerPrincipal;
    private boolean sslEnabled = false;
    private String truststorePath;
    private String truststorePassword;

    public HMSClientWrapper() {
    }

    public void setSaslEnabled(boolean saslEnabled) {
        this.saslEnabled = saslEnabled;
    }

    public void setKerberosServerPrincipal(String kerberosServerPrincipal) {
        this.kerberosServerPrincipal = kerberosServerPrincipal;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public void setTruststorePath(String truststorePath) {
        this.truststorePath = truststorePath;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    /**
     * Connect to HMS using raw Thrift transport.
     */
    public boolean connect(String hmsUri) {
        LOG.info("Connecting to HMS at: {} (sasl={}, ssl={})", hmsUri, saslEnabled, sslEnabled);

        try {
            URI uri = new URI(hmsUri);
            String host = uri.getHost();
            int port = uri.getPort();

            if (StringUtils.isBlank(host)) {
                LOG.error("Invalid HMS URI - no host: {}", hmsUri);
                return false;
            }
            if (port <= 0) {
                port = 9083;
            }

            LOG.info("Parsed HMS endpoint: host={}, port={}", host, port);

            return connectViaThrift(host, port);

        } catch (Exception e) {
            LOG.error("Failed to parse HMS URI: {}", hmsUri, e);
            return false;
        }
    }

    private boolean connectViaThrift(String host, int port) {
        try {
            LOG.info("Creating Thrift connection to {}:{} (timeout={}ms, sasl={}, ssl={})",
                     host, port, DEFAULT_SOCKET_TIMEOUT_MS, saslEnabled, sslEnabled);

            Class<?> ttransportClass = Class.forName(TTRANSPORT_CLASS);
            Object baseTransport;

            if (sslEnabled) {
                baseTransport = createSSLTransport(host, port, ttransportClass);
            } else {
                baseTransport = createPlainSocket(host, port, ttransportClass);
            }

            if (baseTransport == null) {
                LOG.error("Failed to create base transport");
                return false;
            }

            Object finalTransport;
            if (saslEnabled) {
                finalTransport = wrapWithSASL(baseTransport, host, ttransportClass);
                if (finalTransport == null) {
                    LOG.error("Failed to wrap transport with SASL");
                    cleanupTransport(baseTransport, ttransportClass);
                    return false;
                }
            } else {
                finalTransport = baseTransport;
            }

            this.transport = finalTransport;

            Method isOpenMethod = ttransportClass.getMethod("isOpen");
            Boolean alreadyOpen = (Boolean) isOpenMethod.invoke(this.transport);
            if (!Boolean.TRUE.equals(alreadyOpen)) {
                LOG.info("Opening Thrift transport...");
                Method openMethod = ttransportClass.getMethod("open");
                openMethod.invoke(this.transport);
            }
            LOG.info("Thrift transport opened successfully");

            Class<?> tprotocolClass = Class.forName(TPROTOCOL_CLASS);
            Class<?> tbinaryClass = Class.forName(TBINARY_PROTOCOL_CLASS);
            Constructor<?> protocolCtor = tbinaryClass.getConstructor(ttransportClass);
            Object protocol = protocolCtor.newInstance(this.transport);

            Class<?> clientClass = Class.forName(THRIFT_CLIENT_CLASS);
            Constructor<?> clientCtor = clientClass.getConstructor(tprotocolClass);
            thriftClient = clientCtor.newInstance(protocol);
            LOG.info("ThriftHiveMetastore.Client created successfully");

            connected = true;
            LOG.info("Connected to HMS at {}:{}", host, port);
            return true;

        } catch (ClassNotFoundException e) {
            LOG.error("Required Thrift class not found: {}. Ensure libthrift and hive-standalone-metastore-common JARs are in classpath.", e.getMessage());
            cleanupOnFailure();
            return false;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            LOG.error("Failed to connect to HMS: {}", cause != null ? cause.getMessage() : e.getMessage());
            if (cause != null) {
                LOG.error("Root cause:", cause);
            }
            cleanupOnFailure();
            return false;
        } catch (Exception e) {
            LOG.error("Failed to connect to HMS via Thrift: {}", e.getMessage(), e);
            cleanupOnFailure();
            return false;
        }
    }

    private Object createPlainSocket(String host, int port, Class<?> ttransportClass) throws Exception {
        Class<?> tsocketClass = Class.forName(TSOCKET_CLASS);
        Constructor<?> tsocketCtor = tsocketClass.getConstructor(String.class, int.class, int.class);
        Object tsocket = tsocketCtor.newInstance(host, port, DEFAULT_SOCKET_TIMEOUT_MS);
        LOG.info("TSocket created");

        Object wrapped = wrapTransport(tsocket, ttransportClass);
        return wrapped != null ? wrapped : tsocket;
    }

    private Object createSSLTransport(String host, int port, Class<?> ttransportClass) {
        try {
            Class<?> sslFactoryClass = Class.forName(TSSL_TRANSPORT_FACTORY_CLASS);
            Class<?> sslParamsClass = Class.forName(TSSL_TRANSPORT_PARAMS_CLASS);

            Object sslParams = sslParamsClass.getDeclaredConstructor().newInstance();

            if (StringUtils.isNotBlank(truststorePath)) {
                Method setTrustStore = sslParamsClass.getMethod("setTrustStore", String.class, String.class);
                setTrustStore.invoke(sslParams, truststorePath, truststorePassword != null ? truststorePassword : "");
                LOG.info("SSL truststore configured: {}", truststorePath);
            }

            Method getClientSocket = sslFactoryClass.getMethod("getClientSocket",
                String.class, int.class, int.class, sslParamsClass);
            Object sslTransport = getClientSocket.invoke(null, host, port, DEFAULT_SOCKET_TIMEOUT_MS, sslParams);
            LOG.info("SSL transport created to {}:{}", host, port);
            return sslTransport;
        } catch (ClassNotFoundException e) {
            LOG.error("SSL transport classes not found. Ensure libthrift JAR supports TSSLTransportFactory: {}", e.getMessage());
        } catch (Exception e) {
            LOG.error("Failed to create SSL transport to {}:{}: {}", host, port, e.getMessage(), e);
        }
        return null;
    }

    private Object wrapWithSASL(Object baseTransport, String host, Class<?> ttransportClass) {
        try {
            String principal = kerberosServerPrincipal;
            if (StringUtils.isBlank(principal)) {
                LOG.error("Kerberos server principal is required for SASL but not configured");
                return null;
            }

            String[] principalParts = principal.split("[/@]");
            String servicePrincipalName = principalParts.length > 0 ? principalParts[0] : "hive";
            String serverHost = principalParts.length > 1 ? principalParts[1] : host;

            if ("_HOST".equalsIgnoreCase(serverHost)) {
                serverHost = host;
            }

            LOG.info("Creating SASL GSSAPI transport: service={}, host={}", servicePrincipalName, serverHost);

            Class<?> saslClass = Class.forName(TSASL_CLIENT_TRANSPORT_CLASS);

            java.util.Map<String, String> saslProps = new java.util.HashMap<>();
            saslProps.put("javax.security.sasl.qop", "auth");
            saslProps.put("javax.security.sasl.server.authentication", "true");

            // libthrift >= 0.9.x (including 0.16) exposes a 7-arg constructor with
            // an authorizationId as the 2nd parameter. Some much older Thrift versions
            // had a 6-arg variant without authorizationId. Try 7-arg first, fall back
            // to 6-arg for backwards-compatibility.
            Constructor<?> saslCtor;
            Object saslTransport;
            try {
                saslCtor = saslClass.getConstructor(
                    String.class,          // mechanism
                    String.class,          // authorizationId
                    String.class,          // protocol
                    String.class,          // serverName
                    java.util.Map.class,   // props
                    javax.security.auth.callback.CallbackHandler.class,
                    ttransportClass        // transport
                );
                saslTransport = saslCtor.newInstance(
                    "GSSAPI", null, servicePrincipalName, serverHost, saslProps, null, baseTransport);
            } catch (NoSuchMethodException nsme7) {
                LOG.debug("7-arg TSaslClientTransport ctor not found, trying 6-arg legacy variant");
                saslCtor = saslClass.getConstructor(
                    String.class,          // mechanism
                    String.class,          // protocol
                    String.class,          // serverName
                    java.util.Map.class,   // props
                    javax.security.auth.callback.CallbackHandler.class,
                    ttransportClass        // transport
                );
                saslTransport = saslCtor.newInstance(
                    "GSSAPI", servicePrincipalName, serverHost, saslProps, null, baseTransport);
            }
            LOG.info("SASL GSSAPI transport created");
            return saslTransport;

        } catch (ClassNotFoundException e) {
            LOG.error("TSaslClientTransport not found. Ensure libthrift supports SASL: {}", e.getMessage());
        } catch (Exception e) {
            LOG.error("Failed to create SASL transport: {}", e.getMessage(), e);
        }
        return null;
    }

    /**
     * Connect with Kerberos authentication using a Subject from UserGroupInformation.
     */
    public boolean connectAsKerberosUser(String hmsUri) {
        LOG.info("Connecting to HMS with Kerberos authentication at: {}", hmsUri);
        try {
            Class<?> ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
            Method getLoginUser = ugiClass.getMethod("getLoginUser");
            Object loginUser = getLoginUser.invoke(null);

            if (loginUser == null) {
                LOG.warn("No Kerberos login user available, falling back to plain connect");
                return connect(hmsUri);
            }

            Method doAs = ugiClass.getMethod("doAs", PrivilegedExceptionAction.class);
            Boolean result = (Boolean) doAs.invoke(loginUser, (PrivilegedExceptionAction<Boolean>) () -> connect(hmsUri));
            return Boolean.TRUE.equals(result);

        } catch (ClassNotFoundException e) {
            LOG.warn("Hadoop UserGroupInformation not available, falling back to plain connect");
            return connect(hmsUri);
        } catch (Exception e) {
            LOG.error("Failed to connect using Kerberos: {}", e.getMessage(), e);
            return false;
        }
    }

    private Object wrapTransport(Object tsocket, Class<?> ttransportClass) {
        try {
            Class<?> tframedClass = Class.forName(TFRAMED_TRANSPORT_CLASS);
            Constructor<?> ctor = tframedClass.getConstructor(ttransportClass);
            Object framed = ctor.newInstance(tsocket);
            LOG.info("Using TFramedTransport");
            return framed;
        } catch (ClassNotFoundException e) {
            LOG.debug("TFramedTransport not found");
        } catch (Exception e) {
            LOG.debug("Could not create TFramedTransport: {}", e.getMessage());
        }

        try {
            Class<?> tbufferedClass = Class.forName(TBUFFERED_TRANSPORT_CLASS);
            Constructor<?> ctor = tbufferedClass.getConstructor(ttransportClass);
            Object buffered = ctor.newInstance(tsocket);
            LOG.info("Using TBufferedTransport");
            return buffered;
        } catch (ClassNotFoundException e) {
            LOG.debug("TBufferedTransport not found");
        } catch (Exception e) {
            LOG.debug("Could not create TBufferedTransport: {}", e.getMessage());
        }

        return null;
    }

    public List<String> getAllDatabases() throws Exception {
        ensureConnected();
        Method method = thriftClient.getClass().getMethod("get_all_databases");
        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(thriftClient);
        return result != null ? result : new ArrayList<>();
    }

    public List<String> getAllTables(String dbName) throws Exception {
        ensureConnected();
        Method method = thriftClient.getClass().getMethod("get_all_tables", String.class);
        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(thriftClient, dbName);
        return result != null ? result : new ArrayList<>();
    }

    public DatabaseInfo getDatabase(String dbName) throws Exception {
        ensureConnected();
        Method method = thriftClient.getClass().getMethod("get_database", String.class);
        Object database = method.invoke(thriftClient, dbName);

        if (database == null) {
            return null;
        }

        DatabaseInfo info = new DatabaseInfo();
        info.name = (String) database.getClass().getMethod("getName").invoke(database);
        info.locationUri = (String) database.getClass().getMethod("getLocationUri").invoke(database);
        return info;
    }

    public TableInfo getTable(String dbName, String tableName) throws Exception {
        ensureConnected();
        Method method = thriftClient.getClass().getMethod("get_table", String.class, String.class);
        Object table = method.invoke(thriftClient, dbName, tableName);

        if (table == null) {
            return null;
        }

        TableInfo info = new TableInfo();
        info.dbName = (String) table.getClass().getMethod("getDbName").invoke(table);
        info.tableName = (String) table.getClass().getMethod("getTableName").invoke(table);
        info.tableType = (String) table.getClass().getMethod("getTableType").invoke(table);

        Object sd = table.getClass().getMethod("getSd").invoke(table);
        if (sd != null) {
            info.location = (String) sd.getClass().getMethod("getLocation").invoke(sd);
        }

        return info;
    }

    public long getCurrentNotificationEventId() throws Exception {
        ensureConnected();
        Method method = thriftClient.getClass().getMethod("get_current_notificationEventId");
        Object result = method.invoke(thriftClient);

        if (result != null) {
            Method getEventIdMethod = result.getClass().getMethod("getEventId");
            return (Long) getEventIdMethod.invoke(result);
        }

        return -1;
    }

    public List<NotificationEventInfo> getNextNotification(long lastEventId, int maxEvents) throws Exception {
        ensureConnected();
        List<NotificationEventInfo> results = new ArrayList<>();

        try {
            Class<?> requestClass = Class.forName("org.apache.hadoop.hive.metastore.api.NotificationEventRequest");
            Object request = requestClass.getDeclaredConstructor().newInstance();
            requestClass.getMethod("setLastEvent", long.class).invoke(request, lastEventId);
            requestClass.getMethod("setMaxEvents", int.class).invoke(request, maxEvents);

            Method method = thriftClient.getClass().getMethod("get_next_notification", requestClass);
            Object response = method.invoke(thriftClient, request);

            if (response != null) {
                Method getEventsMethod = response.getClass().getMethod("getEvents");
                @SuppressWarnings("unchecked")
                List<?> events = (List<?>) getEventsMethod.invoke(response);

                if (events != null) {
                    for (Object event : events) {
                        NotificationEventInfo info = new NotificationEventInfo();
                        info.eventId = (Long) event.getClass().getMethod("getEventId").invoke(event);
                        info.eventType = (String) event.getClass().getMethod("getEventType").invoke(event);
                        info.dbName = (String) event.getClass().getMethod("getDbName").invoke(event);
                        info.tableName = (String) event.getClass().getMethod("getTableName").invoke(event);
                        try {
                            info.message = (String) event.getClass().getMethod("getMessage").invoke(event);
                        } catch (Exception msgEx) {
                            LOG.debug("Could not extract message from event {}: {}", info.eventId, msgEx.getMessage());
                        }
                        results.add(info);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error getting notifications from HMS: {}", e.getMessage());
            throw new Exception("Failed to get HMS notifications: " + e.getMessage(), e);
        }

        return results;
    }

    public boolean testConnection() {
        try {
            getAllDatabases();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void close() {
        if (transport != null) {
            try {
                Class<?> ttransportClass = Class.forName(TTRANSPORT_CLASS);
                closeTransport(transport, ttransportClass);
            } catch (Exception e) {
                LOG.debug("Error closing Thrift transport", e);
            }
            transport = null;
        }
        thriftClient = null;
        connected = false;
    }

    private void cleanupOnFailure() {
        if (transport != null) {
            try {
                Class<?> ttransportClass = Class.forName(TTRANSPORT_CLASS);
                closeTransport(transport, ttransportClass);
            } catch (Exception e) {
                LOG.debug("Error during cleanup: {}", e.getMessage());
            }
            transport = null;
        }
        thriftClient = null;
        connected = false;
    }

    private void closeTransport(Object t, Class<?> ttransportClass) {
        try {
            Method isOpenMethod = ttransportClass.getMethod("isOpen");
            Boolean isOpen = (Boolean) isOpenMethod.invoke(t);
            if (Boolean.TRUE.equals(isOpen)) {
                Method closeMethod = ttransportClass.getMethod("close");
                closeMethod.invoke(t);
            }
        } catch (Exception e) {
            LOG.debug("Error closing transport: {}", e.getMessage());
        }
    }

    private void cleanupTransport(Object t, Class<?> ttransportClass) {
        try {
            closeTransport(t, ttransportClass);
        } catch (Exception e) {
            LOG.debug("Error cleaning up transport: {}", e.getMessage());
        }
    }

    public boolean isConnected() {
        if (!connected || transport == null) {
            return false;
        }
        try {
            Class<?> ttransportClass = Class.forName(TTRANSPORT_CLASS);
            Method isOpenMethod = ttransportClass.getMethod("isOpen");
            return (Boolean) isOpenMethod.invoke(transport);
        } catch (Exception e) {
            return false;
        }
    }

    private void ensureConnected() {
        if (!connected || thriftClient == null) {
            throw new IllegalStateException("Not connected to HMS");
        }
    }

    public static class DatabaseInfo {
        public String name;
        public String locationUri;
    }

    public static class TableInfo {
        public String dbName;
        public String tableName;
        public String tableType;
        public String location;

        public boolean isManaged() {
            return tableType == null || "MANAGED_TABLE".equalsIgnoreCase(tableType);
        }
    }

    public static class NotificationEventInfo {
        public long eventId;
        public String eventType;
        public String dbName;
        public String tableName;
        public String message;
    }
}
