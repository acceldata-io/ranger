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

package org.apache.ranger.authorization.hive.metastore;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * RangerRMSMetaStoreEventListener is a Hive Metastore event listener that
 * sends notifications to Ranger RMS when tables and databases are created,
 * modified, or deleted.
 *
 * Configuration properties (in hive-site.xml):
 * - ranger.rms.notification.url: URL of Ranger RMS notification endpoint
 *   Default: http://localhost:6080/service/rms/notification
 * - ranger.rms.notification.username: Username for Ranger authentication
 *   Default: admin
 * - ranger.rms.notification.password: Password for Ranger authentication
 *   Default: admin
 * - ranger.rms.hive.service.name: Name of Hive service in Ranger
 *   Default: hive
 * - ranger.rms.notification.enabled: Enable/disable notifications
 *   Default: true
 * - ranger.rms.notification.ssl.verify: Verify SSL certificates
 *   Default: true
 *
 * To enable, add to hive-site.xml:
 * <property>
 *   <name>hive.metastore.event.listeners</name>
 *   <value>org.apache.ranger.authorization.hive.metastore.RangerRMSMetaStoreEventListener</value>
 * </property>
 */
public class RangerRMSMetaStoreEventListener extends MetaStoreEventListener {
    private static final Logger LOG = LoggerFactory.getLogger(RangerRMSMetaStoreEventListener.class);

    private static final String CONFIG_RMS_URL = "ranger.rms.notification.url";
    private static final String CONFIG_RMS_USERNAME = "ranger.rms.notification.username";
    private static final String CONFIG_RMS_PASSWORD = "ranger.rms.notification.password";
    private static final String CONFIG_HIVE_SERVICE_NAME = "ranger.rms.hive.service.name";
    private static final String CONFIG_NOTIFICATION_ENABLED = "ranger.rms.notification.enabled";
    private static final String CONFIG_SSL_VERIFY = "ranger.rms.notification.ssl.verify";

    private static final String DEFAULT_RMS_URL = "http://localhost:6080/service/rms/notification";
    private static final String DEFAULT_USERNAME = "admin";
    private static final String DEFAULT_PASSWORD = "admin";
    private static final String DEFAULT_HIVE_SERVICE_NAME = "hive";

    private static final String CHANGE_TYPE_CREATE_DATABASE = "CREATE_DATABASE";
    private static final String CHANGE_TYPE_DROP_DATABASE = "DROP_DATABASE";
    private static final String CHANGE_TYPE_ALTER_DATABASE = "ALTER_DATABASE";
    private static final String CHANGE_TYPE_CREATE_TABLE = "CREATE_TABLE";
    private static final String CHANGE_TYPE_DROP_TABLE = "DROP_TABLE";
    private static final String CHANGE_TYPE_ALTER_TABLE = "ALTER_TABLE";

    private final String rmsUrl;
    private final String username;
    private final String password;
    private final String hiveServiceName;
    private final boolean enabled;
    private final boolean sslVerify;
    private final ExecutorService executor;
    private final String authHeader;

    public RangerRMSMetaStoreEventListener(Configuration config) {
        super(config);

        this.rmsUrl = config.get(CONFIG_RMS_URL, DEFAULT_RMS_URL);
        this.username = config.get(CONFIG_RMS_USERNAME, DEFAULT_USERNAME);
        this.password = config.get(CONFIG_RMS_PASSWORD, DEFAULT_PASSWORD);
        this.hiveServiceName = config.get(CONFIG_HIVE_SERVICE_NAME, DEFAULT_HIVE_SERVICE_NAME);
        this.enabled = config.getBoolean(CONFIG_NOTIFICATION_ENABLED, true);
        this.sslVerify = config.getBoolean(CONFIG_SSL_VERIFY, true);

        String credentials = username + ":" + password;
        this.authHeader = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "RangerRMS-Notification-Sender");
            t.setDaemon(true);
            return t;
        });

        LOG.info("RangerRMSMetaStoreEventListener initialized:");
        LOG.info("  rmsUrl: {}", rmsUrl);
        LOG.info("  hiveServiceName: {}", hiveServiceName);
        LOG.info("  enabled: {}", enabled);
        LOG.info("  sslVerify: {}", sslVerify);

        if (!sslVerify) {
            disableSSLVerification();
        }
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        if (!enabled || dbEvent == null || !dbEvent.getStatus()) {
            return;
        }

        Database db = dbEvent.getDatabase();
        if (db == null) {
            return;
        }

        LOG.info("onCreateDatabase: {}", db.getName());
        sendNotificationAsync(CHANGE_TYPE_CREATE_DATABASE, db.getName(), null, db.getLocationUri(), false);
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
        if (!enabled || dbEvent == null || !dbEvent.getStatus()) {
            return;
        }

        Database db = dbEvent.getDatabase();
        if (db == null) {
            return;
        }

        LOG.info("onDropDatabase: {}", db.getName());
        sendNotificationAsync(CHANGE_TYPE_DROP_DATABASE, db.getName(), null, db.getLocationUri(), false);
    }

    @Override
    public void onAlterDatabase(AlterDatabaseEvent dbEvent) throws MetaException {
        if (!enabled || dbEvent == null) {
            return;
        }

        Database oldDb = dbEvent.getOldDatabase();
        Database newDb = dbEvent.getNewDatabase();

        if (oldDb == null || newDb == null) {
            return;
        }

        String oldLocation = oldDb.getLocationUri();
        String newLocation = newDb.getLocationUri();

        if (!StringUtils.equals(oldLocation, newLocation)) {
            LOG.info("onAlterDatabase: {} (location changed: {} -> {})", newDb.getName(), oldLocation, newLocation);
            sendNotificationAsync(CHANGE_TYPE_ALTER_DATABASE, newDb.getName(), null, newLocation, false);
        }
    }

    @Override
    public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
        if (!enabled || tableEvent == null || !tableEvent.getStatus()) {
            return;
        }

        Table table = tableEvent.getTable();
        if (table == null) {
            return;
        }

        String location = getTableLocation(table);
        boolean isManaged = isManaged(table);

        LOG.info("onCreateTable: {}.{} (location={}, isManaged={})", 
                 table.getDbName(), table.getTableName(), location, isManaged);
        
        sendNotificationAsync(CHANGE_TYPE_CREATE_TABLE, table.getDbName(), table.getTableName(), location, isManaged);
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        if (!enabled || tableEvent == null || !tableEvent.getStatus()) {
            return;
        }

        Table table = tableEvent.getTable();
        if (table == null) {
            return;
        }

        String location = getTableLocation(table);

        LOG.info("onDropTable: {}.{} (location={})", table.getDbName(), table.getTableName(), location);
        sendNotificationAsync(CHANGE_TYPE_DROP_TABLE, table.getDbName(), table.getTableName(), location, false);
    }

    @Override
    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
        if (!enabled || tableEvent == null) {
            return;
        }

        Table oldTable = tableEvent.getOldTable();
        Table newTable = tableEvent.getNewTable();

        if (oldTable == null || newTable == null) {
            return;
        }

        String oldLocation = getTableLocation(oldTable);
        String newLocation = getTableLocation(newTable);

        if (!StringUtils.equals(oldLocation, newLocation)) {
            LOG.info("onAlterTable: {}.{} (location changed: {} -> {})",
                     newTable.getDbName(), newTable.getTableName(), oldLocation, newLocation);
            sendNotificationAsync(CHANGE_TYPE_ALTER_TABLE, newTable.getDbName(), newTable.getTableName(), 
                                  newLocation, isManaged(newTable));
        }
    }

    private String getTableLocation(Table table) {
        if (table != null && table.getSd() != null) {
            return table.getSd().getLocation();
        }
        return null;
    }

    private boolean isManaged(Table table) {
        if (table == null) {
            return false;
        }
        String tableType = table.getTableType();
        return tableType == null || "MANAGED_TABLE".equalsIgnoreCase(tableType);
    }

    private void sendNotificationAsync(String changeType, String databaseName, String tableName, 
                                        String location, boolean isManaged) {
        executor.submit(() -> {
            try {
                sendNotification(changeType, databaseName, tableName, location, isManaged);
            } catch (Exception e) {
                LOG.error("Failed to send RMS notification: changeType={}, database={}, table={}", 
                          changeType, databaseName, tableName, e);
            }
        });
    }

    private void sendNotification(String changeType, String databaseName, String tableName, 
                                   String location, boolean isManaged) {
        if (StringUtils.isBlank(location)) {
            LOG.debug("Skipping notification: no location for {}.{}", databaseName, tableName);
            return;
        }

        try {
            String jsonPayload = buildJsonPayload(changeType, databaseName, tableName, location, isManaged);
            
            LOG.debug("Sending RMS notification to {}: {}", rmsUrl, jsonPayload);

            URL url = new URL(rmsUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization", authHeader);
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(10000);

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            
            if (responseCode >= 200 && responseCode < 300) {
                LOG.info("RMS notification sent successfully: changeType={}, database={}, table={}, responseCode={}",
                         changeType, databaseName, tableName, responseCode);
            } else {
                String errorResponse = readErrorResponse(conn);
                LOG.warn("RMS notification failed: changeType={}, database={}, table={}, responseCode={}, response={}",
                         changeType, databaseName, tableName, responseCode, errorResponse);
            }

            conn.disconnect();

        } catch (Exception e) {
            LOG.error("Error sending RMS notification: changeType={}, database={}, table={}",
                      changeType, databaseName, tableName, e);
        }
    }

    private String buildJsonPayload(String changeType, String databaseName, String tableName, 
                                     String location, boolean isManaged) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"changeType\":\"").append(escapeJson(changeType)).append("\"");
        sb.append(",\"serviceName\":\"").append(escapeJson(hiveServiceName)).append("\"");
        sb.append(",\"databaseName\":\"").append(escapeJson(databaseName)).append("\"");
        
        if (StringUtils.isNotBlank(tableName)) {
            sb.append(",\"tableName\":\"").append(escapeJson(tableName)).append("\"");
        }
        
        if (StringUtils.isNotBlank(location)) {
            sb.append(",\"location\":\"").append(escapeJson(location)).append("\"");
        }
        
        sb.append(",\"isManaged\":").append(isManaged);
        sb.append("}");
        
        return sb.toString();
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
    }

    private String readErrorResponse(HttpURLConnection conn) {
        try {
            if (conn.getErrorStream() != null) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        response.append(line);
                    }
                    return response.toString();
                }
            }
        } catch (Exception e) {
            LOG.debug("Error reading error response", e);
        }
        return "";
    }

    private void disableSSLVerification() {
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() { return null; }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) { }
                    public void checkServerTrusted(X509Certificate[] certs, String authType) { }
                }
            };

            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);

            LOG.warn("SSL verification disabled for RMS notifications");
        } catch (Exception e) {
            LOG.error("Failed to disable SSL verification", e);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down RangerRMSMetaStoreEventListener");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
