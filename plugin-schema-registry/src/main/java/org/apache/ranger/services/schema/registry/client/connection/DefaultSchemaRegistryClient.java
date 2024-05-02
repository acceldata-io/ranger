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

package org.apache.ranger.services.schema.registry.client.connection;

import com.hortonworks.registries.auth.Login;
import com.hortonworks.registries.schemaregistry.client.LoadBalancedFailoverUrlSelector;
import com.hortonworks.registries.schemaregistry.client.UrlSelector;
import org.apache.ranger.services.schema.registry.client.connection.util.SecurityUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.ClientConfig;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.ClientProperties;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.JerseyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import com.hortonworks.registries.shaded.javax.ws.rs.client.ClientBuilder;
import com.hortonworks.registries.shaded.javax.ws.rs.client.WebTarget;
import com.hortonworks.registries.shaded.javax.ws.rs.core.MediaType;
import com.hortonworks.registries.shaded.javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_CONNECTION_TIMEOUT;
import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_READ_TIMEOUT;


public class DefaultSchemaRegistryClient implements ISchemaRegistryClient {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistryClient.class);

    private static final String SCHEMA_REGISTRY_PATH = "/api/v1/schemaregistry";
    private static final String SCHEMAS_PATH = SCHEMA_REGISTRY_PATH + "/schemas/";
    private static final String SCHEMA_REGISTRY_VERSION_PATH = SCHEMA_REGISTRY_PATH + "/version";
    private static final String SSL_ALGORITHM = "TLSv1.2";
    private final com.hortonworks.registries.shaded.javax.ws.rs.client.Client client;
    private final Login login;
    private final UrlSelector urlSelector;
    private final Map<String, SchemaRegistryTargets> urlWithTargets;
    private final Configuration configuration;

    public DefaultSchemaRegistryClient(Map<String, ?> conf) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("Initializing SchemaRegistry client inside the Ranger plugin");
        }
        configuration = new Configuration(conf);
        login = SecurityUtils.initializeSecurityContext(conf);
        ClientConfig config = createClientConfig(conf);
        final boolean SSLEnabled = SecurityUtils.isHttpsConnection(conf);
        ClientBuilder clientBuilder = JerseyClientBuilder.newBuilder()
                .withConfig(config)
                .property(ClientProperties.FOLLOW_REDIRECTS, Boolean.TRUE);
        if (SSLEnabled) {
            SSLContext ctx;
            try {
                ctx = SecurityUtils.createSSLContext(conf, SSL_ALGORITHM);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            clientBuilder.sslContext(ctx);
        }
        client = clientBuilder.build();

        // get list of urls and create given or default UrlSelector.
        urlSelector = createUrlSelector();
        urlWithTargets = new ConcurrentHashMap<>();
    }

    private ClientConfig createClientConfig(Map<String, ?> conf) {
        ClientConfig config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        config.property(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        config.property(ClientProperties.FOLLOW_REDIRECTS, true);
        for (Map.Entry<String, ?> entry : conf.entrySet()) {
            config.property(entry.getKey(), entry.getValue());
        }
        return config;
    }

    private UrlSelector createUrlSelector() {
        UrlSelector urlSelector = null;
        String rootCatalogURL = configuration.getValue(Configuration.SCHEMA_REGISTRY_URL.name());
        String urlSelectorClass = configuration.getValue(Configuration.URL_SELECTOR_CLASS.name());
        if (urlSelectorClass == null) {
            urlSelector = new LoadBalancedFailoverUrlSelector(rootCatalogURL);
        } else {
            try {
                urlSelector = (UrlSelector) Class.forName(urlSelectorClass)
                        .getConstructor(String.class)
                        .newInstance(rootCatalogURL);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException
                    | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        urlSelector.init(configuration.getConfig());

        return urlSelector;
    }

    private static class SchemaRegistryTargets {
        private final WebTarget schemaRegistryVersion;
        private final WebTarget schemasTarget;

        SchemaRegistryTargets(WebTarget rootResource) {
            schemaRegistryVersion = rootResource.path(SCHEMA_REGISTRY_VERSION_PATH);
            schemasTarget = rootResource.path(SCHEMAS_PATH);
        }
    }

    private SchemaRegistryTargets currentSchemaRegistryTargets() {
        String url = urlSelector.select();
        urlWithTargets.computeIfAbsent(url, s -> new SchemaRegistryTargets(client.target(s)));
        return urlWithTargets.get(url);
    }

    private static String encode(String schemaName) {
        try {
            return URLEncoder.encode(schemaName, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /** Send request to the remote service. */
    private String sendWebRequest(WebTarget target) throws Exception {
        Response response = login.doAction(() -> target.request(MediaType.APPLICATION_JSON_TYPE).get(Response.class));

        if(LOG.isDebugEnabled()) {
            LOG.debug("response statusCode = " + response.getStatus());
        }

        if(response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.error("Connection failed. Response StatusCode = " + response.getStatus());
            throw new Exception("Connection failed. StatusCode = " + response.getStatus());
        }

        return response.readEntity(String.class);
    }

    @Override
    public List<String> getSchemaGroups() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> DefaultSchemaRegistryClient.getSchemaGroups()");
        }

        List<String> res = new ArrayList<>();
        try {
            String jsonResponse = sendWebRequest(currentSchemaRegistryTargets().schemasTarget);

            JSONArray mDataList = new JSONObject(jsonResponse).getJSONArray("entities");
            int len = mDataList.length();
            for(int i = 0; i < len; i++) {
                JSONObject entity = mDataList.getJSONObject(i);
                JSONObject schemaMetadata = (JSONObject)entity.get("schemaMetadata");
                String group = (String) schemaMetadata.get("schemaGroup");
                res.add(group);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } catch (Throwable t) {
            throw new RuntimeException("Unexpected error.", t);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== DefaultSchemaRegistryClient.getSchemaGroups(): "
                    + res.size()
                    + " schemaGroups found");
        }

        return res;
    }

    @Override
    public List<String> getSchemaNames(List<String> schemaNames) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> DefaultSchemaRegistryClient.getSchemaNames( " + schemaNames + " )");
        }

        List<String> res = new ArrayList<>();
        try {
            String jsonResponse = sendWebRequest(currentSchemaRegistryTargets().schemasTarget);

            JSONArray mDataList = new JSONObject(jsonResponse).getJSONArray("entities");
            int len = mDataList.length();
            for(int i = 0; i < len; i++) {
                JSONObject entity = mDataList.getJSONObject(i);
                JSONObject schemaMetadata = (JSONObject)entity.get("schemaMetadata");
                String existingSchemaName = (String) schemaMetadata.get("name");
                for(String schemaName:  schemaNames) {
                    applyIfPatternMatches(schemaName, existingSchemaName, name -> res.add(name));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } catch (Throwable t) {
            throw new RuntimeException("Unexpected error. ", t);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== DefaultSchemaRegistryClient.getSchemaNames( " + schemaNames + " ): "
                      + res.size() + " schemaNames found");
        }

        return res;
    }

    @Override
    public List<String> getSchemaBranches(String schemaMetadataName) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> DefaultSchemaRegistryClient.getSchemaBranches( " + schemaMetadataName + " )");
        }

        List<String> res = new ArrayList<>();
        try {
            String jsonResponse = sendWebRequest(currentSchemaRegistryTargets().schemasTarget.path(encode(schemaMetadataName) + "/branches"));

            JSONArray mDataList = new JSONObject(jsonResponse).getJSONArray("entities");
            int len = mDataList.length();
            for(int i = 0; i < len; i++) {
                JSONObject branchInfo = mDataList.getJSONObject(i);
                String existingSchemaName = (String) branchInfo.get("schemaMetadataName");
                String existingBranch = (String) branchInfo.get("name");
                applyIfPatternMatches(schemaMetadataName, existingSchemaName, val -> res.add(existingBranch));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } catch (Throwable t) {
            throw new RuntimeException("Unexpected error.", t);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== DefaultSchemaRegistryClient.getSchemaBranches( " + schemaMetadataName + " ): "
                      + res.size() + " branches found.");
        }

        return res;
    }

    @Override
    public List<String> getSchemaVersions(String schemaMetadataName) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> DefaultSchemaRegistryClient.getSchemaVersions( " + schemaMetadataName + " )");
        }

        List<String> res = new ArrayList<>();
        try {
            String jsonResponse = sendWebRequest(currentSchemaRegistryTargets().schemasTarget.path(encode(schemaMetadataName) + "/versions"));

            JSONArray mDataList = new JSONObject(jsonResponse).getJSONArray("entities");
            int len = mDataList.length();
            for(int i = 0; i < len; i++) {
                JSONObject branchInfo = mDataList.getJSONObject(i);
                String existingSchemaName = (String) branchInfo.get("name");
                Integer existingVersion = (Integer) branchInfo.get("version");

                applyIfPatternMatches(schemaMetadataName, existingSchemaName, val -> res.add(String.valueOf(existingVersion)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } catch (Throwable t) {
            throw new RuntimeException("Unexpected error.", t);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== DefaultSchemaRegistryClient.getSchemaVersions( " + schemaMetadataName + " ): "
                      + res.size() + " versions found.");
        }

        return res;
    }

    @Override
    public void checkConnection() throws Exception {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> DefaultSchemaRegistryClient.checkConnection(): trying to connect to the SR server... ");
        }
        String respStr = sendWebRequest(currentSchemaRegistryTargets().schemaRegistryVersion);

        if (!(respStr.contains("version") && respStr.contains("revision"))) {
            LOG.error("DefaultSchemaRegistryClient.checkConnection(): Connection failed. Bad response body.");
            throw new Exception("Connection failed. Bad response body.");
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== DefaultSchemaRegistryClient.checkConnection(): connection test successful ");
        }
    }

    /** Run the provided function if the value matches the pattern. This method swallows the exception if the pattern or value are invalid. */
    private static void applyIfPatternMatches(String pattern, Object value, Function<String, Object> f) {
        if (pattern == null || pattern.trim().isEmpty()) {
            return;
        }
        if (pattern.trim().equals("*")) pattern = ".*";  // fix regex pattern
        try {
            if (value.toString().matches(pattern)) {
                f.apply(value.toString());
            }
        } catch (Exception ex) {
            LOG.warn("Invalid pattern: " + pattern);
        }
    }

    /** Useful for debugging. */
    private static String getClasspath() {
        StringBuilder str = new StringBuilder();
        ClassLoader cl = DefaultSchemaRegistryClient.class.getClassLoader();

        str.append("Classpath for the SchemaRegistry-Ranger plugin");
        for (URL url: ((URLClassLoader)cl).getURLs()){
            str.append(':').append(url.getFile());
        }

        return str.toString();
    }

}