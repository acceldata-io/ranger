package org.apache.ranger.services.gravitino.client;

import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class GravitinoHttpClient extends BaseClient implements GravitinoClient {
    public GravitinoHttpClient(String serviceName, Map<String, String> connectionConfig) {
        super(serviceName, connectionConfig);
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        Map<String, Object> resp = new HashMap<>();
        try {
            GravitinoHttpClient client = new GravitinoHttpClient(serviceName, configs);
            Properties p = client.getConfigHolder().getRangerSection();
            String baseUrl = p.getProperty("gravitino.url");
            if (baseUrl == null || baseUrl.isEmpty()) {
                BaseClient.generateResponseDataMap(false, "Missing gravitino.url",
                        "Missing gravitino.url", null, "gravitino.url", resp);
                return resp;
            }

            URL url = new URL(baseUrl + "/api/v1/metalakes");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            // TODO: set auth headers if needed (basic/token/TLS client cert)
            // conn.setRequestProperty("Authorization", "Bearer " + token);

            int code = conn.getResponseCode();
            if (code >= 200 && code < 300) {
                BaseClient.generateResponseDataMap(true, "Connection test successful",
                        "Connection test successful", null, null, resp);
            } else {
                BaseClient.generateResponseDataMap(false, "Connection test failed (HTTP " + code + ")",
                        "Connection test failed (HTTP " + code + ")", null, null, resp);
            }

            return resp;
        } catch (Exception e) {
            HadoopException hE = new HadoopException(e.getMessage());
            hE.setStackTrace(e.getStackTrace());
            hE.generateResponseDataMap(false, BaseClient.getMessage(e),
                    "Unable to connect to Gravitino", null, null);
            throw hE;
        }
    }

    @Override
    public List<String> listMetalakes(String prefix) throws Exception {
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = p.getProperty("gravitino.url");

        URL  url = new URL(baseUrl + "/api/v1/metalakes");
        return executeAndParseNames(url, prefix);
    }

    @Override
    public List<String> listCatalogs(String metalake, String prefix) throws Exception {
        if (metalake == null || metalake.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        Properties p = getConfigHolder().getRangerSection();
        String baseUrl = p.getProperty("gravitino.url");

        // TODO: replace with correct endpoint and parse response JSON
        URL url = new URL(baseUrl + "/api/v1/metalakes/" + metalake + "/catalogs");
        return executeAndParseNames(url, prefix);
    }

    private List<String> executeAndParseNames(URL url, String prefix) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        int code = conn.getResponseCode();
        InputStream in = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();

        // TODO: parse JSON -> list of names, filter by prefix if needed
        throw new UnsupportedOperationException("Implement JSON parsing + prefix filtering");
    }


}
