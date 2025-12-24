package org.apache.ranger.services.gravitino.client;

import java.util.Map;

public class GravitinoConnectionManager {

    public static GravitinoClient getClient(
            String serviceName,
            String serviceType,
            Map<String, String> configs) throws Exception {
        if (serviceType.equals("http")) return new GravitinoHttpClient(serviceName, configs);
        throw new Exception("Unsupported service type: " + serviceType);
    }
}
