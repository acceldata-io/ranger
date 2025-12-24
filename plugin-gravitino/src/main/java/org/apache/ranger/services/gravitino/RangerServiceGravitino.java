package org.apache.ranger.services.gravitino;

import org.apache.ranger.plugin.client.HadoopConfigHolder;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.gravitino.client.GravitinoHttpClient;
import org.apache.ranger.services.gravitino.client.GravitinoResourceManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerServiceGravitino extends RangerBaseService{

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<>();
        String serviceName = getServiceName();

        if (configs != null) {
            if (!configs.containsKey(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)) {
                configs.put(HadoopConfigHolder.RANGER_LOGIN_PASSWORD, null);
            }

            try {
                ret = GravitinoHttpClient.connectionTest(serviceName, configs);
            } catch (HadoopException he) {
                throw he;
            }
        }

        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<>();
        String serviceName = getServiceName();
        String serviceType = getServiceType();
        Map<String, String> configs = getConfigs();

        if (configs != null && !configs.containsKey(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)) {
            configs.put(HadoopConfigHolder.RANGER_LOGIN_PASSWORD, null);
        }

        return GravitinoResourceManager.getGravitinoResources(serviceName, serviceType, configs, context);
    }
}