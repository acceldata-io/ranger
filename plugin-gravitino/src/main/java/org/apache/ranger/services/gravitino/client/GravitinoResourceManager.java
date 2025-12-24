package org.apache.ranger.services.gravitino.client;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class GravitinoResourceManager {
    private static final String METALAKE = "metalake";
    private static final String CATALOG = "catalog";

    public static List<String> getGravitinoResources(
            String serviceName,
            String serviceType,
            Map<String, String> configs,
            ResourceLookupContext context
    ) throws Exception {
        String userInput = context.getUserInput();
        String resourceName = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();

        // Parents from UI (if user is selecting child resource)
        List<String> metalakes = resourceMap != null ? resourceMap.get(METALAKE) : null;
        String metalake = (metalakes != null && !metalakes.isEmpty()) ? metalakes.get(0) : null;

        final GravitinoHttpClient client = new GravitinoHttpClient(serviceName, configs);

        Callable<List<String>> task = null;

        if (METALAKE.equalsIgnoreCase(resourceName)) {
            final String needle = (userInput == null) ? "" : userInput;
            task = () -> client.listMetalakes(needle);
        } else if (CATALOG.equalsIgnoreCase(resourceName)) {
            final String needle = (userInput == null) ? "" : userInput;
            final String selectedMetalake = metalake; // must be chosen in UI
            task = () -> client.listCatalogs(selectedMetalake, needle);
        }

        if (task == null) return null;

        // Keep it bounded so UI doesn’t hang
        return TimedEventUtil.timedTask(task, 5, TimeUnit.SECONDS);
    }
}
