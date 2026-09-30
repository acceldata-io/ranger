/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.services.trino.client;

import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class TrinoConnectionManager
{
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectionManager.class);

    protected ConcurrentMap<String, TrinoClient> trinoConnectionCache;
    protected ConcurrentMap<String, Boolean> repoConnectStatusMap;

    public TrinoConnectionManager()
    {
        trinoConnectionCache = new ConcurrentHashMap<>();
        repoConnectStatusMap = new ConcurrentHashMap<>();
    }

    public TrinoClient getTrinoConnection(final String serviceName, final String serviceType, final Map<String, String> configs)
    {
        if (serviceType == null) {
            LOG.error("Asset not found with name " + serviceName, new Throwable());

            return null;
        }

        // a cached connection is handed back as-is: probing it with a query here would double the
        // round trips of every lookup. Callers invalidate it via resetTrinoConnection() if it is stale
        TrinoClient trinoClient = trinoConnectionCache.get(serviceName);

        if (trinoClient != null) {
            return trinoClient;
        }

        if (configs == null) {
            LOG.error("Connection Config not defined for asset :" + serviceName, new Throwable());

            return null;
        }

        final Callable<TrinoClient> connectTrino = new Callable<TrinoClient>() {
            @Override
            public TrinoClient call()
                    throws Exception
            {
                return new TrinoClient(serviceName, configs);
            }
        };

        try {
            trinoClient = TimedEventUtil.timedTask(connectTrino, 5, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            // lookup surfaces a failure here as an empty result, so log the cause in full
            LOG.error("Error connecting to Trino repository: " + serviceName, e);

            trinoClient = null;
        }

        if (trinoClient != null) {
            TrinoClient oldClient = trinoConnectionCache.putIfAbsent(serviceName, trinoClient);

            if (oldClient != null) {
                trinoClient.close();

                trinoClient = oldClient;
            }
        }
        else {
            // another thread may have connected in the meantime
            trinoClient = trinoConnectionCache.get(serviceName);
        }

        repoConnectStatusMap.put(serviceName, trinoClient != null);

        return trinoClient;
    }

    public void resetTrinoConnection(final String serviceName, final TrinoClient staleClient)
    {
        if (staleClient != null && trinoConnectionCache.remove(serviceName, staleClient)) {
            staleClient.close();
        }
    }
}
