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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hands out Trino connections to resource lookups. Connections are pooled per service because building
 * one costs far more than the 1 second budget ranger-admin allows a lookup, and they are handed out
 * exclusively so that concurrent lookups for the same service do not serialize behind a single
 * connection the way a shared client would.
 */
public class TrinoConnectionManager
{
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectionManager.class);

    private static final String POOL_SIZE_PROP    = "lookup.connection.pool.size";
    private static final int    DEFAULT_POOL_SIZE = 2;
    private static final int    MAX_POOL_SIZE     = 10;

    // kept under the 1 second lookup budget so a saturated pool gives up rather than being killed
    private static final long BORROW_WAIT_MS = 800;

    // close() on a half-dead socket can block, and evicting a pool must never stall a lookup
    private static final ExecutorService CLOSER = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r)
        {
            Thread ret = new Thread(r, "trino-client-closer");

            ret.setDaemon(true);

            return ret;
        }
    });

    protected ConcurrentMap<String, ClientPool> trinoConnectionCache;
    protected ConcurrentMap<String, Boolean> repoConnectStatusMap;

    /**
     * Which pool each checked-out connection came from. Looking the pool up by service name is not
     * enough: once a pool is evicted a replacement takes its name, and a connection still in use from
     * the old one would otherwise be handed back into the new pool and keep serving the old endpoint.
     */
    private final ConcurrentMap<TrinoClient, ClientPool> clientOwners = new ConcurrentHashMap<>();

    public TrinoConnectionManager()
    {
        trinoConnectionCache = new ConcurrentHashMap<>();
        repoConnectStatusMap = new ConcurrentHashMap<>();
    }

    /**
     * Checks out a connection for the caller's exclusive use. The caller must hand it back through
     * {@link #returnClient(String, TrinoClient)} or {@link #discardClient(String, TrinoClient)}.
     *
     * @return null when no connection could be established or the pool stayed saturated
     */
    public TrinoClient borrowClient(final String serviceName, final String serviceType, final Map<String, String> configs)
    {
        if (serviceType == null) {
            LOG.error("Asset not found with name " + serviceName, new Throwable());

            return null;
        }

        if (configs == null) {
            LOG.error("Connection Config not defined for asset :" + serviceName, new Throwable());

            return null;
        }

        ClientPool pool = getPool(serviceName, configs);
        TrinoClient ret = pool.idle.poll();

        if (ret == null) {
            ret = pool.size.get() < pool.capacity ? grow(pool, serviceName, configs) : null;
        }

        if (ret == null) {
            // the pool is at capacity: wait briefly for a peer to finish rather than failing outright
            try {
                ret = pool.idle.poll(BORROW_WAIT_MS, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (ret == null) {
                LOG.warn("No Trino connection available for service [" + serviceName + "] within " + BORROW_WAIT_MS
                        + "ms; all " + pool.capacity + " are busy. Raise " + POOL_SIZE_PROP + " if this recurs.");
            }
        }

        repoConnectStatusMap.put(serviceName, ret != null);

        return ret;
    }

    /**
     * Hands a healthy connection back to the pool. A connection whose pool was evicted meanwhile is
     * closed here rather than at eviction time, so an in-flight query is never closed underneath its
     * caller.
     */
    public void returnClient(final String serviceName, final TrinoClient client)
    {
        if (client == null) {
            return;
        }

        ClientPool pool = clientOwners.get(client);

        // the pool it came from may have been evicted while this query was running: closing it now,
        // rather than at eviction time, is what keeps a close() from racing an in-flight query
        if (pool == null || pool.evicted || !pool.idle.offer(client)) {
            retire(client);
        }
    }

    /** Drops a connection that must not be reused, freeing its slot for a fresh one. */
    public void discardClient(final String serviceName, final TrinoClient client)
    {
        if (client != null) {
            retire(client);
        }
    }

    private void retire(final TrinoClient client)
    {
        ClientPool owner = clientOwners.remove(client);

        if (owner != null) {
            owner.size.decrementAndGet();
        }

        closeAsync(client);
    }

    private ClientPool getPool(final String serviceName, final Map<String, String> configs)
    {
        while (true) {
            ClientPool pool = trinoConnectionCache.get(serviceName);

            if (pool != null) {
                if (pool.configs.equals(configs)) {
                    return pool;
                }

                // the admin edited jdbc.url/username/password: pooled connections still point at the
                // old endpoint, and connectionTest() would not reveal it because it builds a fresh client
                LOG.info("Configuration for service [" + serviceName + "] changed; reconnecting to Trino");

                if (trinoConnectionCache.remove(serviceName, pool)) {
                    evict(pool);
                }

                continue;
            }

            ClientPool created = new ClientPool(configs, poolSize(configs));
            ClientPool existing = trinoConnectionCache.putIfAbsent(serviceName, created);

            if (existing == null) {
                return created;
            }
        }
    }

    private TrinoClient grow(final ClientPool pool, final String serviceName, final Map<String, String> configs)
    {
        if (pool.size.incrementAndGet() > pool.capacity) {
            pool.size.decrementAndGet();

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

        TrinoClient ret;

        try {
            ret = TimedEventUtil.timedTask(connectTrino, 5, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            // lookup surfaces a failure here as an empty result, so log the cause in full
            LOG.error("Error connecting to Trino repository: " + serviceName, e);

            ret = null;
        }

        if (ret == null) {
            pool.size.decrementAndGet();

            return null;
        }

        clientOwners.put(ret, pool);

        return ret;
    }

    /**
     * Retires a pool. Only idle connections are closed here; any that are checked out are closed by
     * their borrower in {@link #returnClient(String, TrinoClient)} once the query finishes.
     */
    private void evict(final ClientPool pool)
    {
        pool.evicted = true;

        for (TrinoClient client = pool.idle.poll(); client != null; client = pool.idle.poll()) {
            retire(client);
        }
    }

    private static void closeAsync(final TrinoClient client)
    {
        try {
            CLOSER.submit(new Runnable() {
                @Override
                public void run()
                {
                    client.close();
                }
            });
        }
        catch (Exception e) {
            LOG.warn("Could not schedule a Trino connection for closing; closing inline", e);

            client.close();
        }
    }

    private static int poolSize(final Map<String, String> configs)
    {
        String configured = configs.get(POOL_SIZE_PROP);

        if (configured == null || configured.trim().isEmpty()) {
            return DEFAULT_POOL_SIZE;
        }

        try {
            int ret = Integer.parseInt(configured.trim());

            if (ret < 1) {
                return DEFAULT_POOL_SIZE;
            }

            return Math.min(ret, MAX_POOL_SIZE);
        }
        catch (NumberFormatException e) {
            LOG.warn("Invalid value [" + configured + "] for " + POOL_SIZE_PROP + "; using " + DEFAULT_POOL_SIZE);

            return DEFAULT_POOL_SIZE;
        }
    }

    private static final class ClientPool
    {
        private final Map<String, String>        configs;
        private final int                        capacity;
        private final BlockingQueue<TrinoClient> idle = new LinkedBlockingQueue<>();
        private final AtomicInteger              size = new AtomicInteger();

        private volatile boolean evicted;

        private ClientPool(Map<String, String> configs, int capacity)
        {
            // defensive copy: the configs map is owned by ranger-admin and may be reused
            this.configs  = new HashMap<>(configs);
            this.capacity = capacity;
        }
    }
}
