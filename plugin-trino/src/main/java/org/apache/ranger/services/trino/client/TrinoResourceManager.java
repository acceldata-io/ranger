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

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class TrinoResourceManager
{
    private static final Logger LOG = LoggerFactory.getLogger(TrinoResourceManager.class);
    private static final String CATALOG = "catalog";
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private static final String COLUMN = "column";

    // shared so that connections are reused across lookups: resource lookup runs under a 1 second
    // budget in ranger-admin, which is not enough to build a new connection on every keystroke
    private static final TrinoConnectionManager CONNECTION_MANAGER = new TrinoConnectionManager();

    private TrinoResourceManager()
    {
        // no instantiation
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs)
            throws Exception
    {
        Map<String, Object> ret = null;

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> TrinoResourceManager.connectionTest() ServiceName: " + serviceName + " Configs: " + configs);
        }

        try {
            ret = TrinoClient.connectionTest(serviceName, configs);
        }
        catch (Exception e) {
            LOG.error("<== TrinoResourceManager.connectionTest() Error: " + e);

            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== TrinoResourceManager.connectionTest() Result : " + ret);
        }

        return ret;
    }

    public static List<String> getTrinoResources(String serviceName, String serviceType, Map<String, String> configs, ResourceLookupContext context)
            throws Exception
    {
        String userInput = context.getUserInput();
        String resource = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        List<String> resultList = null;
        List<String> catalogList = null;
        List<String> schemaList = null;
        List<String> tableList = null;
        List<String> columnList = null;
        String catalogName = null;
        String schemaName = null;
        String tableName = null;
        String columnName = null;

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== TrinoResourceMgr.getTrinoResources() UserInput: \"" + userInput + "\" resource : " + resource + " resourceMap: " + resourceMap);
        }

        if (userInput != null && resource != null) {
            if (resourceMap != null && !resourceMap.isEmpty()) {
                catalogList = resourceMap.get(CATALOG);
                schemaList = resourceMap.get(SCHEMA);
                tableList = resourceMap.get(TABLE);
                columnList = resourceMap.get(COLUMN);
            }

            switch (resource.trim().toLowerCase()) {
                case CATALOG:
                    catalogName = userInput;
                    break;
                case SCHEMA:
                    schemaName = userInput;
                    break;
                case TABLE:
                    tableName = userInput;
                    break;
                case COLUMN:
                    columnName = userInput;
                    break;
                default:
                    break;
            }
        }

        if (serviceName != null && userInput != null) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("==> TrinoResourceManager.getTrinoResources() UserInput: \"" + userInput + "\" configs: " + configs + " catalogList: " + catalogList + " tableList: " + tableList + " columnList: " + columnList);
                }

                if (columnName != null && !columnName.isEmpty()) {
                    // Column names are matched by the wildcardmatcher
                    columnName += "*";
                }

                TrinoClient trinoClient = CONNECTION_MANAGER.borrowClient(serviceName, serviceType, configs);

                if (trinoClient != null) {
                    try {
                        try {
                            resultList = lookup(trinoClient, catalogName, schemaName, tableName, columnName, catalogList, schemaList, tableList, columnList);
                        }
                        catch (Exception e) {
                            if (!TrinoClient.isConnectionFailure(e)) {
                                throw e;
                            }

                            // the borrowed connection was closed by the coordinator; drop it and retry once
                            LOG.warn("Lookup failed on the borrowed Trino connection for service [" + serviceName + "]; reconnecting and retrying once", e);

                            CONNECTION_MANAGER.discardClient(serviceName, trinoClient);

                            trinoClient = CONNECTION_MANAGER.borrowClient(serviceName, serviceType, configs);

                            if (trinoClient == null) {
                                throw e;
                            }

                            resultList = lookup(trinoClient, catalogName, schemaName, tableName, columnName, catalogList, schemaList, tableList, columnList);
                        }

                        CONNECTION_MANAGER.returnClient(serviceName, trinoClient);

                        // cleared so the catch below cannot hand the same connection back twice
                        trinoClient = null;
                    }
                    catch (Exception e) {
                        // a connection that failed on the retry too must not go back into the pool
                        if (TrinoClient.isConnectionFailure(e)) {
                            CONNECTION_MANAGER.discardClient(serviceName, trinoClient);
                        }
                        else {
                            CONNECTION_MANAGER.returnClient(serviceName, trinoClient);
                        }

                        throw e;
                    }
                }
            }
            catch (Exception e) {
                LOG.error("Unable to get Trino resource", e);

                throw e;
            }
        }

        return resultList;
    }

    private static List<String> lookup(final TrinoClient trinoClient, final String catalogName, final String schemaName,
            final String tableName, final String columnName, final List<String> catalogList, final List<String> schemaList,
            final List<String> tableList, final List<String> columnList)
            throws Exception
    {
        Callable<List<String>> callableObj = null;

        // exactly one of the four names is set by the caller, so a non-null value identifies the level
        // being looked up. An empty value means the policy form opened the dropdown without any typing,
        // which the client turns into an unfiltered query rather than no query at all.
        // An unfiltered query is the slowest case and still has to fit the 1 second lookup budget; raise
        // ranger.servicetype.trino.resource.lookup.timeout.value.in.ms in ranger-admin-site.xml, or
        // resource.lookup.timeout.value.in.ms on the service itself, if a deployment needs longer
        if (catalogName != null) {
            callableObj = new Callable<List<String>>() {
                @Override
                public List<String> call()
                        throws Exception
                {
                    return trinoClient.getCatalogList(catalogName, catalogList);
                }
            };
        }
        else if (schemaName != null) {
            callableObj = new Callable<List<String>>() {
                @Override
                public List<String> call()
                        throws Exception
                {
                    return trinoClient.getSchemaList(schemaName, catalogList, schemaList);
                }
            };
        }
        else if (tableName != null) {
            callableObj = new Callable<List<String>>() {
                @Override
                public List<String> call()
                        throws Exception
                {
                    return trinoClient.getTableList(tableName, catalogList, schemaList, tableList);
                }
            };
        }
        else if (columnName != null) {
            callableObj = new Callable<List<String>>() {
                @Override
                public List<String> call()
                        throws Exception
                {
                    return trinoClient.getColumnList(columnName, catalogList, schemaList, tableList, columnList);
                }
            };
        }

        if (callableObj == null) {
            LOG.error("Could not initiate a TrinoClient timedTask");

            return null;
        }

        // no monitor needed: borrowClient() hands the connection out exclusively, so concurrent
        // lookups for the same service run on separate connections instead of queueing behind one
        return TimedEventUtil.timedTask(callableObj, 5, TimeUnit.SECONDS);
    }
}
