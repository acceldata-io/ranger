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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopConfigHolder;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TrinoClient
        extends BaseClient implements Closeable
{
    public static final String TRINO_USER_NAME_PROP = "user";
    public static final String TRINO_PASSWORD_PROP = "password";

    private static final Logger LOG = LoggerFactory.getLogger(TrinoClient.class);
    private static final String ERR_MSG = "You can still save the repository and start creating "
            + "policies, but you would not be able to use autocomplete for "
            + "resource names. Check ranger_admin.log for more info.";

    private static final String JDBC_DRIVER_CLASS_NAME_PROP = "jdbc.driverClassName";
    private static final String JDBC_URL_PROP = "jdbc.url";
    private static final String DEFAULT_DRIVER_CLASS_NAME = "io.trino.jdbc.TrinoDriver";
    // trino-jdbc matches URL property names case-sensitively and spells this one "SSL"
    private static final String TRINO_SSL_PROP = "SSL";

    /**
     * Dotless configuration names that belong to Ranger rather than the driver. Everything containing
     * a dot is filtered separately, so only these need listing explicitly.
     */
    private static final Set<String> RANGER_OWNED_PROPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "username", "password", "keytabfile", "authtype", "namerules",
            "lookupprincipal", "lookupkeytab", "rangerprincipal", "rangerkeytab",
            "commonNameForCertificate", "clusterName")));

    // the Trino driver spins up an http client per instance, so share one per driver class
    private static final ConcurrentMap<String, Driver> DRIVERS = new ConcurrentHashMap<>();

    private Connection con;

    public TrinoClient(String serviceName)
            throws Exception
    {
        super(serviceName, null);

        init();
    }

    public TrinoClient(String serviceName, Map<String, String> properties)
            throws Exception
    {
        super(serviceName, properties);

        init();
    }

    private void init()
            throws Exception
    {
        Subject.doAs(getLoginSubject(), new PrivilegedAction<Void>()
        {
            public Void run()
            {
                initConnection();

                return null;
            }
        });
    }

    private void initConnection()
    {
        Properties prop = getConfigHolder().getRangerSection();
        String driverClassName = StringUtils.trimToNull(prop.getProperty(JDBC_DRIVER_CLASS_NAME_PROP));
        String url = StringUtils.trimToNull(prop.getProperty(JDBC_URL_PROP));

        if (url == null) {
            String msgDesc = "initConnection: " + JDBC_URL_PROP + " is not configured. "
                    + "Expected a value of the form jdbc:trino://<host>:<port>.";
            HadoopException hdpException = new HadoopException(msgDesc);

            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERR_MSG, null, JDBC_URL_PROP);

            throw hdpException;
        }

        if (driverClassName == null) {
            driverClassName = DEFAULT_DRIVER_CLASS_NAME;
        }

        Driver driver = getDriver(driverClassName);
        Properties trinoProperties = buildConnectionProperties(prop, url);
        Connection connection;

        try {
            connection = driver.connect(url, trinoProperties);
        }
        catch (SQLException e) {
            String msgDesc = "Unable to connect to Trino instance at [" + url + "].";
            HadoopException hdpException = new HadoopException(msgDesc, e);

            hdpException.generateResponseDataMap(false, getMessage(e), msgDesc + ERR_MSG, null, JDBC_URL_PROP);

            throw hdpException;
        }
        catch (Throwable t) {
            String msgDesc = "initConnection: Unable to connect to Trino instance at [" + url + "]: " + t;
            HadoopException hdpException = new HadoopException(msgDesc, t);

            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERR_MSG, null, JDBC_URL_PROP);

            throw hdpException;
        }

        if (connection == null) {
            String msgDesc = "initConnection: Trino driver [" + driverClassName + "] does not accept the URL [" + url + "].";
            HadoopException hdpException = new HadoopException(msgDesc);

            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERR_MSG, null, JDBC_URL_PROP);

            throw hdpException;
        }

        con = connection;
    }

    private Driver getDriver(String driverClassName)
    {
        Driver ret = DRIVERS.get(driverClassName);

        if (ret == null) {
            try {
                Driver driver = (Driver) Class.forName(driverClassName).newInstance();
                Driver existing = DRIVERS.putIfAbsent(driverClassName, driver);

                ret = existing != null ? existing : driver;
            }
            catch (Throwable t) {
                // covers ClassNotFoundException, InstantiationException, IllegalAccessException,
                // ExceptionInInitializerError, SecurityException and UnsupportedClassVersionError
                String msgDesc = "initConnection: Unable to load the Trino JDBC driver [" + driverClassName + "]: " + t;
                HadoopException hdpException = new HadoopException(msgDesc, t);

                hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERR_MSG, null, JDBC_DRIVER_CLASS_NAME_PROP);

                throw hdpException;
            }
        }

        return ret;
    }

    private Properties buildConnectionProperties(Properties prop, String url)
    {
        Properties ret = new Properties();
        // Trino fails the connection outright if a property is supplied both in the URL and here
        Map<String, String> urlProperties = getUrlProperties(url);

        // forward driver settings configured on the service (truststore, Kerberos, extraCredentials,
        // source, ...) so that a deployment does not have to encode all of them into the URL
        addConfiguredProperties(prop, urlProperties, ret);

        String userName = StringUtils.trimToNull(prop.getProperty(HadoopConfigHolder.RANGER_LOGIN_USER_NAME_PROP));

        if (userName != null && !urlProperties.containsKey(TRINO_USER_NAME_PROP)) {
            ret.put(TRINO_USER_NAME_PROP, userName);
        }

        String password = getDecryptedPassword(prop);

        if (password != null && !urlProperties.containsKey(TRINO_PASSWORD_PROP)) {
            if (isSecureUrl(urlProperties, ret)) {
                ret.put(TRINO_PASSWORD_PROP, password);
            }
            else {
                LOG.warn("Ignoring the password configured for service [" + getSerivceName() + "]: Trino requires TLS for "
                        + "username/password authentication. Either set " + TRINO_SSL_PROP + " to true, in "
                        + JDBC_URL_PROP + " (for example jdbc:trino://<host>:<port>?SSL=true) or as a service "
                        + "configuration, or clear the password.");
            }
        }

        return ret;
    }

    /**
     * Copies service configuration entries through to the driver. Anything Trino does not recognise
     * fails the whole connection with "Unrecognized connection property", so the filter has to be
     * conservative: Ranger's own settings are excluded by name, and so is every name containing a dot,
     * because no Trino connection property has one while nearly every Ranger key does. That second rule
     * is what keeps arbitrary keys added through "Add New Configurations" from reaching the driver.
     */
    private void addConfiguredProperties(Properties prop, Map<String, String> urlProperties, Properties ret)
    {
        for (String name : prop.stringPropertyNames()) {
            if (RANGER_OWNED_PROPS.contains(name) || name.indexOf('.') > -1) {
                continue;
            }

            if (urlProperties.containsKey(name)) {
                LOG.warn("Ignoring the service configuration [" + name + "] for service [" + getSerivceName()
                        + "]: it is already set in " + JDBC_URL_PROP + ", and Trino rejects a property given twice.");

                continue;
            }

            String value = StringUtils.trimToNull(prop.getProperty(name));

            if (value != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Forwarding service configuration [" + name + "] to the Trino driver");
                }

                ret.put(name, value);
            }
        }
    }

    private String getDecryptedPassword(Properties prop)
    {
        String ret = null;

        try {
            ret = PasswordUtils.decryptPassword(getConfigHolder().getPassword());
        }
        catch (Exception ex) {
            LOG.info("Password decryption failed");

            ret = null;
        }
        finally {
            if (ret == null) {
                ret = prop.getProperty(HadoopConfigHolder.RANGER_LOGIN_PASSWORD);
            }
        }

        // Trino rejects an empty password with "Connection property password value is empty"
        return StringUtils.isEmpty(ret) ? null : ret;
    }

    /**
     * Collects the properties already carried by the URL query string, parsed the way trino-jdbc does
     * it: strip the "jdbc:" prefix so the remainder is a hierarchical URI, then read getQuery(), which
     * returns the query with percent-escapes already decoded. URLDecoder must not be used here, as it
     * additionally maps '+' to a space, which java.net.URI does not, so a name or value would end up
     * differing from what the driver sees.
     * <p>
     * Names are kept in their original case because the driver matches them case-sensitively: folding
     * "USER" to "user" would make us suppress the configured username for a URL the driver does not
     * in fact recognise.
     */
    private static Map<String, String> getUrlProperties(String url)
    {
        Map<String, String> ret = new HashMap<>();
        String query;

        try {
            query = new URI(StringUtils.removeStartIgnoreCase(url, "jdbc:")).getQuery();
        }
        catch (URISyntaxException e) {
            // a malformed URL is the driver's to report, with a far better message than we could give
            LOG.debug("Could not parse the query string of [" + url + "]", e);

            return ret;
        }

        if (query == null) {
            return ret;
        }

        for (String param : query.split("&")) {
            int separator = param.indexOf('=');
            String name = (separator > -1 ? param.substring(0, separator) : param).trim();

            if (!name.isEmpty()) {
                ret.put(name, separator > -1 ? param.substring(separator + 1).trim() : "");
            }
        }

        return ret;
    }

    /**
     * Tells a dropped connection apart from a failure the query itself caused. Only the former is worth
     * reconnecting for: retrying an access-denied or bad-syntax failure would double the latency of a
     * predictable error inside the 1 second lookup budget and consume another connection from the pool.
     * The cause is walked because the query methods wrap the original SQLException in a HadoopException,
     * and visited throwables are tracked by identity so a cyclic cause chain cannot spin forever.
     */
    static boolean isConnectionFailure(Throwable t)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());

        for (Throwable cause = t; cause != null && visited.add(cause); cause = cause.getCause()) {
            if (cause instanceof SQLNonTransientConnectionException || cause instanceof SQLRecoverableException
                    || cause instanceof IOException) {
                return true;
            }

            if (cause instanceof SQLException) {
                String sqlState = ((SQLException) cause).getSQLState();

                // SQL standard class 08: connection exception
                if (sqlState != null && sqlState.startsWith("08")) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean isSecureUrl(Map<String, String> urlProperties, Properties trinoProperties)
    {
        // Trino JDBC URLs are jdbc:trino://host[:port][?params]: TLS is signalled only by SSL=true,
        // never by the scheme, and port 443 does not imply it either. The flag counts from either
        // the URL or the service configuration, since both reach the driver the same way
        return Boolean.parseBoolean(urlProperties.get(TRINO_SSL_PROP))
                || Boolean.parseBoolean(trinoProperties.getProperty(TRINO_SSL_PROP));
    }

    /**
     * Builds the LIKE clause for a resource name typed into the Ranger policy form, where '*' is the
     * wildcard. Trino uses SQL LIKE semantics, so '*' has to become '%'. Every other LIKE
     * metacharacter is passed through, so a typed '_' or '%' keeps its SQL wildcard meaning.
     */
    private static String getLikeClause(String needle)
    {
        if (needle == null || needle.isEmpty() || needle.equals("*")) {
            return "";
        }

        return " LIKE '" + StringEscapeUtils.escapeSql(needle).replace('*', '%') + "%'";
    }

    private static boolean hasWildcard(List<String> values)
    {
        if (values != null) {
            for (String value : values) {
                if (value != null && value.indexOf('*') > -1) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Replaces wildcard entries with the names they match. Policies routinely hold a parent value of
     * '*', which cannot be used directly in SHOW SCHEMAS FROM "..." / SHOW TABLES FROM "..."."...".
     */
    private static List<String> expand(List<String> patterns, List<String> available)
    {
        List<String> ret = new ArrayList<>();

        for (String pattern : patterns) {
            if (pattern == null) {
                continue;
            }

            if (pattern.indexOf('*') < 0) {
                if (!ret.contains(pattern)) {
                    ret.add(pattern);
                }

                continue;
            }

            for (String value : available) {
                if (FilenameUtils.wildcardMatch(value, pattern) && !ret.contains(value)) {
                    ret.add(value);
                }
            }
        }

        return ret;
    }

    private List<String> expandCatalogs(List<String> catalogs)
            throws HadoopException
    {
        return hasWildcard(catalogs) ? expand(catalogs, getCatalogs(null, null)) : catalogs;
    }

    private List<String> expandSchemas(String catalog, List<String> schemas)
            throws HadoopException
    {
        return hasWildcard(schemas) ? expand(schemas, getSchemas(null, Collections.singletonList(catalog), null)) : schemas;
    }

    private List<String> expandTables(String catalog, String schema, List<String> tables)
            throws HadoopException
    {
        return hasWildcard(tables)
                ? expand(tables, getTables(null, Collections.singletonList(catalog), Collections.singletonList(schema), null))
                : tables;
    }

    private List<String> getCatalogs(String needle, List<String> catalogs)
            throws HadoopException
    {
        List<String> ret = new ArrayList<>();

        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            // Cannot use a prepared statement for this as trino does not support that
            String sql = "SHOW CATALOGS" + getLikeClause(needle);

            try {
                stat = con.createStatement();
                rs = stat.executeQuery(sql);

                while (rs.next()) {
                    String catalogName = rs.getString(1);

                    if (catalogs != null && catalogs.contains(catalogName)) {
                        continue;
                    }

                    ret.add(catalogName);
                }
            }
            catch (SQLTimeoutException sqlt) {
                String msgDesc = "Time Out, Unable to execute SQL [" + sql + "].";
                HadoopException hdpException = new HadoopException(msgDesc, sqlt);

                hdpException.generateResponseDataMap(false, getMessage(sqlt), msgDesc + ERR_MSG, null, null);

                throw hdpException;
            }
            catch (SQLException se) {
                String msg = "Unable to execute SQL [" + sql + "]. ";
                HadoopException he = new HadoopException(msg, se);

                he.generateResponseDataMap(false, getMessage(se), msg + ERR_MSG, null, null);

                throw he;
            }
            finally {
                close(rs);
                close(stat);
            }
        }
        return ret;
    }

    public List<String> getCatalogList(String needle, final List<String> catalogs)
            throws HadoopException
    {
        final String ndl = needle;
        final List<String> catList = catalogs;
        List<String> dbs = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run()
            {
                List<String> ret = null;
                try {
                    ret = getCatalogs(ndl, catList);
                }
                catch (HadoopException he) {
                    LOG.error("<== TrinoClient.getCatalogList() :Unable to get the Database List", he);
                    throw he;
                }
                return ret;
            }
        });

        return dbs;
    }

    private List<String> getSchemas(String needle, List<String> catalogs, List<String> schemas)
            throws HadoopException
    {
        List<String> ret = new ArrayList<>();

        if (con != null && catalogs != null && !catalogs.isEmpty()) {
            String likeClause = getLikeClause(needle);
            String lastSql = null;
            SQLException lastError = null;

            for (String catalog : expandCatalogs(catalogs)) {
                Statement stat = null;
                ResultSet rs = null;
                String sql = "SHOW SCHEMAS FROM \"" + StringEscapeUtils.escapeSql(catalog) + "\"" + likeClause;

                try {
                    stat = con.createStatement();
                    rs = stat.executeQuery(sql);

                    while (rs.next()) {
                        String schema = rs.getString(1);

                        if (schemas != null && schemas.contains(schema)) {
                            continue;
                        }

                        ret.add(schema);
                    }
                }
                catch (SQLException sqle) {
                    if (isConnectionFailure(sqle)) {
                        // the connection itself is gone: surfacing it lets the caller replace the
                        // client, where suppressing it would silently return a partial result
                        throw newQueryException("TrinoClient.getSchemas()", sql, sqle);
                    }

                    // a catalog the lookup user cannot read must not fail the whole lookup
                    LOG.warn("Unable to execute SQL [" + sql + "].", sqle);

                    lastSql = sql;
                    lastError = sqle;
                }
                finally {
                    close(rs);
                    close(stat);
                }
            }

            if (ret.isEmpty() && lastError != null) {
                throw newQueryException("TrinoClient.getSchemas()", lastSql, lastError);
            }
        }

        return ret;
    }

    public List<String> getSchemaList(String needle, List<String> catalogs, List<String> schemas)
            throws HadoopException
    {
        final String ndl = needle;
        final List<String> cats = catalogs;
        final List<String> shms = schemas;
        List<String> schemaList = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>()
        {
            @Override
            public List<String> run()
            {
                List<String> ret = null;
                try {
                    ret = getSchemas(ndl, cats, shms);
                }
                catch (HadoopException he) {
                    LOG.error("<== TrinoClient.getSchemaList() :Unable to get the Schema List", he);

                    throw he;
                }

                return ret;
            }
        });

        return schemaList;
    }

    private List<String> getTables(String needle, List<String> catalogs, List<String> schemas, List<String> tables)
            throws HadoopException
    {
        List<String> ret = new ArrayList<>();

        if (con != null && catalogs != null && !catalogs.isEmpty() && schemas != null && !schemas.isEmpty()) {
            String likeClause = getLikeClause(needle);
            String lastSql = null;
            SQLException lastError = null;

            for (String catalog : expandCatalogs(catalogs)) {
                for (String schema : expandSchemas(catalog, schemas)) {
                    Statement stat = null;
                    ResultSet rs = null;
                    String sql = "SHOW tables FROM \"" + StringEscapeUtils.escapeSql(catalog) + "\".\""
                            + StringEscapeUtils.escapeSql(schema) + "\"" + likeClause;

                    try {
                        stat = con.createStatement();
                        rs = stat.executeQuery(sql);

                        while (rs.next()) {
                            String table = rs.getString(1);

                            if (tables != null && tables.contains(table)) {
                                continue;
                            }

                            ret.add(table);
                        }
                    }
                    catch (SQLException sqle) {
                        if (isConnectionFailure(sqle)) {
                            // the connection itself is gone: surfacing it lets the caller replace the
                            // client, where suppressing it would silently return a partial result
                            throw newQueryException("TrinoClient.getTables()", sql, sqle);
                        }

                        // a schema the lookup user cannot read must not fail the whole lookup
                        LOG.warn("Unable to execute SQL [" + sql + "].", sqle);

                        lastSql = sql;
                        lastError = sqle;
                    }
                    finally {
                        close(rs);
                        close(stat);
                    }
                }
            }

            if (ret.isEmpty() && lastError != null) {
                throw newQueryException("TrinoClient.getTables()", lastSql, lastError);
            }
        }

        return ret;
    }

    public List<String> getTableList(String needle, List<String> catalogs, List<String> schemas, List<String> tables)
            throws HadoopException
    {
        final String ndl = needle;
        final List<String> cats = catalogs;
        final List<String> shms = schemas;
        final List<String> tbls = tables;
        List<String> tableList = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run()
            {
                List<String> ret = null;
                try {
                    ret = getTables(ndl, cats, shms, tbls);
                }
                catch (HadoopException he) {
                    LOG.error("<== TrinoClient.getTableList() :Unable to get the Column List", he);

                    throw he;
                }
                return ret;
            }
        });

        return tableList;
    }

    private List<String> getColumns(String needle, List<String> catalogs, List<String> schemas, List<String> tables, List<String> columns)
            throws HadoopException
    {
        List<String> ret = new ArrayList<>();

        if (con != null && catalogs != null && !catalogs.isEmpty() && schemas != null && !schemas.isEmpty()
                && tables != null && !tables.isEmpty()) {
            String regex = needle != null && !needle.isEmpty() ? needle : null;
            String lastSql = null;
            SQLException lastError = null;

            for (String catalog : expandCatalogs(catalogs)) {
                for (String schema : expandSchemas(catalog, schemas)) {
                    for (String table : expandTables(catalog, schema, tables)) {
                        Statement stat = null;
                        ResultSet rs = null;
                        String sql = "SHOW COLUMNS FROM \"" + StringEscapeUtils.escapeSql(catalog) + "\"." +
                            "\"" + StringEscapeUtils.escapeSql(schema) + "\"." +
                            "\"" + StringEscapeUtils.escapeSql(table) + "\"";

                        try {
                            stat = con.createStatement();
                            rs = stat.executeQuery(sql);

                            while (rs.next()) {
                                String column = rs.getString(1);

                                if (columns != null && columns.contains(column)) {
                                    continue;
                                }

                                if (regex == null || FilenameUtils.wildcardMatch(column, regex)) {
                                    ret.add(column);
                                }
                            }
                        }
                        catch (SQLException sqle) {
                            if (isConnectionFailure(sqle)) {
                                // the connection itself is gone: surfacing it lets the caller replace
                                // the client, where suppressing it would return a partial result
                                throw newQueryException("TrinoClient.getColumns()", sql, sqle);
                            }

                            // a table the lookup user cannot read must not fail the whole lookup
                            LOG.warn("Unable to execute SQL [" + sql + "].", sqle);

                            lastSql = sql;
                            lastError = sqle;
                        }
                        finally {
                            close(rs);
                            close(stat);
                        }
                    }
                }
            }

            if (ret.isEmpty() && lastError != null) {
                throw newQueryException("TrinoClient.getColumns()", lastSql, lastError);
            }
        }

        return ret;
    }

    private HadoopException newQueryException(String context, String sql, SQLException sqle)
    {
        String msgDesc = sqle instanceof SQLTimeoutException
                ? "Time Out, Unable to execute SQL [" + sql + "]."
                : "Unable to execute SQL [" + sql + "].";
        HadoopException hdpException = new HadoopException(msgDesc, sqle);

        hdpException.generateResponseDataMap(false, getMessage(sqle), msgDesc + ERR_MSG, null, null);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== " + context + " Error : ", sqle);
        }

        return hdpException;
    }

    public List<String> getColumnList(String needle, List<String> catalogs, List<String> schemas, List<String> tables, List<String> columns)
            throws HadoopException
    {
        final String ndl = needle;
        final List<String> cats = catalogs;
        final List<String> shms = schemas;
        final List<String> tbls = tables;
        final List<String> cols = columns;
        List<String> columnList = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run()
            {
                List<String> ret = null;
                try {
                    ret = getColumns(ndl, cats, shms, tbls, cols);
                }
                catch (HadoopException he) {
                    LOG.error("<== TrinoClient.getColumnList() :Unable to get the Column List", he);

                    throw he;
                }

                return ret;
            }
        });

        return columnList;
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> connectionProperties)
            throws Exception
    {
        TrinoClient client = null;
        Map<String, Object> resp = new HashMap<String, Object>();
        boolean status = false;
        List<String> testResult = null;

        try {
            client = new TrinoClient(serviceName, connectionProperties);

            if (client != null) {
                testResult = client.getCatalogList("*", null);

                if (testResult != null && testResult.size() != 0) {
                    status = true;
                }
            }

            if (status) {
                String msg = "Connection test successful";

                generateResponseDataMap(status, msg, msg, null, null, resp);
            }
            else {
                String msg = "Unable to retrieve any catalogs using given parameters.";

                generateResponseDataMap(status, msg, msg + ERR_MSG, null, null, resp);
            }
        }
        catch (Exception e) {
            throw e;
        }
        finally {
            if (client != null) {
                client.close();
            }
        }

        return resp;
    }

    public void close()
    {
        Subject.doAs(getLoginSubject(), new PrivilegedAction<Void>()
        {
            public Void run()
            {
                close(con);

                return null;
            }
        });
    }

    private void close(Connection con)
    {
        try {
            if (con != null) {
                con.close();
            }
        }
        catch (SQLException e) {
            LOG.error("Unable to close Trino SQL connection", e);
        }
    }

    public void close(Statement stat)
    {
        try {
            if (stat != null) {
                stat.close();
            }
        }
        catch (SQLException e) {
            LOG.error("Unable to close SQL statement", e);
        }
    }

    public void close(ResultSet rs)
    {
        try {
            if (rs != null) {
                rs.close();
            }
        }
        catch (SQLException e) {
            LOG.error("Unable to close ResultSet", e);
        }
    }
}
