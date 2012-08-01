package com.hpccsystems.jdbcdriver;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author rpastrana
 */

public class HPCCConnection implements Connection
{
    public static final String   ECLRESULTLIMDEFAULT      = "100";
    public static final String   CLUSTERDEFAULT           = "hthor";
    public static final String   QUERYSETDEFAULT          = "hthor";
    public static final String   SERVERADDRESSDEFAULT     = "localhost";
    public static final String   WSECLWATCHPORTDEFAULT    = "8010";
    public static final String   WSECLPORTDEFAULT         = "8002";
    public static final String   WSECLDIRECTPORTDEFAULT   = "8010";
    public static final int      FETCHPAGESIZEDEFAULT     = 100;
    public static final String   LAZYLOADDEFAULT          = "true";
    public static final int      CONNECTTIMEOUTMILDEFAULT = 1000;

    private boolean              closed;
    private HPCCDatabaseMetaData metadata;
    private Properties           props;
    private String               serverAddress;
    private Properties           clientInfo;

    public HPCCConnection(Properties props)
    {
        closed = false;

        this.serverAddress = SERVERADDRESSDEFAULT;

        if (props.containsKey("ServerAddress"))
            this.serverAddress = props.getProperty("ServerAddress");
        else
            props.setProperty("ServerAddress", this.serverAddress);

        this.props = props;

        if (!this.props.containsKey("Cluster"))
            this.props.setProperty("Cluster", CLUSTERDEFAULT);

        if (!this.props.containsKey("QuerySet"))
            this.props.setProperty("QuerySet", QUERYSETDEFAULT);

        if (!this.props.containsKey("WsECLWatchAddress"))
            this.props.setProperty("WsECLWatchAddress", serverAddress);

        if (!this.props.containsKey("WsECLWatchPort"))
            this.props.setProperty("WsECLWatchPort", WSECLWATCHPORTDEFAULT);

        if (!this.props.containsKey("WsECLAddress"))
            this.props.setProperty("WsECLAddress", serverAddress);

        if (!this.props.containsKey("WsECLPort"))
            this.props.setProperty("WsECLPort", WSECLPORTDEFAULT);

        if (!this.props.containsKey("WsECLDirectAddress"))
            this.props.setProperty("WsECLDirectAddress", serverAddress);

        if (!this.props.containsKey("WsECLDirectPort"))
            this.props.setProperty("WsECLDirectPort", WSECLDIRECTPORTDEFAULT);

        if (!this.props.containsKey("username"))
            this.props.setProperty("username", "");

        if (!this.props.containsKey("password"))
            this.props.setProperty("password", "");

        if (!this.props.containsKey("PageSize") || !HPCCJDBCUtils.isNumeric(this.props.getProperty("PageSize")))
            this.props.setProperty("PageSize", String.valueOf(FETCHPAGESIZEDEFAULT));

        if (!this.props.containsKey("ConnectTimeoutMilli")
                || !HPCCJDBCUtils.isNumeric(this.props.getProperty("ConnectTimeoutMilli")))
            this.props.setProperty("ConnectTimeoutMilli", String.valueOf(CONNECTTIMEOUTMILDEFAULT));

        boolean setdefaultreslim = false;
        if (this.props.containsKey("EclResultLimit"))
        {
            String eclreslim = this.props.getProperty("EclResultLimit").trim();
            try
            {
                if (HPCCJDBCUtils.isNumeric(eclreslim))
                {
                    if (Integer.valueOf(eclreslim).intValue() <= 0)
                        setdefaultreslim = true;
                }
                else
                {
                    if (!eclreslim.equalsIgnoreCase("ALL"))
                        setdefaultreslim = true;
                }
            }
            catch (Exception e)
            {
                setdefaultreslim = true;
            }
        }
        else
            setdefaultreslim = true;

        if (setdefaultreslim)
        {
            this.props.setProperty("EclResultLimit", ECLRESULTLIMDEFAULT);
            System.out.println("Invalid Numeric EclResultLimit value detected, using default value: "
                    + ECLRESULTLIMDEFAULT);
        }

        String basicAuth = createBasicAuth(this.props.getProperty("username"), props.getProperty("password"));

        this.props.put("BasicAuth", basicAuth);

        if (!this.props.containsKey("LazyLoad"))
            this.props.setProperty("LazyLoad", LAZYLOADDEFAULT);

        metadata = new HPCCDatabaseMetaData(props);

        // TODO not doing anything w/ this yet, just exposing it to comply w/ API definition...
        clientInfo = new Properties();

        System.out.println("EclConnection initialized - server: " + this.serverAddress);
    }

    public static String createBasicAuth(String username, String passwd)
    {
        return "Basic " + HPCCJDBCUtils.Base64Encode((username + ":" + passwd).getBytes(), false);
    }

    public Properties getProperties()
    {
        return props;
    }

    public String getProperty(String propname)
    {
        return props.getProperty(propname, "");
    }

    public String getServerAddress()
    {
        return serverAddress;
    }

    public void setServerAddress(String serverAddress)
    {
        this.serverAddress = serverAddress;
    }

    public HPCCDatabaseMetaData getDatabaseMetaData()
    {
        return metadata;
    }

    public void setMetadata(HPCCDatabaseMetaData metadata)
    {
        this.metadata = metadata;
    }

    public Statement createStatement() throws SQLException
    {
        return new HPCCPreparedStatement(this, null);
    }

    public PreparedStatement prepareStatement(String query) throws SQLException
    {
        return new HPCCPreparedStatement(this, query);
    }

    public CallableStatement prepareCall(String sql) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: prepareCall(string sql) Not supported yet.");
    }

    public String nativeSQL(String sql) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: nativeSQL(string sql) Not supported yet.");
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: setAutoCommit(boolean autoCommit) Not supported yet.");
    }

    public boolean getAutoCommit() throws SQLException
    {
        return true;
    }

    public void commit() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: commit Not supported yet.");
    }

    public void rollback() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: rollback Not supported yet.");
    }

    public void close() throws SQLException
    {
        closed = true;
    }

    public boolean isClosed() throws SQLException
    {
        return closed;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        return metadata;
    }

    public void setReadOnly(boolean readOnly) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: setReadOnly Not supported yet.");
    }

    public boolean isReadOnly() throws SQLException
    {
        return true;
    }

    public void setCatalog(String catalog) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: setCatalog Not supported yet.");
    }

    public String getCatalog() throws SQLException
    {
        return props.getProperty("Cluster", CLUSTERDEFAULT);
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: settransactionisolation Not supported yet.");
    }

    public int getTransactionIsolation() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: getTransactionIsolation Not supported yet.");
    }

    public SQLWarning getWarnings() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: getWarnings Not supported yet.");
    }

    public void clearWarnings() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: clearWarnings Not supported yet.");
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        System.out.println("##Statement EclConnection::createStatement(resulttype, resultsetcon)##");
        return new HPCCPreparedStatement(this, null);
    }

    public PreparedStatement prepareStatement(String query, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        System.out.println("##EclConnection::createStatement(" + query + ", resultsetype, resultsetcon)##");
        return new HPCCPreparedStatement(this, query);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclConnection: prepareCall(String sql, int resultSetType, int resultSetConcurrency) Not supported yet.");
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: getTypeMap Not supported yet.");
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: setTypeMap Not supported yet.");
    }

    public void setHoldability(int holdability) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: setHoldability Not supported yet.");
    }

    public int getHoldability() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: getHoldability Not supported yet.");
    }

    public Savepoint setSavepoint() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: setSavepoint Not supported yet.");
    }

    public Savepoint setSavepoint(String name) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: setSavepoint Not supported yet.");
    }

    public void rollback(Savepoint savepoint) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: rollback Not supported yet.");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: releaseSavepoint Not supported yet.");
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: createStatement Not supported yet.");
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclConnection: prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) Not supported yet.");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclConnection: prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) Not supported yet.");
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclConnection: prepareStatement(String sql, int autoGeneratedKeys) Not supported yet.");
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclConnection: prepareStatement(String sql, int[] columnIndexes) Not supported yet.");
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclConnection:  prepareStatement(String sql, String[] columnNames) Not supported yet.");
    }

    public Clob createClob() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: createClob Not supported yet.");
    }

    public Blob createBlob() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: createBlob Not supported yet.");
    }

    public NClob createNClob() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: createNClob Not supported yet.");
    }

    public SQLXML createSQLXML() throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: createSQLXML Not supported yet.");
    }

    public boolean isValid(int timeout) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: isValid Not supported yet.");
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException
    {
        System.out.println("ECLCONNECTION SETCLIENTINFO");
        clientInfo.put(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException
    {
        System.out.println("ECLCONNECTION SETCLIENTINFO");
        clientInfo = properties;
    }

    public String getClientInfo(String name) throws SQLException
    {
        System.out.println("ECLCONNECTION GETCLIENTINFO");
        return (String) clientInfo.getProperty(name);
    }

    public Properties getClientInfo() throws SQLException
    {
        System.out.println("ECLCONNECTION GETCLIENTINFO");
        return clientInfo;
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclConnection: createArrayOf(String typeName, Object[] elements) Not supported yet.");
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclConnection: createStruct(String typeName, Object[] attributes)Not supported yet.");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: unwrap(Class<T> iface) Not supported yet.");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        throw new UnsupportedOperationException("EclConnection: isWrapperFor(Class<?> iface) sNot supported yet.");
    }
}
