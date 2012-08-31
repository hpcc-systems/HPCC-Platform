package com.hpccsystems.jdbcdriver;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

/**
 *
 * @author rpastrana
 */

public class HPCCPreparedStatement implements PreparedStatement
{
    private boolean                  closed        = false;
    private int                      maxRows       = 100;
    private String                   query;
    private Connection               connection;
    private HashMap<Integer, Object> parameters    = new HashMap<Integer, Object>();
    private ArrayList<SQLWarning>    warnings;
    private HashMap<String, String>  indexToUseMap = new HashMap<String, String>();

    private HPCCResultSet            result;

    public HPCCPreparedStatement(Connection connection, String query)
    {
        System.out.println("ECLPreparedStatement::ECLPreparedStatement: " + query);
        this.query = query;
        this.connection = connection;
        warnings = new ArrayList<SQLWarning>();
    }

    public boolean isIndexSet(String sourcefilename)
    {
        return indexToUseMap.get(sourcefilename) != null;
    }

    public void setIndexToUse(String sourcefilename, String indexfilename)
    {
        indexToUseMap.put(sourcefilename, indexfilename);
    }

    public String getIndexToUse(String sourcefilename)
    {
        return indexToUseMap.get(sourcefilename);
    }

    public ResultSet executeQuery() throws SQLException
    {
        return new HPCCResultSet(this, query, parameters);
    }

    public int executeUpdate() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT: executeUpdate Not supported yet.");
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), sqlType);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setString(int parameterIndex, String x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT: setAsciiStream Not supported yet.");
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT: setUnicodeStream Not supported yet.");
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT: setBinaryStream Not supported yet.");
    }

    public void clearParameters() throws SQLException
    {
        parameters.clear();
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        System.out.println("ECLPrepStamt::setObject Parameter Index = " + parameterIndex + ", Object = " + x
                + ", targetSqlType = " + targetSqlType);
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        System.out.println("ECLPrepStamt::setObject Parameter Index = " + parameterIndex + ", Object = " + x);
        parameters.put(new Integer(parameterIndex), x);
    }

    public boolean execute() throws SQLException
    {
        result = new HPCCResultSet(this, query, parameters);
        System.out.println("ECLPreparedStatement: execute rowcount: " + result.getRowCount());
        return result.getRowCount() > 0 ? true : false;
    }

    public void addBatch() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT: addBatch Not supported yet.");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setCharacterStream Not supported yet.");
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setArray(int parameterIndex, Array x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        System.out.println("ECLPreparedStatement: getMetaData");
        return result.getMetaData();
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setDate Not supported yet.");
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setTime Not supported yet.");
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setTimestamp Not supported yet.");
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setNull Not supported yet.");
    }

    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), x);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        System.out.println("ECLPREPSTATEMENT getParameterMetaData");
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:getParameterMetaData Not supported yet.");
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setRowId Not supported yet.");
    }

    public void setNString(int parameterIndex, String value) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setNCharacterStream Not supported yet.");
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), value);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setClob Not supported yet.");
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setBlob Not supported yet.");
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setNClob Not supported yet.");
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        parameters.put(new Integer(parameterIndex), xmlObject);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
    {
        System.out.println("ECLPrepStamt::setObject Parameter Index = " + parameterIndex + ", Object = " + x
                + ", targetSqlType = " + targetSqlType + ", scaleOrLength = " + scaleOrLength);
        parameters.put(new Integer(parameterIndex), x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setAsciiStream Not supported yet.");
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setBinaryStream Not supported yet.");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setCharacterStream Not supported yet.");
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setAsciiStream Not supported yet.");
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setBinaryStream Not supported yet.");
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setCharacterStream Not supported yet.");
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setNCharacterStream Not supported yet.");
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setClob Not supported yet.");
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setBlob Not supported yet.");
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setNClob Not supported yet.");
    }

    public ResultSet executeQuery(String query) throws SQLException
    {
        System.out.println("ECLPreparedStatement: executeQuery(" + query + ")");
        result = new HPCCResultSet(this, query, parameters);
        System.out.println("ECLPreparedStatement: executeQuery returned " + result.getRowCount() + " rows, and "
                + result.getMetaData().getColumnCount() + " columns");
        System.out.println("results object id: " + (Object) result.hashCode());

        return result;
    }

    public int executeUpdate(String sql) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:executeUpdate Not supported yet.");
    }

    public void close() throws SQLException
    {
        connection.close();
        query = "";
        result = null;
        closed = true;
    }

    public int getMaxFieldSize() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT: getMaxFieldSizeNot supported yet.");
    }

    public void setMaxFieldSize(int max) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setMaxFieldSize Not supported yet.");
    }

    public int getMaxRows() throws SQLException
    {
        return maxRows;
    }

    public void setMaxRows(int max) throws SQLException
    {
        maxRows = max;
    }

    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setEscapeProcessing Not supported yet.");
    }

    public int getQueryTimeout() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:getQueryTimeout Not supported yet.");
    }

    public void setQueryTimeout(int seconds) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setQueryTimeout Not supported yet.");
    }

    public void cancel() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:cancel Not supported yet.");
    }

    public SQLWarning getWarnings() throws SQLException
    {
        return (warnings.size() <= 0) ? null : warnings.get(1);
    }

    public void clearWarnings() throws SQLException
    {
        warnings.clear();
    }

    public void setCursorName(String name) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setCursorName Not supported yet.");
    }

    public boolean execute(String sql) throws SQLException
    {
        System.out.println("ECLPREPSTMT execute(" + sql + ")");
        query = sql;
        result = new HPCCResultSet(this, query, parameters);
        System.out.println("ECLPreparedStatement: execute rowcount: " + result.getRowCount());
        return result.getRowCount() > 0 ? true : false;
    }

    public ResultSet getResultSet() throws SQLException
    {
        return result;
    }

    public int getUpdateCount() throws SQLException
    {
        return 0;
    }

    public boolean getMoreResults() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:getMoreResults Not supported yet.");
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setFetchDirection Not supported yet.");
    }

    public int getFetchDirection() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:getFetchDirection Not supported yet.");
    }

    public void setFetchSize(int rows) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setFetchSize Not supported yet.");
    }

    public int getFetchSize() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:getFetchSize Not supported yet.");
    }

    public int getResultSetConcurrency() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:getResultSetConcurrency Not supported yet.");
    }

    public int getResultSetType() throws SQLException
    {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public void addBatch(String sql) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:addBatch Not supported yet.");
    }

    public void clearBatch() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:clearBatch Not supported yet.");
    }

    public int[] executeBatch() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:executeBatch Not supported yet.");
    }

    public Connection getConnection() throws SQLException
    {
        return connection;
    }

    public boolean getMoreResults(int current) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:getMoreResults Not supported yet.");
    }

    public ResultSet getGeneratedKeys() throws SQLException
    {
        return null;
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:executeUpdate Not supported yet.");
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:executeUpdate Not supported yet.");
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:executeUpdate Not supported yet.");
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException(
                "ECLPREPSTATEMNT:execute(String sql, int autoGeneratedKeys) Not supported yet.");
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
        throw new UnsupportedOperationException(
                "ECLPREPSTATEMNT:execute(String sql, int[] columnIndexes) Not supported yet.");
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException
    {
        throw new UnsupportedOperationException(
                "ECLPREPSTATEMNT:execute(String sql, String[] columnNames) Not supported yet.");
    }

    public int getResultSetHoldability() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:getResultSetHoldability Not supported yet.");
    }

    public boolean isClosed() throws SQLException
    {
        return closed;
    }

    public void setPoolable(boolean poolable) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:setPoolable Not supported yet.");
    }

    public boolean isPoolable() throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:isPoolable Not supported yet.");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:unwrap Not supported yet.");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        throw new UnsupportedOperationException("ECLPREPSTATEMNT:isWrapperFor Not supported yet.");
    }
}
