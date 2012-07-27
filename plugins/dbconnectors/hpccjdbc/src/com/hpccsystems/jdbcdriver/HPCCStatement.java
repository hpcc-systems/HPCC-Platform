package com.hpccsystems.jdbcdriver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 *
 * @author ChalaAX, rpastrana
 */

public class HPCCStatement implements Statement
{

    public ResultSet executeQuery(String sql) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: executeQuery(String sql) Not supported yet.");
    }

    public int executeUpdate(String sql) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: executeUpdate(String sql) Not supported yet.");
    }

    public void close() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: close() Not supported yet.");
    }

    public int getMaxFieldSize() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getMaxFieldSize() Not supported yet.");
    }

    public void setMaxFieldSize(int max) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: setMaxFieldSize(int max) Not supported yet.");
    }

    public int getMaxRows() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getMaxRows() Not supported yet.");
    }

    public void setMaxRows(int max) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: setMaxRows(int max) Not supported yet.");
    }

    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: setEscapeProcessing(boolean enable) Not supported yet.");
    }

    public int getQueryTimeout() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement:  getQueryTimeout() Not supported yet.");
    }

    public void setQueryTimeout(int seconds) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: setQueryTimeout(int seconds) Not supported yet.");
    }

    public void cancel() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: cancel()  Not supported yet.");
    }

    public SQLWarning getWarnings() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getWarnings Not supported yet.");
    }

    public void clearWarnings() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: clearWarnings() Not supported yet.");
    }

    public void setCursorName(String name) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement:  setCursorName(String name) Not supported yet.");
    }

    public boolean execute(String sql) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: execute(String sql) Not supported yet.");
    }

    public ResultSet getResultSet() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getResultSet Not supported yet.");
    }

    public int getUpdateCount() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement:getUpdateCount  Not supported yet.");
    }

    public boolean getMoreResults() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getMoreResults Not supported yet.");
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: only ResultSet.FETCH_FORWARD supported");
    }

    public int getFetchDirection() throws SQLException
    {
        return ResultSet.FETCH_FORWARD;
    }

    public void setFetchSize(int rows) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: setFetchSize(int rows) Not supported yet.");
    }

    public int getFetchSize() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getFetchSize() Not supported yet.");
    }

    public int getResultSetConcurrency() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getResultSetConcurrency() Not supported yet.");
    }

    public int getResultSetType() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement:  getResultSetType() Not supported yet.");
    }

    public void addBatch(String sql) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: addBatch(String sql) Not supported yet.");
    }

    public void clearBatch() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: clearBatch() Not supported yet.");
    }

    public int[] executeBatch() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: executeBatch() Not supported yet.");
    }

    public Connection getConnection() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getConnection() Not supported yet.");
    }

    public boolean getMoreResults(int current) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getMoreResults(int current) Not supported yet.");
    }

    public ResultSet getGeneratedKeys() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getGeneratedKeys() Not supported yet.");
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclStatement: executeUpdate(String sql, int autoGeneratedKeys) Not supported yet.");
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclStatement:  executeUpdate(String sql, int[] columnIndexes) Not supported yet.");
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclStatement: executeUpdate(String sql, String[] columnNames) Not supported yet.");
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclStatement: execute(String sql, int autoGeneratedKeys) Not supported yet.");
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclStatement:  execute(String sql, int[] columnIndexes) Not supported yet.");
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException
    {
        throw new UnsupportedOperationException(
                "EclStatement: execute(String sql, String[] columnNames) Not supported yet.");
    }

    public int getResultSetHoldability() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: getResultSetHoldability Not supported yet.");
    }

    public boolean isClosed() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: isClosed() Not supported yet.");
    }

    public void setPoolable(boolean poolable) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: Not supported yet.");
    }

    public boolean isPoolable() throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: Not supported yet.");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: Not supported yet.");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        throw new UnsupportedOperationException("EclStatement: Not supported yet.");
    }

}
