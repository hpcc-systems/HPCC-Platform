package com.hpccsystems.jdbcdriver;

import java.util.List;

/**
 * @author rpastrana
 */

public class HPCCColumnMetaData
{
    private String                   columnName;
    private int                      index;
    private int                      sqlType;

    private String                   eclType;
    private String                   tableName;
    private int                      columnSize;
    private int                      decimalDigits;
    private int                      radix;
    private String                   nullable;
    private String                   remarks;
    private String                   columnDefault;
    // private Properties restrictions;
    private int                      paramType;
    private String                   javaClassName;
    private String                   constantValue;
    private int                      columnType;
    private String                   alias;
    private List<HPCCColumnMetaData> funccols;

    final static int                 COLUMN_TYPE_DATA     = 1;
    final static int                 COLUMN_TYPE_CONSTANT = 2;
    final static int                 COLUMN_TYPE_FNCTION  = 3;

    public HPCCColumnMetaData(String columnName, int index, int sqlType, String constant, String eclType)
    {
        constantValue = constant;
        this.columnName = columnName;
        this.index = index;
        this.sqlType = sqlType;
        this.paramType = HPCCDatabaseMetaData.procedureColumnUnknown;
        javaClassName = HPCCDatabaseMetaData.convertSQLtype2JavaClassName(this.sqlType);
        this.eclType = eclType;
        columnType = COLUMN_TYPE_CONSTANT;
        funccols = null;
        alias = null;
    }

    public HPCCColumnMetaData(String columnName, int index, int sqlType)
    {
        this.columnName = columnName;
        this.index = index;
        this.sqlType = sqlType;
        this.paramType = HPCCDatabaseMetaData.procedureColumnUnknown;
        javaClassName = HPCCDatabaseMetaData.convertSQLtype2JavaClassName(this.sqlType);
        constantValue = null;
        columnType = COLUMN_TYPE_DATA;
        funccols = null;
        alias = null;
    }

    public HPCCColumnMetaData(String columnName, int index, List<HPCCColumnMetaData> columns)
    {
        this.columnName = columnName;
        this.index = index;
        this.sqlType = java.sql.Types.OTHER;
        this.paramType = HPCCDatabaseMetaData.procedureColumnUnknown;
        javaClassName = HPCCDatabaseMetaData.convertSQLtype2JavaClassName(this.sqlType);
        constantValue = null;
        columnType = COLUMN_TYPE_FNCTION;
        funccols = columns;
        alias = null;
    }

    public void setConstantValue(String value)
    {
        constantValue = value;
    }

    public boolean isConstant()
    {
        return constantValue != null;
    }

    public String getConstantValue()
    {
        return constantValue;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getColumnNameOrAlias()
    {
        if (alias != null)
            return alias;
        else
            return columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public int getIndex()
    {
        return index;
    }

    public void setIndex(int index)
    {
        this.index = index;
    }

    public int getParamType()
    {
        return paramType;
    }

    public void setParamType(int paramType)
    {
        this.paramType = paramType;
    }

    // public Properties getRestrictions() {
    // return restrictions;
    // }
    //
    // public void addRestriction(String restriction, String value)
    // {
    // if (restrictions == null)
    // restrictions = new Properties();
    //
    // this.restrictions.put(restriction, value);
    // }
    //
    // public String getRestrictionStringValue(String restriction)
    // {
    // return (String)this.restrictions.get(restriction);
    // }

    public void setSqlType(int type)
    {
        sqlType = type;
    }

    public int getSqlType()
    {
        return sqlType;
    }

    public String getEclType()
    {
        return eclType;
    }

    public void setEclType(String eclType)
    {
        this.eclType = eclType;
        this.sqlType = HPCCDatabaseMetaData.convertECLtype2SQLtype(eclType.toUpperCase());
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public int getColumnSize()
    {
        return columnSize;
    }

    public void setColumnSize(int columnSize)
    {
        this.columnSize = columnSize;
    }

    public int getDecimalDigits()
    {
        return decimalDigits;
    }

    public void setDecimalDigits(int decimalDigits)
    {
        this.decimalDigits = decimalDigits;
    }

    public int getRadix()
    {
        return radix;
    }

    public void setRadix(int radix)
    {
        this.radix = radix;
    }

    public String getNullable()
    {
        return nullable;
    }

    public void setNullable(String nullable)
    {
        this.nullable = nullable;
    }

    public String getRemarks()
    {
        return remarks;
    }

    public void setRemarks(String remarks)
    {
        this.remarks = remarks;
    }

    public String getColumnDefault()
    {
        return columnDefault;
    }

    public void setColumnDefault(String columnDefault)
    {
        this.columnDefault = columnDefault;
    }

    public String getJavaClassName()
    {
        return javaClassName;
    }

    public int getColumnType()
    {
        return columnType;
    }

    public void setColumnType(int columnType)
    {
        this.columnType = columnType;
    }

    public List<HPCCColumnMetaData> getFunccols()
    {
        return funccols;
    }

    public String getAlias()
    {
        return alias;
    }

    public void setAlias(String alias)
    {
        this.alias = alias;
    }

    @Override
    public String toString()
    {
        return "Name: " + this.columnName + " SQL Type: " + sqlType + " ECL Type: " + eclType;
    }
}
