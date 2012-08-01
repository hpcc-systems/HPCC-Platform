package com.hpccsystems.jdbcdriver;

/**
 * @author ChalaAX
 */

public class HPCCColumn
{
    private String name;
    private String value;

    public HPCCColumn(String name, String value)
    {
        this.name = name;
        this.value = value;
    }

    public String getName()
    {
        return name;
    }

    public String getValue()
    {
        return value;
    }
}
