package com.hpccsystems.ecljdbc;

/**
 * @author ChalaAX
 */
public class EclColumn
{
    private String name;
    private String value;

    public EclColumn(String name, String value)
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
