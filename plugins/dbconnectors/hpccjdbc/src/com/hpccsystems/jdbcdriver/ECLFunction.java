package com.hpccsystems.jdbcdriver;

public class ECLFunction
{
    private String             name;
    private boolean            acceptsWilCard;
    private boolean            acceptsMultipleInputs;
    private HPCCColumnMetaData returnType;

    public ECLFunction(String thename, boolean acceptswild, HPCCColumnMetaData returntype, boolean multipleInput)
    {
        name = thename;
        acceptsWilCard = acceptswild;
        returnType = returntype;
        acceptsMultipleInputs = multipleInput;
    }

    public ECLFunction(String thename, HPCCColumnMetaData returntype)
    {
        name = thename;
        acceptsWilCard = false;
        returnType = returntype;
    }

    public String getName()
    {
        return name;
    }

    public boolean acceptsWilCard()
    {
        return acceptsWilCard;
    }

    public HPCCColumnMetaData getReturnType()
    {
        return returnType;
    }

    public boolean acceptsMultipleInputs()
    {
        return acceptsMultipleInputs;
    }
}
