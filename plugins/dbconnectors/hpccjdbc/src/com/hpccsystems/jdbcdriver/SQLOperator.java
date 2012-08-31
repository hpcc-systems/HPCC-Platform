package com.hpccsystems.jdbcdriver;

import java.util.ArrayList;
import java.util.List;

public class SQLOperator
{
    private static List<String> validOps;

    // When adding a new opereator, make sure to add it to validOps array
    public static final String  eq        = new String("=");
    public static final String  neq       = new String("<>");
    public static final String  neq2      = new String("!=");
    public static final String  isNull    = new String("IS NULL");
    public static final String  isNotNull = new String("IS NOT NULL");
    public static final String  gt        = new String(">");
    public static final String  lt        = new String("<");
    public static final String  gte       = new String(">=");
    public static final String  lte       = new String("<=");
    public static final String  and       = new String("AND");
    public static final String  or        = new String("OR");
    public static final String  not       = new String("NOT");
    public static final String  exists    = new String("EXISTS");
    public static final String  like      = new String("LIKE");
    public static final String  in        = new String("IN");

    static
    {
        validOps = new ArrayList<String>();
        validOps.add(eq);
        validOps.add(neq);
        validOps.add(neq2);
        validOps.add(isNull);
        validOps.add(isNotNull);
        validOps.add(gt);
        validOps.add(lt);
        validOps.add(gte);
        validOps.add(lte);
        validOps.add(and);
        validOps.add(or);
        validOps.add(not);
        validOps.add(exists);
        validOps.add(like);
        validOps.add(in);
    }

    private final String        value;

    private static void populateValidOps()
    {
        validOps = new ArrayList<String>();

        validOps.add(eq);
        validOps.add(neq);
        validOps.add(isNull);
        validOps.add(isNotNull);
        validOps.add(gt);
        validOps.add(lt);
        validOps.add(gte);
        validOps.add(lte);
        validOps.add(and);
        validOps.add(or);
        validOps.add(not);
        validOps.add(exists);
        validOps.add(like);
        validOps.add(in);
    }

    public SQLOperator(String operator)
    {
        if (validOps == null)
            populateValidOps();

        if (validOps.contains(operator.toUpperCase()))
            value = operator.toUpperCase();
        else
            value = null;
    }

    static public String parseOperatorFromFragmentStr(String fragment)
    {
        String trimmedFragment = fragment.trim();
        String operator = null;

        if (trimmedFragment.indexOf(SQLOperator.gte) != -1)
            operator = SQLOperator.gte;
        else if (trimmedFragment.indexOf(SQLOperator.lte) != -1)
            operator = SQLOperator.lte;
        else if (trimmedFragment.indexOf(SQLOperator.neq) != -1)
            operator = SQLOperator.neq;
        else if (trimmedFragment.indexOf(SQLOperator.eq) != -1)
            operator = SQLOperator.eq;
        else if (trimmedFragment.indexOf(SQLOperator.gt) != -1)
            operator = SQLOperator.gt;
        else if (trimmedFragment.indexOf(SQLOperator.lt) != -1)
            operator = SQLOperator.lt;

        return operator;
    }

    public String getValue()
    {
        return value;
    }

    public boolean isValid()
    {
        return validOps.contains(value);
    }

    @Override
    public String toString()
    {
        return value == null ? "" : value;
    }
}
