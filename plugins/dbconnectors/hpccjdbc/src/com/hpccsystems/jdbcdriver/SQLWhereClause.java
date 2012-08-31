package com.hpccsystems.jdbcdriver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class SQLWhereClause
{
    private List<SQLExpressionFragment> expressions;
    private List<String>                expressionUniqueColumnNames;
    private int                         expressionsCount;
    private int                         operatorsCount;
    private boolean                     orOperatorUsed;

    public SQLWhereClause()
    {
        expressions = new ArrayList<SQLExpressionFragment>();
        expressionUniqueColumnNames = new ArrayList<String>();
        expressionsCount = 0;
        operatorsCount = 0;
        orOperatorUsed = false;
    }

    public void addExpression(SQLExpressionFragment expression)
    {
        expressions.add(expression);
        if (expression.getType() == SQLExpressionFragment.LOGICAL_EXPRESSION_TYPE)
        {
            expressionsCount++;
            if (!expressionUniqueColumnNames.contains(expression.getPrefixValue()))
                expressionUniqueColumnNames.add(expression.getPrefixValue());
        }
        else
        {
            operatorsCount++;
            if (expression.getOperator().getValue().equals(SQLOperator.or))
                orOperatorUsed = true;
        }
    }

    public Iterator<SQLExpressionFragment> getExpressions()
    {
        return expressions.iterator();
    }

    public int getExpressionsCount()
    {
        return expressionsCount;
    }

    public int getOperatorsCount()
    {
        return operatorsCount;
    }

    @Override
    public String toString()
    {
        String clause = new String("");
        Iterator<SQLExpressionFragment> it = expressions.iterator();
        while (it.hasNext())
        {
            clause += ((SQLExpressionFragment) it.next()).toString();
        }
        return clause;
    }

    public String fullToString()
    {
        String clause = new String("");
        Iterator<SQLExpressionFragment> it = expressions.iterator();
        while (it.hasNext())
        {
            clause += ((SQLExpressionFragment) it.next()).fullToString();
        }
        return clause;
    }

    public String toStringTranslateSource(HashMap<String, String> map)
    {
        String clause = new String("");
        Iterator<SQLExpressionFragment> it = expressions.iterator();
        while (it.hasNext())
        {
            clause += ((SQLExpressionFragment) it.next()).toStringTranslateSource(map);
        }
        return clause;
    }

    public String[] getExpressionNames()
    {
        String[] names = new String[getExpressionsCount()];
        Iterator<SQLExpressionFragment> it = expressions.iterator();
        int i = 0;
        while (it.hasNext())
        {
            SQLExpressionFragment exp = it.next();
            if (exp.getType() == SQLExpressionFragment.LOGICAL_EXPRESSION_TYPE)
            {
                names[i++] = exp.getPrefixValue();
            }
        }
        return names;
    }

    public SQLExpressionFragment getExpressionFromName(String name)
    {
        Iterator<SQLExpressionFragment> it = expressions.iterator();
        while (it.hasNext())
        {
            SQLExpressionFragment exp = it.next();
            if (exp.getType() == SQLExpressionFragment.LOGICAL_EXPRESSION_TYPE && exp.getPrefixValue().equals(name)) { return exp; }
        }
        return null;
    }

    public boolean containsKey(String name)
    {
        Iterator<SQLExpressionFragment> it = expressions.iterator();
        while (it.hasNext())
        {
            SQLExpressionFragment exp = it.next();
            if (exp.getType() == SQLExpressionFragment.LOGICAL_EXPRESSION_TYPE && exp.getPrefixValue().equals(name))
                return true;
        }
        return false;
    }

    public int getUniqueColumnCount()
    {
        return expressionUniqueColumnNames.size();
    }

    public String[] getUniqueExpressionNames()
    {
        String[] names = new String[getUniqueColumnCount()];
        Iterator<String> it = expressionUniqueColumnNames.iterator();
        int i = 0;
        while (it.hasNext())
        {
            names[i++] = it.next();
        }
        return names;
    }

    public boolean isOrOperatorUsed()
    {
        return orOperatorUsed;
    }
}
