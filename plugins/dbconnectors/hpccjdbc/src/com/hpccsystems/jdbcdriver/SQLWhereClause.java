package com.hpccsystems.jdbcdriver;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.hpccsystems.jdbcdriver.SQLExpression.ExpressionType;
import com.hpccsystems.jdbcdriver.SQLFragment.FragmentType;

public class SQLWhereClause
{
    private List<SQLExpression> expressions;
    private List<String>        expressionUniqueColumnNames;
    private int                 expressionsCount;
    private int                 operatorsCount;
    private boolean             orOperatorUsed;

    public SQLWhereClause()
    {
        expressions = new ArrayList<SQLExpression>();
        expressionUniqueColumnNames = new ArrayList<String>();
        expressionsCount = 0;
        operatorsCount = 0;
        orOperatorUsed = false;
    }

    public void addExpression(SQLExpression expression)
    {
        expressions.add(expression);
        if (expression.getExpressionType() == ExpressionType.LOGICAL_EXPRESSION_TYPE)
        {
            expressionsCount++;
            if (!expressionUniqueColumnNames.contains(expression.getPrefixValue()))
                expressionUniqueColumnNames.add(expression.getPrefixValue());

            if (!expressionUniqueColumnNames.contains(expression.getPostfixValue()))
                expressionUniqueColumnNames.add(expression.getPostfixValue());
        }
        else
        {
            operatorsCount++;
            if (expression.getOperator().getValue().equals(SQLOperator.or))
                orOperatorUsed = true;
        }
    }

    public Iterator<SQLExpression> getExpressions()
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
        Iterator<SQLExpression> it = expressions.iterator();
        while (it.hasNext())
        {
            clause += ((SQLExpression) it.next()).toString();
        }
        return clause;
    }

    public String fullToString()
    {
        String clause = new String("");
        Iterator<SQLExpression> it = expressions.iterator();
        while (it.hasNext())
        {
            clause += ((SQLExpression) it.next()).fullToString();
        }
        return clause;
    }

    public String toStringTranslateSource(HashMap<String, String> map)
    {
        String clause = new String("");
        Iterator<SQLExpression> it = expressions.iterator();
        while (it.hasNext())
        {
            clause += ((SQLExpression) it.next()).toStringTranslateSource(map);
        }
        return clause;
    }

    public String[] getExpressionColumnNames()
    {
        String[] colnames = new String[getExpressionsCount()];
        Iterator<SQLExpression> it = expressions.iterator();
        int i = 0;
        while (it.hasNext())
        {
            SQLExpression exp = it.next();
            if (exp.getExpressionType() == ExpressionType.LOGICAL_EXPRESSION_TYPE)
            {
                if (exp.getPrefixType() == FragmentType.FIELD_TYPE)
                    colnames[i++] = exp.getPrefixValue();
                if (exp.getPostfixType() == FragmentType.FIELD_TYPE)
                    colnames[i++] = exp.getPostfixValue();
            }
        }
        return colnames;
    }

    public SQLExpression getExpressionFromColumndName(String colname)
    {
        Iterator<SQLExpression> it = expressions.iterator();
        while (it.hasNext())
        {
            SQLExpression exp = it.next();
            if (exp.getExpressionType() == ExpressionType.LOGICAL_EXPRESSION_TYPE)
            {

                if(exp.getPrefixType() == FragmentType.FIELD_TYPE && exp.getPrefixValue().equals(colname) ||
                   exp.getPostfixType() == FragmentType.FIELD_TYPE && exp.getPostfixValue().equals(colname))
                {
                    return exp;
                }
            }
        }
        return null;
    }

    public boolean containsKey(String colname)
    {
        Iterator<SQLExpression> it = expressions.iterator();
        while (it.hasNext())
        {
            SQLExpression exp = it.next();
            if (exp.getExpressionType() == ExpressionType.LOGICAL_EXPRESSION_TYPE)
            {
                if(exp.getPrefixType() == FragmentType.FIELD_TYPE && exp.getPrefixValue().equals(colname) ||
                        exp.getPostfixType() == FragmentType.FIELD_TYPE && exp.getPostfixValue().equals(colname))
                {
                    return true;
                }
            }
        }
        return false;
    }

    public int getUniqueColumnCount()
    {
        return expressionUniqueColumnNames.size();
    }

    public String[] getUniqueExpressionColumnNames()
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

    public void updateFragmentColumnsParent(List<SQLTable> sqlTables) throws Exception
    {
        Iterator<SQLExpression> it = expressions.iterator();
        while (it.hasNext())
        {
            ((SQLExpression) it.next()).updateFragmentTables(sqlTables);
        }
    }

    public void parseWhereClause(String whereclause) throws SQLException
    {
        String splitedwhereands[] = whereclause.split("(\\s+(?i)and\\s+)|(\\s*,\\s*)");

        for (int i = 0; i < splitedwhereands.length; i++)
        {
            String splitedwhereandors[] = splitedwhereands[i].split("\\s+(?i)or\\s+");

            SQLExpression andoperator = new SQLExpression("AND");

            for (int y = 0; y < splitedwhereandors.length; y++)
            {
                SQLExpression exp = new SQLExpression( ExpressionType.LOGICAL_EXPRESSION_TYPE);
                SQLExpression orperator = new SQLExpression("OR");

                exp.ParseExpression(splitedwhereandors[y]);

                addExpression(exp);

                if (y < splitedwhereandors.length - 1)
                    addExpression(orperator);
            }

            if (i < splitedwhereands.length - 1)
                addExpression(andoperator);
        }
    }
}
