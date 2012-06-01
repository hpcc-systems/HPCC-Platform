package com.hpccsystems.jdbcdriver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SQLWhereClause
{
	private List<SQLExpression> expressions;
	private List<String> expressionUniqueColumnNames;
	private int expressionsCount;
	private int operatorsCount;
	private boolean orOperatorUsed;

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
		if (expression.getType() == SQLExpression.LOGICAL_EXPRESSION_TYPE)
		{
			expressionsCount++;
			if (!expressionUniqueColumnNames.contains(expression.getName()))
					expressionUniqueColumnNames.add(expression.getName());
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

	public int getExpressionsCount() {
		return expressionsCount;
	}

	public int getOperatorsCount() {
		return operatorsCount;
	}

	@Override
	public String toString()
	{
		String clause = new String("");
		Iterator<SQLExpression> it = expressions.iterator();
		while (it.hasNext())
		{
			clause += ((SQLExpression)it.next()).toString();
		}
		return clause;
	}

	public String [] getExpressionNames()
	{
		String [] names = new String[getExpressionsCount()];
		Iterator<SQLExpression> it = expressions.iterator();
		int i = 0;
		while (it.hasNext())
		{
			SQLExpression exp = it.next();
			if (exp.getType() == SQLExpression.LOGICAL_EXPRESSION_TYPE)
			{
				names[i++] = exp.getName();
			}
		}
		return names;
	}

	public SQLExpression getExpressionFromName(String name)
	{
		Iterator<SQLExpression> it = expressions.iterator();
		while (it.hasNext())
		{
			SQLExpression exp = it.next();
			if (exp.getType() == SQLExpression.LOGICAL_EXPRESSION_TYPE && exp.getName().equals(name))
			{
				return exp;
			}
		}
		return null;
	}

	public boolean containsKey(String name)
	{
		Iterator<SQLExpression> it = expressions.iterator();
		while (it.hasNext())
		{
			SQLExpression exp = it.next();
			if (exp.getType() == SQLExpression.LOGICAL_EXPRESSION_TYPE && exp.getName().equals(name))
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
		String [] names = new String[getUniqueColumnCount()];
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
