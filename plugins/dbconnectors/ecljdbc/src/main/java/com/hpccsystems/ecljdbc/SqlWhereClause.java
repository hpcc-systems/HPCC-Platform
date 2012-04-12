package com.hpccsystems.ecljdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SqlWhereClause
{
	private List<SqlExpression> expressions;
	private List<String> expressionUniqueColumnNames;
	private int expressionsCount;
	private int operatorsCount;
	private boolean orOperatorUsed;

	public SqlWhereClause()
	{
		expressions = new ArrayList<SqlExpression>();
		expressionUniqueColumnNames = new ArrayList<String>();
		expressionsCount = 0;
		operatorsCount = 0;
		orOperatorUsed = false;
	}

	public void addExpression(SqlExpression expression)
	{
		expressions.add(expression);
		if (expression.getType() == SqlExpression.LOGICAL_EXPRESSION_TYPE)
		{
			expressionsCount++;
			if (!expressionUniqueColumnNames.contains(expression.getName()))
					expressionUniqueColumnNames.add(expression.getName());
		}
		else
		{
			operatorsCount++;
			if (expression.getOperator().getValue().equals(SqlOperator.or))
				orOperatorUsed = true;
		}
	}

	public Iterator<SqlExpression> getExpressions()
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
		Iterator<SqlExpression> it = expressions.iterator();
		while (it.hasNext())
		{
			clause += ((SqlExpression)it.next()).toString();
		}
		return clause;
	}

	public String [] getExpressionNames()
	{
		String [] names = new String[getExpressionsCount()];
		Iterator<SqlExpression> it = expressions.iterator();
		int i = 0;
		while (it.hasNext())
		{
			SqlExpression exp = it.next();
			if (exp.getType() == SqlExpression.LOGICAL_EXPRESSION_TYPE)
			{
				names[i++] = exp.getName();
			}
		}
		return names;
	}

	public SqlExpression getExpressionFromName(String name)
	{
		Iterator<SqlExpression> it = expressions.iterator();
		while (it.hasNext())
		{
			SqlExpression exp = it.next();
			if (exp.getType() == SqlExpression.LOGICAL_EXPRESSION_TYPE && exp.getName().equals(name))
			{
				return exp;
			}
		}
		return null;
	}

	public boolean containsKey(String name)
	{
		Iterator<SqlExpression> it = expressions.iterator();
		while (it.hasNext())
		{
			SqlExpression exp = it.next();
			if (exp.getType() == SqlExpression.LOGICAL_EXPRESSION_TYPE && exp.getName().equals(name))
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
