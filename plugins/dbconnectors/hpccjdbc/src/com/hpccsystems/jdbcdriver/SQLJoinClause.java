package com.hpccsystems.jdbcdriver;

import java.sql.SQLException;

public class SQLJoinClause
{
	public static final short DEFAULT = 1;
	public static final short INNER = 2;
	public static final short OUTER = 3;

	private short type = DEFAULT;
	private SQLWhereClause OnClause;
	private SQLTable joinTable = null;
	private SQLTable sourceTable = null;

	public SQLJoinClause()
	{
		this.type = DEFAULT;
		this.OnClause = new SQLWhereClause();
	}

	public SQLJoinClause(short type)
	{
		this.type = type;
		this.OnClause = new SQLWhereClause();
	}

	public boolean parseClause(String joinOnClause) throws SQLException
	{
		boolean success = false;

		if (joinOnClause.length()>0)
		{
			String joinSplit [] = joinOnClause.split("\\s+(?i)join\\s+");
			if (joinSplit.length > 1)
			{
				if (joinSplit[0].trim().length() == 0)
				{
					type = DEFAULT;
				}
				else if(joinSplit[0].trim().equalsIgnoreCase("INNER"))
				{
					type = INNER;
				}
				else if(joinSplit[0].trim().equalsIgnoreCase("OUTER"))
				{
					type = OUTER;
				}
				else
					throw new SQLException("Error: No join clause found");
			}
			else
				throw new SQLException("Erro: No join clause found");

			String clausesplit [] = joinSplit[1].split("\\s+(?i)on\\s+", 2);

			if (clausesplit.length > 1)
			{
				String splittablefromalias [] = clausesplit[0].trim().split("\\s+(?i)as(\\s+|$)");
				if (splittablefromalias.length == 1)
				{
					String splittablebyblank [] = splittablefromalias[0].trim().split("\\s+");
					joinTable = new SQLTable(splittablebyblank[0]);
					if (splittablebyblank.length==2)
						joinTable.setAlias(splittablebyblank[1].trim());
					else if (splittablebyblank.length>2)
						throw new SQLException("Error: " + splittablefromalias[0]);
				}
				else if (splittablefromalias.length == 2)
				{
					joinTable = new SQLTable(splittablefromalias[0].trim());
					joinTable.setAlias(splittablefromalias[1].trim());
				}
				else
					throw new SQLException("Invalid SQL: " + clausesplit[0]);

				String splitedands [] = clausesplit[1].split(" and | AND |,");

				for (int i = 0; i < splitedands.length; i++)
				{
					String splitedandors [] = splitedands[i].split(" or | OR ");

					SQLExpressionFragment andoperator = new SQLExpressionFragment("AND");

					for (int y = 0; y < splitedandors.length; y++)
					{
						SQLExpressionFragment exp = new SQLExpressionFragment(SQLExpressionFragment.LOGICAL_EXPRESSION_TYPE);
						SQLExpressionFragment orperator = new SQLExpressionFragment("OR");

						String trimmedExpression = splitedandors[y].trim();
						String operator = null;

						//order matters here!
						if (trimmedExpression.indexOf(SQLOperator.gte)!=-1)
							operator = SQLOperator.gte;
						else if (trimmedExpression.indexOf(SQLOperator.lte)!=-1)
							operator = SQLOperator.lte;
						else if (trimmedExpression.indexOf(SQLOperator.neq)!=-1)
							operator = SQLOperator.neq;
						else if (trimmedExpression.indexOf(SQLOperator.eq)!=-1)
							operator = SQLOperator.eq;
						else if (trimmedExpression.indexOf(SQLOperator.gt)!=-1)
							operator = SQLOperator.gt;
						else if (trimmedExpression.indexOf(SQLOperator.lt)!=-1)
							operator = SQLOperator.lt;
						else
							throw new SQLException("Invalid logical operator found: " + trimmedExpression);

						String splitedsqlexp [] = splitedandors[y].trim().split(operator);

						if (splitedsqlexp.length <= 0 ) //something went wrong, only the operator was found?
							throw new SQLException("Invalid SQL \"Where\" clause found around: " + splitedandors[y]);

						exp.setPrefix(parseExpressionFragment(splitedsqlexp[0].trim()));

						exp.setOperator(operator);
						if (!exp.isOperatorValid())
							throw new SQLException("Error: Invalid operator found: ");

						if (splitedsqlexp.length>1)
						{
							exp.setPostfix(parseExpressionFragment(splitedsqlexp[1].trim()));
						}

						OnClause.addExpression(exp);

						if (y < splitedandors.length-1)
							OnClause.addExpression(orperator);
					}

					if (i < splitedands.length-1)
						OnClause.addExpression(andoperator);
				}
			}
			else
				throw new SQLException("Error: 'Join' clause does not contain 'On' cluase.");
		}

		return success;
	}

	private String [] parseExpressionFragment(String fragment)
	{
		String [] parsedFrag = new String [2];

		String fragsplit [] = fragment.split("\\.",2);
		if (fragsplit.length == 1)
		{
			parsedFrag[0] = "";
			parsedFrag[1] = fragsplit[0].trim();
		}
		else
		{
			parsedFrag[0] = searchForPossibleTableName(fragsplit[0].trim());
			parsedFrag[1] = fragsplit[1].trim();
		}

		return parsedFrag;
	}

	/*
	 * Returns table name if the tablename or alias match
	 * Otherwise return empty string
	 */
	private String searchForPossibleTableName(String searchname)
	{
		if(searchname.equals(joinTable.getAlias()) || searchname.equals(joinTable.getName()))
			return joinTable.getName();
		else if (sourceTable != null && (searchname.equals(sourceTable.getName()) || searchname.equals(sourceTable.getAlias())))
			return sourceTable.getName();
		else
			return searchname;
	}

	public SQLWhereClause getOnClause()
	{
		return this.OnClause;
	}

	public short getType()
	{
		return this.type;
	}

	private String getTypeStr()
	{
		switch (type)
		{
			case INNER:
				return " INNER JOIN ";
			case OUTER:
				return " OUTER JOIN ";
			default:
				return " JOIN ";
		}
	}

	public String getECLTypeStr()
	{
		switch (type)
		{
			case OUTER:
				return " FULL OUTER ";
			case INNER:
			default:
				return " INNER ";
		}
	}

	public String getJoinTableName()
	{
		return joinTable.getName();
	}

	public String getJoinTableAlias()
	{
		return joinTable.getAlias();
	}

	public SQLTable getSourceTable()
	{
		return sourceTable;
	}

	public void setSourceTable(SQLTable sourceTable)
	{
		this.sourceTable = sourceTable;
	}

	@Override
	public String toString()
	{
		return getTypeStr() + getJoinTableName() + " ON " + OnClause.fullToString();
	}
}
