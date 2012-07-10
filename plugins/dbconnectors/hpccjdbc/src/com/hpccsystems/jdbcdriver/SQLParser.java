package com.hpccsystems.jdbcdriver;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

public class SQLParser
{
	public final static short 	SQL_TYPE_UNKNOWN = -1;
	public final static short 	SQL_TYPE_SELECT = 1;
	public final static short 	SQL_TYPE_SELECTCONST = 2;
	public final static short 	SQL_TYPE_CALL = 3;

	private SQLTable queryTable;
	private int sqlType;
	private List<HPCCColumnMetaData> selectColumns;
	private SQLWhereClause whereClause;
	private SQLJoinClause joinClause;
	private String[] columnGroupByNames;
	private String[] columnOrderByNames;
	private String[] procInParamValues;
	private String storedProcName;
	private int limit;
	private boolean columnsVerified;
	private String indexHint;

	public void process(String insql) throws SQLException
	{
		System.out.println("INCOMING SQL: " + insql);
		columnsVerified = false;
		limit = -1;
		queryTable = null;
		selectColumns = new ArrayList<HPCCColumnMetaData>();
		whereClause = new SQLWhereClause();
		joinClause = null;
		procInParamValues = new String[0];
		storedProcName = null;
		sqlType = SQL_TYPE_UNKNOWN;
		indexHint = null;

		insql = HPCCJDBCUtils.removeAllNewLines(insql);
		String insqlupcase = insql.toUpperCase();

		if (insql.matches("^(?i)alter\\s+(.*?)"))
		{
			throw new SQLException("ALTER TABLE statements are not supported.");
		}
		else if (insql.matches("^(?i)drop\\s+(.*?)"))
		{
			throw new SQLException("DROP statements are not supported.");
		}
		else if (insql.matches("^(?i)insert\\s+(.*?)"))
		{
			throw new SQLException("INSERT statements are not supported.");
		}
		else if (insql.matches("^(?i)update\\s+(.*?)"))
		{
			throw new SQLException("UPDATE statements are not supported.");
		}
		else if (insql.matches("^(?i)call\\s+(.*?)"))
		{
			sqlType = SQL_TYPE_CALL;
			int callstrpos = insqlupcase.lastIndexOf("CALL ");
			int storedprocstrpos = insql.lastIndexOf("(");
			int paramlistend =  insql.lastIndexOf(")");
			String paramToken = "";

			if (storedprocstrpos == -1)
				storedProcName = insql.substring(callstrpos+5);
			else
			{
				if (paramlistend == -1)
					throw new SQLException("Missing closing param in: " + insql);
				storedProcName = insql.substring(callstrpos+5, storedprocstrpos);
				paramToken = insql.substring(storedprocstrpos+1, paramlistend);
			}

			if(paramToken.length() >0 )
			{
				StringTokenizer tokenizer = new StringTokenizer(paramToken, ",");
				procInParamValues = new String[tokenizer.countTokens()];
				int i = 0;
				while (tokenizer.hasMoreTokens())
				{
					procInParamValues[i++] = tokenizer.nextToken().trim();
				}
			}
		}
		else if (insql.matches("^(?i)select\\s+(.*?)"))
		{
			if (insql.matches("^(?i)select(.*?)\\s+(?i)union\\s+.*"))
				throw new SQLException("SELECT UNIONS are not supported.");

			sqlType = SQL_TYPE_SELECT;
			int fromstrpos  = insqlupcase.lastIndexOf(" FROM ");

			if (fromstrpos == -1)
			{
				if (parseConstantSelect(insql))
				{
					System.out.println("Found Select <constant>");
					sqlType = SQL_TYPE_SELECTCONST;
					return;
				}
				else
					throw new SQLException("Malformed SQL. Missing FROM statement.");
			}

			int useindexstrpos = insqlupcase.lastIndexOf(" USE INDEX(");
			/* Allow multiple spaces between USE and INDEX?
			 * if (useindexstrpos > 0)
			{
				String afterUSE = upperSql.substring(useindexstrpos+4);
				int inderelativepos = afterUSE.indexOf("INDEX(");

				if(inderelativepos < 0)
					throw new SQLException(" Malformed SQL statement near \'USE\' ");
				else if(inderelativepos > 1)
				{
					String use [] = afterUSE.split("INDEX");
					if(use[0].trim().length() != 0)
						throw new SQLException(" Malformed SQL statement near \'USE\' ");
				}

			}*/

			int joinPos  = insqlupcase.lastIndexOf(" JOIN ");
			int wherePos = insqlupcase.lastIndexOf(" WHERE ");
			int groupPos = insqlupcase.lastIndexOf(" GROUP BY ");
			int orderPos = insqlupcase.lastIndexOf(" ORDER BY ");
			int limitPos = insqlupcase.lastIndexOf(" LIMIT ");

			if ( useindexstrpos != -1 && useindexstrpos < fromstrpos)
				throw new SQLException("Malformed SQL. USING clause placement.");

			if (joinPos != -1 && joinPos < fromstrpos)
					throw new SQLException("Malformed SQL. Join clause placement.");

			if (wherePos != -1 && wherePos < fromstrpos)
				throw new SQLException("Malformed SQL. WHERE clause placement.");

			try
			{
				if (limitPos != -1)
				{
					limit = Integer.valueOf(insqlupcase.substring(limitPos+6).trim());
					insqlupcase = insqlupcase.substring(0, limitPos);
				}
			}
			catch (NumberFormatException ne)
			{
				throw new SQLException("Error near :\'" + insqlupcase.substring(limitPos)+"\'");
			}

			String orderByToken = "";
			String groupByToken = "";

			if (groupPos != -1 && (orderPos == -1 || groupPos < orderPos))
			{
				if (orderPos == -1)
				{
					groupByToken = insqlupcase.substring(groupPos + 10);
				}
				else
				{
					groupByToken = insqlupcase.substring(groupPos + 10, orderPos);
					orderByToken = insqlupcase.substring(orderPos + 10);
				}

				insqlupcase = insqlupcase.substring(0, groupPos);

			}
			else if (orderPos != -1 && (groupPos == -1 || orderPos < groupPos))
			{
				if (groupPos == -1)
				{
					orderByToken = insqlupcase.substring(orderPos + 10);
				}
				else
				{
					orderByToken = insqlupcase.substring(orderPos + 10, groupPos);
					groupByToken = insqlupcase.substring(groupPos + 10);
				}
				insqlupcase = insqlupcase.substring(0, orderPos);
			}

			if (orderByToken.length()>0)
			{
				StringTokenizer tokenizer = new StringTokenizer(orderByToken, ",");

				columnOrderByNames = new String[tokenizer.countTokens()];
				int i = 0;
				while (tokenizer.hasMoreTokens())
				{
					String orderbycolumn = tokenizer.nextToken().trim();
					boolean orderbyascending = true;

					int dirPos = orderbycolumn.lastIndexOf("ASC");
					if (dirPos == -1)
						dirPos = orderbycolumn.lastIndexOf("DESC");

					//not else if from above if!!
					if (dirPos != -1)
					{
						orderbyascending = orderbycolumn.contains("ASC");
						orderbycolumn = orderbycolumn.substring(0,dirPos).trim();
					}
					columnOrderByNames[i++] = (orderbyascending == true ? "" : "-") + orderbycolumn;
				}
			}

			if (groupByToken.length()>0)
			{
				StringTokenizer tokenizer = new StringTokenizer(groupByToken, ",");
				columnGroupByNames = new String[tokenizer.countTokens()];
				int i = 0;
				while (tokenizer.hasMoreTokens())
				{
					columnGroupByNames[i++] = tokenizer.nextToken().trim();
				}
			}

			insql = insql.substring(0,insqlupcase.length());
			String fullTableName = null;

			if (joinPos != -1)
			{
				fullTableName = insql.substring(fromstrpos + 6, joinPos).split("\\s+(?i)inner|(?i)outer\\s*")[0];
			}
			else if (useindexstrpos != -1)
			{
				fullTableName = insql.substring(fromstrpos + 6, useindexstrpos);
			}
			else if (wherePos != -1)
			{
				fullTableName = insql.substring(fromstrpos + 6, wherePos);
			}
			else
			{
				fullTableName = insql.substring(fromstrpos + 6);
			}

			String splittablefromalias [] = fullTableName.trim().split("\\s+(?i)as(\\s+|$)");
			if (splittablefromalias.length == 1)
			{
				String splittablebyblank [] = splittablefromalias[0].trim().split("\\s+");
				queryTable = new SQLTable(splittablebyblank[0].trim());
				if (splittablebyblank.length==2)
					queryTable.setAlias(splittablebyblank[1].trim());
				else if (splittablebyblank.length>2)
					throw new SQLException("Invalid SQL: " + splittablefromalias[0]);
			}
			else if (splittablefromalias.length == 2)
			{
				queryTable = new SQLTable(splittablefromalias[0].trim());
				queryTable.setAlias(splittablefromalias[1].trim());
			}
			else
				throw new SQLException("Invalid SQL: " + fullTableName);

			if (fromstrpos <= 7)
				throw new SQLException("Invalid SQL: Missing select column(s).");

			StringTokenizer comatokens = new StringTokenizer(insql.substring(7, fromstrpos), ",");

			for(int sqlcolpos = 1; comatokens.hasMoreTokens();)
			{
				HPCCColumnMetaData colmetadata = null;
				String colassplit [] = comatokens.nextToken().split("\\s+(?i)as\\s+");
				String col = colassplit [0].trim();
				String coltable = queryTable.getName();

				if (col.contains("("))
				{
					int funcparampos = 1;
					List<HPCCColumnMetaData> funccols = new ArrayList<HPCCColumnMetaData>();

					String funcname = col.substring(0,col.indexOf('('));
					ECLFunction func= ECLFunctions.getEclFunction(funcname.toUpperCase());

					if (func == null)
						throw new SQLException("ECL Function " + funcname + "is not currently supported");

					col = col.substring(col.indexOf('(')+1).trim();

					if (col.contains(")"))
					{
						col = col.substring(0, col.indexOf(")")).trim();
						if (col.length()>0)
						{
							funccols.add(new HPCCColumnMetaData(col,funcparampos++,java.sql.Types.OTHER));
						}
					}
					else
					{
						funccols.add(new HPCCColumnMetaData(col,funcparampos++,java.sql.Types.OTHER));
						while (comatokens.hasMoreTokens())
						{
							col = comatokens.nextToken().trim();
							if (col.contains(")"))
							{
								col = col.substring(0, col.indexOf(")"));
								funccols.add(new HPCCColumnMetaData(col,funcparampos++,java.sql.Types.OTHER));
								break;
							}
							funccols.add(new HPCCColumnMetaData(col,funcparampos++,java.sql.Types.OTHER));
						}
					}

					if (ECLFunctions.verifyEclFunction(funcname, funccols))
						colmetadata = new HPCCColumnMetaData(funcname, sqlcolpos++, funccols);
					else
						throw new SQLException("Function " + funcname + " does not map to ECL as written");

					colmetadata.setSqlType(func.getReturnType().getSqlType());
				}

				if (colmetadata == null)
					colmetadata = new HPCCColumnMetaData(col,sqlcolpos++,java.sql.Types.OTHER);

				colmetadata.setTableName(queryTable.getName());

				if (colassplit.length > 1)
					colmetadata.setAlias(colassplit[1]);

				selectColumns.add(colmetadata);
			}

			if (useindexstrpos != -1)
			{
				String useindexstr = insql.substring(useindexstrpos + 11);
				int useindexend = useindexstr.indexOf(")");
				if (useindexend < 0 )
					throw new SQLException("Malformed USE INDEX() clause.");
				indexHint = useindexstr.substring(0,useindexend).trim();
				System.out.println(indexHint);
			}

			if (wherePos != -1)
			{
				String strWhere = insql.substring(wherePos + 7);
				String splitedwhereands [] = strWhere.split(" and | AND |,");

				for (int i = 0; i < splitedwhereands.length; i++)
				{
					String splitedwhereandors [] = splitedwhereands[i].split(" or | OR ");

					SQLExpressionFragment andoperator = new SQLExpressionFragment("AND");

					for (int y = 0; y < splitedwhereandors.length; y++)
					{
						SQLExpressionFragment exp = new SQLExpressionFragment(SQLExpressionFragment.LOGICAL_EXPRESSION_TYPE);
						SQLExpressionFragment orperator = new SQLExpressionFragment("OR");

						String trimmedExpression = splitedwhereandors[y].trim();
						String operator = null;

						//order matters here!
						if (trimmedExpression.indexOf(SQLOperator.gte)!=-1)
							operator = SQLOperator.gte;
						else if (trimmedExpression.indexOf(SQLOperator.lte)!=-1)
							operator = SQLOperator.lte;
						else if (trimmedExpression.indexOf(SQLOperator.neq)!=-1)
							operator = SQLOperator.neq;
						else if (trimmedExpression.indexOf(SQLOperator.neq2)!=-1)
							operator = SQLOperator.neq2;
						else if (trimmedExpression.indexOf(SQLOperator.eq)!=-1)
							operator = SQLOperator.eq;
						else if (trimmedExpression.indexOf(SQLOperator.gt)!=-1)
							operator = SQLOperator.gt;
						else if (trimmedExpression.indexOf(SQLOperator.lt)!=-1)
							operator = SQLOperator.lt;
						else
							throw new SQLException("Invalid logical operator found: " + trimmedExpression);

						String splitedsqlexp [] = splitedwhereandors[y].trim().split(operator);

						if (splitedsqlexp.length <= 0 ) //something went wrong, only the operator was found?
							throw new SQLException("Invalid SQL Where cluse found around: " + splitedwhereandors[y]);

						exp.setPrefix(splitedsqlexp[0]);
						if (exp.getPrefixParent().length() > 0 &&
								(!exp.getPrefixParent().equals(queryTable.getName()) && !exp.getPrefixParent().equals(queryTable.getAlias()) ))
							throw new SQLException("Invalid field found: " + splitedsqlexp[0]);

						exp.setOperator(operator);
						if (!exp.isOperatorValid())
							throw new SQLException("Error: Invalid operator found: ");

						if (splitedsqlexp.length>1)
							exp.setPostfix(splitedsqlexp[1].trim());

						if (exp.getPostfixParent().length() > 0 &&
								(!exp.getPrefixParent().equals(queryTable.getName()) && !exp.getPrefixParent().equals(queryTable.getAlias())))
							throw new SQLException("Invalid field found: " + splitedsqlexp[1]);

						whereClause.addExpression(exp);

						if (y < splitedwhereandors.length-1)
							whereClause.addExpression(orperator);
					}

					if (i < splitedwhereands.length-1)
						whereClause.addExpression(andoperator);
				}

				insqlupcase = insqlupcase.substring(0, wherePos);
			}

			if (joinPos != -1)
			{
				joinClause = new SQLJoinClause();
				joinClause.setSourceTable(queryTable);

				int inJoinPos  = insqlupcase.lastIndexOf(" INNER JOIN ");

				if (inJoinPos != -1)
				{
					joinClause.parseClause(insql.substring(inJoinPos,insqlupcase.length()));
				}
				else
				{
					int outJoinPos  = insqlupcase.lastIndexOf(" OUTER JOIN ");

					if (outJoinPos != -1)
					{
						joinClause.parseClause(insql.substring(outJoinPos,insqlupcase.length()));
					}
					else
					{
						joinClause.parseClause(insql.substring(joinPos,insqlupcase.length()));
					}
				}
				System.out.println(joinClause.toString());
			}
		}
		else
			throw new SQLException("Invalid SQL found - only supports CALL and/or SELECT statements.");
	}

	private boolean parseConstantSelect(String sql) throws SQLException
	{
		String sqlUpper = sql.toUpperCase();
		int wherePos = sqlUpper.lastIndexOf(" WHERE ");
		int groupPos = sqlUpper.lastIndexOf(" GROUP BY ");
		int orderPos = sqlUpper.lastIndexOf(" ORDER BY ");
		int limitPos = sqlUpper.lastIndexOf(" LIMIT ");

		if (wherePos > 0 || groupPos > 0 || orderPos > 0)
			return false;
		try
		{
			if (limitPos > 0)
			{
				limit = Integer.valueOf(sqlUpper.substring(limitPos+6).trim());
				sql = sqlUpper.substring(0, limitPos);
			}
		}
		catch (NumberFormatException ne)
		{
			throw new SQLException("Error near :\'" + sql.substring(limitPos)+"\'");
		}

		//At this point we have select <something>
		StringTokenizer comatokens = new StringTokenizer(sql.substring(6), ",");

		for (int pos = 1; comatokens.hasMoreTokens();)
		{
			String colassplit [] = comatokens.nextToken().split("\\s+(?i)as\\s+");
			String col = colassplit [0].trim();

			HPCCColumnMetaData colmetadata = null;

			if(HPCCJDBCUtils.isLiteralString(col))
			{
				colmetadata = new HPCCColumnMetaData("ConstStr"+pos, pos++, java.sql.Types.VARCHAR);
				colmetadata.setEclType("STRING");
			}
			else if (HPCCJDBCUtils.isNumeric(col))
			{
				colmetadata = new HPCCColumnMetaData("ConstNum"+pos, pos++, java.sql.Types.NUMERIC);
				colmetadata.setEclType("INTEGER");
			}

			colmetadata.setColumnType(HPCCColumnMetaData.COLUMN_TYPE_CONSTANT);
			colmetadata.setConstantValue(col);

			if (colassplit.length > 1)
				colmetadata.setAlias(colassplit[1]);

			selectColumns.add(colmetadata);
		}

		return true;
	}

	public boolean columnsHasWildcard()
	{
		Iterator<HPCCColumnMetaData> it = selectColumns.iterator();
		while (it.hasNext())
		{
			if(it.next().getColumnName().contains("*"))
				return true;
		}
		return false;
	}

	public int orderByCount()
	{
		return columnOrderByNames==null ? 0 : columnOrderByNames.length;
	}

	public boolean hasOrderByColumns()
	{
		return columnOrderByNames != null && columnOrderByNames.length>0 ? true : false;
	}

	public int groupByCount()
	{
		return columnGroupByNames==null ? 0 : columnGroupByNames.length;
	}

	public String getOrderByColumn(int index)
	{

		return (columnOrderByNames == null || index < 0 || index >= columnOrderByNames.length) ? "" : columnOrderByNames[index];
	}

	public String getOrderByString()
	{
		StringBuilder tmp = new StringBuilder("");
		for (int i = 0; i<columnOrderByNames.length; i++)
		{
			tmp.append(columnOrderByNames[i]);
			if (i!=columnOrderByNames.length-1)
				tmp.append(',');
		}
		return tmp.toString();
	}

	public String getOrderByString(char delimiter)
	{
		StringBuilder tmp = new StringBuilder("");
		for (int i = 0; i<columnOrderByNames.length; i++)
		{
			tmp.append(columnOrderByNames[i]);
			if (i!=columnOrderByNames.length-1)
				tmp.append(delimiter);
		}
		return tmp.toString();
	}

	public String getGroupByString()
	{
		StringBuilder tmp = new StringBuilder("");
		for (int i = 0; i<columnGroupByNames.length; i++)
		{
			tmp.append(columnGroupByNames[i]);
			if (i!=columnGroupByNames.length-1)
				tmp.append(',');
		}
		return tmp.toString();
	}

	public String getGroupByString(char delimiter)
	{
		StringBuilder tmp = new StringBuilder("");
		for (int i = 0; i<columnGroupByNames.length; i++)
		{
			tmp.append(columnGroupByNames[i]);
			if (i!=columnGroupByNames.length-1)
				tmp.append(delimiter);
		}
		return tmp.toString();
	}

	public String getGroupByColumn(int index)
	{

		return (columnGroupByNames == null || index < 0 || index >= columnGroupByNames.length) ? "" : columnGroupByNames[index];
	}

	public boolean hasGroupByColumns()
	{
		return columnGroupByNames != null && columnGroupByNames.length>0 ? true : false;
	}

	public boolean hasLimitBy()
	{
		return limit == -1 ? false : true;
	}

	public String [] getStoredProcInParamVals()
	{
		return procInParamValues;
	}

	public String getStoredProcName()
	{
		return storedProcName;
	}

	public String getTableAlias()
	{
		return queryTable.getAlias();
	}

	public int getSqlType()
	{
		return sqlType;
	}

	public int getLimit()
	{
		return limit;
	}

	public String getTableName()
	{
		return queryTable.getName();
	}

	public String[] getColumnNames()
	{
		Iterator<HPCCColumnMetaData> it = selectColumns.iterator();
		String [] selcols = new String[selectColumns.size()];
		for (int i = 0 ; it.hasNext(); i++)
			selcols[i] = it.next().getColumnName();

		return selcols;
	}

	public void populateParametrizedExpressions(Map inParameters) throws SQLException
	{
		if (inParameters.size()>0)
		{
			if (whereClause != null && whereClause.getExpressionsCount() > 0)
			{
				Iterator<SQLExpressionFragment> expressionit = whereClause.getExpressions();
				int paramIndex = 0;
				while (expressionit.hasNext())
				{
					SQLExpressionFragment exp = expressionit.next();
					if (exp.isParametrized())
					{
						String value = (String)inParameters.get(new Integer(++paramIndex));
						if (value == null)
							throw new SQLException("Could not bound parametrized expression(" + exp +") to parameter");
						exp.setPostfix(value);
					}
				}
			}
			else if (procInParamValues.length > 0)
			{
				int paramindex = 0;
				for (int columindex = 0; columindex < procInParamValues.length;columindex++)
				{
					if(isParametrized(procInParamValues[columindex]))
					{
						String value = (String)inParameters.get(new Integer(++paramindex));
						if (value == null)
							throw new SQLException("Could not bound parameter");
						procInParamValues[columindex] = value;
					}
				}
			}
		}
	}

	boolean isParametrized( String param)
	{
		return  (param.contains("${")|| param.equals("?"));
	}

	public int getWhereClauseExpressionsCount()
	{
		return whereClause.getExpressionsCount();
	}

	public String[] getWhereClauseNames()
	{
		return whereClause.getExpressionNames();
	}

	public String[] getUniqueWhereClauseNames()
	{
		return whereClause.getUniqueExpressionNames();
	}

	public SQLExpressionFragment getExpressionFromName(String name)
	{
		return whereClause.getExpressionFromName(name);
	}

	public boolean whereClauseContainsKey(String name)
	{
		return whereClause.containsKey(name);
	}

	public String getWhereClauseString()
	{
		return whereClause.toString();
	}

	public int getUniqueWhereColumnCount()
	{
		return whereClause.getUniqueColumnCount();
	}

	public boolean whereClauseContainsOrOperator()
	{
		return whereClause.isOrOperatorUsed();
	}

	public List<HPCCColumnMetaData> getSelectColumns()
	{
		return selectColumns;
	}

	public void expandWildCardColumn(HashMap<String, HPCCColumnMetaData> allFields)
	{
		Iterator<HPCCColumnMetaData> it = selectColumns.iterator();

		//for loop iterator b/c we need the index for each column.
		for(int i = 0; it.hasNext(); i++)
		{
			if (it.next().getColumnName().equals("*"))
			{
				System.out.println("Expanding wildcard, select columns order might be altered");//fine for now, we do need to address at some point
				selectColumns.remove(i);

				Iterator<Entry<String, HPCCColumnMetaData>> availablefields = allFields.entrySet().iterator();
				while (availablefields.hasNext())
				{
					HPCCColumnMetaData element = (HPCCColumnMetaData)availablefields.next().getValue();
					selectColumns.add(element);
				}
				break;
			}
		}
	}

	public boolean areColumnsVerified()
	{
		return columnsVerified;
	}

	public void verifyAndProcessALLSelectColumns(HashMap<String, HPCCColumnMetaData> availableCols) throws Exception
	{
		if (areColumnsVerified())
			return;

		for (int i = 0; i < selectColumns.size(); i++)
		{
			verifyAndProcessAllColumn(selectColumns.get(i), availableCols);
		}

		if (columnsHasWildcard())
			expandWildCardColumn(availableCols);

		columnsVerified = true;
	}

	public void verifyAndProcessAllColumn(HPCCColumnMetaData column, HashMap<String, HPCCColumnMetaData> availableCols) throws Exception
	{
		String fieldName = column.getColumnName();
		String tableName = queryTable.getName();

		String colsplit [] = fieldName.split("\\.");

		if (colsplit.length == 2)
		{
			tableName = searchForPossibleTableName(colsplit[0]);

			if (tableName.equals(""))
				throw new Exception("Invalid column found: " + fieldName);

			fieldName = colsplit[1];
		}
		else if (colsplit.length > 2)
			throw new Exception("Invalid column found: " + fieldName);

		if (!availableCols.containsKey(tableName+"."+fieldName))
		{
			if (!fieldName.trim().equals("*"))
			{
				if  (column.getColumnType() == HPCCColumnMetaData.COLUMN_TYPE_FNCTION)
				{
					if (column.getAlias() == null)
						column.setAlias(fieldName + "Out");

					List<HPCCColumnMetaData>funccols = column.getFunccols();
					for (int y = 0; y<funccols.size(); y++)
					{
						verifyAndProcessAllColumn(funccols.get(y), availableCols);
					}
				}
				else if(HPCCJDBCUtils.isLiteralString(fieldName))
				{
					column.setColumnName("ConstStr"+ column.getIndex());
					column.setEclType("STRING");
					column.setSqlType(java.sql.Types.VARCHAR);
					column.setColumnType(HPCCColumnMetaData.COLUMN_TYPE_CONSTANT);
					column.setConstantValue(fieldName);
				}
				else if (HPCCJDBCUtils.isNumeric(fieldName))
				{
					column.setColumnName("ConstNum" + column.getIndex());
					column.setEclType("INTEGER");
					column.setSqlType(java.sql.Types.NUMERIC);
					column.setColumnType(HPCCColumnMetaData.COLUMN_TYPE_CONSTANT);
					column.setConstantValue(fieldName);
				}
				else
					throw new Exception("Invalid column found: " + fieldName);
			}
		}
		else
		{
			column.setTableName(tableName);
			column.setColumnName(fieldName);
			column.setEclType(availableCols.get(tableName+"."+fieldName).getEclType());
		}
	}

	/*
	 * Returns table name if the tablename or alias match
	 * Otherwise return empty string
	 */
	private String searchForPossibleTableName(String searchname)
	{
		if(searchname.equals(queryTable.getAlias()) || searchname.equals(queryTable.getName()))
			return queryTable.getName();
		else if ( hasJoinClause() && (searchname.equals(joinClause.getJoinTableName()) || searchname.equals(joinClause.getJoinTableAlias())))
			return joinClause.getJoinTableName();
		else
			return "";
	}

	public String getIndexHint()
	{
		return indexHint;
	}

	public boolean hasJoinClause()
	{
		return joinClause == null ? false : true;
	}

	public SQLJoinClause getJoinClause()
	{
		return joinClause;
	}
}
