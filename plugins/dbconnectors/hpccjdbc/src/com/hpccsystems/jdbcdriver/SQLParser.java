package com.hpccsystems.jdbcdriver;

import java.sql.SQLException;
import java.util.*;

public class SQLParser
{
	public final static short 	SQL_TYPE_UNKNOWN = -1;
	public final static short 	SQL_TYPE_SELECT = 1;
	public final static short 	SQL_TYPE_SELECTCONST = 2;
	public final static short 	SQL_TYPE_CALL = 3;

	private String tableName;
	private String tableAlias;
	private int sqlType;
	private List<HPCCColumnMetaData> selectColumns;
	private SQLWhereClause whereclause;
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
		tableName = null;
		tableAlias = null;
		selectColumns = new ArrayList<HPCCColumnMetaData>();
		whereclause = new SQLWhereClause();
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
			if (insql.matches("^(?i)select(.*?)\\s+(?i)join\\s+.*"))
				throw new SQLException("SELECT JOINS are not supported.");

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

			int wherePos = insqlupcase.lastIndexOf(" WHERE ");
			int groupPos = insqlupcase.lastIndexOf(" GROUP BY ");
			int orderPos = insqlupcase.lastIndexOf(" ORDER BY ");
			int limitPos = insqlupcase.lastIndexOf(" LIMIT ");

			if ( useindexstrpos != -1 && useindexstrpos < fromstrpos)
				throw new SQLException("Malformed SQL. USING clause placement.");

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
			if (wherePos == -1 && useindexstrpos == -1)
			{
				fullTableName = insql.substring(fromstrpos + 6).trim();
			}
			else
			{
				fullTableName = insql.substring(fromstrpos + 6, (useindexstrpos == -1) ? wherePos : useindexstrpos).trim();
			}

			String splittablefromalias [] = fullTableName.split("\\s+(?i)as(\\s+|$)");
			if (splittablefromalias != null && splittablefromalias.length > 0)
			{
				if (!splittablefromalias[0].contains(" "))
					tableName = splittablefromalias[0].trim();
				else
					throw new SQLException("Invalid SQL: " + splittablefromalias[0]);
			}
			else
				throw new SQLException("Invalid SQL: Missing table name.");

			if (splittablefromalias.length > 1)
				tableAlias = splittablefromalias[1].trim();

			if (fromstrpos <= 7)
				throw new SQLException("Invalid SQL: Missing select column(s).");

			StringTokenizer comatokens = new StringTokenizer(insql.substring(7, fromstrpos), ",");

			for(int sqlcolpos = 1; comatokens.hasMoreTokens();)
			{
				HPCCColumnMetaData colmetadata = null;
				String colassplit [] = comatokens.nextToken().split("\\s+(?i)as\\s+");
				String col = colassplit [0].trim();
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
							if(col.contains("."))
								col = handleComplexField(col);

							funccols.add(new HPCCColumnMetaData(col,funcparampos++,java.sql.Types.OTHER));
						}
					}
					else
					{
						if(col.contains("."))
							col = handleComplexField(col);

						funccols.add(new HPCCColumnMetaData(col,funcparampos++,java.sql.Types.OTHER));
						while (comatokens.hasMoreTokens())
						{
							col = comatokens.nextToken().trim();
							if (col.contains(")"))
							{
								col = col.substring(0, col.indexOf(")"));
								if(col.contains("."))
									col = handleComplexField(col);
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
				}
				else if(col.contains("."))
				{
					col = handleComplexField(col);
				}

				if (colmetadata == null)
					colmetadata = new HPCCColumnMetaData(col,sqlcolpos++,java.sql.Types.OTHER);

				colmetadata.setTableName(tableName);

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

					SQLExpression andoperator = new SQLExpression("AND");

					for (int y = 0; y < splitedwhereandors.length; y++)
					{
						SQLExpression exp = new SQLExpression(SQLExpression.LOGICAL_EXPRESSION_TYPE);
						SQLExpression orperator = new SQLExpression("OR");

						String trimmedExpression = splitedwhereandors[y].trim();
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


						String splitedsqlexp [] = splitedwhereandors[y].trim().split(operator);

						if (splitedsqlexp.length <= 0 ) //something went wrong, only the operator was found?
							throw new SQLException("Invalid SQL Where cluse found around: " + splitedwhereandors[y]);

						String wherecol = splitedsqlexp[0].trim();
						String wherecoltable = tableName;
						String colsplit [] = wherecol.split("\\.");
						if(wherecol.contains("."))
						{
							if (colsplit.length > 1)
							{
								if (!colsplit[0].equals(tableName) && !colsplit[0].equals(tableAlias))
									throw new SQLException("Invalid field found: " + wherecol);
								else
									wherecol = colsplit[colsplit.length-1];
							}
						}

						exp.setName(wherecol);
						exp.setParentSource(wherecoltable);
						exp.setOperator(operator);

						if (splitedsqlexp.length>1)
							exp.setValue(splitedsqlexp[1].trim());

						whereclause.addExpression(exp);

						if (y < splitedwhereandors.length-1)
							whereclause.addExpression(orperator);
					}

					if (i < splitedwhereands.length-1)
						whereclause.addExpression(andoperator);
				}
				//System.out.println(whereclause);
			}
		}
		else
			throw new SQLException("Invalid SQL found - only supports CALL and/or SELECT statements.");
	}

	private String handleComplexField(String fullFieldName) throws SQLException
	{
		String colsplit [] = fullFieldName.split("\\.");
		if (colsplit.length > 1)
		{
			if (!colsplit[0].equals(tableName) && !colsplit[0].equals(tableAlias))
				throw new SQLException("Invalid field found: " + fullFieldName);
			else
				return colsplit[colsplit.length-1];
		}
		return null;
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
		return tableAlias;
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
		return tableName;
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
			if (whereclause != null && whereclause.getExpressionsCount() > 0)
			{
				Iterator<SQLExpression> expressionit = whereclause.getExpressions();
				int paramIndex = 0;
				while (expressionit.hasNext())
				{
					SQLExpression exp = expressionit.next();
					if (exp.isParametrized())
					{
						String value = (String)inParameters.get(new Integer(++paramIndex));
						if (value == null)
							throw new SQLException("Could not bound parametrized expression(" + exp +") to parameter");
						exp.setValue(value);
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
		return whereclause.getExpressionsCount();
	}

	public String[] getWhereClauseNames()
	{
		return whereclause.getExpressionNames();
	}

	public String[] getUniqueWhereClauseNames()
	{
		return whereclause.getUniqueExpressionNames();
	}

	public SQLExpression getExpressionFromName(String name)
	{
		return whereclause.getExpressionFromName(name);
	}

	public boolean whereClauseContainsKey(String name)
	{
		return whereclause.containsKey(name);
	}

	public String getWhereClauseString()
	{
		return whereclause.toString();
	}

	public int getUniqueWhereColumnCount()
	{
		return whereclause.getUniqueColumnCount();
	}

	public boolean whereClauseContainsOrOperator()
	{
		return whereclause.isOrOperatorUsed();
	}

	public List<HPCCColumnMetaData> getSelectColumns()
	{
		return selectColumns;
	}

	public void expandWildCardColumn(Enumeration<Object> allFields)
	{
		Iterator<HPCCColumnMetaData> it = selectColumns.iterator();
		for(int i = 0; it.hasNext(); i++)
		{
			if (it.next().getColumnName().equals("*"))
			{
				System.out.println("Expanding wildcard, select columns order might be altered");//fine for now, we do need to address at some point
				selectColumns.remove(i);
				while (allFields.hasMoreElements())
				{
					HPCCColumnMetaData element = (HPCCColumnMetaData)allFields.nextElement();
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

	public void verifySelectColumns(DFUFile dfufile) throws Exception
	{
		if (areColumnsVerified())
			return;

		for (int i = 0; i < selectColumns.size(); i++)
		{
			verifyColumn(selectColumns.get(i), dfufile);
		}

		if (columnsHasWildcard())
			expandWildCardColumn(dfufile.getAllFields());

		columnsVerified = true;
	}

	public void verifyColumn(HPCCColumnMetaData column, DFUFile dfufile) throws Exception
	{
		String fieldName = column.getColumnName();

		if (!dfufile.containsField(column))
		{
			if (!fieldName.trim().equals("*"))
			{
				if  (column.getColumnType() == HPCCColumnMetaData.COLUMN_TYPE_FNCTION)
				{
					if (column.getAlias() == null)
						column.setAlias(fieldName + "Out");
					List<HPCCColumnMetaData>funccols = column.getFunccols();
					for (int i = 0; i<funccols.size(); i++)
					{
						verifyColumn(funccols.get(i), dfufile);
					}
				}
				else if(HPCCJDBCUtils.isLiteralString(fieldName))
				{
					column.setColumnName("ConstStr"+ column.getIndex());
					column.setEclType("STRING");
					column.setSQLType(java.sql.Types.VARCHAR);
					column.setColumnType(HPCCColumnMetaData.COLUMN_TYPE_CONSTANT);
					column.setConstantValue(fieldName);
				}
				else if (HPCCJDBCUtils.isNumeric(fieldName))
				{
					column.setColumnName("ConstNum" + column.getIndex());
					column.setEclType("INTEGER");
					column.setSQLType(java.sql.Types.NUMERIC);
					column.setColumnType(HPCCColumnMetaData.COLUMN_TYPE_CONSTANT);
					column.setConstantValue(fieldName);
				}
				else
					throw new Exception("Invalid column found");
			}
		}
		else
		{
			column.setEclType(dfufile.getFieldMetaData(fieldName).getEclType());
		}
	}

	public String getIndexHint()
	{
		return indexHint;
	}

	public static void main(String[] args) throws Exception
	{
		SQLParser parser = new SQLParser();
		parser.process("select city, zip, count(*) from tutorial::rp::tutorialperson where zip ='33445' limit 1000");
		System.out.println(parser.getWhereClauseString());
		System.out.println(parser.getTableName());
		if (parser.hasLimitBy())
			System.out.println(parser.getLimit());
	}
}
