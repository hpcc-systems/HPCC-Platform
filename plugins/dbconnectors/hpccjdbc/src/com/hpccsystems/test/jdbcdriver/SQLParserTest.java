package com.hpccsystems.test.jdbcdriver;

import java.util.List;

import com.hpccsystems.jdbcdriver.HPCCColumnMetaData;
import com.hpccsystems.jdbcdriver.SQLParser;

public class SQLParserTest
{
	static private SQLParser parser;

	static
	{
		parser	= new SQLParser();
	}

	static private boolean testSelectALL(String tablename)
	{
		boolean success = true;
		try
		{
			parser.process("select * from " + tablename);
			success = parser.getTableName().equals(tablename) ? true : false;

		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			success = false;
		}

		return success;
	}

	static private boolean testTableAlias(String tablename, String alias)
	{
		boolean success = true;
		try
		{
			parser.process("select * from " + tablename + " AS " + alias);
			success &= parser.getTableName().equals(tablename) ? true : false;
			success &= parser.getTableAlias().equals(alias) ? true : false;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			success = false;
		}

		return success;
	}

	static private boolean testFreeHandSQL(String sql)
	{
		boolean success = true;
		try
		{
			parser.process(sql);

			int sqltype = parser.getSqlType();

			System.out.print("SQL request type: ");
			List<HPCCColumnMetaData> cols = parser.getSelectColumns();
			switch (sqltype)
			{
			case SQLParser.SQL_TYPE_SELECT:
				System.out.println("SELECT");
				System.out.print("Select Columns: [ ");
				for (int i = 0; i < cols.size(); i++)
				{
					System.out.print(cols.get(i).getColumnName() + "( " + cols.get(i).getAlias()  + " )");
				}
				System.out.println(" ] ");
				System.out.println("Table Name: " + parser.getTableName());
				System.out.println("Table Alias: " + parser.getTableAlias());
				System.out.println("Index Hint: " + parser.getIndexHint());
				System.out.println("Where Clause: " + parser.getWhereClauseString());
				if (parser.hasGroupByColumns())
					System.out.println("Group By: " + parser.getGroupByString(','));
				if (parser.hasOrderByColumns())
					System.out.println("Group By: " + parser.getOrderByString(','));
				break;
			case SQLParser.SQL_TYPE_SELECTCONST:
				System.out.println("SELECT CONSTANT");
				System.out.println("Select Columns: [ ");
				for (int i = 0; i < cols.size(); i++)
				{
					System.out.println(cols.get(i).getColumnName() + "( " + cols.get(i).getAlias());
				}
				System.out.println(" ] ");
				break;
			case SQLParser.SQL_TYPE_CALL:
				System.out.println("CALL");
				System.out.println("Stored Procedure name: " + parser.getStoredProcName());
				String spvals [] = parser.getStoredProcInParamVals();
				System.out.print("Stored Procedure input params : (");
				for(int i = 0; i < spvals.length; i++)
				{
					System.out.print(spvals[i] + " ");
				}
				System.out.println(")");
				break;
			case SQLParser.SQL_TYPE_UNKNOWN:
			default:
				System.out.print("not detected");
				break;
			}

			if (parser.hasLimitBy())
				System.out.println("Limit by: " + parser.getLimit());

		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			success = false;
		}

		return success;
	}

	public static void main(String[] args) throws Exception
	{
		boolean success = true;

		try
		{
			success &= testSelectALL("mytablename");
			success &= testTableAlias("mytablename", "mytablealias");
			success &= testFreeHandSQL("select city as mycity, persons.zip, count(*) from tutorial::rp::tutorialperson as persons USE INDEX(myindex) where zip ='33445' limit 1000");
			success &= testFreeHandSQL("call peoplebyzip(33445) limit 1000");
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			success = false;
		}

		System.out.println("\nParser test " + (success ? "passed! " : "failed!"));
	}
}
