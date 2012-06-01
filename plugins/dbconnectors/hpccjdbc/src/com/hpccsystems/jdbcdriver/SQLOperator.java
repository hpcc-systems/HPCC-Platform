package com.hpccsystems.jdbcdriver;

import java.util.ArrayList;
import java.util.List;

public class SQLOperator
{
	private static List<String> validOps;
	public static final String eq = new String("=");
	public static final String neq = new String("<>");
	public static final String isNull = new String("IS NULL");
	public static final String isNotNull = new String("IS NOT NULL");
	public static final String gt = new String(">");
	public static final String lt = new String("<");
	public static final String gte = new String(">=");
	public static final String lte = new String("<=");
	public static final String and = new String("AND");
	public static final String or = new String("OR");
	public static final String not = new String("NOT");
	public static final String exists = new String("EXISTS");
	public static final String like = new String("LIKE");
	public static final String in = new String("IN");

	private final String value;

	public String getValue()
	{
		return value;
	}

	public boolean isValid()
	{
		return value == null ? false : true;
	}

	public SQLOperator(String operator)
	{
		if (validOps == null)
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

		if( validOps.contains(operator.toUpperCase()))
				value = operator.toUpperCase();
		else
				value = null;
	}

	@Override
	public String toString()
	{
		return value == null ? "" : value;
	}
}
