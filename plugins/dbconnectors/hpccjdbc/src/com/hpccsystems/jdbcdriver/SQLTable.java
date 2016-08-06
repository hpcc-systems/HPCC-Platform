package com.hpccsystems.jdbcdriver;

public class SQLTable
{
	private String name;
	private String alias;

	public SQLTable(String name)
	{
		this.name = name;
		this.alias = name;
	}

	public SQLTable(String name, String alias)
	{
		this.name = name;
		this.alias = alias;
	}

	public SQLTable(String name, String alias, DFUFile dfufile)
	{
		this.name = name;
		this.alias = alias;
	}

	public String getName()
	{
		return name;
	}

	public String getAlias()
	{
		return alias;
	}

	public void setAlias(String alias)
	{
		this.alias = alias;
	}

	public String translateIfAlias(String possibleAlias)
	{
		if (possibleAlias.equals(alias) ||  possibleAlias.equals(name))
			return name;
		else
			return null;
	}
}
