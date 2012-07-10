package com.hpccsystems.jdbcdriver;

public class SQLTable
{
	private String name;
	private String alias;
	private DFUFile dfufile;

	public SQLTable(String name)
	{
		this.name = name;
		this.alias = name;
		this.dfufile = null;
	}

	public SQLTable(String name, String alias)
	{
		this.name = name;
		this.alias = alias;
		this.dfufile = null;
	}

	public SQLTable(String name, String alias, DFUFile dfufile)
	{
		this.name = name;
		this.alias = alias;
		this.dfufile = dfufile;
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

	public DFUFile getDfufile()
	{
		return dfufile;
	}

	public void setDfufile(DFUFile dfufile)
	{
		this.dfufile = dfufile;
	}

	public String translateIfAlias(String possibleAlias)
	{
		if (possibleAlias.equals(alias) ||  possibleAlias.equals(name))
			return name;
		else
			return null;
	}
}
