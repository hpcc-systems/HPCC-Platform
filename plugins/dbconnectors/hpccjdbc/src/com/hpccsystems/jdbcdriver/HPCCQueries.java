package com.hpccsystems.jdbcdriver;

import java.util.Enumeration;
import java.util.Properties;

public class HPCCQueries {

	private Properties queries;
	private String clusterName;

	public HPCCQueries(String cluster)
	{
		clusterName = cluster;
		queries = new Properties();
	}

	public void put(String name, HPCCQuery query)
	{
		queries.put(name, query);
	}

	public String getClusterName()
	{
		return clusterName;
	}
	public Enumeration<Object> getQueries()
	{
		return queries.elements();
	}

	public HPCCQuery getQuery(String eclqueryname)
	{
		return (HPCCQuery)queries.get(eclqueryname);
	}
}
