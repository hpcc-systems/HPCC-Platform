package com.hpccsystems.ecljdbc;

import java.util.Enumeration;
import java.util.Properties;

public class EclQueries {

	private Properties queries;
	private String clusterName;

	public EclQueries(String cluster)
	{
		clusterName = cluster;
		queries = new Properties();
	}

	public void put(String name, EclQuery query)
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

	public EclQuery getQuery(String eclqueryname)
	{
		return (EclQuery)queries.get(eclqueryname);
	}
}
