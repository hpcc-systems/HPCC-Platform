package com.hpccsystems.jdbcdriver;

import java.util.Enumeration;
import java.util.Properties;

public class HPCCQueries
{

    private Properties queries;

    public HPCCQueries()
    {
        queries = new Properties();
    }

    public void put(HPCCQuery query)
    {
        queries.put(query.getQuerySet() + "::" + query.getName(), query);
    }

    public Enumeration<Object> getQueries()
    {
        return queries.elements();
    }

    public HPCCQuery getQuerysetQuery(String eclqueryname)
    {
        String querysplit[] = eclqueryname.split("::");
        if (querysplit.length > 2)
        {
            String name = "";
            for (int i = 1; i < querysplit.length; i++)
                name += "::" + querysplit[i];

            return getQuery(querysplit[0], name);
        }
        else if (querysplit.length == 2)
            return getQuery(querysplit[0], querysplit[1]);
        else if (querysplit.length == 1)
            return getQuery(querysplit[0]);
        else
            return null;
    }

    public HPCCQuery getQuery(String queryset, String eclqueryname)
    {
        return (HPCCQuery) queries.get(queryset + "::" + eclqueryname);
    }

    public HPCCQuery getQuery(String eclqueryname)
    {
        return (HPCCQuery) queries.get(eclqueryname);
    }

    public int getLength()
    {
        return queries.size();
    }

    public boolean containsQueryName(String eclqueryname)
    {
        return queries.containsKey(eclqueryname);
    }

    public boolean containsQueryName(String clustername, String eclqueryname)
    {
        return queries.containsKey((clustername.length() > 0 ? clustername + "::" : "") + eclqueryname);
    }
}
