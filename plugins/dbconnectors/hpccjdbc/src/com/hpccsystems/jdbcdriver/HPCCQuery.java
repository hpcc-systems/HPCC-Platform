package com.hpccsystems.jdbcdriver;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HPCCQuery
{
    private String                   ID;
    private String                   Name;
    private String                   WUID;
    private String                   DLL;
    private String                   Alias;
    private String                   QuerySet;

    private boolean                  Suspended;
    private List<String>             ResultDatasets;
    private List<HPCCColumnMetaData> schema;
    private String                   defaulttable;
    private List<HPCCColumnMetaData> defaultfields;

    private final static String      DEFAULTDATASETNAME = "BIOUTPUT";

    public HPCCQuery()
    {
        ID = "";
        Name = "";
        WUID = "";
        Suspended = false;
        Alias = "";
        QuerySet = "";
        ResultDatasets = new ArrayList<String>();
        schema = new ArrayList<HPCCColumnMetaData>();
        defaulttable = null;
        defaultfields = new ArrayList<HPCCColumnMetaData>();
    }

    public String getAlias()
    {
        return Alias;
    }

    public void setAlias(String alias)
    {
        Alias = alias;
    }

    public String[] getAllTableFieldsStringArray(String tablename)
    {
        String fields[] = new String[defaultfields.size()];
        int i = 0;
        Iterator<HPCCColumnMetaData> it = defaultfields.iterator();
        while (it.hasNext())
        {
            HPCCColumnMetaData field = (HPCCColumnMetaData) it.next();
            fields[i++] = field.getColumnName();
        }

        return fields;
    }

    public void addResultElement(HPCCColumnMetaData elem) throws Exception
    {
        if (defaulttable != null)
        {
            if (elem.getTableName().equalsIgnoreCase(defaulttable)
                    || elem.getParamType() == DatabaseMetaData.procedureColumnIn)
            {
                defaultfields.add(elem);
            }

            schema.add(elem);
        }
        else
            throw new Exception("Cannot add Result Elements before adding Result Dataset");

    }

    public void addResultDataset(String ds)
    {
        if (defaulttable == null || ds.equalsIgnoreCase(DEFAULTDATASETNAME))
        {
            defaulttable = ds;
            defaultfields.clear();
        }
        ResultDatasets.add(ds);
    }

    public List<String> getAllTables()
    {
        return ResultDatasets;
    }

    public List<HPCCColumnMetaData> getAllFields()
    {
        return defaultfields;
    }

    public String getID()
    {
        return ID;
    }

    public void setID(String iD)
    {
        ID = iD;
    }

    public String getName()
    {
        return Name;
    }

    public void setName(String name)
    {
        Name = name;
    }

    public String getWUID()
    {
        return WUID;
    }

    public void setWUID(String wUID)
    {
        WUID = wUID;
    }

    public String getDLL()
    {
        return DLL;
    }

    public void setDLL(String dLL)
    {
        DLL = dLL;
    }

    public boolean isSuspended()
    {
        return Suspended;
    }

    public void setSuspended(boolean suspended)
    {
        Suspended = suspended;
    }

    @Override
    public String toString()
    {
        String tmp = Name + " " + WUID;

        tmp += "tables: {";
        Iterator<String> iterator = ResultDatasets.iterator();
        while (iterator.hasNext())
        {
            tmp += " " + iterator.next();
        }

        tmp += "}";

        tmp += "elements: {";
        Iterator<HPCCColumnMetaData> iterator2 = schema.iterator();
        while (iterator2.hasNext())
        {
            tmp += " " + iterator2.next();
        }

        tmp += "}";

        return tmp;
    }

    public boolean containsField(String tablename, String fieldname)
    {
        Iterator<HPCCColumnMetaData> it = defaultfields.iterator();
        while (it.hasNext())
        {
            HPCCColumnMetaData field = (HPCCColumnMetaData) it.next();
            if (field.getTableName().equalsIgnoreCase(tablename) && field.getColumnName().equalsIgnoreCase(fieldname))
                return true;
        }

        return false;
    }

    public HPCCColumnMetaData getFieldMetaData(String fieldname)
    {
        Iterator<HPCCColumnMetaData> it = schema.iterator();
        while (it.hasNext())
        {
            HPCCColumnMetaData field = (HPCCColumnMetaData) it.next();
            if (field.getColumnName().equalsIgnoreCase(fieldname))
                return field;
        }

        return null;
    }

    public Iterator<HPCCColumnMetaData> getColumnsMetaDataIterator()
    {
        return schema.iterator();
    }

    public String getDefaultTableName()
    {
        return defaulttable;
    }

    public boolean containsField(String fieldname)
    {
        Iterator<HPCCColumnMetaData> it = defaultfields.iterator();
        while (it.hasNext())
        {
            HPCCColumnMetaData field = (HPCCColumnMetaData) it.next();
            if (field.getColumnName().equalsIgnoreCase(fieldname))
                return true;
        }

        return false;
    }

    public ArrayList<HPCCColumnMetaData> getAllNonInFields()
    {
        ArrayList<HPCCColumnMetaData> expectedretcolumns = new ArrayList();

        Iterator<HPCCColumnMetaData> it = defaultfields.iterator();
        while (it.hasNext())
        {
            HPCCColumnMetaData field = (HPCCColumnMetaData) it.next();
            if (field.getParamType() != HPCCDatabaseMetaData.procedureColumnIn)
                expectedretcolumns.add(field);
        }

        return expectedretcolumns;
    }

    public ArrayList<HPCCColumnMetaData> getAllInFields()
    {
        ArrayList<HPCCColumnMetaData> inparams = new ArrayList();

        Iterator<HPCCColumnMetaData> it = defaultfields.iterator();
        while (it.hasNext())
        {
            HPCCColumnMetaData field = (HPCCColumnMetaData) it.next();
            if (field.getParamType() == HPCCDatabaseMetaData.procedureColumnIn)
                inparams.add(field);
        }

        return inparams;
    }

    public String getQuerySet()
    {
        return QuerySet;
    }

    public void setQueryset(String qsname)
    {
        QuerySet = qsname;
    }

    public boolean isQueryNameMatch(String fullname)
    {
        String querysplit[] = fullname.split("::");
        String inname = "";
        String inqs = "";
        if (querysplit.length > 2)
        {
            for (int i = 1; i < querysplit.length; i++)
                inname += "::" + querysplit[i];
            inqs = querysplit[0];
        }
        else if (querysplit.length == 2)
        {
            inqs = querysplit[0];
            inname = querysplit[1];
        }
        else if (querysplit.length == 1)
            inname = querysplit[0];
        else
            return false;

        if (!inname.equalsIgnoreCase(this.Name) || (inqs.length() > 0 && !inqs.equalsIgnoreCase(this.QuerySet)))
            return false;

        return true;
    }
}
