package com.hpccsystems.ecljdbc;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EclQuery
{
	private String ID;
	private String Name;
	private String WUID;
	private String DLL;
	private String Alias;

	private boolean Suspended;
	private List<String> ResultDatasets;
	private List<EclColumnMetaData> schema;
	private String defaulttable;
	private List<EclColumnMetaData> defaultfields;

	private final static String DEFAULTDATASETNAME = "BIOUTPUT";

	public EclQuery()
	{
		ID = "";
		Name = "";
		WUID = "";
		Suspended = false;
		Alias = "";
		ResultDatasets = new ArrayList<String>();
		schema = new ArrayList<EclColumnMetaData>();
		defaulttable = null;
		defaultfields = new ArrayList<EclColumnMetaData>();;
	}

	public String getAlias() {
		return Alias;
	}

	public void setAlias(String alias) {
		Alias = alias;
	}

	public String [] getAllTableFieldsStringArray(String tablename)
	{
		//ArrayList<String> fields = new ArrayList<String>();

		//Iterator<EclColumnMetaData> it = schema.iterator();
		String fields [] = new String [defaultfields.size()];
		int i = 0;
		Iterator<EclColumnMetaData> it = defaultfields.iterator();
		while (it.hasNext())
		{
			EclColumnMetaData field = (EclColumnMetaData)it.next();
			//if (field.getTableName().equals(tablename))
				//fields.add(field.getColumnName());
				fields[i++] = field.getColumnName();
		}


	    /*String f [] = new String [fields.size()];
	    for (String string : fields)
	    {
	    	f[i++] = string;
		}*/

		return fields;
	}

	public void addResultElement(EclColumnMetaData elem) throws Exception
	{
		if (defaulttable != null)
		{
			if (elem.getTableName().equalsIgnoreCase(defaulttable) || elem.getParamType() == DatabaseMetaData.procedureColumnIn)
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

	/*Doesn't make since if we're mapping one ECL query to one DB table*/
	public List<String> getAllTables()
	{
		return ResultDatasets;
	}


	public List<EclColumnMetaData> getAllFields()
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

	public String getName() {
		return Name;
	}

	public void setName(String name) {
		Name = name;
	}

	public String getWUID() {
		return WUID;
	}

	public void setWUID(String wUID) {
		WUID = wUID;
	}

	public String getDLL() {
		return DLL;
	}

	public void setDLL(String dLL) {
		DLL = dLL;
	}

	public boolean isSuspended() {
		return Suspended;
	}

	public void setSuspended(boolean suspended) {
		Suspended = suspended;
	}

	@Override
	public String toString()
	{
		String tmp = Name + " " + WUID;

		tmp += "tables: {";
		Iterator<String> iterator = ResultDatasets.iterator();
		while (iterator.hasNext()) {
			tmp += " " + iterator.next();
		}

		tmp += "}";

		tmp += "elements: {";
		Iterator<EclColumnMetaData> iterator2 = schema.iterator();
		while (iterator2.hasNext()) {
			tmp += " " + iterator2.next();
		}

		tmp += "}";

		return tmp;
	}

	public boolean containsField(String tablename, String fieldname)
	{
		//Iterator<EclColumnMetaData> it = schema.iterator();
		Iterator<EclColumnMetaData> it = defaultfields.iterator();
		while (it.hasNext())
		{
			EclColumnMetaData field = (EclColumnMetaData)it.next();
			if (field.getTableName().equalsIgnoreCase(tablename) && field.getColumnName().equalsIgnoreCase(fieldname))
				return true;
		}

		return false;
	}

	public EclColumnMetaData getFieldMetaData(String fieldname)
	{
		Iterator<EclColumnMetaData> it = schema.iterator();
		while (it.hasNext())
		{
			EclColumnMetaData field = (EclColumnMetaData)it.next();
			if (field.getColumnName().equalsIgnoreCase(fieldname))
				return field;
		}

		return null;
	}

	public Iterator<EclColumnMetaData> getColumnsMetaDataIterator()
	{
		return schema.iterator();
	}

	public String getDefaultTableName()
	{
		return defaulttable;
	}

	public boolean containsField(String fieldname) {
		Iterator<EclColumnMetaData> it = defaultfields.iterator();
		while (it.hasNext())
		{
			EclColumnMetaData field = (EclColumnMetaData)it.next();
			if (field.getColumnName().equalsIgnoreCase(fieldname))
				return true;
		}

		return false;
	}

	public ArrayList<EclColumnMetaData> getAllNonInFields()
	{
		ArrayList<EclColumnMetaData> expectedretcolumns = new ArrayList();

		Iterator<EclColumnMetaData> it = defaultfields.iterator();
		while (it.hasNext())
		{
			EclColumnMetaData field = (EclColumnMetaData)it.next();
			if (field.getParamType() != EclDatabaseMetaData.procedureColumnIn)
				expectedretcolumns.add(field);
		}

		return expectedretcolumns;
	}

	public ArrayList<EclColumnMetaData> getAllInFields()
	{
		ArrayList<EclColumnMetaData> inparams = new ArrayList();

		Iterator<EclColumnMetaData> it = defaultfields.iterator();
		while (it.hasNext())
		{
			EclColumnMetaData field = (EclColumnMetaData)it.next();
			if (field.getParamType() == EclDatabaseMetaData.procedureColumnIn)
				inparams.add(field);
		}

		return inparams;
	}
}
