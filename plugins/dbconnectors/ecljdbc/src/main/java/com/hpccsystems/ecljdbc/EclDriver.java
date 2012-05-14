package com.hpccsystems.ecljdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;

public class EclDriver implements Driver
{
	static
	{
		try
		{
			EclDriver driver = new EclDriver();
			DriverManager.registerDriver(driver);
			System.out.println("EclDriver initialized");
		}
		catch (SQLException ex)
		{
			ex.printStackTrace();
		}
	}

	public EclDriver()
	{
	}


	public Connection connect(String url, Properties info) throws SQLException
	{
		String serverAddress = info.getProperty("ServerAddress");
		String cluster = info.getProperty("Cluster");

		try
		{
			StringTokenizer urltokens = new StringTokenizer(url,";");
			while (urltokens.hasMoreTokens())
			{
				String token = urltokens.nextToken();
				if (token.contains("="))
				{
					StringTokenizer keyvalues = new StringTokenizer(token, "=");
					while (keyvalues.hasMoreTokens())
					{
						String key = keyvalues.nextToken();
						String value = keyvalues.nextToken();
						if (!info.containsKey(key))
							info.put(key, value);
					}
				}
			}
		}
		catch (Exception e)
		{
			System.out.println("Issue parsing URL! \"" + url +"\"" );
		}


		System.out.println("EclDriver::connect" + serverAddress + ":" + cluster);
		return new EclConnection(info);
	}


	public boolean acceptsURL(String url) throws SQLException {
		return true;
	}


	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
	{
		DriverPropertyInfo[] infoArray = new DriverPropertyInfo[1];
		infoArray[0] = new DriverPropertyInfo("ip", "IP Address");
		return infoArray;
	}


	public int getMajorVersion()
	{
		return EclVersionTracker.HPCCMajor;
	}


	public int getMinorVersion() {
		return EclVersionTracker.HPCCMinor;
	}


	public boolean jdbcCompliant() {
		return true;
	}

	public static void main(String[] args) {
		EclDriver d = new EclDriver();
		EclConnection conn;
		try
		{
			Properties info = new Properties();
			info.put("ServerAddress", "192.168.124.128"); //Mine
			//info.put("ServerAddress", "10.239.20.80"); //clo
			//info.put("ServerAddress", "10.239.219.10"); //fishbeck
			//info.put("ServerAddress", "172.25.237.145"); //arjuna

			info.put("Cluster", "thor"); //analagous to DB server instance
			info.put("DefaultQuery", "fetchpeoplebyzipservice"); //analagous to default DB name
			info.put("WsECLWatchPort", "8010");
			//info.put("EclLimit", "100");
			info.put("WsECLPort", "8002");
			info.put("WsECLDirectPort", "8008");
			info.put("username", "_rpastrana");
			info.put("password", "ch@ng3m3");

			//conn = (EclConnection) d.connect("url:jdbc:ecl;ServerAddress=192.168.124.128;Cluster=myroxie",info);
			//conn = (EclConnection) d.connect("url:jdbc:ecl;ServerAddress=10.239.20.80;Cluster=thor;EclLimit=8",info);
			conn = (EclConnection) d.connect("url:jdbc:ecl;ServerAddress=10.239.219.10;Cluster=thor;EclLimit=8",info);


			//PreparedStatement p = conn.prepareStatement("SELECT 2");
			//PreparedStatement p = conn.prepareStatement("select  * from thor::full_test_distributed_index where lname = BRYANT , fname = whoknows group by zips order by birth_month DESC");
			//PreparedStatement p = conn.prepareStatement("select  * from thor::full_test_distributed_index where lname = BRYANT AND fname = SILVA ");

			//PreparedStatement p = conn.prepareStatement("select  x, 'firstname', 1  from tutorial::rp::tutorialperson where  firstname >= ? and firstname >= ? limit 1000");
			//PreparedStatement p = conn.prepareStatement("select  count(*) from tutorial::rp::tutorialperson where  firstname = 'A' or firstname = 'Z' limit 1000");
			//PreparedStatement p = conn.prepareStatement("select  firstname from tutorial::rp::tutorialperson where  middlename =' ' ");
			//PreparedStatement p = conn.prepareStatement("select  * from tutorial::rp::tutorialperson where zip = ? and  middlename>='W'    city='DELRAY BEACH' limit 1000" );
			//PreparedStatement p = conn.prepareStatement("select * from tutorial::rp::tutorialperson where firstname = 'MARICHELLE'  order by lastname ASC, firstname DESC limit 1000");
			//PreparedStatement p = conn.prepareStatement("select count(*) from tutorial::rp::tutorialperson");
			//PreparedStatement p = conn.prepareStatement("select tutorial::rp::peoplebyzipindex.zip from \n tutorial::rp::peoplebyzipindex order by zip");
			//PreparedStatement p = conn.prepareStatement("select zip, count( * ) from tutorial::rp::tutorialperson  group by zip");
			//PreparedStatement p = conn.prepareStatement("select city, zip, count(*) from tutorial::rp::tutorialperson where zip ='33445' limit 1000");
			//PreparedStatement p = conn.prepareStatement("select city from tutorial::rp::tutorialperson USE INDEX(tutorial::rp::peoplebyzipindex2) where zip = ? ");
			//PreparedStatement p = conn.prepareStatement("select count(*) from tutorial::rp::tutorialperson USE INDEX(0) where zip > ?");
			//PreparedStatement p = conn.prepareStatement("select count(city)  from tutorial::rp::tutorialperson where zip = '33445'");//where zip = '33445'");
			//PreparedStatement p = conn.prepareStatement("select * from enron::final where tos = 'randy.young@enron.com' limit 1000");
			PreparedStatement p = conn.prepareStatement("select count(*), zip from tutorial::rp::tutorialperson ");



			//PreparedStatement p = conn.prepareStatement("select tbl.* from progguide::exampledata::peopleaccts tbl");
			//PreparedStatement p = conn.prepareStatement("select firstname, lastname, middlename, city, street, state, zip from tutorial::rp::tutorialperson where firstname = VIMA LIMIT 1000");
		//PreparedStatement p = conn.prepareStatement("select tbl.* from progguide::exampledata::people tbl");

			//PreparedStatement p = conn.prepareStatement("select * from certification::full_test_distributed limit 100");
			//PreparedStatement p = conn.prepareStatement("select * from certification::full_test_distributed where birth_state = FL LIMIT 1000");
			//PreparedStatement p = conn.prepareStatement("select * from customer::customer");
			//PreparedStatement p = conn.prepareStatement("select count(*) from tutorial::rp::tutorialperson");
			//PreparedStatement p = conn.prepareStatement("select * from tutorial::rp::tutorialperson");


			//PreparedStatement p = conn.prepareStatement("select tbl.* from .::xdbcsample tbl");

			//PreparedStatement p = conn.prepareStatement("select tbl.* from fetchpeoplebyzipservice tbl where zipvalue=33445  order by fname DESC group by zip limit 10");
			//PreparedStatement p = conn.prepareStatement("select tbl.* from fetchpeoplebyzipservice tbl where zipvalue=33445 group by zip, fname order by fname DESC, lname, zip ASC limit 10");
			//PreparedStatement p = conn.prepareStatement("call fetchpeoplebyzipservice(?)");
			//PreparedStatement p = conn.prepareStatement("select tbl.* from bestdemo tbl ");
			//PreparedStatement p = conn.prepareStatement("select * fname from fetchpeoplebyzipservice");


			//PreparedStatement p = conn.prepareStatement("select * from 'Result 1' where zipvalue=33445");

			//PreparedStatement p = conn.prepareStatement("select * from Timeline_Total_Property_Sales");
			//PreparedStatement p = conn.prepareStatement("select * from motiondemo");

			p.clearParameters();
			p.setObject(1, "'33445'");
			//p.setObject(1, "'A'");
			//p.setObject(2, "'D'");
			EclResultSet qrs = (EclResultSet)((EclPreparedStatement)p).executeQuery();

			ResultSetMetaData meta = qrs.getMetaData();
			System.out.println();

			for (int i = 1; i <= meta.getColumnCount(); i++)
			{
				System.out.print("[*****" + meta.getColumnName(i) + "******]");
			}
			System.out.println();
			for (int i = 1; i <= meta.getColumnCount(); i++)
			{
				System.out.print("[*****" + EclDatabaseMetaData.convertSQLtype2JavaClassName(meta.getColumnType(i)) + "******]");
			}

			while (qrs.next())
			{
				System.out.println();
				for (int i = 1; i <= meta.getColumnCount(); i++)
				{
					System.out.print("[ " + qrs.getObject(i) + " ]");
				}
			}

			System.out.println("\nTotal Records found: " + qrs.getRowCount());

//			p.clearParameters();
//			//p.setObject(1, "'33445'");
//			p.setObject(1, "'ROBERT'");
//			p.setObject(2, "'LO'");
//			qrs = (EclResultSet)((EclPreparedStatement)p).executeQuery();
//
//
//			for (int i = 1; i <= meta.getColumnCount(); i++)
//			{
//				System.out.print("[*****" + meta.getColumnName(i) + "******]");
//			}
//
//			while (qrs.next())
//			{
//				System.out.println();
//				for (int i = 1; i <= meta.getColumnCount(); i++)
//				{
//					System.out.print("[ " + qrs.getObject(i) + " ]");
//				}
//			}
//
//			System.out.println("\nTotal Records found: " + qrs.getRowCount());

			//String tabname = meta.getTableName(1);
			//int count = meta.getColumnCount();
			//String label = meta.getColumnLabel(2);
			//String typename = meta.getColumnTypeName(1);

			//System.out.println("Table name: " + tabname + "tablecolumncount: " + count);
			/* while (qrs.next())
			 {
				 String fnvalue = qrs.getString("firstname");
				 String lnvalue = qrs.getString("lastname");
				 //String fnvalue = qrs.getString(1);
				 //String lnvalue = qrs.getString(2);
				 System.out.println("name: " + fnvalue + " " + lnvalue);
			 }*/

			//String hpccname = conn.getMetaData().getDatabaseProductVersion();
			//int major = conn.getMetaData().getDatabaseMajorVersion();
			//int minor = conn.getMetaData().getDatabaseMinorVersion();

			//System.out.println("Connected to : " + hpccname +" version:" + major +"."+ minor);
			//ECLDATABASEMETADATA GETCOLUMNS catalog: myroxie, schemaPattern: null, tableNameP
			//attern: tutorial::rp::tutorialperson, columnNamePattern: null
			// ResultSet columns = conn.getMetaData().getColumns("myroxie", null,"tutorial::rp::tutorialperson", null);

			/*ResultSet typeinfo = conn.getMetaData().getTypeInfo();
			 while (typeinfo.next())
			 {
				 System.out.print("col 1: " + typeinfo.getObject(1) + "\t");
				 System.out.print("col 2: " + typeinfo.getObject(2) + "\t");
				 System.out.print("col 3: " + typeinfo.getObject(3) + "\t");
				 System.out.print("col 4: " + typeinfo.getObject(4) + "\t");
				 System.out.print("col 5: " + typeinfo.getObject(5) + "\t");
				 System.out.print("col 6: " + typeinfo.getObject(6) + "\t");
				 System.out.print("col 7: " + typeinfo.getObject(7) + "\t");
				 System.out.print("col 8: " + typeinfo.getObject(8) + "\t");

					System.out.println("   " + typeinfo.getString("TYPE_NAME")+"("+typeinfo.getString("LOCAL_TYPE_NAME") + ")");
			}*/

			/*ResultSet columns = conn.getMetaData().getColumns(null, "",	"", "%");

			 while (columns.next())
			 {
				 columns.getMetaData().getColumnType(1);
				 columns.getMetaData().getColumnTypeName(1);
				 System.out.print("col 1: " + columns.getObject(1) + "\t");
				 System.out.print("col 2: " + columns.getObject(2) + "\t");
				 System.out.print("col 3: " + columns.getObject(3) + "\t");
				 System.out.print("col 4: " + columns.getObject(4) + "\t");
				 System.out.print("col 5: " + columns.getObject(5) + "\t");
				 System.out.print("col 6: " + columns.getObject(6) + "\t");
				 System.out.print("col 7: " + columns.getObject(7) + "\t");
				 System.out.print("col 8: " + columns.getObject(8) + "\t");

					System.out.println("   " + columns.getString("TABLE_NAME")+"."+columns.getString("COLUMN_NAME")	+ "(" + columns.getString("TYPE_CAT") +")");
			}
			*/

			//ECLDATABASEMETADATA GETTABLES catalog: myroxie, schemaPattern: null, tableNamePattern: %
			//ResultSet tables = conn.getMetaData().getTables(null,null,null,	new String[] {""});
			//ResultSet tables = conn.getMetaData().getTables("myroxie", null, "%",new String[] {""} );

			//System.out.println("Tables found: ");
			//while (tables.next()) {
			//	System.out.println("   " + tables.getString("TABLE_NAME") + " Remarks: \'" + tables.getString("REMARKS")+"\'");
		//	}


			/*
			ResultSet procs = conn.getMetaData().getProcedures(null, null, null);

			System.out.println("procs found: ");
			while (procs.next()) {
				System.out.println("   " + procs.getString("PROCEDURE_NAME"));
			}
			*/
			/*
			ResultSet proccols = conn.getMetaData().getProcedureColumns(null, null, null, null);

			System.out.println("procs cols found: ");
			while (proccols.next()) {
				System.out.println("   " + proccols.getString("COLUMN_NAME"));
			}
			*/
			//getProcedureColumns catalog: null, schemaPattern: null, procedureNamePattern: fetchpeoplebyzipservice columnanmepat: null

		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
