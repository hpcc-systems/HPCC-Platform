package com.hpccsystems.jdbcdriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;

public class HPCCDriver implements Driver
{
	static
	{
		try
		{
			HPCCDriver driver = new HPCCDriver();
			DriverManager.registerDriver(driver);
			System.out.println("EclDriver initialized");
		}
		catch (SQLException ex)
		{
			ex.printStackTrace();
		}
	}

	public HPCCDriver()	{}

	public Connection connect(String url, Properties info) throws SQLException
	{
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
						else
							System.out.println("Connection property: " + key + " found in info properties and URL, ignoring URL value");
					}
				}
			}
		}
		catch (Exception e)
		{
			System.out.println("Issue parsing URL! \"" + url +"\"" );
		}

		String serverAddress = info.getProperty("ServerAddress");
		System.out.println("EclDriver::connect" + serverAddress);
		
		return new HPCCConnection(info);
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
		return HPCCVersionTracker.HPCCMajor;
	}

	public int getMinorVersion()
	{
		return HPCCVersionTracker.HPCCMinor;
	}

	public boolean jdbcCompliant() {
		return true;
	}

	public static void main(String[] args) {
		HPCCDriver d = new HPCCDriver();
		HPCCConnection conn;
		try
		{
			Properties info = new Properties();
			info.put("ServerAddress", "192.168.124.128");

			info.put("LazyLoad", "false");
			info.put("Cluster", "myroxie");
			info.put("QuerySet", "thor");
			info.put("WsECLWatchPort", "8010");
			info.put("EclResultLimit", "ALL");
			info.put("WsECLPort", "8002");
			info.put("WsECLDirectPort", "8008");
			info.put("username", "myhpccusername");
			info.put("password", "myhpccpass");

			//conn = (EclConnection) d.connect("url:jdbc:ecl;ServerAddress=10.239.219.10;Cluster=thor;EclResultLimit=8",info);
			conn = (HPCCConnection) d.connect("url:jdbc:ecl;",info);

			PreparedStatement p = conn.prepareStatement(
			//"select  * from thor::full_test_distributed_index where lname = BRYANT , fname = whoknows group by zips order by birth_month DESC"
			//"select  * from thor::full_test_distributed_index where lname = BRYANT AND fname = SILVA "

			//"select  x, 'firstname', 1  from tutorial::rp::tutorialperson where  firstname >= ? and firstname >= ? limit 1000"
			//"select  count(*) from tutorial::rp::tutorialperson where  firstname = 'A' or firstname = 'Z' limit 1000"
			//"select  firstname from tutorial::rp::tutorialperson where  middlename =' ' "
			//"select  * from tutorial::rp::tutorialperson where zip = ? and  middlename>='W'    city='DELRAY BEACH' limit 1000" );
			//"select * from tutorial::rp::tutorialperson where firstname = 'MARICHELLE'  order by lastname ASC, firstname DESC limit 1000"
			//"select count(*) from tutorial::rp::tutorialperson"
			//"select tutorial::rp::peoplebyzipindex.zip from \n tutorial::rp::peoplebyzipindex order by zip"
			//"select zip, count( * ) from tutorial::rp::tutorialperson  group by zip"
			//"select city, zip, count(*) from tutorial::rp::tutorialperson where zip ='33445' limit 1000"
			//"select city from tutorial::rp::tutorialperson USE INDEX(tutorial::rp::peoplebyzipindex2) where zip = ? "
			//"select count(*) from tutorial::rp::tutorialperson USE INDEX(0) where zip > ?"
			//"select count(city) as citycount from tutorial::rp::tutorialperson where zip = '33445'"//where zip = '33445'"
			//"select * from enron::final where tos = 'randy.young@enron.com' limit 1000"
			//"select count(*), zip from tutorial::rp::tutorialperson where zip = '33445' "
			//"select zip from tutorial::rp::tutorialperson where zip < '32605' group by zip"
			//"select MAX(firstname), lastname from tutorial::rp::tutorialperson  limit 1000"
			//"select 1"
			//"select count(persons.zip) as zipcount, persons.city as mycity from tutorial::rp::tutorialperson as persons where persons.city = 'ABBEVILLE' "
			//"select min(zip) as maxzip from tutorial::rp::tutorialperson as persons where persons.city = 'ABBEVILLE' "
			//"select 1 as ONE"
			//"call myroxie::fetchpeoplebyzipservice(33445)"
			//"call fetchpeoplebyzipservice(33445)"
			"call fetchpeoplebyzipservice(33445)"
			//"select MIN(zip), city from tutorial::rp::tutorialperson where zip  > '33445'"

			//"select tbl.* from progguide::exampledata::peopleaccts tbl"
			//"select firstname, lastname, middlename, city, street, state, zip from tutorial::rp::tutorialperson where firstname = VIMA LIMIT 1000"
			//"select tbl.* from progguide::exampledata::people tbl"

			//"select * from certification::full_test_distributed limit 100"
			//"select * from certification::full_test_distributed where birth_state = FL LIMIT 1000"
			//"select * from customer::customer"
			//"select count(*) from tutorial::rp::tutorialperson"
			//"select * from tutorial::rp::tutorialperson"

			//"select tbl.* from .::xdbcsample tbl"
			//"select tbl.* from fetchpeoplebyzipservice tbl where zipvalue=33445  order by fname DESC group by zip limit 10"
			//"select tbl.* from fetchpeoplebyzipservice tbl where zipvalue=33445 group by zip, fname order by fname DESC, lname, zip ASC limit 10"
			//"call fetchpeoplebyzipservice(?)"
			//"select tbl.* from bestdemo tbl "
			//"select * fname from fetchpeoplebyzipservice"

			);

			p.clearParameters();
			p.setObject(1, "'33445'");
			//p.setObject(1, "'A'");
			//p.setObject(2, "'D'");
			HPCCResultSet qrs = (HPCCResultSet)((HPCCPreparedStatement)p).executeQuery();

			ResultSetMetaData meta = qrs.getMetaData();
			System.out.println();

			for (int i = 1; i <= meta.getColumnCount(); i++)
			{
				System.out.print("[*****" + meta.getColumnName(i) + "*****]");
			}
			System.out.println("");
			for (int i = 1; i <= meta.getColumnCount(); i++)
			{
				System.out.print("[^^^^^" + meta.getColumnLabel(i) + "^^^^^]");
			}
			System.out.println();
			for (int i = 1; i <= meta.getColumnCount(); i++)
			{
				System.out.print("[+++++" + HPCCDatabaseMetaData.convertSQLtype2JavaClassName(meta.getColumnType(i)) + "+++++]");
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
			//}

			ResultSet procs = conn.getMetaData().getProcedures(null, null, null);

			System.out.println("procs found: ");
			while (procs.next())
			{
				System.out.println("   " + procs.getString("PROCEDURE_NAME"));
				ResultSet proccols = conn.getMetaData().getProcedureColumns(null,null,procs.getString("PROCEDURE_NAME"),"%");
				while (proccols.next())
				{
					System.out.println(proccols.getString("PROCEDURE_NAME")+"."+ proccols.getString("COLUMN_NAME"));
				}
			}

			/*
			ResultSet proccols = conn.getMetaData().getProcedureColumns(null, null, null, null);

			System.out.println("procs cols found: ");
			while (proccols.next()) {
				System.out.println("   " + proccols.getString("COLUMN_NAME"));
			}
			*/
			//getProcedureColumns catalog: null, schemaPattern: null, procedureNamePattern: fetchpeoplebyzipservice columnanmepat: null
		}
		catch (SQLException e)
		{
			System.err.println(e.getMessage());
		}
	}
}
