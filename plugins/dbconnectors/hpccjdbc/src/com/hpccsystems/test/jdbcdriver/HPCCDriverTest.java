package com.hpccsystems.test.jdbcdriver;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.hpccsystems.jdbcdriver.HPCCConnection;
import com.hpccsystems.jdbcdriver.HPCCDatabaseMetaData;
import com.hpccsystems.jdbcdriver.HPCCDriver;
import com.hpccsystems.jdbcdriver.HPCCPreparedStatement;
import com.hpccsystems.jdbcdriver.HPCCResultSet;

public class HPCCDriverTest
{
	static private HPCCDriver driver;

	static
	{
		driver	= new HPCCDriver();
	}

	private static boolean  testLazyLoading(Properties conninfo)
	{
		boolean success = true;
		try
		{
			conninfo.put("LazyLoad", "true");
			HPCCConnection connection = (HPCCConnection) driver.connect("",conninfo);

			System.out.println("No query nor file loading should have occured yet.");

			ResultSet procs = connection.getMetaData().getProcedures(null, null, null);

			System.out.println("Queries should be cached now. No files should be cached yet.");

			System.out.println("procs found: ");
			while (procs.next())
			{
				System.out.println("   " + procs.getString("PROCEDURE_NAME"));
			}

			ResultSet tables = connection.getMetaData().getTables(null, null, "%",new String[] {""} );

			System.out.println("Tables found: ");
			while (tables.next())
			{
				System.out.println("   " + tables.getString("TABLE_NAME") + " Remarks: \'" + tables.getString("REMARKS")+"\'");
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			success = false;
		}
		return success;
	}

	private static HPCCConnection  connectViaProps(Properties conninfo)
	{
		HPCCConnection connection = null;
		try
		{
			connection = (HPCCConnection) driver.connect("",conninfo);
		}
		catch (Exception e)	{}
		return connection;
	}

	private static HPCCConnection  connectViaUrl(String conninfo)
	{
		HPCCConnection connection = null;
		try
		{
			connection = (HPCCConnection) driver.connect(conninfo, null);
		}
		catch (Exception e)	{}
		return connection;
	}

	private static boolean printouttables(HPCCConnection connection)
	{
		boolean success = true;
		try
		{
			ResultSet tables = connection.getMetaData().getTables(null, null, "%", null);

			System.out.println("Tables found: ");
			while (tables.next())
				System.out.println("\t" + tables.getString("TABLE_NAME"));
		}
		catch (Exception e)
		{
			success = false;
		}
		return success;
	}

	private static boolean printouttablecols(HPCCConnection connection)
	{
		boolean success = true;
		try
		{
			ResultSet tablecols = connection.getMetaData().getColumns(null, null, "%", "%");

			System.out.println("Table cols found: ");
			while (tablecols.next())
				System.out.println("\t" +
			tablecols.getString("TABLE_NAME") +
			"::" +
			tablecols.getString("COLUMN_NAME") +
			"( " + tablecols.getString("TYPE_NAME") + " )");
		}
		catch (Exception e)
		{
			success = false;
		}
		return success;
	}

	private static boolean printoutprocs(HPCCConnection connection)
	{
		boolean success = true;
		try
		{
			ResultSet procs = connection.getMetaData().getProcedures(null, null, null);

			System.out.println("procs found: ");
			while (procs.next())
				System.out.println("\t" + procs.getString("PROCEDURE_NAME"));

		}
		catch (Exception e)
		{
			success = false;
		}
		return success;
	}

	private static boolean  printoutproccols(HPCCConnection connection)
	{
		boolean success = true;
		try
		{
			ResultSet proccols = connection.getMetaData().getProcedureColumns(null, null, null, null);

			System.out.println("procs cols found: ");
			while (proccols.next())
				System.out.println("\t" + proccols.getString("PROCEDURE_NAME") + proccols.getString("PROCEDURE_NAME") + "::" +proccols.getString("COLUMN_NAME") + " (" + proccols.getInt("COLUMN_TYPE") + ")");

		}
		catch (Exception e)
		{
			success = false;
		}
		return success;
	}

	private static boolean  executeFreeHandSQL(Properties conninfo, String SQL, List<String> params)
	{
		boolean success = true;
		try
		{
			HPCCConnection connectionprops = connectViaProps(conninfo);
			if (connectionprops == null)
				throw new Exception("Could not connect with properties object");

			PreparedStatement p = connectionprops.prepareStatement(SQL);
			p.clearParameters();

			for(int i = 0; i< params.size(); i++)
			{
				p.setObject(i+1, params.get(i));
			}

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
		}
		catch (Exception e)
		{
			System.err.println(e.getMessage());
			success = false;
		}
		return success;
	}

	private static boolean getDatabaseInfo(HPCCConnection conn)
	{
		boolean success = true;
		try
		{
			HPCCDatabaseMetaData meta =  (HPCCDatabaseMetaData)conn.getMetaData();
			String hpccname = meta.getDatabaseProductName();
			String hpccprodver = meta.getDatabaseProductVersion();
			int major = meta.getDatabaseMajorVersion();
			int minor = meta.getDatabaseMinorVersion();
			String sqlkeywords = meta.getSQLKeywords();

			System.out.println("HPCC System Info:");
			System.out.println("\tProduct Name: " + hpccname);
			System.out.println("\tProduct Version: " + hpccprodver);
			System.out.println("\tProduct Major: " + major);
			System.out.println("\tProduct Minor: " + minor);
			System.out.println("\tDriver Name: " + meta.getDriverName());
			System.out.println("\tDriver Major: " + meta.getDriverMajorVersion());
			System.out.println("\tDriver Minor: " + meta.getDriverMinorVersion());
			System.out.println("\tSQL Key Words: " + sqlkeywords);
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			success = false;
		}
		return success;
	}

	private static boolean runFullTest(Properties propsinfo, String urlinfo)
	{
		boolean success = true;
		try
		{
			//success &= testLazyLoading(propsinfo);

			HPCCConnection connectionprops = connectViaProps(propsinfo);
			if (connectionprops == null)
				throw new Exception("Could not connect with properties object");

			success &= getDatabaseInfo(connectionprops);

			HPCCConnection connectionurl = connectViaUrl(urlinfo);
			if (connectionurl == null)
				throw new Exception("Could not connect with URL");

			success &= getDatabaseInfo(connectionurl);

			PreparedStatement p = connectionprops.prepareStatement(
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

//					p.clearParameters();
//					//p.setObject(1, "'33445'");
//					p.setObject(1, "'ROBERT'");
//					p.setObject(2, "'LO'");
//					qrs = (EclResultSet)((EclPreparedStatement)p).executeQuery();
//					for (int i = 1; i <= meta.getColumnCount(); i++)
//					{
//						System.out.print("[*****" + meta.getColumnName(i) + "******]");
//					}
//
//					while (qrs.next())
//					{
//						System.out.println();
//						for (int i = 1; i <= meta.getColumnCount(); i++)
//						{
//							System.out.print("[ " + qrs.getObject(i) + " ]");
//						}
//					}
//					System.out.println("\nTotal Records found: " + qrs.getRowCount());

					success &= printouttables(connectionprops);
					success &= printouttablecols(connectionprops);
					success &= printoutprocs(connectionprops);
					success &= printoutproccols(connectionprops);

		}
		catch (Exception e)
		{
			success = false;
		}
		return success;
	}

	public static void main(String[] args)
	{
		boolean success = true;

		try
		{
			Properties info = new Properties();
			List<String> params = new ArrayList<String>();
			String infourl = "";

			if (args.length == 0)
			{
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

				infourl = "url:jdbc:ecl;ServerAddress=192.168.124.128;Cluster=myroxie;EclResultLimit=8";

				//success &= runFullTest(info, infourl);

				params.add("'33445'");
				params.add("'90210'");

				success &= executeFreeHandSQL(info, "select count(persons.zip) as zipcount, persons.city as mycity , zip from super::super::tutorial::rp::tutorialperson as persons where persons.zip > ? AND persons.zip < ? group by zip limit 100", params);
			}
			else
			{
				System.out.println("********************************************************************");
				System.out.println("HPCC JDBC Test Package Usage:");
				System.out.println(" Connection Parameters: paramname==paramvalue");
				System.out.println(" eg. ServerAddress==192.168.124.128");
				System.out.println(" Prepared Statement param value: \"param\"==paramvalue");
				System.out.println(" eg. param==\'33445\'");
				System.out.println("");
				System.out.println(" By default full test is executed.");
				System.out.println(" To execute free hand sql:");
				System.out.println("  freehandsql==<SQL STATEMENT>");
				System.out.println("  eg. freehandsql==\"select * from tablename where zip=? limit 100\"");
				System.out.println("********************************************************************\n");

				String freehand = null;
				for (int i = 0; i < args.length; i++)
				{
					String [] propsplit = args[i].split("==");
					if( propsplit.length == 1)
					{
						info.put(propsplit[0], "true");
						System.out.println("added prop: " + propsplit[0] + " = true");
					}
					else if ( propsplit.length == 2)
					{
						if (propsplit[0].equalsIgnoreCase("param"))
						{
							params.add(propsplit[1]);
							System.out.println("added param( " + (params.size()) + " ) = " + propsplit[1]);
						}
						else
						{
							info.put(propsplit[0], propsplit[1]);
							System.out.println("added prop: " + propsplit[0] + " = " + propsplit[1]);
						}
					}
					else
						System.out.println("arg["+i+"] ignored");
				}

				if (info.containsKey("freehandsql"))
				{
					freehand = info.getProperty("freehandsql");
					success &= executeFreeHandSQL(info, freehand, params);
				}
				else
					success &= runFullTest(info, infourl);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			success = false;
		}

		System.out.println("\nHPCC Driver test " + (success ? "passed! " : "failed!"));
	}
}
