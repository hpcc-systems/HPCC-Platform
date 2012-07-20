package com.hpccsystems.test.jdbcdriver;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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

	@SuppressWarnings("unused")
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

	private static boolean  createStandAloneDataMetadata(Properties conninfo)
	{
		boolean success = true;
		try
		{
			HPCCDatabaseMetaData dbmetadata = new HPCCDatabaseMetaData(conninfo);
			success = getDatabaseInfo(dbmetadata);
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

	@SuppressWarnings("unused")
	private static boolean printouttable(HPCCConnection connection, String tablename)
	{
		boolean success = true;
		try
		{
			ResultSet table = connection.getMetaData().getTables(null, null, tablename, null);

			while (table.next())
				System.out.println("\t" + table.getString("TABLE_NAME"));
		}
		catch (Exception e)
		{
			success = false;
		}
		return success;
	}

	@SuppressWarnings("unused")
	private static boolean printoutExportedKeys(HPCCConnection connection)
	{
		boolean success = true;
		try
		{
			ResultSet keys = connection.getMetaData().getExportedKeys(null,null, null);

			//while (table.next())
				//System.out.println("\t" + table.getString("TABLE_NAME"));
		}
		catch (Exception e)
		{
			success = false;
		}
		return success;
	}

	@SuppressWarnings("unused")
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

	@SuppressWarnings("unused")
	private static boolean printouttablecols(HPCCConnection connection, String tablename)
	{
		boolean success = true;
		try
		{
			ResultSet tablecols = connection.getMetaData().getColumns(null, null, tablename, "%");

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

	private static boolean printoutalltablescols(HPCCConnection connection)
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

	@SuppressWarnings("unused")
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

	private static boolean  testSelect1(HPCCConnection connection)
	{
		boolean success = true;
		try
		{
			PreparedStatement p = connection.prepareStatement("Select 1 AS ONE");

			HPCCResultSet qrs = (HPCCResultSet)((HPCCPreparedStatement)p).executeQuery();

			System.out.println("---------Testing Select 1---------------");

			while (qrs.next())
			{
				if(qrs.getInt(1) != 1)
					success = false;
			}

			System.out.println("\tTest Success: " + success);
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
		try
		{
			return getDatabaseInfo((HPCCDatabaseMetaData)conn.getMetaData());
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			return false;
		}
	}

	private static boolean getDatabaseInfo(HPCCDatabaseMetaData dbmetadata)
	{
		boolean success = true;
		try
		{
			String hpccname = dbmetadata.getDatabaseProductName();
			String hpccprodver = dbmetadata.getDatabaseProductVersion();
			int major = dbmetadata.getDatabaseMajorVersion();
			int minor = dbmetadata.getDatabaseMinorVersion();
			String sqlkeywords = dbmetadata.getSQLKeywords();

			System.out.println("HPCC System Info:");
			System.out.println("\tProduct Name: " + hpccname);
			System.out.println("\tProduct Version: " + hpccprodver);
			System.out.println("\tProduct Major: " + major);
			System.out.println("\tProduct Minor: " + minor);
			System.out.println("\tDriver Name: " + dbmetadata.getDriverName());
			System.out.println("\tDriver Major: " + dbmetadata.getDriverMajorVersion());
			System.out.println("\tDriver Minor: " + dbmetadata.getDriverMinorVersion());
			System.out.println("\tSQL Key Words: " + sqlkeywords);
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			success = false;
		}
		return success;
	}

	public static synchronized boolean printOutResultSet(HPCCResultSet resultset, long threadid )
	{
		System.out.println("Servicing thread id: " + threadid);

		int padvalue = 20;
		boolean isSuccess = true;
		try
		{
			ResultSetMetaData meta = resultset.getMetaData();

			System.out.println();

			for (int i = 1; i <= meta.getColumnCount(); i++)
			{
				String colname = meta.getColumnName(i);
				System.out.print("[");

				for (int y = 0; y < (colname.length()>=padvalue ? 0 : (padvalue - colname.length())/2); y++)
					System.out.print(" ");
				System.out.print(colname);

				for (int y = 0; y < (colname.length()>=padvalue ? 0 : (padvalue - colname.length())/2); y++)
					System.out.print(" ");

				System.out.print("]");
			}
			System.out.println("");

			for (int i = 1; i <= meta.getColumnCount(); i++)
			{
				String collabel = meta.getColumnLabel(i);
				System.out.print("[");

				for (int y = 0; y < (collabel.length()>=padvalue ? 0 : (padvalue - collabel.length())/2); y++)
					System.out.print("^");
				System.out.print(collabel);

				for (int y = 0; y < (collabel.length()>=padvalue ? 0 : (padvalue - collabel.length())/2); y++)
					System.out.print("^");

				System.out.print("]");
			}
			System.out.println();

			for (int i = 1; i <= meta.getColumnCount(); i++)
			{
				String coltype = HPCCDatabaseMetaData.convertSQLtype2JavaClassName(meta.getColumnType(i));
				System.out.print("[");

				for (int y = 0; y < (coltype.length()>=padvalue ? 0 : (padvalue - coltype.length())/2); y++)
					System.out.print(" ");
				System.out.print(coltype);

				for (int y = 0; y < (coltype.length()>=padvalue ? 0 : (padvalue - coltype.length())/2); y++)
					System.out.print(" ");

				System.out.print("]");
			}

			while (resultset.next())
			{
				System.out.println();
				for (int i = 1; i <= meta.getColumnCount(); i++)
				{
					String result = (String)resultset.getObject(i);
					System.out.print("[");

					for (int y = 0; y < (result.length()>=padvalue ? 0 : padvalue - result.length()); y++)
						System.out.print(" ");
					System.out.print(result);
					System.out.print("]");
				}
			}

			System.out.println("\nTotal Records found: " + resultset.getRowCount());
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			isSuccess = false;
		}
		return isSuccess;
	}

	private static boolean runFullTest(Properties propsinfo, String urlinfo)
	{
		List<HPCCDriverTestThread> runnables = new ArrayList<HPCCDriverTestThread>();

		boolean success = true;
		try
		{
			//success &= testLazyLoading(propsinfo);

			success &= createStandAloneDataMetadata(propsinfo);

			HPCCConnection connectionprops = connectViaProps(propsinfo);
			if (connectionprops == null)
				throw new Exception("Could not connect with properties object");

			success &= getDatabaseInfo(connectionprops);

			HPCCConnection connectionurl = connectViaUrl(urlinfo);
			if (connectionurl == null)
				throw new Exception("Could not connect with URL");

			success &= getDatabaseInfo(connectionurl);

			//PreparedStatement p = connectionprops.prepareStatement(
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
					//"call fetchpeoplebyzipservice(33445)"
					//"select * from .::doughenschen__infinity_rollup_best1"
					//"select * from regress::hthor::dg_memfile where u2 = ? limit 100"
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

					//);

					Properties params = new Properties();
					params.put("1", "'BAYLISS'");
					HPCCDriverTestThread workThread1 = new HPCCDriverTestThread(connectionprops, "select * from regress::hthor_payload::dg_flat where dg_lastname = ? limit 100", params);
					runnables.add(workThread1);

					Properties params2 = new Properties();
					params2.put("1", "65535");
					HPCCDriverTestThread workThread2 = new HPCCDriverTestThread(connectionprops, "select * from regress::hthor::dg_memfile where u2 = ? limit 100", params2);
					runnables.add(workThread2);

					Properties params3 = new Properties();
					params3.put("1", "65535");
					HPCCDriverTestThread workThread3 = new HPCCDriverTestThread(connectionprops, "select * from regress::hthor::dg_memfile where u2 < ? and u2 != 65536 limit 10000", params3);
					runnables.add(workThread3);

					for (HPCCDriverTestThread thrd : runnables)
					{
						thrd.start();
					}

					boolean threadsrunning;
					do
					{
						threadsrunning = false;
						for (HPCCDriverTestThread thrd : runnables)
						{
							threadsrunning = thrd.isRunning() || threadsrunning;
						}
						Thread.sleep(250);
					}
					while (threadsrunning);

					for (HPCCDriverTestThread thrd : runnables)
					{
						success &= thrd.isSuccess();
					}

					success &= testSelect1(connectionprops);
					//success &= printoutExportedKeys(connectionprops);
					//success &= printouttable(connectionprops, ".::doughenschen__infinity_rollup_best1");
					//success &= printouttables(connectionprops);
					success &= printoutalltablescols(connectionprops);
					//success &= printouttablecols(connectionprops,".::doughenschen__infinity_rollup_best1");
					//success &= printoutprocs(connectionprops);
					//success &= printoutproccols(connectionprops);

		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
			success = false;
		}
		return success;
	}

	public static void main(String[] args)
	{
		boolean success = true;
		boolean expectedFailure = false;

		try
		{
			Properties info = new Properties();
			List<String> params = new ArrayList<String>();
			String infourl = "";

			if(args.length <= 0)
			{
				info.put("ServerAddress", "192.168.124.128");
				info.put("LazyLoad", "false");
				info.put("Cluster", "myroxie");
				info.put("QuerySet", "thor");
				info.put("WsECLWatchPort", "8010");
				info.put("WsECLDirectPort", "8008");
				info.put("EclResultLimit", "ALL");

				infourl = "url:jdbc:ecl;ServerAddress=192.168.124.128;Cluster=myroxie;EclResultLimit=8";

				//success &= runFullTest(info, infourl);

				params.add("'33445'");
				params.add("'90210'");

				success &= executeFreeHandSQL(info, "select count(persons.lastname) as zipcount, persons.city as mycity , zip from tutorial::rp::tutorialperson persons USE INDEX(0) limit 100", params);
				success &= executeFreeHandSQL(info, "select count(*), persons.firstname, persons.lastname from tutorial::rp::tutorialperson as persons limit 10", params);
				success &= executeFreeHandSQL(info, "select persons.firstname, persons.lastname from tutorial::rp::tutorialperson as persons where persons.firstname < 'a' limit 10", params);
				success &= executeFreeHandSQL(info, "select count(persons.firstname), persons.firstname, persons.lastname from tutorial::rp::tutorialperson as persons where persons.zip < '33445' limit 10", params);
				success &= executeFreeHandSQL(info, "select min(persons.firstname), persons.firstname, persons.lastname from tutorial::rp::tutorialperson as persons where persons.firstname < 'a' limit 10", params);
				success &= executeFreeHandSQL(info, "select max(persons.firstname), persons.firstname, persons.lastname from tutorial::rp::tutorialperson as persons where persons.zip < '33445' limit 10", params);

				//Join where first table is index, and join table is not
				success &= executeFreeHandSQL(info, "select persons.firstname, persons.lastname from tutorial::rp::peoplebyzipindex3 as persons outer join 	tutorial::rp::tutorialperson as people2 on people2.firstname = persons.firstname where firstname > 'A' limit 10", params);

				//Join where first table is logical file, and join table is index file
				//Seems to fail with this message:
				//' Keyed joins only support LEFT OUTER/ONLY'
				expectedFailure |= !(executeFreeHandSQL(info, "select persons.firstname, persons.lastname from tutorial::rp::tutorialperson2 as persons outer join tutorial::rp::peoplebyzipindex2 as people2 on people2.firstname = persons.firstname limit 10", params));

				//Join where both tables are index files
				//Seems to fail with this message:
				//' Keyed joins only support LEFT OUTER/ONLY'
				expectedFailure |= !(executeFreeHandSQL(info, "select persons.firstname, persons.lastname from tutorial::rp::peoplebyzipindex3 as persons outer join 	tutorial::rp::peoplebyzipindex2 as people2 on people2.firstname = persons.firstname limit 10", params));

				success &= executeFreeHandSQL(info, "select min(persons.firstname), persons.firstname, persons.lastname from tutorial::rp::tutorialperson as persons where persons.firstname < 'a' limit 10", params);
				success &= executeFreeHandSQL(info, "select max(persons.firstname), persons.firstname, persons.lastname from tutorial::rp::tutorialperson as persons where persons.zip < '33445' limit 10", params);

				expectedFailure |= !(executeFreeHandSQL(info, "select max(persons.lastname), persons.firstname, persons.lastname from tutorial::rp::tutorialperson as persons outer join 	tutorial::rp::tutorialperson as people2 on people2.firstname = persons.firstname where persons.firstname > 'a' order by persons.lastname ASC, persons.firstname DESC limit 10", params));

				success &= executeFreeHandSQL(info, "select count(persons.zip) as zipcount, persons.city as mycity , zip from super::super::tutorial::rp::tutorialperson as persons where persons.zip > ? AND persons.zip < ? group by zip limit 100", params);
				expectedFailure |= !(executeFreeHandSQL(info, "select count(persons.zip) as zipcount, persons.city as mycity , zip, p2.ball from super::super::tutorial::rp::tutorialperson as persons join thor::motionchart_motion_chart_test_fixed as p2 on p2.zip = persons.zip where persons.zip > ? group by zip limit 10", params));
				success &= executeFreeHandSQL(info, "select acct.account, acct.personid, persons.firstname, persons.lastname from progguide::exampledata::people as persons outer join 	progguide::exampledata::accounts as acct on acct.personid = persons.personid  where persons.personid > 5 limit 10", params);
				success &= executeFreeHandSQL(info, "select count(persons.personid), persons.firstname, persons.lastname from progguide::exampledata::people as persons  limit 10", params);
				expectedFailure |=  !(executeFreeHandSQL(info, "select count(persons.zip) as zipcount, persons.city as mycity , zip, p2.ball from super::super::tutorial::rp::tutorialperson as persons join tutorial::rp::tutorialperson2 as p2 on p2.zip = persons.zip where persons.zip > ? group by zip limit 10", params));

			}
			else
			{
				System.out.println("********************************************************************");
				System.out.println("HPCC JDBC Test Package Usage:");
				System.out.println(" Connection Parameters: paramname==paramvalue");
				System.out.println(" eg. ServerAddress==192.168.124.128");
				System.out.println(" Prepared Statement param value: \"param\"==paramvalue");
				System.out.println(" eg. param==\'33445\'");
				System.out.println();
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

		System.out.println("\nHPCC Driver test " + (success ? "passed" : "failed") + (expectedFailure ? "; Expected failure(s) detected " : "" ) + " - Verify results.");
	}
}
