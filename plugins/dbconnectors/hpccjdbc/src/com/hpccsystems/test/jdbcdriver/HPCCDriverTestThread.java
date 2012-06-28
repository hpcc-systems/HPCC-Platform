package com.hpccsystems.test.jdbcdriver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import com.hpccsystems.jdbcdriver.HPCCConnection;
import com.hpccsystems.jdbcdriver.HPCCPreparedStatement;
import com.hpccsystems.jdbcdriver.HPCCResultSet;

public class HPCCDriverTestThread extends Thread
{
	private HPCCConnection theconnection;
	private String thesql;
	private Properties theparams;
	private boolean success;
	private boolean running;

	public HPCCDriverTestThread(HPCCConnection connection, String SqlStr, Properties parameters)
	{
		theconnection = connection;
		thesql = SqlStr;
		theparams = parameters;
		success = true;
		running = false;
	}

	@Override
	public void run()
	{
		PreparedStatement prepstatement;
		try
		{
			running = true;
			prepstatement = theconnection.prepareStatement(thesql);

			prepstatement.clearParameters();
			for(int i = 1; i <= theparams.size(); i++)
				prepstatement.setObject(i, theparams.getProperty(String.valueOf(i)));

			HPCCResultSet qrs = (HPCCResultSet)((HPCCPreparedStatement)prepstatement).executeQuery();
			HPCCDriverTest.printOutResultSet(qrs, Thread.currentThread().getId());
		}
		catch (Exception e)
		{
			e.printStackTrace();
			success = false;
		}
		finally
		{
			running = false;
		}
	}

	public boolean isSuccess()
	{
		return success;
	}

	public boolean isRunning()
	{
		return running;
	}
}
