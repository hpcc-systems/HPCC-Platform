package com.hpccsystems.jdbcdriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
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

    public HPCCDriver()
    {
    }

    public Connection connect(String url, Properties info) throws SQLException
    {
        Properties connprops = new Properties();

        if (info != null && info.size() > 0)
            connprops.putAll(info);

        try
        {
            StringTokenizer urltokens = new StringTokenizer(url, ";");
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
                        if (!connprops.containsKey(key))
                            connprops.put(key, value);
                        else
                            System.out.println("Connection property: " + key
                                    + " found in info properties and URL, ignoring URL value");
                    }
                }
            }
        }
        catch (Exception e)
        {
            System.out.println("Issue parsing URL! \"" + url + "\"");
        }

        String serverAddress = connprops.getProperty("ServerAddress");
        System.out.println("EclDriver::connect" + serverAddress);

        return new HPCCConnection(connprops);
    }

    public boolean acceptsURL(String url) throws SQLException
    {
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

    public boolean jdbcCompliant()
    {
        return true;
    }

}
