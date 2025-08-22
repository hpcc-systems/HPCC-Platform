/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems(R).

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

package com.HPCCSystems;

import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class HpccLogHandler extends OutputStream
{
    private static boolean initialized = false;
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    /**
     * Initialize the log4j configuration to redirect output to HPCC native logging
     * @param configFilePath Optional path to a custom log4j2.xml configuration file
     */
    public static synchronized void initialize(String configFilePath)
    {
        if (initialized)
        {
            return;
        }

        try
        {
            createLog4jConfig(configFilePath);
            initialized = true;
        }
        catch (Exception e)
        {
            System.err.println("Failed to initialize HpccLogHandler: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Create and configure log4j to use our custom appender
     * @param configFilePath Optional path to a custom log4j2.xml configuration file
     */
    private static void createLog4jConfig(String configFilePath) throws IOException
    {
        String configXml;

        if (configFilePath != null && !configFilePath.trim().isEmpty())
        {
            Path customConfigPath = Paths.get(configFilePath);
            if (Files.exists(customConfigPath))
            {
                configXml = new String(Files.readAllBytes(customConfigPath));
                HpccUtils.log("Using custom log4j configuration from: " + configFilePath);
            }
            else
            {
                HpccUtils.log("Custom config log4j file not found at: " + configFilePath + ", using default configuration");
                configXml = getDefaultConfigXml();
            }
        }
        else
        {
            HpccUtils.log("Using default log4j configuration");
            configXml = getDefaultConfigXml();
        }

        final Path log4jConfigPath = Paths.get(System.getProperty("user.dir"), "log4j2.xml");
        Files.write(log4jConfigPath, configXml.getBytes());

        System.setOut(new java.io.PrintStream(new HpccLogHandler(), true));
        System.setErr(new java.io.PrintStream(new HpccLogHandler(), true));
    }

    /**
     * Get the default log4j2 XML configuration
     * @return Default XML configuration as a string
     */
    private static String getDefaultConfigXml()
    {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<Configuration status=\"INFO\">\n" +
            "  <Appenders>\n" +
            "    <Console name=\"HpccConsole\" target=\"SYSTEM_OUT\">\n" +
            "      <PatternLayout pattern=\"%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n\"/>\n" +
            "    </Console>\n" +
            "  </Appenders>\n" +
            "  <Loggers>\n" +
            "    <Root level=\"info\">\n" +
            "      <AppenderRef ref=\"HpccConsole\"/>\n" +
            "    </Root>\n" +
            "  </Loggers>\n" +
            "</Configuration>";
    }

    @Override
    public void write(int b) throws IOException
    {
        if (b == '\n' || b == '\r')
            flush();
        else
            buffer.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        for (int i = off; i < off + len; i++)
            write(b[i]);
    }

    @Override
    public void flush() throws IOException
    {
        if (buffer.size() > 0)
        {
            String message = buffer.toString().trim();
            if (!message.isEmpty())
                HpccUtils.log(message);
            buffer.reset();
        }
    }

    @Override
    public void close() throws IOException
    {
        flush();
        super.close();
    }
}
