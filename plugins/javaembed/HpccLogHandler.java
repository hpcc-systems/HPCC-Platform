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
    public static synchronized void initialize(String level, String pattern)
    {
        if (initialized)
        {
            return;
        }

        try
        {
            createLog4jConfig(level, pattern);
            HpccUtils.log("HpccLogHandler initialized, Enabled Java log redirection.");
            initialized = true;
        }
        catch (Exception e)
        {
            HpccUtils.log("Failed to initialize HpccLogHandler: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Create log4j config with custom level and pattern
     * @param logLevel The logging level to set
     * @param logPattern The logging pattern to use
     */
    private static void createLog4jConfig(String level, String pattern) throws IOException
    {
        // Use default pattern if not provided and ensure surrounding quotes are removed
        if (pattern == null || pattern.isEmpty())
            pattern = "%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n";
        else if (pattern.length() >= 2 && pattern.charAt(0) == '"' && pattern.charAt(pattern.length() - 1) == '"')
            pattern = pattern.substring(1, pattern.length() - 1);

        String configXml = "<Configuration status=\"WARN\">\n" +
                "    <Appenders>\n" +
                "        <Console name=\"Console\" target=\"SYSTEM_OUT\">\n" +
                "            <PatternLayout pattern=\"" + pattern + "\"/>\n" +
                "        </Console>\n" +
                "    </Appenders>\n" +
                "    <Loggers>\n" +
                "        <Root level=\"" + level + "\">\n" +
                "            <AppenderRef ref=\"Console\"/>\n" +
                "        </Root>\n" +
                "    </Loggers>\n" +
                "</Configuration>";

        final Path log4jConfigPath = Paths.get(System.getProperty("user.dir"), "log4j2.xml");
        Files.write(log4jConfigPath, configXml.getBytes());

        System.setOut(new java.io.PrintStream(new HpccLogHandler(), true));
        System.setErr(new java.io.PrintStream(new HpccLogHandler(), true));
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
