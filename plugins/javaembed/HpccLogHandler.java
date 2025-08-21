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

import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Level;
import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.io.InputStream;
import java.util.Properties;

/**
 * Custom log handler that intercepts Java logging (including log4j via slf4j bridge)
 * and forwards messages to the HPCC logging system. Also configures log4j to output
 * to console for interception.
 */
public class HpccLogHandler extends Handler
{
}