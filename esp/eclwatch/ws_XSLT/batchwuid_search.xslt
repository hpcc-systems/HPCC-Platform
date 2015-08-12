<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html"/>
    <xsl:template match="BatchWUQuery">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>
                <script language="JavaScript1.2">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        function submit_wuid()
                        {
                            var r=String(document.getElementById("Wuid").value).match(/[Ww](\d{8}-\d{6}(-\d+)?)/);
                            if(r && r[1])
                            {
                                document.getElementById("Wuid").value='W'+r[1];
                                return true;
                            }
                            alert('Wrong WUID');
                            return false;
                        }
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam" onload="nof5();">
            <h4>Open Workunit:</h4>
                <form action="/WsBatchWorkunits/BatchWUInfo" method="get" onsubmit="return submit_wuid()">
                    <table>
                        <tr>
                            <td>WUID:</td>
                            <td>
                                <input name="Wuid" size="25" type="text"/>
                            </td>
                            <td>
                                <input type="submit" value="Open" class="sbutton"/>
                            </td>
                        </tr>
                    </table>
                </form>
            <br/>
            <h4>Search Batch Workunits:</h4>
                <form action="/WsBatchWorkunits/BatchWUQuery"  method="get">
                    <table>
                        <!--tr>
                            <td>Login ID:</td>
                            <td>
                                <select id="LoginID" name="LoginID" size="1">
                                    <option></option>
                                    <xsl:for-each select="LoginID">
                                        <option>
                                            <xsl:value-of select="."/>
                                        </option>
                                    </xsl:for-each>
                                </select>
                            </td>
                        </tr-->
                        <tr>
                            <td>Login ID:</td>
                            <td>
                                <input name="LoginID" size="12" type="text"/>
                            </td>
                        </tr>
                        <tr>
                            <td>Customer Name:</td>
                            <td>
                                <input name="CustomerName" size="12" type="text"/>
                            </td>
                        </tr>
                        <!--tr>
                            <td>Process Description:</td>
                            <td>
                                <input name="ProcessDescription" size="12" type="text"/>
                            </td>
                        </tr-->
                        <tr>
                            <td>Machine:</td>
                            <td>
                                <input name="Machine" size="12" type="text"/>
                            </td>
                        </tr>
                        <!--tr>
                            <td>GW Type:</td>
                            <td>
                                <input type="checkbox" id="GW_WEB" value="Web" checked = "1"/>
                                <input type="checkbox" id="GW_FTP" value="FTP" checked = "1"/>
                                <input type="checkbox" id="GW_NEWFTP" value="New FTP" checked = "1"/>
                                <input type="checkbox" id="GW_OSC" value="OSC" checked = "1"/>
                            </td>
                        </tr-->
                        <tr>
                            <td>Input File Name:</td>
                            <td>
                                <input name="InputFileName" size="12" type="text"/>
                            </td>
                        </tr>
                        <tr>
                            <td>Output File Name:</td>
                            <td>
                                <input name="OutputFileName" size="12" type="text"/>
                            </td>
                        </tr>
                        <tr>
                            <td>Status:</td>
                            <td>
                                <select name="Status" size="1">
                                    <option/>
                                    <option value='unknown'>unknown</option>
                                    <option value='submitted'>submitted</option>
                                    <option value='scheduled'>scheduled</option>
                                    <option value='compiled'>compiled</option>
                                    <option value='running'>running</option>
                                    <option value='completed'>completed</option>
                                    <option value='wait'>wait</option>
                                    <option value='failed'>failed</option>
                                    <option value='archived'>archived</option>
                                    <option value='aborting'>aborting</option>
                                    <option value='aborted'>aborted</option>
                                    <option value='blocked'>blocked</option>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <input type="submit" value="Find" class="sbutton"/>
                            </td>
                        </tr>
                    </table>
                </form>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
