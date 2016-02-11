<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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
    <xsl:template match="/WUGetZAPInfoResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Editing Group Members</title>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
                <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <script language="JavaScript1.2">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    function toggleElement(ElementId)
                    {
                        var obj = document.getElementById(ElementId);
                        explink = document.getElementById('explink' + ElementId.toLowerCase());
                        if (obj)
                        {
                          if (obj.style.visibility == 'visible')
                          {
                            obj.style.display = 'none';
                            obj.style.visibility = 'hidden';
                            if (explink)
                              explink.className = 'wusectionexpand';
                          }
                          else
                          {
                            obj.style.display = 'inline';
                            obj.style.visibility = 'visible';
                            if (explink)
                              explink.className = 'wusectioncontract';
                            reloadSection(ElementId);
                          }
                        }
                    }
                    function onReport()
                    {
                        var opener = window.opener;
                        if (opener)
                        {
                            if (!opener.closed)
                            {
                                var wuid = document.getElementById("WUID").value;
                                var espIP = document.getElementById("ESPIPAddress").value;
                                var thorIP = document.getElementById("ThorIPAddress").value;
                                var buildVersion = document.getElementById("BuildVersion").value;
                                var desc = document.getElementById("ProblemDescription").value;
                                var history = document.getElementById("WhatChanged").value;
                                var timing = document.getElementById("WhereSlow").value;
                                var password = document.getElementById("Password").value;
                                var thorSlaveLog = document.getElementById("IncludeThorSlaveLog").checked;
                                opener.createZAPInfo(wuid, espIP, thorIP, buildVersion, desc, history, timing, password, thorSlaveLog);
                            }
                            window.close();
                        }
                        else
                        {
                            alert('Lost reference to parent window.  Please traverse the path again!');
                            unselect();
                        }
                    }
                    function onLoad()
                    {
                        document.getElementById("ProblemDescription").value = "";
                        document.getElementById("WhatChanged").value = "";
                        document.getElementById("WhereSlow").value = "";
                        document.getElementById("Password").value = "";
                    }
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam" onload="nof5();onLoad()">
                <h3 style="text-align: center;">Zipped Analysis Package</h3>
                <p/>
                <form action="" method="POST">
                    <input type="hidden" id="WUID" name="WUID" value="{WUID}"/>
                    <input type="hidden" id="ESPIPAddress" name="ESPIPAddress" value="{ESPIPAddress}"/>
                    <input type="hidden" id="ThorIPAddress" name="ThorIPAddress" value="{ThorIPAddress}"/>
                    <input type="hidden" id="BuildVersion" name="BuildVersion" value="{BuildVersion}"/>
                    <div id="general" xmlns="http://www.w3.org/1999/xhtml">
                        <table>
                            <tr>
                                <td style="width: 250px">
                                    <b>WUID: </b>
                                </td>
                                <td>
                                    <xsl:value-of select="WUID"/>
                                </td>
                            </tr>
                            <tr>
                                <td>
                                    <b>ESP Build Version: </b>
                                </td>
                                <td>
                                    <xsl:value-of select="BuildVersion"/>
                                </td>
                            </tr>
                            <tr>
                                <td>
                                    <b>ESP Network Address: </b>
                                </td>
                                <td>
                                    <xsl:value-of select="ESPIPAddress"/>
                                </td>
                            </tr>
                            <xsl:if test="string-length(ThorIPAddress)">
                                <tr>
                                    <td>
                                        <b>Thor Network Address: </b>
                                    </td>
                                    <td>
                                        <xsl:value-of select="ThorIPAddress"/>
                                    </td>
                                </tr>
                            </xsl:if>
                        </table>
                    </div>
                    <xsl:if test="string-length(Archive)">
                        <br/>
                        <div>
                            <div class="wugroup">
                                <div class="WuGroupHdrLeft">
                                    <A href="javascript:void(0)" onclick="toggleElement('querysection');" id="explinkquerysection" class="wusectionexpand">Archive</A>
                                </div>
                            </div>
                            <div id="querysection" class="wusectioncontent">
                                <div>
                                    <textarea id="query" readonly="true" wrap="off" rows="10" STYLE="width:600">
                                        <xsl:value-of select="Archive"/>
                                    </textarea>
                                </div>
                            </div>
                        </div>
                    </xsl:if>
                    <br/>
                    <div id="detailsinput" xmlns="http://www.w3.org/1999/xhtml">
                        <table>
                            <tr>
                                <td valign="top" style="width: 450px">
                                    <b>Problem Description:</b> Please fill in details about what might be going wrong. Is it a SOAP call? ...
                                </td>
                                <td>
                                    <textarea rows="10" cols="72" id="ProblemDescription" name="ProblemDescription">&#160;</textarea>
                                </td>
                            </tr>
                            <tr>
                                <td valign="top" style="width: 450px">
                                    <b>History:</b> Please fill in details about when the job last ran. What has changed since then?...
                                </td>
                                <td>
                                    <textarea rows="10" cols="72" id="WhatChanged" name="WhatChanged">&#160;</textarea>
                                </td>
                            </tr>
                            <tr>
                                <td valign="top" style="width: 450px">
                                    <b>Timing:</b> Please fill in details about where the job is going slow. What do the timings say?...
                                </td>
                                <td>
                                    <textarea rows="10" cols="72" id="WhereSlow" name="WhereSlow">&#160;</textarea>
                                </td>
                            </tr>
                            <tr>
                                <td></td>
                                <td>
                                    <input type="checkbox" title="Include Thor Slave logs" id="IncludeThorSlaveLog" name="IncludeThorSlaveLog">Include Thor Slave logs (It may take a long time to report the logs from multiple Thor Slave nodes.)</input>
                                </td>
                            </tr>
                            <tr>
                                <td></td>
                                <td>
                                    Password to open ZAP (optional):<input type="password" id="Password" name="Password"/>
                                </td>
                            </tr>
                            <tr>
                                <td></td>
                                <td>
                                    <input type="submit" value="Create Report" name="Report" onclick="onReport()"/>
                                </td>
                            </tr>
                        </table>
                    </div>
                </form>
            </body>
        </html>
    </xsl:template>
    <xsl:template match="*|@*|text()"/>
</xsl:stylesheet>
