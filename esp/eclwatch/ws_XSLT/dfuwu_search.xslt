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
    <xsl:template match="DFUWUSearchResponse">
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
                    function onChangeType()
                    {
                      if (document.getElementById("Type").selectedIndex > 0)
                      {
                        document.getElementById("Cluster").disabled = true;
                      }
                      else
                      {
                        document.getElementById("Cluster").disabled = false;
                      }
                    }
                    /*function submit_wuid()
                    {
                        var r=String(document.getElementById("wuid").value).match(/[Dd](\d{8}-\d{6}(-\d+)?)/);
                        if(r && r[1])
                        {
                            document.getElementById("wuid").value='D'+r[1];
                            return true;
                        }
                        alert('Wrong WUID');
                        return false;
                    }*/
                    function onFindClick()
                    {
                        var url = "/FileSpray/GetDFUWorkunits";
                        var first = true;
                        
                        var owner = document.getElementById("Owner").value;
                        if(owner)
                        {
                            url += "?Owner=" + owner;
                            first = false;
                        }
                    
                        var clusterS = document.getElementById("Cluster").selectedIndex;
                        if(clusterS > 0)
                        {
                            var cluster = document.getElementById("Cluster").options[clusterS].text;
                            if (first)
                            {
                                url += "?Cluster=" + cluster;
                                first = false;
                            }
                            else
                            {
                                url += "&Cluster=" + cluster;
                            }
                        }
                
                        var typeS = document.getElementById("Type").selectedIndex;
                        if(typeS > 0)
                        {
                            var type = document.getElementById("Type").options[typeS].text;
                            if (first)
                            {
                                url += "?Type=" + type;
                                first = false;
                            }
                            else
                            {
                                url += "&Type=" + type;
                            }
                        }
                    
                        var stateS = document.getElementById("StateReq").selectedIndex;
                        if(stateS > 0)
                        {
                            var type = document.getElementById("StateReq").options[stateS].text;
                            if (first)
                            {
                                url += "?StateReq=" + type;
                                first = false;
                            }
                            else
                            {
                                url += "&StateReq=" + type;
                            }
                        }
                    
                        var jobname = document.getElementById("Jobname").value;
                        if(jobname)
                        {
                            if (first)
                            {
                                url += "?Jobname=" + jobname;
                                first = false;
                            }
                            else
                            {
                                url += "&Jobname=" + jobname;
                            }
                        }
                    
                        document.location.href=url;
                    
                        return;
                    }       

                    function onOpenClick()
                    {
                        var name = document.getElementById("wuid").value;
                        if(name)
                        { 
                            document.location.href="/FileSpray/GetDFUWorkunit?wuid=" + name;
                        }   
                        else
                        {
                            onFindClick();
                        }   
                        return;
                    }       

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onChangeType()">
                <h4>Open DFU Workunit:</h4>
                <!--form action="/FileSpray/GetDFUWorkunit" method="get" onsubmit="return submit_wuid()"-->
                    <table>
                        <tr>
                            <td>Wuid:</td>
                            <td>
                                <input id="wuid" name="wuid" size="25" type="text"/>
                            </td>
                            <td>
                                <input type="submit" value="Open" class="sbutton" onclick="onOpenClick()"/>
                            </td>
                        </tr>
                    </table>
                <!--/form-->
                <br/><br/>
                <h4>Search DFU Workunits:</h4>
                <!--form action="/FileSpray/GetDFUWorkunits"  method="get"-->
                    <table>
                        <tr>
                            <td>Type:</td>
                            <td>
                                <select name="Type" id="Type" size="1" onchange="onChangeType()">
                                    <option>non-archived workunits</option>
                                    <option>archived workunits</option>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td>Username:</td>
                            <td>
                                <input id="Owner" size="12" type="text"/>
                            </td>
                        </tr>
                        <tr>
                            <td>Cluster:</td>
                            <td>
                                <select id="Cluster" name="Cluster" size="1">
                                    <option></option>
                                    <xsl:for-each select="ClusterNames/ClusterName">
                                        <option>
                                            <xsl:value-of select="."/>
                                        </option>
                                    </xsl:for-each>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td>State:</td>
                            <td>
                                <select id="StateReq" size="1">
                                    <option/>
                                    <option>unknown</option>
                                    <option>submitted</option>
                                    <option>scheduled</option>
                                    <option>compiled</option>
                                    <option>running</option>
                                    <option>finished</option>
                                    <option>failed</option>
                                    <option>aborting</option>
                                    <option>aborted</option>
                                    <option>blocked</option>
                                    <option>monitoring</option>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td>Job Name:</td>
                            <td>
                                <input id="Jobname" size="12" type="text"/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <input type="submit" value="Find" class="sbutton" onclick="onFindClick()"/>
                            </td>
                        </tr>
                    </table>
                <!--/form-->
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
