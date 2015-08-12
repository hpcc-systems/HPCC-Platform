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
    <xsl:template match="RoxieQuerySearchResponse">
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
          function onFindClick()
                    {
                        var url = "/WsRoxieQuery/RoxieQueryList";
                        
                        var clusterC = document.getElementById("ClusterName").selectedIndex;
                        if(clusterC > -1)
                        {
                            var cluster = document.getElementById("ClusterName").options[clusterC].text;
                            url += "?Cluster=" + cluster;
                        }

                        var clusterS = document.getElementById("Suspended").selectedIndex;
                        if(clusterS > -1)
                        {
                            var suspended = document.getElementById("Suspended").options[clusterS].text;
                            url += "&Suspended=" + suspended;
                        }
                        document.location.href=url;

                        return;
                    }       

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();">
                <h4>Search Roxie Query:</h4>
                <table>
                    <tr><th colspan="2"/></tr>
                    <tr>
                        <td><b>Cluster:</b></td>
                        <td>
                            <select id="ClusterName" name="ClusterName" size="1">
                                <xsl:for-each select="ClusterNames/ClusterName">
                                    <option>
                                        <xsl:value-of select="."/>
                                    </option>
                                </xsl:for-each>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td><b>Suspended:</b></td>
                        <td>
                            <select id="Suspended" name="Suspended" size="1">
                                <xsl:for-each select="SuspendedSelections/SuspendedSelection">
                                    <option>
                                        <xsl:value-of select="."/>
                                    </option>
                                </xsl:for-each>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td/>
                        <td>
                            <input type="submit" value="Search" class="sbutton" onclick="onFindClick()"/>
                        </td>
                    </tr>
                </table>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
