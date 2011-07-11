<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
