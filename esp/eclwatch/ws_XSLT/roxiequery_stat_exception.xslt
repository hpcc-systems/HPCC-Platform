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
    <xsl:template match="/StatisticsReportExceptionListResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <script language="JavaScript1.2" src="/esp/files_/popup.js">null</script>
                    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                    <title>Roxie Query</title>
                    <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
          <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                    <script type="text/javascript" src="files_/scripts/sortabletable.js">
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                    </script>
                    <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                    </script>
                    <script language="JavaScript1.2">
                        <xsl:text disable-output-escaping="yes"><![CDATA[
                            function onLoad()
                            {
                                initSelection('resultsTable');
                                var table = document.getElementById('resultsTable');
                                if (table)
                                    sortableTable = new SortableTable(table, table, ["String", "String", "String", "String", "String", "String", "String", "Number", "String"]);
                            }        
                        
                            var sortableTable = null;
                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <h3>Roxie Query Statistics Report:</h3>
                <xsl:choose>
                    <xsl:when test="not(RoxieQueries/QueryStatisticsException[1])">
                        No data found.
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates/>
                    </xsl:otherwise>
                </xsl:choose>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="RoxieQueries">
        <table class="sort-table" id="resultsTable">
            <colgroup>
                <col width="100"/>
                <col width="100"/>
                <col width="100"/>
                <col width="100" class="number"/>
            </colgroup>
            <thead>
            <tr class="grey">
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Roxie IP</th>
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Receive Time</th>
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Error Class</th>
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Error Code</th>
            </tr>
            </thead>
            <tbody>
            <xsl:apply-templates select="QueryStatisticsException">
                <xsl:sort select="ReceiveTime"/>
            </xsl:apply-templates>
         </tbody>
        </table>
    </xsl:template>
    
    <xsl:template match="QueryStatisticsException">
        <tr onmouseenter="this.bgColor = '#F0F0FF'">
            <xsl:choose>
                <xsl:when test="position() mod 2">
                    <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                    <xsl:attribute name="onmouseleave">this.bgColor = '#FFFFFF'</xsl:attribute>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                    <xsl:attribute name="onmouseleave">this.bgColor = '#F0F0F0'</xsl:attribute>
                </xsl:otherwise>
            </xsl:choose>
            <td align="left">
                <a href="javascript:go('/WsRoxieQuery/StatisticsReportExceptionDetail?ID={ID}')">
                    <xsl:value-of select="ReceiveTime"/>
                </a>
            </td>
            <td>
                <xsl:value-of select="RoxieIP"/>
            </td>
            <td align="left">
                <xsl:value-of select="ErrorClass"/>
            </td>
            <td>
                <xsl:value-of select="ErrorCode"/>
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
