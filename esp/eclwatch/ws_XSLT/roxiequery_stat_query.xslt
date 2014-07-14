<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    <xsl:template match="/StatisticsReportQueryListResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <script language="JavaScript1.2" src="/esp/files_/popup.js">null</script>
                    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                    <title>Roxie Query</title>
                    <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
          <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
          <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                    <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                    </script>
                    <script language="JavaScript1.2">
                        <xsl:text disable-output-escaping="yes"><![CDATA[
                            function onLoad()
                            {
                                initSelection('resultsTable');
                            }                                 
                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <h3>Roxie Query Statistics Report:</h3>
                <xsl:choose>
                    <xsl:when test="not(RoxieQueries/QueryStatisticsItem[1])">
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
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
            </colgroup>
            <thead>
            <tr class="grey">
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Query Name</th>
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Receive Time</th>
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Time Taken</th>
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Memory</th>
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Priority</th>
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'">Result Size</th>
            </tr>
            </thead>
            <tbody>
            <xsl:apply-templates select="QueryStatisticsItem">
                <xsl:sort select="ReceiveTime"/>
            </xsl:apply-templates>
         </tbody>
        </table>
    </xsl:template>
    
    <xsl:template match="QueryStatisticsItem">
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
                <a href="javascript:go('/WsRoxieQuery/StatisticsReportQueryDetail?ID={ID}')">
                    <xsl:value-of select="QueryName"/>
                </a>
            </td>
            <td>
                <xsl:value-of select="ReceiveTime"/>
            </td>
            <td>
                <xsl:value-of select="TimeTaken"/>
            </td>
            <td>
                <xsl:value-of select="Memory"/>
            </td>
            <td>
                <xsl:value-of select="Priority"/>
            </td>
            <td>
                <xsl:value-of select="ResultSize"/>
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
