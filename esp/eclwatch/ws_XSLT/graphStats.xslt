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

<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:param name="wuid" select="''"/>
    <xsl:param name="graphName" select="''"/>
    <xsl:output method="html"/>
    <xsl:variable name="debugMode" select="0"/>
    <xsl:variable name="filePath">
        <xsl:choose>
            <xsl:when test="$debugMode">c:/development/bin/debug/files</xsl:when>
            <xsl:otherwise>/esp/files_</xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <title>Graph Statistics</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="{$filePath}/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <script type="text/javascript" src="{$filePath}/scripts/sortabletable.js">
                    <xsl:text disable-output-escaping="yes">&#160;</xsl:text>
                </script>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <script type="text/javascript">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        var sortableTable = null;
                        
                        function onLoad()
                        {
                            var table = document.getElementById('resultsTable');
                            if (table)
                                sortableTable = new SortableTable(table, table, 
                                    ["Number", "String", "Number", "Number", "Number", "Number"]);                                  
                        }
                        function findNode(id)
                        {
                            if (window.opener)
                                window.opener.selectVertex(id);
                        }
                ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <h3>Graph Statistics</h3>
        <table class="sort-table" id="resultsTable">
                    <thead>
                        <tr>
                            <th>Id</th>
                            <th>Label</th>
                            <th>Seeks</th>
                            <th>Scans</th>
                            <th>Local Time</th>
                            <th>Total Time</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!--xsl:apply-templates select="//node[att/@name='totalTime' or att/@name='localTime' or att/@name='seeks' or att/@name='scans']"/-->
                        <xsl:apply-templates select="//node[@label]"/>
                    </tbody>
                </table>
            </body>
        </html>
    </xsl:template>
    <xsl:template match="node[@id!='']">
        <tr>
            <td>
                <a href="javascript:findNode({@id})" title="Find node in graph...">
                    <xsl:value-of select="@id"/>
                </a>
            </td>
            <xsl:call-template name="addCell">
                <xsl:with-param name="val" select="@label"/>
            </xsl:call-template>
            <xsl:call-template name="addCell">
                <xsl:with-param name="val" select="att[@name='seeks']/@value"/>
            </xsl:call-template>
            <xsl:call-template name="addCell">
                <xsl:with-param name="val" select="att[@name='scans']/@value"/>
            </xsl:call-template>
            <xsl:call-template name="addCell">
                <xsl:with-param name="val" select="att[@name='localTime']/@value"/>
            </xsl:call-template>
            <xsl:call-template name="addCell">
                <xsl:with-param name="val" select="att[@name='totalTime']/@value"/>
            </xsl:call-template>
        </tr>
    </xsl:template>
    <xsl:template name="addCell">
        <xsl:param name="val"/>
        <td>
            <xsl:choose>
                <xsl:when test="$val!=''">
                    <xsl:value-of select="$val"/>
                </xsl:when>
                <xsl:otherwise>&#160;</xsl:otherwise>
            </xsl:choose>
        </td>
    </xsl:template>
</xsl:stylesheet>
