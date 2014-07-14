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
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <script type="text/javascript">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        function findNode(id)
                        {
                            if (window.opener)
                                window.opener.selectVertex(id);
                        }
                ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5()">
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
