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
    <xsl:template match="Messages">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <title>XRef - Errors/Warnings</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <style type="text/css">
                    body { background-color: #white;}
                    table.list { border-collapse: collapse; border: double solid #777; font: 10pt arial, helvetica, sans-serif; }
                    table.list th, table.list td { border: 1 solid #777; padding:2px; }
                    .grey { background-color: #ddd;}
                    .number { text-align:right; }
                </style>
            </head>
      <body class="yui-skin-sam" onload="nof5();">
                <h3>XRef messages on cluster '<xsl:value-of select="Cluster"/>':</h3>
                <h4>Errors:</h4>
                <xsl:choose> 
                <xsl:when test="Error">
                    <table style="text-align:left;" class="list">
                        <colgroup style="vertical-align:top;padding-right:10px;" span="2">
                        </colgroup>
                        <tr class="grey">
                            <th align="left">File</th>
                            <th align="left">Message</th>
                        </tr>
                        <tbody>
                            <xsl:for-each select="Error">
                                <tr>
                                    <td align="left"><xsl:value-of select="File"/></td>
                                    <td align="left"><xsl:value-of select="Text"/></td>
                                </tr>
                            </xsl:for-each>
                        </tbody>
                    </table>
                </xsl:when> 
                <xsl:otherwise>
                    None.
                </xsl:otherwise>
                </xsl:choose>
                <h4>Warnings:</h4>
                <xsl:choose> 
                <xsl:when test="Warning">
                    <table style="text-align:left;" class="list">
                        <colgroup style="vertical-align:top;padding-right:10px;" span="2">
                        </colgroup>
                        <tr class="grey">
                            <th align="left">File</th>
                            <th align="left">Message</th>
                        </tr>
                        <tbody>
                            <xsl:for-each select="Warning">
                                <tr>
                                    <td align="left"><xsl:value-of select="File"/></td>
                                    <td align="left"><xsl:value-of select="Text"/></td>
                                </tr>
                            </xsl:for-each>
                        </tbody>
                    </table>
                </xsl:when> 
                <xsl:otherwise>
                    None.
                </xsl:otherwise>
                </xsl:choose>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
