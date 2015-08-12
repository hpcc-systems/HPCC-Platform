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
