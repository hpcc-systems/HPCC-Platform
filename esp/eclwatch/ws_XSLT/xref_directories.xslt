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
    <xsl:template match="Directories">
        <script type="text/javascript" language="javascript">
           cluster = '<xsl:value-of select="Cluster"/>';
        <![CDATA[
            function doDelete()
            {
                document.location.href='/WsDFUXRef/DFUXRefCleanDirectories?Cluster='+ cluster;
                return true;
            }
        ]]></script>
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <title>XRef - Directories</title>
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
                <h3>XRef directories on cluster '<xsl:value-of select="Cluster"/>':</h3>
                <xsl:choose> 
                <xsl:when test="Directory">
                    <table>
                        <tr>
                            <input type="button" id="CleanDirs" name="CleanDirs" value="Delete Empty Directories" onclick="return doDelete();"/>
                        </tr>
                    </table>
                    <table style="text-align:left;" class="list">
                        <colgroup style="vertical-align:top;padding-right:10px;" span="7">
                        </colgroup>
                        <tr class="grey">
                            <th align="left">Directory</th>
                            <th align="left">Files</th>
                            <th align="left">Total Size</th>
                            <th align="left">Max Node</th>
                            <th align="left">Max Size</th>
                            <th align="left">Min Node</th>
                            <th align="left">Min Size</th>
                            <th align="left">Skew(+)</th>
                            <th align="left">Skew(-)</th>
                        </tr>
                        <tbody>
                            <xsl:for-each select="Directory">
                                <tr>
                                    <td align="left"><xsl:value-of select="Name"/></td>
                                    <td align="left"><xsl:value-of select="Num"/></td>
                                    <td align="left"><xsl:value-of select="Size"/></td>
                                    <td align="left"><xsl:value-of select="MaxIP"/></td>
                                    <td align="left"><xsl:value-of select="MaxSize"/></td>
                                    <td align="left"><xsl:value-of select="MinIP"/></td>
                                    <td align="left"><xsl:value-of select="MinSize"/></td>
                                    <td align="left"><xsl:value-of select="PositiveSkew"/></td>
                                    <td align="left"><xsl:value-of select="NegativeSkew"/></td>
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
