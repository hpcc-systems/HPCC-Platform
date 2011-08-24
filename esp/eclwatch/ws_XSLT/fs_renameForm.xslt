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
    <xsl:param name="fullHtml" select="1"/>
    <xsl:param name="includeFormTag" select="1"/>
    <xsl:param name="method" select="'Rename'"/>
    <xsl:param name="sourceLogicalName" select="''"/>
    <xsl:template match="/Environment">
    <xsl:choose>
      <xsl:when test="ErrorMessage">
        <h4>
          <xsl:value-of select="$method"/> File: 
          <xsl:value-of select="ErrorMessage"/>
        </h4>
      </xsl:when>
      <xsl:otherwise>
        <xsl:choose>
            <xsl:when test="$fullHtml">
                <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
                    <head>
                        <title>Spray / Despray result</title>
            <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
            <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
            <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
            <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
          </head>
          <body class="yui-skin-sam" onload="nof5();onLoad()">
                        <form method="POST" action="/FileSpray/{$method}">
                            <xsl:call-template name="generateForm"/>
                        </form>
                    </body>
                </html>
            </xsl:when>
            <xsl:otherwise>
                <xsl:choose>
                    <xsl:when test="$includeFormTag">
                        <form method="POST" action="/FileSpray/{$method}">
                            <xsl:call-template name="generateForm"/>
                        </form>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:call-template name="generateForm"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:otherwise>
        </xsl:choose>
      </xsl:otherwise>
    </xsl:choose>
    </xsl:template>
    
    <xsl:template name="generateForm">
        <script type="text/javascript" language="javascript">
            method = '<xsl:value-of select="$method"/>';
            <![CDATA[
                function onLoad()
                {
                    handleSubmitBtn();
                }
                
                function handleSubmitBtn()
                {
                    sourceFile = document.getElementById('srcname').value;
                    destFile = document.getElementById('dstname').value;
                    sourceIn = document.getElementById('srcname').value == '';
                    destIn = document.getElementById('dstname').value == '';
                    if (sourceIn || destIn || (sourceFile == destFile))
                        document.getElementById('submitBtn').disabled = true;
                    else
                        document.getElementById('submitBtn').disabled = false;
                }
            ]]>
        </script>
        <table name="table1">
            <tr>
                <th colspan="2">
                    <h3>Rename File</h3>
                </th>
            </tr>
            <tr>
                <td height="10"/>
            </tr>
            <tr>
                <td colspan="2">
                    <b>Source</b>
                </td>
            </tr>
            <xsl:variable name="origName">
                <xsl:choose>
                    <xsl:when test="$sourceLogicalName = ''">
                        <xsl:value-of select="Software/DfuWorkunit/Source/OrigName"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="$sourceLogicalName"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <tr>
                <td>Logical File:</td>
                <td>
                    <xsl:choose>
                        <xsl:when test="$origName=''">
                            <input type="text" id="srcname" name="srcname" size="70" value="" onKeyUp="handleSubmitBtn()" onchange="handleSubmitBtn()"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="$origName"/>
                            <input type="hidden" id="srcname" name="srcname" value="{$origName}"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </td>
            </tr>
            <tr>
                <td height="10"/>
            </tr>
            <tr>
                <td colspan="2">
                    <b>Destination</b>
                </td>
            </tr>
            <tr>
                <td>Logical File:</td>
                <td>
                    <input type="text" id="dstname" name="dstname" size="70" value="" onKeyUp="handleSubmitBtn()" onchange="handleSubmitBtn()"/>
                </td>
            </tr>
                <tr>
                    <td height="10"/>
                </tr>
            <tr>
                <td>Overwrite:</td>
                <td>
                    <input type="checkbox" id="overwrite" name="overwrite"/>
                </td>
            </tr>
        </table>
        <xsl:if test="$fullHtml='1'">
            <table>
                <tr>
                    <td/>
                    <td>
                        <input type="submit" id="submitBtn" name="submitBtn" value="Submit" disabled="true"/>
                    </td>
                </tr>
            </table>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>
