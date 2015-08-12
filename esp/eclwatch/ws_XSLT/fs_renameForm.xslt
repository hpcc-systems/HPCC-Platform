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
