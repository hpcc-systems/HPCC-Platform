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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
<xsl:output method="html"/>
    <xsl:output method="html"/>
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Operation Result</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
    </head>
    <body class="yui-skin-sam" onload="nof5();">
            <xsl:apply-templates/>
        </body> 
        </html>
    </xsl:template>
    <xsl:template match="SuperfileActionResponse">
<table>
<tbody>
<th align="left">
<h2>Operation Result</h2>
</th>
    <tr>
        <td>
            <xsl:value-of select="superfile"/> operation succeeded.
        </td>
    </tr>
    <xsl:if test="retcode=0">
        <tr>
            <td>
                <br/>
                <br/>
                <a href="javascript:go('/WsDfu/DFUInfo?Name={superfile}')">
                    <xsl:value-of select="superfile"/>
                </a>
            </td>
        </tr>
    </xsl:if>
</tbody>
</table>

    </xsl:template>

    <xsl:template match="SuperfileAddRawResponse">
<table>
<tbody>
<th align="left">
<h2>Operation Result</h2>
</th>
<tr>
<td>
        <xsl:choose>
            <xsl:when test="retcode=0">
                <xsl:value-of select="superfile"/> subfile adding succeeded.
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="rtitle"/> subfile adding failed.
            </xsl:otherwise>
        </xsl:choose>
</td>
</tr>
<tr>
<td>
<br/>
<br/>
<!--a href="javascript:go('/WsDfu/SuperfileList?superfile={superfile}')"><xsl:value-of select="superfile"/></a-->
<a href="javascript:go('/WsDfu/DFUInfo?Name={superfile}')"><xsl:value-of select="superfile"/></a>
</td>
</tr>
</tbody>
</table>

    </xsl:template>

</xsl:stylesheet>
