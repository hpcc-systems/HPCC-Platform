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
            <title>Permissions</title>
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
    <xsl:template match="PermissionActionResponse">
<table>
<tbody>
<th align="left">
<h2>Permission Change Result</h2>
</th>
<tr>
<td>
        <xsl:choose>
            <xsl:when test="retcode=0">
            Permission successfully changed.
            </xsl:when>
            <xsl:otherwise>
            Permission change unsuccessful,
            </xsl:otherwise>
        </xsl:choose>
        <xsl:value-of select="retmsg"/>
</td>
</tr>
<tr>
<td>
<br/>
<br/>
<xsl:choose>
<xsl:when test="string-length(AccountName)">
<a href="javascript:go('/ws_access/AccountPermissions?IsGroup={IsGroup}&amp;IncludeGroup=true&amp;AccountName={AccountName}')">Permissions for <xsl:value-of select="AccountName"/></a>
</xsl:when>
<xsl:otherwise>
<a href="javascript:go('/ws_access/ResourcePermissions?basedn={basedn}&amp;rtype={rtype}&amp;rtitle={rtitle}&amp;name={rname}&amp;prefix={prefix}')">Permissions for <xsl:value-of select="rname"/></a>
<br/>
<a href="javascript:go('/ws_access/Resources?basedn={basedn}&amp;rtype={rtype}&amp;rtitle={rtitle}&amp;prefix={prefix}')"><xsl:value-of select="rtitle"/> Permissions</a>
</xsl:otherwise>
</xsl:choose>
</td>
</tr>
</tbody>
</table>

    </xsl:template>
</xsl:stylesheet>
