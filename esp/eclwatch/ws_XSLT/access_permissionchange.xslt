<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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
