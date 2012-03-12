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
                <script language="JavaScript1.2">
                   <xsl:text disable-output-escaping="yes"><![CDATA[
                         function go(url)
                         {
                            document.location.href=url;
                         }
                   ]]></xsl:text>
                </script>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>User Account Result</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
    </head>
      <body class="yui-skin-sam">
        <xsl:apply-templates/>
      </body>
        </html>
    </xsl:template>
    <xsl:template match="UserInfoEditResponse">
<table>
<tbody>
<th align="left">
<h2>User Account Edit Result for user <xsl:value-of select="username"/> </h2>
</th>
<tr>
<td>
<xsl:value-of select="retmsg"/>
</td>
</tr>
<tr>
<td>
<br/>
<br/>
<a href="javascript:go('/ws_access/Users')">Users</a></td>
</tr>
</tbody>
</table>

    </xsl:template>
</xsl:stylesheet>
