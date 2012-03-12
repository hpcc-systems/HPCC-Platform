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

    <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html"/>
    <xsl:template match="/BasednsResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Permissions</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script type="text/javascript" src="files_/scripts/sortabletable.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2">
            <xsl:text disable-output-escaping="yes"><![CDATA[
                function getSelected(o)
                {
                    if (o.tagName=='INPUT' && o.type == 'checkbox' && o.value != 'on')
                        return o.checked ? '\n'+o.value : '';

                    var s='';
                    var ch=o.children;
                    if (ch)
                        for (var i in ch)
                            s=s+getSelected(ch[i]);
                    return s;
                }

                function onLoad()
                {
                    initSelection('resultsTable');
                    var table = document.getElementById('resultsTable');
                    if (table)
                        sortableTable = new SortableTable(table, table, ["String", "None"]);
                }

                function onSubmit(o, theaction)
                {
                    document.forms[0].action = ""+theaction;
                    return true;
                }
                var sortableTable = null;
            ]]></xsl:text>
            </script>
        </head>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
            <h3>Permissions</h3>
            <p/>
            <xsl:choose>
                <xsl:when test="not(Basedns/Basedn[1])">
          <h4>No permission found.</h4>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:apply-templates/>
                </xsl:otherwise>
            </xsl:choose>
        </body>
        </html>
    </xsl:template>

    <xsl:template match="Basedns">
        <table class="sort-table" id="resultsTable">
        <colgroup>
            <col width="300"/>
            <col width="300"/>
            <col width="100"/>
        </colgroup>
        <thead>
        <tr class="grey">
        <th align="left">Name</th>
        <th align="left">BaseDN</th>
        <th>Operation</th>
        </tr>
        </thead>
        <tbody>
        <xsl:apply-templates select="Basedn">
            <xsl:sort select="name"/>
        </xsl:apply-templates>
        </tbody>
        </table>
    </xsl:template>

    <xsl:template match="Basedn">
        <tr onmouseenter="this.bgColor = '#F0F0FF'">
        <xsl:choose>
            <xsl:when test="position() mod 2">
                <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                <xsl:attribute name="onmouseleave">this.bgColor = '#FFFFFF'</xsl:attribute>
            </xsl:when>
            <xsl:otherwise>
                <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                <xsl:attribute name="onmouseleave">this.bgColor = '#F0F0F0'</xsl:attribute>
            </xsl:otherwise>
        </xsl:choose>
        <td align="left">
        <xsl:value-of select="name"/>
        </td>
        <td align="left">
        <xsl:value-of select="basedn"/>
        </td>
        <td align="center">
            <a href="javascript:go('/ws_access/Resources?basedn={basedn}&amp;rtype={rtype}&amp;rtitle={rtitle}')">edit</a>
        </td>
        </tr>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>

</xsl:stylesheet>
