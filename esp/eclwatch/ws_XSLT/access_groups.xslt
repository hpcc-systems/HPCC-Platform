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
    <xsl:template match="/GroupResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Groups</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
            <script type="text/javascript" src="files_/scripts/sortabletable.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="JavaScript1.2">
            <xsl:text disable-output-escaping="yes"><![CDATA[
                function onRowCheck(checked)
                {
                    document.getElementById('listitems').deleteBtn.disabled = checkedCount == 0;
          document.getElementById("listitems").exportBtn.disabled = checkedCount == 0;
                    document.getElementById('listitems').addBtn.disabled = checkedCount != 0;
                }

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
                        sortableTable = new SortableTable(table, table, ["None", "String", "String", "None"]);
                }

                function onSubmit(o, theaction)
                {
                    document.getElementById('listitems').action = ""+theaction;
                    return true;
                }

        function onExport()
                {
                    document.getElementById('ActionType').value='Export';
          document.forms['listitems'].submit();
                    return true;
                }
        function onDelete()
                {
          ret = confirm('Are you sure you want to delete the following group(s) ?\n\n'+getSelected(document.getElementById('listitems')).substring(1,1000));
          if (ret)
          {
                      document.getElementById('ActionType').value='Delete';
            document.forms['listitems'].submit();
          }
                    return true;
                }
                var sortableTable = null;
            ]]></xsl:text>
            </script>
        </head>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
            <h3>Groups</h3>
      <xsl:choose>
        <xsl:when test="not(Groups/Group[1])">
          <table>
            <tr>
              <td colspan="4">
                <b>No group found.</b>
              </td>
            </tr>
            <tr>
              <td/>
            </tr>
            <tr>
              <td/>
            </tr>
            <tr>
              <td>
                <input type="button" class="sbutton" id="addBtn" value="  Add  " onClick="javascript:go('/esp/files_/groupadd.html')"/>
              </td>
            </tr>
          </table>
        </xsl:when>
        <xsl:otherwise>
            <form id="listitems" action="/ws_access/GroupAction" method="post">
        <input type="hidden" id="ActionType" name="ActionType" value="Delete"/>
            <table class="sort-table" id="resultsTable">
            <colgroup>
                <col width="5"/>
                <col width="150"/>
                <col width="200"/>
            </colgroup>
            <thead>
            <tr class="grey">
            <th id="selectAll1">
            <xsl:if test="Groups/Group[2]">
                <!--xsl:attribute name="name">selectAll1</xsl:attribute-->
                <input type="checkbox" title="Select or deselect all groups" onclick="selectAll(this.checked)"/>
            </xsl:if>
            </th>
            <th align="left">name</th>
            <th>Operation</th>
            </tr>
            </thead>
            <tbody>

            <xsl:apply-templates/>

            </tbody>
            </table>
            <xsl:if test="Groups/Group[2]">
                <table class="select-all">
                <tr>
                <th id="selectAll2">
                <input type="checkbox" title="Select or deselect all groups" onclick="selectAll(this.checked)"/>
                </th>
                <th align="left" colspan="7">Select All / None</th>
                </tr>
                </table>
            </xsl:if>
            <table id="btnTable" style="margin:20 0 0 0">
            <colgroup>
                <col span="8" width="100"/>
            </colgroup>
            <tr>
        <td>
          <input type="button" class="sbutton" id="exportBtn" name="action1" value="Export Users" disabled="true" onclick="return onExport();"/>
        </td>
            <td>
            <input type="button" class="sbutton" id="deleteBtn" name="action" value="Delete" disabled="true" onclick="return onDelete()"/>
            </td>
            <td>
            <input type="button" class="sbutton" id="addBtn" value="  Add  " onClick="javascript:go('/esp/files_/groupadd.html')"/>
            </td>
            </tr>
            </table>
            </form>
    </xsl:otherwise>
    </xsl:choose>
        </body>
        </html>
    </xsl:template>

    <xsl:template match="Groups">
        <xsl:apply-templates select="Group">
            <xsl:sort select="name"/>
        </xsl:apply-templates>
    </xsl:template>

    <xsl:template match="Group">
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
        <td>
        <input type="checkbox" name="groupnames_i{position()}" value="{name}" onclick="return clicked(this)"/>
        </td>
        <td align="left">
        <xsl:value-of select="name"/>
        </td>
        <td>
      <xsl:if test="name != 'Authenticated Users'">
        <a href="javascript:go('/ws_access/GroupEdit?groupname={name}')">members</a>
            <xsl:text disable-output-escaping="yes"> </xsl:text>
      </xsl:if>
      <a href="javascript:go('/ws_access/AccountPermissions?IsGroup=true&amp;AccountName={name}')">Permissions</a>
        </td>
        </tr>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>

</xsl:stylesheet>
