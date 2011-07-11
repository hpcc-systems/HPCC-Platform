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
  <xsl:param name="statusCode" select="/GetIndexInfoForDisplayResponse/StatusCode"/>
  <xsl:param name="statusMessage" select="/GetIndexInfoForDisplayResponse/StatusMessage" />
  <xsl:output method="html"/>
  <xsl:template match="GetIndexInfoForDisplayResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>

        <script language="JavaScript1.2">
          var originalFile = '<xsl:value-of select="OriginalFile" />';
          var parentIndex = 1;
          var defaultParent = '';

          function onLoad()
          {
          <xsl:for-each select="ParentFiles/ParentFile">
            if ('<xsl:value-of select="Name" />' == parent.window.parentName)
            {
              parentIndex = <xsl:value-of select="position()+1" />;
              parent.window.parentName = '<xsl:value-of select="Name" />';
            }
            </xsl:for-each>
            if (parent.window.parentName == '')
            {
              parent.window.parentName = defaultParent;
            }
            var childSelect = document.getElementById('ParentCell');
            if (childSelect)
            {
              var parentSelect = parent.document.getElementById('ParentCell');
              if (parentSelect)
              {
                parentSelect.innerHTML = childSelect.innerHTML;
                var selectList = parent.document.getElementById('ParentSelect');
                if (selectList)
                {
                  selectList.selectedIndex = parentIndex-1;
                }
                
              }
            }
            <xsl:text disable-output-escaping="yes"><![CDATA[
            if (parent.window.file != null && parent.window.file.length == 0 && parent.window.file != originalFile)
            {
              parent.window.file = originalFile;
              if (parent.window.file.length != 0)
              {
                parent.window.onFilesSelectByName(parent.window.file);
              }
            }
            ]]></xsl:text>
          }
        </script>
            </head>
            <body class="yui-skin-sam" onload="onLoad()">
        <table>
          <tr>
            <td id="ParentCell">
              <xsl:choose>
                <xsl:when test="count(./ParentFiles/*) = 0">
                  <select id="ParentSelect" name="ParentSelect" style="width:100%;">
                    <option value="None">None</option>
                  </select>
                </xsl:when>
                <xsl:when test="count(./ParentFiles/*) &gt; 0">
                  <select id="ParentSelect" name="ParentSelect" onchange="onParentSelect(this.value);" style="width:100%;">
                    <option value="None">None</option>
                    <xsl:apply-templates select="ParentFiles/*" />
                  </select>
                </xsl:when>
              </xsl:choose>
            </td>
          </tr>
        </table>
      </body>
        </html>
    </xsl:template>

  <xsl:template match="ParentFile">
    <option value="{Name}">
      <xsl:choose>
        <xsl:when test="string-length(Description)">
          <xsl:value-of select="Description"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="Name"/>
        </xsl:otherwise>
      </xsl:choose>
    </option>
  </xsl:template>

</xsl:stylesheet>
