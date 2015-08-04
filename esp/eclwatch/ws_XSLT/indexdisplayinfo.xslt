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
