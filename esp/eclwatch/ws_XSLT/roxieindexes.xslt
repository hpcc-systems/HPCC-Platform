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
  <xsl:param name="statusCode" select="/QueryIndexesForOriginalFileResponse/StatusCode"/>
  <xsl:param name="statusMessage" select="/QueryIndexesForOriginalFileResponse/StatusMessage" />
  <xsl:output method="html"/>
  <xsl:template match="QueryIndexesForOriginalFileResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>

        <script language="JavaScript1.2">
          var keyIndex = 1;
          var defaultKey = '';
          var renamedFrom = '';
          
          function onLoad()
          {
          <xsl:for-each select="DFUFiles/DFUFile">
            if ('<xsl:value-of select="Name" />' == parent.window.selectedKey)
              {
              keyIndex = <xsl:value-of select="position()" />;
              parent.window.selectedKey = '<xsl:value-of select="Name" />';
              if (parent.window.key == '') {
                parent.window.key = '<xsl:value-of select="Name" />';
              }
              renamedFrom = '<xsl:value-of select="RenamedFromList/RenamedFrom" />';
            }
              <xsl:if test="position() = 1">
            defaultKey = '<xsl:value-of select="Name" />';
              </xsl:if>
            </xsl:for-each>
            if (parent.window.selectedKey == '')
            {
              parent.window.selectedKey = defaultKey;
              parent.window.key = defaultKey;;
            }

          <xsl:text disable-output-escaping="yes"><![CDATA[
            var childKeys = document.getElementById('KeysCell');
            if (childKeys)
            {
              var parentKeys = parent.document.getElementById('KeysCell');
              if (parentKeys)
              {
                parentKeys.innerHTML = childKeys.innerHTML;
                var selectList = parent.document.getElementById('KeysSelect');
                if (selectList)
                {
                  selectList.selectedIndex = keyIndex-1;
                  parent.document.getElementById('loadfields').disabled = '';
                }
              }
            }
            if (!parent.window.msgToDisplay.length > 0) {
              // if there's no error check the rename.
              if (!parent.window.layoutLoaded && parent.window.key.length > 0)
              {
                //parent.window.BrowseRoxieFile(parent.window.key);
              }
              else
              {
                parent.window.fileLookupFromKey();
                //parent.window.BrowseRoxieFile(parent.window.key);
              }
            }
            else {
              // on error, if there's an alternative, then set parent.window.key to that and leave the selected key.
              if (parent.window.key != renamedFrom) {
                parent.window.key = renamedFrom;;
                //parent.window.BrowseRoxieFile(parent.window.key);
              }
            }
          }
          ]]></xsl:text>


          </script>
            </head>
      <body class="yui-skin-sam" onload="onLoad()">
          <table>
            <tr>
              <td id="KeysCell">
                <xsl:choose>
                  <xsl:when test="count(./DFUFiles/*) = 0">
                    <select id="KeysSelect" name="KeysSelect" style="width:100%;">
                      <option value="None">
                        There are no indexes for this file.
                      </option>
                    </select>
                  </xsl:when>
                  <xsl:when test="$statusCode &gt; -1 and count(./DFUFiles/*) &gt; 0">
                    <select id="KeysSelect" name="KeysSelect" onchange="onKeysSelect(this.value);" style="width:100%;">
                      <xsl:apply-templates select="DFUFiles/*" />
                    </select>
                  </xsl:when>
                  <xsl:when test="$statusCode &lt; 0">
                    Error: <xsl:value-of select="$statusMessage" />
                  </xsl:when>
                </xsl:choose>
              </td>
            </tr>
          </table>
      </body>
        </html>
    </xsl:template>

  <xsl:template match="DFUFile">
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
