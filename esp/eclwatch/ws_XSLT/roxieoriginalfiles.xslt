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
  <xsl:param name="statusCode" select="/QueryOriginalFilesResponse/StatusCode"/>
  <xsl:param name="statusMessage" select="/QueryOriginalFilesResponse/StatusMessage" />
  <xsl:output method="html"/>
  <xsl:template match="QueryOriginalFilesResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <link type="text/css" rel="StyleSheet" href="files_/css/espdefault.css"/>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>

        <script language="JavaScript1.2">
          var fileIndex = 1;
          var filesFound = true;
          <xsl:if test="count(./DFUFiles/*) = 0">
          filesFound = false;
          </xsl:if>

            <xsl:for-each select="DFUFiles/DFUFile">
          parent.window.FilesArray[<xsl:value-of select="position()" />] = '<xsl:value-of select="Name" />';
          if ('<xsl:value-of select="Name" />' == parent.window.file)
          {
            fileIndex = <xsl:value-of select="position()" />;
          }
          </xsl:for-each>

          <xsl:text disable-output-escaping="yes"><![CDATA[
          function onLoad()
          {
            var childFiles = document.getElementById('FilesCell');
            if (childFiles)
            {
              var parentFiles = parent.document.getElementById('FilesCell');
              if (parentFiles)
              {
                parentFiles.innerHTML = childFiles.innerHTML;
                var selectList = parent.document.getElementById('FilesSelect');
                if (selectList)
                {
                    if (!parent.window.loadFromSearchRoxieFiles)
                    {
                      if (parent.window.file.length == 0 && parent.window.key.length > 0)
                      {
                        selectList.selectedIndex = -1;
                      }
                      else
                      {
                        selectList.selectedIndex = fileIndex - 1;
                        if (filesFound)
                        {
                          parent.window.onFilesSelect(fileIndex);
                        }
                      }
                    }
                    else
                    {
                      selectList.selectedIndex = -1;
                      if (parent.window.name0.length > 0)
                      {
                        parent.window.fileLookupFromKey(); 
                      }
                    }
                }
              }
            }
          }
          ]]></xsl:text>


          </script>
            </head>
      <body class="yui-skin-sam" onload="onLoad()">
          <table>
            <tr>
              <td id="FilesCell">
                <xsl:choose>
                  <xsl:when test="count(./DFUFiles/*) = 0">
                    <select id="FilesSelect" name="FilesSelect" style="width:100%;">
                      <option value="None">No files on this cluster.</option>
                    </select>
                  </xsl:when>
                  <xsl:when test="$statusCode &gt; -1 and count(./DFUFiles/*) &gt; 0">
                    <select id="FilesSelect" name="FilesSelect" onchange="onFilesSelect(this.value);" style="width:100%;">
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
    <option value="{position()}">
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
