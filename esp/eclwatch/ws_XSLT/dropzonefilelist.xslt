<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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
    <xsl:output method="html"/>
    <xsl:variable name="sortby" select="/QueryFileListResponse/Sortby"/>
    <xsl:variable name="descending" select="/QueryFileListResponse/Descending"/>
    <xsl:variable name="parametersforsorting" select="/QueryFileListResponse/ParametersForSorting"/>

  <xsl:template match="/DropZoneFilesResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <xsl:text disable-output-escaping="yes"><![CDATA[
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
                    <link type="text/css" rel="styleSheet" href="/esp/files/css/sortabletable.css"/>
          <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
          <script language="JavaScript1.2" src="/esp/files/scripts/multiselect.js"></script>
        ]]></xsl:text>
        
                <script language="JavaScript1.2">
          function onLoad()
          {
          var sectionDiv = document.getElementById("DropzoneFileData");
          if (sectionDiv)
          {
          var parentSectionDiv = parent.document.getElementById("DropzoneFileData");
          if (parentSectionDiv)
          {
          parentSectionDiv.innerHTML = sectionDiv.innerHTML;
          }
          }

          //parent.window.initSelection('resultsTable');
          }
        </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <form name="DropzoneFileForm" >
                    <div id="DropzoneFileData">
                        <xsl:choose>
                            <xsl:when test="not(Files/File[1])">
                <br/><br/>No file found in this dropzone.<br/><br/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:apply-templates/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </div>
                </form>
      </body>
        </html>
    </xsl:template>
    
    <xsl:template match="Files">
            <table class="sort-table" id="resultsTable" width="800">
                <colgroup>
                    <col/>
          <col style="text-align:right"/>
          <col style="text-align:right"/>
                </colgroup>
                <thead>
                <tr class="grey">
                  <th>Name</th>
          <th>Size</th>
          <th width="180">Date</th>
                </tr>
                </thead>
                <tbody>
                <xsl:apply-templates select="File"/>
                </tbody>
            </table>
    </xsl:template>
    
    <xsl:template match="File">
    <xsl:variable name="size" select="number(filesize)"/>
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
        <!--xsl:choose>
          <xsl:when test="$size > 8000000">
            <xsl:value-of select="name"/>
          </xsl:when>
          <xsl:otherwise>
            <a href="javascript:go('/FileSpray/DownloadFile?Name={name}&amp;NetAddress={../../NetAddress}&amp;Path={../../Path}&amp;OS={../../OS}')">
              <xsl:value-of select="name"/>
            </a>
          </xsl:otherwise>
        </xsl:choose-->
        <a href="javascript:go('/FileSpray/DownloadFile?Name={name}&amp;NetAddress={../../NetAddress}&amp;Path={../../Path}&amp;OS={../../OS}')">
          <xsl:value-of select="name"/>
        </a>
            </td>
      <td align="right">
        <xsl:value-of select="filesize"/>
            </td>
      <td align="center">
        <xsl:value-of select="modifiedtime"/>
      </td>
    </tr>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
