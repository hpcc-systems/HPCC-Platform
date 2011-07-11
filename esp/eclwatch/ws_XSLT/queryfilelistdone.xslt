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
  <xsl:output method="html"/>
  <xsl:variable name="sortby" select="/QueryFileListResponse/Sortby"/>
  <xsl:variable name="descending" select="/QueryFileListResponse/Descending"/>
  <xsl:variable name="parametersforsorting" select="/QueryFileListResponse/ParametersForSorting"/>

  <xsl:template match="/QueryFileListResponse">
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
          var gobackURL = '<xsl:value-of select="/QueryFileListResponse/GoBackURL"/>';;
          function onLoad()
          {
            initSelection('resultsTable');

            var sectionDiv = document.getElementById("RoxieFileData");
            if (sectionDiv)
            {
              var parentSectionDiv = parent.document.getElementById("RoxieFileData");
              if (parentSectionDiv)
              {
                parentSectionDiv.innerHTML = sectionDiv.innerHTML;
              }
            }

            parent.window.initSelection('resultsTable');

            if (gobackURL != '')
              parent.document.getElementById('backBtn').disabled = false;
          }
        </script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <form name="RoxieFileForm" >
          <div id="RoxieFileData">
            <xsl:choose>
              <xsl:when test="Exception">
                Exception: <xsl:value-of select="Exception"/><br/><br/>
              </xsl:when>
              <xsl:when test="not(QueryFiles/RoxieDFULogicalFile[1])">
                No file found.<br/><br/>
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
    
  <xsl:template match="QueryFiles">
    <table>
      <tr>
        <th>Total <b><xsl:value-of select="/QueryFileListResponse/NumFiles"/></b> files.</th>
      </tr>
    </table>

    <table class="sort-table" id="resultsTable">
      <colgroup>
        <col/>
        <xsl:if test="/QueryFileListResponse/FileType='Non Super Files'">
          <col/>
        </xsl:if>
        <col/>
        <col/>
        <col/>
        <col class="number"/>
        <col class="number"/>
      </colgroup>
      <thead>
        <tr class="grey">
          <th/>
          <xsl:choose>
            <xsl:when test="$sortby='Name' and $descending &lt; 1">
              <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Name', 1)">Logical Name<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
            </xsl:when>
            <xsl:when test="$sortby='Name'">
              <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Name', 0)">Logical Name<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
            </xsl:when>
            <xsl:when test="$sortby!=''">
              <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Name', 0)">Logical Name</th>
            </xsl:when>
            <xsl:otherwise>
              <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Name', 1)">Logical Name</th>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:if test="/QueryFileListResponse/FileType='Non Super Files'">
            <xsl:choose>
              <xsl:when test="$sortby='Type' and $descending &lt; 1">
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Type', 1)">Type<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
              </xsl:when>
              <xsl:when test="$sortby='Type'">
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Type', 0)">Type<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
              </xsl:when>
              <xsl:otherwise>
                <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Type', 1)">Type</th>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:if>
          <xsl:choose>
            <xsl:when test="$sortby='Size' and $descending &lt; 1">
              <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Size', 1)">Size<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
            </xsl:when>
            <xsl:when test="$sortby='Size'">
              <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Size', 0)">Size<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
            </xsl:when>
            <xsl:otherwise>
              <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Size', 1)">Size</th>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:choose>
            <xsl:when test="$sortby='Records' and $descending &lt; 1">
              <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Records', 1)">Records<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
            </xsl:when>
            <xsl:when test="$sortby='Records'">
              <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Records', 0)">Records<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
            </xsl:when>
            <xsl:otherwise>
              <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Records', 1)">Records</th>
            </xsl:otherwise>
          </xsl:choose>
          <xsl:choose>
            <xsl:when test="$sortby='Parts' and $descending &lt; 1">
              <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Parts', 1)">Parts<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
            </xsl:when>
            <xsl:when test="$sortby='Parts'">
              <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Parts', 0)">Parts<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
            </xsl:when>
            <xsl:otherwise>
              <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Parts', 0)">Parts</th>
            </xsl:otherwise>
          </xsl:choose>
        </tr>
      </thead>
      <tbody>
        <xsl:apply-templates select="RoxieDFULogicalFile"/>
      </tbody>
    </table>
  </xsl:template>
    
  <xsl:template match="RoxieDFULogicalFile">
    <xsl:variable name="href">
      <xsl:value-of select="concat('/WsRoxieQuery/QueryFileDetails?LogicalName=', Name, '&amp;Cluster=', ClusterName, '&amp;Query=', Directory, '&amp;Type=', Description)"/>
    </xsl:variable>
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
        <input type="checkbox" name="IDs_i{position()}" value="{Name}@{ClusterName}" onclick="return clicked(this)"/>
        <xsl:variable name="popup">
          return QueryFilePopup('<xsl:value-of select="Name"/>', '<xsl:value-of select="ClusterName"/>', '<xsl:value-of select="Directory"/>', '<xsl:value-of select="Description"/>', '<xsl:value-of select="BrowseData"/>', '<xsl:value-of select="position()"/>')
        </xsl:variable>
        <xsl:attribute name="oncontextmenu">
          <xsl:value-of select="$popup"/>
        </xsl:attribute>
        <img id="mn{position()}" class="menu1" src="/esp/files/img/menu1.png" onclick="{$popup}"/>
      </td>
      <td align="left">
        <xsl:choose>
          <xsl:when test="isSuperfile=1">
            <I><b>
            <xsl:value-of select="Name"/>
            </b></I>
          </xsl:when>
          <xsl:otherwise>
            <a title="File details..." href="{$href}">
              <xsl:value-of select="Name"/>
            </a>
          </xsl:otherwise>
        </xsl:choose>
      </td>
      <xsl:if test="/QueryFileListResponse/FileType='Non Super Files'">
        <td>
        <xsl:if test="Description='IndexFile'">
          <xsl:value-of select="Description"/>
        </xsl:if>
        </td>
      </xsl:if>
      <td>
        <xsl:value-of select="Totalsize"/>
      </td>
      <td>
        <xsl:value-of select="RecordCount"/>
      </td>
      <td>
        <xsl:value-of select="Parts"/>
      </td>
    </tr>
  </xsl:template>

  <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
