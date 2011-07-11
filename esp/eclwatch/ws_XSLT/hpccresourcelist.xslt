<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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

  <xsl:template match="/BrowseResourcesResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <xsl:text disable-output-escaping="yes"><![CDATA[
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
                    <link type="text/css" rel="styleSheet" href="/esp/files/css/sortabletable.css"/>
          <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
          <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
          <script type="text/javascript" src="/esp/files/scripts/sortabletable.js">
           <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
          </script>
          <script type="text/javascript" src="/esp/files/scripts/multiselect.js">></script>
        ]]></xsl:text>
        
                <script language="JavaScript1.2">
          function onLoad()
          {
            initSelection('resultsTable');
            var table = document.getElementById('resultsTable');
            if (table)
            {
              sortableTable = new SortableTable(table, table, ["String", "String", "String", "String", "String"]);
            }
          }
        </script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <form id="HPCCResourceForm" name="HPCCResourceForm" >
          <xsl:choose>
            <xsl:when test="not(HPCCResourceRepositories/HPCCResourceRepository[1])">
              <br/><br/>No resource found from your installation.<br/><br/>
              <xsl:if test="string-length(PortalURL)">
                <br/>You may visit <a href="{PortalURL}" target="_blank">
                  <xsl:value-of select="PortalURL"/>
                </a> for resources.
              </xsl:if>
            </xsl:when>
            <xsl:otherwise>
              <br/>
              <b>Click a link below to download a version from your installation.
              <xsl:if test="string-length(PortalURL)">
                <br/>You may visit <a href="{PortalURL}" target="_blank">
                <xsl:value-of select="PortalURL"/>
                </a> for other versions.
              </xsl:if>
              </b><br/><br/>
              <xsl:apply-templates/>
            </xsl:otherwise>
          </xsl:choose>
        </form>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="HPCCResourceRepositories">
    <table>
      <xsl:apply-templates select="HPCCResourceRepository"/>
    </table>
  </xsl:template>

  <xsl:template match="HPCCResourceRepository">
    <tr>
      <td>
        <b>
          <xsl:value-of select="Name"/>:
        </b>
      </td>
    </tr>
    <tr>
      <td>
        <xsl:apply-templates select="HPCCResources"/>
      </td>
    </tr>
    <tr>
      <td>
      </td>
    </tr>
  </xsl:template>

  <xsl:template match="HPCCResources">
            <table class="sort-table" id="resultsTable" width="800">
                <colgroup>
          <col width="200"/>
          <col width="100"/>
          <col width="500"/>
                </colgroup>
                <thead>
                <tr class="grey">
          <th>Name</th>
          <th>Version</th>
          <th>Description</th>
        </tr>
                </thead>
                <tbody>
          <xsl:apply-templates select="HPCCResource"/>
                </tbody>
            </table>
    </xsl:template>
    
    <xsl:template match="HPCCResource">
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
        <xsl:choose>
          <xsl:when test="string-length(Name)">
            <a href="javascript:go('/FileSpray/DownloadFile?Name={FileName}&amp;Path={../../Path}&amp;NetAddress={../../../../NetAddress}&amp;OS={../../../../OS}')">
              <xsl:value-of select="Name"/>
            </a>
          </xsl:when>
          <xsl:otherwise>
            <a href="javascript:go('/FileSpray/DownloadFile?Name={FileName}&amp;Path={../../Path}&amp;NetAddress={../../../../NetAddress}&amp;OS={../../../../OS}')">
              <xsl:value-of select="FileName"/>
            </a>
          </xsl:otherwise>
        </xsl:choose>
      </td>
      <td align="left">
        <xsl:value-of select="Version"/>
        </td>
      <td align="left">
        <xsl:value-of select="Description"/>
      </td>
    </tr>
  </xsl:template>

    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
