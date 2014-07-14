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
          <script type="text/javascript" src="/esp/files/scripts/multiselect.js">></script>
        ]]></xsl:text>
        
                <script language="JavaScript1.2">
          function onLoad()
          {
            initSelection('resultsTable');
          }
        </script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <form id="HPCCResourceForm" name="HPCCResourceForm" >
            <xsl:choose>
                <xsl:when test="UseResource = 0">
                    <xsl:choose>
                        <xsl:when test="string-length(PortalURL)">
                            Please visit <a href="{PortalURL}" target="_blank">
                              <xsl:value-of select="PortalURL"/>
                            </a> for resources.
                        </xsl:when>
                        <xsl:otherwise>
                            No web link is configured for downloading resources.
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:choose>
                        <xsl:when test="not(HPCCResourceRepositories/HPCCResourceRepository[1])">
                            <br/><br/>No resource found from your installation.<br/><br/>
                            <xsl:choose>
                                <xsl:when test="string-length(PortalURL)">
                                <br/>You may visit <a href="{PortalURL}" target="_blank">
                                    <xsl:value-of select="PortalURL"/>
                                </a> for resources.
                                </xsl:when>
                                <xsl:otherwise>
                                    No web link is configured for downloading resource.
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        <xsl:otherwise>
                            <br/>
                            <b>
                                Click a link below to download a version from your installation.
                                <xsl:if test="string-length(PortalURL)">
                                    <br/>You may visit <a href="{PortalURL}" target="_blank">
                                        <xsl:value-of select="PortalURL"/>
                                    </a> for other versions.
                                </xsl:if>
                            </b>
                            <br/>
                            <br/>
                            <xsl:apply-templates/>
                        </xsl:otherwise>
                    </xsl:choose>
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
