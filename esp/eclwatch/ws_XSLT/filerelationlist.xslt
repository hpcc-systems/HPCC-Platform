<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    <xsl:output method="html"/>
  <xsl:template match="/FileRelationListResponse">
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
          <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
          <script language="JavaScript1.2" src="/esp/files/scripts/multiselect.js">&#160;</script>
        ]]></xsl:text>
        
                <script language="JavaScript1.2">
          var gobackURL = '<xsl:value-of select="/FileRelationListResponse/GoBackURL"/>';
          var fileName = '<xsl:value-of select="/FileRelationListResponse/FileName"/>';
          var clusterID = '<xsl:value-of select="/FileRelationListResponse/ClusterID"/>';
          var relationTypeID = '<xsl:value-of select="/FileRelationListResponse/RelationTypeID"/>';
          function onLoad()
          {
          var sectionDiv = document.getElementById("RoxieFileData");
          if (sectionDiv)
          {
          var parentSectionDiv = parent.document.getElementById("RoxieFileData");
          if (parentSectionDiv)
          {
          parentSectionDiv.innerHTML = sectionDiv.innerHTML;
          }
          }
          }

          <xsl:text disable-output-escaping="yes"><![CDATA[
          function go_back()
          {
          if (gobackURL == '')
          return;

          url = gobackURL + "?FileName=" + fileName + "&ClusterID=" + clusterID;
          url = url + ("&RelationTypeID=" + relationTypeID);
          go(url);

          return;
          }
          ]]></xsl:text>
        </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <form name="RoxieFileForm" >
          <input id="backBtn" type="button" value="Go Back" onclick="go_back()"/>
          <div id="RoxieFileData">
                        <xsl:choose>
                            <xsl:when test="string-length(/FileRelationListResponse/ExceptionMessage)">
                <xsl:value-of select="/FileRelationListResponse/ExceptionMessage"/>
                            </xsl:when>
              <xsl:when test="not(FileRelations/FileRelation[1])">
                No file relationship found.<br/><br/>
              </xsl:when>
              <xsl:otherwise>
                <table>
                  <tr>
                    <th>
                      Total <b>
                        <xsl:value-of select="/FileRelationListResponse/NumFiles"/>
                      </b> file relationships.
                    </th>
                  </tr>
                </table>

                <table class="sort-table" id="resultsTable">
                  <colgroup>
                    <col/>
                    <col/>
                    <col/>
                    <col/>
                  </colgroup>
                  <thead>
                    <tr>
                      <th>Index</th>
                      <th>Index Alias</th>
                      <xsl:choose>
                        <xsl:when test="(/FileRelationListResponse/RelationType = 'Parent File and Index')">
                          <th>Parent File</th>
                          <th>Parent File Alias</th>
                        </xsl:when>
                        <xsl:otherwise>
                          <th>Original File</th>
                          <th>Original File Alias</th>
                        </xsl:otherwise>
                      </xsl:choose>
                    </tr>
                  </thead>
                  <xsl:apply-templates select="FileRelations"/>
                </table>
                            </xsl:otherwise>
                        </xsl:choose>
                    </div>
          <!--xsl:if test="(FileRelations/FileRelation[40])">
            <input id="backBtn1" type="button" value="Go Back" onclick="go_back()"/>
          </xsl:if-->
        </form>
      </body>
        </html>
    </xsl:template>

  <xsl:template match="FileRelations">
    <xsl:apply-templates select="FileRelation">
      <xsl:sort select="Index"/>
    </xsl:apply-templates>
  </xsl:template>
  
    <xsl:template match="FileRelation">
    <xsl:variable name="href1">
      <xsl:value-of select="concat('/WsDfu/DFUInfo?Name=', Index)"/>
    </xsl:variable>
    <xsl:variable name="href2">
      <xsl:value-of select="concat('/WsDfu/DFUInfo?Name=', Primary)"/>
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
            <td align="left">
         <a title="File details..." href="{$href1}">
                  <xsl:value-of select="Index"/>
         </a>
            </td>
      <td align="left">
                 <xsl:value-of select="IndexName"/>
            </td>
      <td align="left">
        <a title="File details..." href="{$href2}">
          <xsl:value-of select="Primary"/>
        </a>
      </td>
      <td align="left">
        <xsl:value-of select="PrimaryName"/>
      </td>
    </tr>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
