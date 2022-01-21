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
    <xsl:output method="html"/>
    <xsl:variable name="num" select="/RoxieQueryListResponse/NumFiles"/>
    
    <xsl:template match="/ListRoxieQueryForFilesResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Roxie Queries</title>
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
                <script language="JavaScript1.2" id="menuhandlers">
          <xsl:text disable-output-escaping="yes"><![CDATA[
            var oMenu;
            function RoxieQueryPopup(id, cluster, posid)
            {
              function details()
              {
                document.location.href='/WsRoxieQuery/QueryDetails?QueryID='+ id + '&Cluster=' + cluster;
                          }
                          function showAllFiles()
                          {
                              document.location.href='/WsRoxieQuery/QueryFileList?FileType=All Files&QueryID='+ id + '&Cluster=' + cluster;
                          }
                          function showSuperFiles()
                          {
                              document.location.href='/WsRoxieQuery/QueryFileList?FileType=Super Files&QueryID='+ id + '&Cluster=' + cluster;
                          }
                          function showSubFiles()
                          {
                              document.location.href='/WsRoxieQuery/QueryFileList?FileType=Sub-Files&QueryID='+ id + '&Cluster=' + cluster;
                          }
                          function showDataFiles()
                          {
                              document.location.href='/WsRoxieQuery/QueryFileList?FileType=Non Super Files&QueryID='+ id + '&Cluster=' + cluster;
                          }

              var xypos = YAHOO.util.Dom.getXY('mn' + posid);
              if (oMenu)
              {
                oMenu.destroy();
              }
              oMenu = new YAHOO.widget.Menu("logicalfilecontextmenu", {position: "dynamic", xy: xypos} );
              oMenu.clearContent();

              oMenu.addItems([
              { text: "Details", onclick: { fn: details } },
              //{ text: "ShowAllFiles", onclick: { fn: showAllFiles } },
              { text: "ShowSuperFiles", onclick: { fn: showSuperFiles } },
              //{ text: "ShowSubFiles", onclick: { fn: showSubFiles } },
              { text: "ShowNonSuperFiles", onclick: { fn: showDataFiles } }
              ]);

              oMenu.render("listroxiequerymenu");
              oMenu.show();
              return false;
            }
                        function onLoad()
                        {
                            initSelection('resultsTable');
                            initSelection('resultsTable1');
                            initSelection('resultsTable2');
                        }       
          ]]></xsl:text>                      
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <h3>Roxie Queries for Selected Files</h3>
                <xsl:apply-templates/>
        <div id="listroxiequerymenu" />
            </body>
        </html>
    </xsl:template>
    

    <xsl:template match="RoxieClusterFileQueries">
        <table class="sort-table" id="resultsTable">
            <colgroup>
                <col width="20%"/>
                <col width="40%"/>
                <col width="40%"/>
            </colgroup>
            <thead>
            <tr class="grey">
                <th>Cluster Name</th>
                <th>File Name</th>
                <th>Query Name</th>
            </tr>
            </thead>
            <tbody>
            <xsl:apply-templates select="RoxieClusterFileQuery"/>
            </tbody>
        </table>
    </xsl:template>
    
    <xsl:template match="RoxieClusterFileQuery">
        <xsl:variable name="cluster"><xsl:value-of select="ClusterName"/></xsl:variable>
        <tr>
            <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
            <td align="left">
                <xsl:value-of select="ClusterName"/>
            </td>
            <td/>
            <td/>
        </tr>
        <xsl:for-each select="RoxieFileQueries/RoxieFileQuery">
            <tr>
                <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                <td/>
                <td align="left">
                    <xsl:value-of select="FileName"/>
                </td>
                <td/>
            </tr>
            <xsl:for-each select="QueryNames/QueryName">
                <tr>
                    <xsl:choose>
                        <xsl:when test="position() mod 2">
                            <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                        </xsl:otherwise>
                    </xsl:choose>    
                    <td/>
                    <td/>
                    <td align="left">
            <xsl:variable name="popup1">return RoxieQueryPopup('<xsl:value-of select="."/>', '<xsl:value-of select="$cluster"/>', '<xsl:value-of select="position()"/>')</xsl:variable>
                        <xsl:attribute name="oncontextmenu"><xsl:value-of select="$popup1"/></xsl:attribute>
                        <img id="mn{position()}" class="menu1" src="/esp/files/img/menu1.png" onclick="{$popup1}"></img>
                        <xsl:value-of select="."/>
                    </td>
                </tr>
            </xsl:for-each>
        </xsl:for-each>
    </xsl:template>
    
    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
