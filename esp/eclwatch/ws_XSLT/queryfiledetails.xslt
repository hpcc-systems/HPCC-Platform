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
    <xsl:variable name="fileType" select="/QueryFileDetailsResponse/Type"/>
    <xsl:template match="/QueryFileDetailsResponse">
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
        
                <script language="JavaScript1.2" id="menuhandlers">
                    var filetype = '<xsl:value-of select="/QueryFileDetailsResponse/Type"/>';;
                    var cluster = '<xsl:value-of select="/QueryFileDetailsResponse/Cluster"/>';

          <xsl:text disable-output-escaping="yes"><![CDATA[
          var oMenu;
                    function QueryFilePopup(id, query, PosId)
                    {
                        function details()
                        {
                            if (filetype == 'SuperKeyFile')
                                document.location.href='/WsRoxieQuery/QueryFileDetails?LogicalName='+ id + '&Cluster=' + cluster
                                    + '&Query=' + query + '&Type=NonSuperKeyFile';
                            else
                                document.location.href='/WsRoxieQuery/QueryFileDetails?LogicalName='+ id + '&Cluster=' + cluster
                                    + '&Query=' + query + '&Type=SuperKeyFile';
                        }
                        function showQueries()
                        {
                            document.location.href='/WsRoxieQuery/RoxieQueryList?LogicalName='+ id + '&Cluster=' + cluster;
                        }
                        var xypos = YAHOO.util.Dom.getXY('mn' + PosId);
            if (oMenu)
            {
            oMenu.destroy();
            }
            oMenu = new YAHOO.widget.Menu("logicalfilecontextmenu", {position: "dynamic", xy: xypos} );
            oMenu.clearContent();

                      oMenu.addItems([
                    { text: "Details", onclick: { fn: details } },
                    { text: "ShowQueries", onclick: { fn: showQueries } }
                    ]);
                        
                        //showPopup(menu,(window.event ? window.event.screenX : 0),  (window.event ? window.event.screenY : 0));
            oMenu.render("roxiequerylistmenu");
            oMenu.show();
                        return false;
                    }

                        function onLoad()
                        {
                            initSelection('resultsTable');
                        }

                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <xsl:choose>
                    <xsl:when test="$fileType = 'SuperKeyFile'">
                        <h3>Super Key Name<xsl:if test="LogicalName/text()">: <xsl:value-of select="LogicalName"/></xsl:if></h3>
                        <br/>
                        <form>
                            <table style="text-align:left;" cellspacing="10">
                                <colgroup style="vertical-align:top;padding-right:10px;" span="2"/>
                                <tr>
                                    <th>Total record count:</th>
                                    <td><xsl:value-of select="RecordCount"/></td>
                                </tr>
                                <tr>
                                    <th>Total Size:</th>
                                    <td><xsl:value-of select="Totalsize"/></td>
                                </tr>
                            </table>
                        </form>
                        <br/>
                        <form>
                            <xsl:choose>
                                <xsl:when test="(QuerySuperFiles/RoxieDFULogicalFile[1])">
                                    <table>
                                        <tr>
                                            <td><h4>Subfile list:</h4></td>
                                        </tr>
                                        <tr>
                                            <td><xsl:apply-templates select="QuerySuperFiles"/></td>
                                        </tr>
                                    </table>
                                </xsl:when>
                                <xsl:otherwise>
                                    No further information found.
                                </xsl:otherwise>
                            </xsl:choose>
                        </form>
                    </xsl:when>
                    <xsl:otherwise>
                        <h3>File Name<xsl:if test="LogicalName/text()">: <xsl:value-of select="LogicalName"/></xsl:if></h3>
                        <br/>
                        <xsl:choose>
                            <xsl:when test="(QueryFiles/RoxieDFULogicalFile[1])">
                                <form>
                                    <table>
                                        <tr>
                                            <td><h4>Superfile list:</h4></td>
                                        </tr>
                                        <tr>
                                            <td><xsl:apply-templates select="QueryFiles"/></td>
                                        </tr>
                                    </table>
                                </form>
                            </xsl:when>
                            <xsl:otherwise>
                                    No further information found.
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
                <input id="backBtn" type="button" value="Go Back" onclick="history.go(-1)"> </input>
        <div id="roxiequerylistmenu" />
      </body>
        </html>
    </xsl:template>
    

    <xsl:template match="QuerySuperFiles">
        <form id="listitems">
            <table class="sort-table" id="resultsTable">
                <colgroup>
                    <col/>
                    <col/>
                    <col class="number"/>
                    <col class="number"/>
                </colgroup>
                <thead>
                <tr class="grey">
                    <th></th>
                    <th>Logical Name</th>
                    <th>Record Count</th>
                    <th>Tital Size</th>
                </tr>
                </thead>
                <tbody>
                <xsl:apply-templates select="RoxieDFULogicalFile"/>
                </tbody>
            </table>
        </form>
    </xsl:template>
    
    <xsl:template match="QueryFiles">
        <form id="listitems">
            <table class="sort-table" id="resultsTable">
                <colgroup>
                    <col/>
                    <col/>
                    <col/>
                </colgroup>
                <thead>
                <tr class="grey">
                    <th></th>
                    <th>Logical Name</th>
                    <th>Query Name</th>
                </tr>
                </thead>
                <tbody>
                <xsl:apply-templates select="RoxieDFULogicalFile"/>
                </tbody>
            </table>
        </form>
    </xsl:template>
    
    <xsl:template match="RoxieDFULogicalFile">
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
            <xsl:choose>
                <xsl:when test="string($fileType)='SuperKeyFile'">
                    <td>
            <xsl:variable name="popup">
              return QueryFilePopup('<xsl:value-of select="Name"/>', '<xsl:value-of select="Directory"/>', '<xsl:value-of select="position()"/>')
            </xsl:variable>
            <xsl:attribute name="oncontextmenu">
              <xsl:value-of select="$popup"/>
            </xsl:attribute>
            <xsl:if test="string-length(Name)">
              <img id="mn{position()}" class="menu1" src="/esp/files/img/menu1.png" onclick="{$popup}"></img>
            </xsl:if>
                </td>
                    <td align="left">
                        <xsl:value-of select="Name"/>
                    </td>
                    <td>
                        <xsl:value-of select="RecordCount"/>
                    </td>
                    <td>
                        <xsl:value-of select="Totalsize"/>
                    </td>
                </xsl:when>
                <xsl:otherwise>
                    <td>
            <xsl:variable name="popup">
            return QueryFilePopup('<xsl:value-of select="Name"/>', '<xsl:value-of select="Directory"/>', '<xsl:value-of select="position()"/>')
          </xsl:variable>
          <xsl:attribute name="oncontextmenu">
            <xsl:value-of select="$popup"/>
          </xsl:attribute>
          <xsl:if test="string-length(Name)">
            <img id="mn{position()}" class="menu1" src="/esp/files/img/menu1.png" onclick="{$popup}"></img>
          </xsl:if>
          </td>
                    <td align="left">
                        <xsl:value-of select="Name"/>
                    </td>
                    <td>
                        <xsl:value-of select="Directory"/>
                    </td>
                </xsl:otherwise>
            </xsl:choose>
        </tr>
    </xsl:template>
    
    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
