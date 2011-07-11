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
    <xsl:variable name="num" select="/RoxieQueryListResponse/NumFiles"/>
    <xsl:variable name="clustername" select="/RoxieQueryListResponse/Cluster"/>
    <xsl:variable name="logicalname" select="/RoxieQueryListResponse/LogicalName"/>
    <xsl:variable name="pagesize" select="/RoxieQueryListResponse/PageSize"/>
    <xsl:variable name="lastpagefrom" select="/RoxieQueryListResponse/LastPageFrom"/>
    <xsl:variable name="prevpagefrom" select="/RoxieQueryListResponse/PrevPageFrom"/>
    <xsl:variable name="nextpagefrom" select="/RoxieQueryListResponse/NextPageFrom"/>
    <xsl:variable name="sortby" select="/RoxieQueryListResponse/Sortby"/>
    <xsl:variable name="descending" select="/RoxieQueryListResponse/Descending"/>
    <xsl:variable name="parametersforpaging" select="/RoxieQueryListResponse/ParametersForPaging"/>
    <xsl:variable name="parametersforsorting" select="/RoxieQueryListResponse/ParametersForSorting"/>

    <xsl:template match="/RoxieQueryListResponse">
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
          <script language="JavaScript1.2" src="/esp/files/scripts/multiselect.js"></script>
          <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
        ]]></xsl:text>

                <script language="JavaScript1.2" id="menuhandlers">
                    var cluster = '<xsl:value-of select="$clustername"/>';;
                    var logicalName = '<xsl:value-of select="$logicalname"/>';;
                    var parametersForSorting='<xsl:value-of select="$parametersforsorting"/>';
          var graphWuid = '<xsl:value-of select="WUID"/>';

          <xsl:text disable-output-escaping="yes"><![CDATA[
          var oMenu;

          function RoxieQueryPopup(id, PosId, Wuid)
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
                        function showGVCGraph()
                        {
              //document.location.href='/WsWorkunits/GVCAjaxGraph?Name=' + Wuid + '&GraphName=graph1';
              document.location.href='/WsRoxieQuery/GVCAjaxGraph?Name=' + id + '&GraphName=graph1&Cluster=' + cluster;
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
          { text: "ShowSuperFiles", onclick: { fn: showSuperFiles }, disabled: true },
          { text: "ShowNonSuperFiles", onclick: { fn: showDataFiles }, disabled: true },
          { text: "ShowGVCGraph", onclick: { fn: showGVCGraph }, disabled: (Wuid.length<1 ? true : false) }
          ]);

          oMenu.render("roxiequerylistmenu");
          oMenu.show();
          return false;
          }
        ]]></xsl:text>
        </script>

                <script language="JavaScript1.2">
                    function ChangeHeader(o1, headerid)
                    {
                        if (headerid%2)
                        {
                            o1.bgColor = '#CCCCCC';
                        }
                        else
                        {
                            o1.bgColor = '#F0F0FF';
                        }
                    }
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        function onRowCheck(checked)
                        {
                            document.getElementById("ListFilesBtn").disabled = checkedCount == 0;
                        }   
                        function selectAll1(select)
                        {   
                            document.getElementById("All1").checked = select;
                            selectAll(select);
                        }
                        function getSelected(o)
                        {
                            if (o.tagName=='INPUT')
                                return o.checked ? '\n'+o.value : '';
                        
                            var s='';
                            var ch=o.children;
                            if (ch)
                                for (var i in ch)
                                s=s+getSelected(ch[i]);
                             return s;
                        }
                        function headerClicked(headername, descending)
                        {
                            if (parametersForSorting)
                                document.location.href='/WsRoxieQuery/RoxieQueryList?'+parametersForSorting+'&Sortby='+headername+'&Descending='+descending;
                            else
                                document.location.href='/WsRoxieQuery/RoxieQueryList?Sortby='+headername+'&Descending='+descending;
                        }                
                        
                        function onLoad()
                        {
                            initSelection('resultsTable');
                        }                             
                        function getNewPage()
                        {
                            var startFrom = document.getElementById("PageStartFrom").value;
                            var pageEndAt = document.getElementById("PageEndAt").value;
                            if (startFrom < 1)
                                startFrom = 1;
                            if ((pageEndAt < 1) || (pageEndAt - startFrom < 0))
                                pageEndAt = 100;
                            var size = pageEndAt - startFrom + 1;

                            document.location.href = '/WsRoxieQuery/RoxieQueryList?PageStartFrom='+startFrom+'&amp;PageSize='+size;

                            return false;
                        }
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam" onload="nof5();onLoad()">
                <xsl:choose>
                    <xsl:when test="string-length(LogicalName)">
                        <h3>Roxie Queries for File: <xsl:value-of select="LogicalName"/> on Cluster: <xsl:value-of select="Cluster"/> </h3>
                    </xsl:when>
                    <xsl:otherwise>
                        <h3>Roxie Queries on Cluster: <xsl:value-of select="Cluster"/> </h3>
                    </xsl:otherwise>
                </xsl:choose>
        <xsl:choose>
                    <xsl:when test="not(RoxieQueries/RoxieQuery[1])">
            No Query found.<br/><br/>
          </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates/>
                    </xsl:otherwise>
                </xsl:choose>
                <input id="backBtn" type="button" value="Go Back" onclick="history.go(-1)"> </input>
        <div id="roxiequerylistmenu" />
            </body>
        </html>
    </xsl:template>
    

    <xsl:template match="RoxieQueries">
        <form id="newpageform" onsubmit="return getNewPage()" method="post">
            <tr><th>Total <b><xsl:value-of select="/RoxieQueryListResponse/NumFiles"/></b> queries.</th></tr>
            <!--tr><th>Total <b><xsl:value-of select="/RoxieQueryListResponse/NumFiles"/></b> queries. Current page starts from:</th>
            <td><input type="text" id="PageStartFrom" name="PageStartFrom" value="{/RoxieQueryListResponse/PageStartFrom}" size="10"/></td>
            <th> to:</th>
            <td><input type="text" id="PageEndAt" name="PageEndAt" value="{/RoxieQueryListResponse/PageEndAt}" size="10"/></td>
            <td><input type="submit" class="sbutton" value="Submit"/></td></tr-->
        </form>
        <form id="listitems" action="/WsRoxieQuery/QueriesAction" method="post">
            <table class="sort-table" id="resultsTable">
                <colgroup>
                    <col/>
                    <col/>
                    <col/>
                    <col/>
                    <col/>
                    <col/>
                    <col/>
                </colgroup>
                <thead>
                <tr class="grey">
                    <th>
                        <!--xsl:if test="$num > 1">
                            <xsl:attribute name="id">selectAll1</xsl:attribute>
                            <input type="checkbox" id="All1" title="Select or deselect all logical files" onclick="selectAll1(this.checked)"/>
                        </xsl:if-->
                    </th>
                    <xsl:choose>
                       <xsl:when test="$sortby='ID' and $descending &lt; 1">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ID', 1)">ID<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='ID'">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ID', 0)">ID<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby!=''">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ID', 0)">ID</th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ID', 1)">ID</th>
                       </xsl:otherwise>
                   </xsl:choose>
                    <xsl:choose>
                       <xsl:when test="$sortby='WUID' and $descending &lt; 1">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 1)">WUID<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='WUID'">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 0)">WUID<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 0)">WUID</th>
                       </xsl:otherwise>
                   </xsl:choose>
                    <xsl:choose>
                       <xsl:when test="$sortby='DeployedBy' and $descending &lt; 1">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('DeployedBy', 1)">Deployed By<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='DeployedBy'">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('DeployedBy', 0)">Deployed By<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('DeployedBy', 0)">Deployed By</th>
                       </xsl:otherwise>
                   </xsl:choose>
                    <!--xsl:choose>
                       <xsl:when test="$sortby='Cluster' and $descending &lt; 1">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Cluster', 1)">Cluster<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Cluster'">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Cluster', 0)">Cluster<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Cluster', 0)">Cluster</th>
                       </xsl:otherwise>
                   </xsl:choose-->
                    <xsl:choose>
                       <xsl:when test="$sortby='HighPriority' and $descending &lt; 1">
                          <th width="10" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('HighPriority', 1)">High Priority<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='HighPriority'">
                          <th width="10" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('HighPriority', 0)">High Priority<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th width="10" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('HighPriority', 0)">High Priority</th>
                       </xsl:otherwise>
                   </xsl:choose>
                    <xsl:choose>
                       <xsl:when test="$sortby='Suspended' and $descending &lt; 1">
                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Suspended', 1)">Suspended<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Suspended'">
                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Suspended', 0)">Suspended<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Suspended', 0)">Suspended</th>
                       </xsl:otherwise>
                   </xsl:choose>
                    <!--xsl:choose>
                       <xsl:when test="$sortby='UpdatedBy' and $descending &lt; 1">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('UpdatedBy', 1)">Suspended By<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='UpdatedBy'">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('UpdatedBy', 0)">Suspended By<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('UpdatedBy', 0)">Suspended By</th>
                       </xsl:otherwise>
                   </xsl:choose>
                    <xsl:choose>
                       <xsl:when test="$sortby='Label' and $descending &lt; 1">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Label', 1)">Label<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Label'">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Label', 0)">Label<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Label', 0)">Label</th>
                       </xsl:otherwise>
                   </xsl:choose>
                    <xsl:choose>
                       <xsl:when test="$sortby='Asso' and $descending &lt; 1">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Asso', 1)">Asso. Name<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Asso'">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Asso', 0)">Asso. Name<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Asso', 0)">Asso. Name</th>
                       </xsl:otherwise>
                   </xsl:choose-->
                    <xsl:choose>
                       <xsl:when test="$sortby='HasAliases' and $descending &lt; 1">
                          <th width="10" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('HasAliases', 1)">Has Aliases<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='HasAliases'">
                          <th width="10" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('HasAliases', 0)">Has Aliases<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th width="10" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('HasAliases', 0)">Has Aliases</th>
                       </xsl:otherwise>
                   </xsl:choose>
                    <!--xsl:choose>
                       <xsl:when test="$sortby='Error' and $descending &lt; 1">
                          <th align="center" width="10" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Error', 1)">Has Error<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Error'">
                          <th align="center" width="10" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Error', 0)">Has Error<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="center" width="10" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Error', 0)">Has Error</th>
                       </xsl:otherwise>
                   </xsl:choose-->
                    <xsl:choose>
                       <xsl:when test="$sortby='Comment' and $descending &lt; 1">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Comment', 1)">Comment<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Comment'">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Comment', 0)">Comment<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Comment', 0)">Comment</th>
                       </xsl:otherwise>
                   </xsl:choose>
                </tr>
                </thead>
                <tbody>
                <xsl:apply-templates select="RoxieQuery"/>
                </tbody>
            </table>
            <!--tr>
                <xsl:if test="$prevpagefrom &gt; 0">
                    <td><a href="javascript:go('/WsRoxieQuery/RoxieQueryList?{$parametersforpaging}')">First</a></td>
                    <td><a href="javascript:go('/WsRoxieQuery/RoxieQueryList?{$parametersforpaging}&amp;PageStartFrom={$prevpagefrom}')">Prev</a></td>
                </xsl:if>
                <xsl:if test="$nextpagefrom &gt; 0">
                    <td><a href="javascript:go('/WsRoxieQuery/RoxieQueryList?{$parametersforpaging}&amp;PageStartFrom={$nextpagefrom}')">Next</a></td>
                    <td><a href="javascript:go('/WsRoxieQuery/RoxieQueryList?{$parametersforpaging}&amp;PageStartFrom={$lastpagefrom}')">Last</a></td>
                </xsl:if>
            </tr-->
            <br/>
            <!--table id="btnTable" style="margin:20 0 0 0">
                <colgroup>
                    <col span="8" width="100"/>
                </colgroup>
                <tr>
                    <td>
                        <input type="button" class="sbutton" value="Clear" onclick="selectAll1(false)"/>
                    </td>
                    <td>
                        <input type="submit" class="sbutton" id="ListFilesBtn" name="Type" value="ListFiles" disabled="true"/>
                    </td>
                </tr>
            </table-->
        </form>
    </xsl:template>
    
    <xsl:template match="RoxieQuery">
    <xsl:variable name="href">
      <xsl:value-of select="concat('/WsRoxieQuery/QueryDetails?QueryID=', ID, '&amp;Cluster=', $clustername)"/>
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
                <!--input type="checkbox" name="IDs_i{position()}" value="{ID}@{Cluster}" onclick="return clicked(this)"/-->
                <xsl:variable name="popup">
          return RoxieQueryPopup('<xsl:value-of select="ID"/>', '<xsl:value-of select="position()"/>', '<xsl:value-of select="WUID"/>')
        </xsl:variable>
                <xsl:attribute name="oncontextmenu">
          <xsl:value-of select="$popup"/>
        </xsl:attribute>
                <img id="mn{position()}" class="menu1" src="/esp/files/img/menu1.png" onclick="{$popup}"></img>
      </td>
            <td align="left">
        <a title="Query details..." href="{$href}">
          <xsl:value-of select="ID"/>
        </a>
            </td>
            <td align="left">
                <a href="javascript:go('/WsWorkunits/WUInfo?Wuid={WUID}')">
                    <xsl:value-of select="WUID"/>
                </a>
            </td>
            <td align="left">
                <xsl:value-of select="DeployedBy"/>
            </td>
            <!--td align="left">
                <xsl:value-of select="Cluster"/>
            </td-->
            <td align="left">
                <xsl:value-of select="HighPriority"/>
            </td>
            <td align="left">
                <xsl:value-of select="Suspended"/>
            </td>
            <!--td align="left">
                <xsl:value-of select="UpdatedBy"/>
            </td>
            <td align="left">
                <xsl:value-of select="Label"/>
            </td>
            <td align="left">
                <xsl:value-of select="AssociatedName"/>
            </td-->
            <td align="left">
                <xsl:value-of select="HasAliases"/>
            </td>
            <!--td align="left">
                <xsl:value-of select="Error"/>
            </td-->
            <td align="left">
                <xsl:value-of select="Comment"/>
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
