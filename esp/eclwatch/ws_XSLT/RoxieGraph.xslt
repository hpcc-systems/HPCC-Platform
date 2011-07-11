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

<!DOCTYPE xsl:stylesheet [
    <!--define the HTML non-breaking space:-->
    <!ENTITY nbsp "<xsl:text disable-output-escaping='yes'>&amp;nbsp;</xsl:text>">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html" indent="no"/>
    
    <xsl:template match="ShowGraphResponse">
        <xsl:variable name="graphName">
            <xsl:choose>
                <xsl:when test="GraphName!=''"><xsl:value-of select="GraphName"/></xsl:when>
                <xsl:otherwise><xsl:value-of select="GraphNames/Item[1]"/></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <!--meta http-equiv="Content-Type" content="text/html; charset=utf-8"/-->
                <title>
                    <xsl:value-of select="QueryName"/>
                </title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="/esp/files_/css/tabs.css"/>
                <link type="text/css" rel="stylesheet" href="/esp/files_/css/headerFooter.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <style type="text/css">
                    body {
                        padding-bottom: 0;
                    }
                    div#pageBody {
                        padding-bottom: 0;
                    }
                </style>
                <script language="JavaScript1.2" src="/esp/files_/graph.js">0</script>
                <script id="popup_labels" language="JavaScript1.2">0</script>
                <script language="JavaScript1.2">
                var queryName = '<xsl:value-of select="QueryName"/>';
                var graphName = '<xsl:value-of select="$graphName"/>';
                function on_load()
                {
                    var res = test_svg();
                    if (res)
                    {
                        var svg=document.getElementById('SVGGraph');
                        if (svg)
                        {
                            var baseUrl = '/ws_roxieconfig/ProcessGraph?FileName=' + 
                                            queryName + '/' + graphName;
                            svg.src = baseUrl+'.svg';
                            var popupFrame = document.getElementById('popupFrame');
                            popupFrame.src = baseUrl + '.htm';
                        }
                    }
                    return res;
                }
                
                function onGraphLoaded(evt)
                {
                    var obj = document.getElementById('loadingMsg');
                    if (obj) {
                        obj.style.display = 'none';
                        obj.style.visibility = 'hidden';
                    }
                    obj = document.getElementById('viewingTips');
                    if (obj) {
                        obj.style.display = 'inline';
                        obj.style.visibility = 'visible';
                    }
                }

                function open_new_window(popupId)
                {
                    showNodeOrEdgeDetails(popupId, graphName, null, queryName);
                }
                <xsl:text disable-output-escaping="yes"><![CDATA[
                function showGraphStats()
                {
                    var link = document.getElementById('showGraphStats');
                    link.innerText = 'Loading Stats...';
                    var baseUrl = '/ws_roxieconfig/ProcessGraph?FileName=' + 
                                    queryName + '/' + graphName;
                    var url = baseUrl + '.htm&Stats=1';
                    var wnd = window.open("about:blank", "_graphStats_", 
                                            "toolbar=0,location=0,directories=0,status=0,menubar=0," + 
                                            "scrollbars=1, resizable=1, width=640, height=480");
                    link.innerText = 'Graph Stats...';
                    if (wnd)
                    {
                        wnd.location = url;
                        wnd.focus();
                    }
                    else
                        alert(  "Popup window could not be opened!  " + 
                                    "Please disable any popup killer and try again.");
                }
                function onUnload()
                {
                    try {
                        if (statsWnd)
                            statsWnd.close();
                    }
                    catch (e)
                    {
                    }
                }
                ]]>
                </xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();return on_load()" onunload="onUnload()">
                <div id="pageHeader">
                    <div id="tabContainer" width="100%">
                        <ul id="tabNavigator">
                            <li>
                                <a href="/ws_roxieconfig/ListDeployedQueries" class="active">Queries</a>
                            </li>
                            <li>
                                <a href="/ws_roxieconfig/ListDeployedAliases">Aliases</a>
                            </li>
                            <li>
                                <a href="/ws_roxieconfig/ListFilesUsedByQuery?excludeSuperFileNames=1">Data Files</a>
                            </li>
                            <li>
                                <a href="/ws_roxieconfig/ListFilesUsedByQuery?excludeDataFileNames=1">Super Files</a>
                            </li>
                            <li>
                                <a href="/ws_roxieconfig/NavMenuEvent?Cmd=DeployMultiple">Deployments</a>
                            </li>
                            <li>
                                <a href="/ws_roxieconfig/NavMenuEvent?Cmd=DataDeployRemap">Remap Super Files</a>
                            </li>
                        </ul>
                    </div>
                </div>
                
                <div id="pageBody">
                    <table width="100%">
                        <tbody>
                            <tr>
                                <th align="left">Query:&nbsp;<xsl:value-of  select="QueryName"/>
                                </th>
                                <td width="10"/>
                                <th align="left">Graph:&nbsp;
                                    <xsl:choose>
                                        <xsl:when test="GraphNames/Item[2]">
                                            <select 
                                            onchange="go('/ws_roxieconfig/ShowGraph?QueryName={QueryName}&amp;GraphName='+options[selectedIndex].text)">
                                                <xsl:for-each select="GraphNames/Item">
                                                    <option>
                                                        <xsl:if test=".=$graphName">
                                                            <xsl:attribute name="selected">true</xsl:attribute>
                                                        </xsl:if>
                                                        <xsl:value-of select="."/>
                                                    </option>
                                                </xsl:for-each>
                                            </select>
                                        </xsl:when>
                                        <xsl:when test="$graphName">
                                            <xsl:value-of select="$graphName"/>
                                        </xsl:when>
                                        <xsl:otherwise>No graphs found!</xsl:otherwise>
                                    </xsl:choose>
                                </th>
                                <th width="10"/>
                                <th align="right">                                  
                                    <b id="loadingMsg"> [Loading, please wait...]</b>
                                    <span id="viewingTips"  style="display:none; visibility:hidden">
                                        <input type="text" id='findId' size="8" style="text-align:right"
                                            title="Enter id of node to find in graph"/>&nbsp;
                                        <input type="button" id='findBtn' 
                                        onclick="findGraphNode(document.getElementById('findId').value)"
                                        value="Find Node"></input>&nbsp;
                                        <input type="button" id='resetFindBtn' disabled="true"
                                        onclick="resetFindGraphNode()" value="Reset Find"></input>&nbsp;
                                        <xsl:text>[</xsl:text>
                                       <a href="javascript:void(0)" id="showGraphStats"
                                            onclick="showGraphStats(); return false;">Graph Stats...</a>
                                        <xsl:text>][</xsl:text>
                                       <a href="javascript:void(0)"
                                            onclick="showViewingTips(); return false;">Viewing tips...</a>
                                        <xsl:text>]</xsl:text>
                                    </span>
                                </th>
                            </tr>
                        </tbody>
                    </table>
                    
                    <xsl:if test="$graphName!=''">
                        <xsl:call-template name="showGraph">
                            <xsl:with-param name="queryName" select="QueryName"/>
                            <xsl:with-param name="graphName" select="$graphName"/>
                        </xsl:call-template>
                    </xsl:if>
                </div>
            </body>
        </html>
    </xsl:template>
    
    
    <xsl:template name="showGraph">
    <xsl:param name="queryName"/>
    <xsl:param name="graphName"/>
    <object classid="clsid:377B5106-3B4E-4A2D-8520-8767590CAC86" id="SVGGraph"
    codebase="http://download.adobe.com/pub/adobe/magic/svgviewer/win/3.x/3.03/en/SVGView.exe"
    type="image/svg+xml" width="99%" height="95%" style="border:lightblue 1px solid" >
      <param name="src" value="/esp/files_/empty.svg"/>
      <embed src="diagrams.svg"
        type="image/svg+xml" id="SVGGraph"
        pluginspage="http://www.adobe.com/svg/viewer/install/" ></embed>
    </object>

    <span id="SVGLink"/>
        <iframe id="popupFrame" frameborder="0" scrolling="no" 
            style="position:absolute;left:0;top:0;width:400;height:100;visibility:hidden">
      </iframe>
    </xsl:template>
</xsl:stylesheet>
