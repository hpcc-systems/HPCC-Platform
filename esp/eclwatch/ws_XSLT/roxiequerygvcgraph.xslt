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
    <xsl:output method="html" indent="no"/>
    <!--xsl:output method="html"/-->
    
    <xsl:template match="RoxieQueryShowGVCGraphResponse">
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
                var clusterName = '<xsl:value-of select="ClusterName"/>';
                var queryName = '<xsl:value-of select="QueryName"/>';
                var graphName = '<xsl:value-of select="$graphName"/>';
                <xsl:text disable-output-escaping="yes"><![CDATA[
                function on_load()
                {
                    var res = test_svg();
                    if (res)
                    {
                        var svg=document.getElementById('SVGGraph');
                        if (svg)
                        {
                            var baseUrl = '/WsRoxieQuery/RoxieQueryProcessGraph?ClusterName='+clusterName+'&FileName=' + 
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
                function showGraphStats()
                {
                    var link = document.getElementById('showGraphStats');
                    link.innerText = 'Loading Stats...';
                    var baseUrl = '/WsRoxieQuery/RoxieQueryProcessGraph?ClusterName='+clusterName+'&FileName=' + 
                                    queryName + '/' + graphName;
                    var url = baseUrl + '.htm&Stats=1';
                    //var wnd = window.open("about:blank", "_graphStats_", 
                    var wnd = window.open("about:blank", "_blank", 
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
                function load_graph()
                {
                    try 
                    {
                        var island = document.getElementById('xml_xgmml');
                        if (!island)
                        {
                            alert('data island not loaded');
                            return false;
                        }
                        var running = 1; //not running, 2: for running
                        var gvc = document.getElementById('gvc');
                        gvc.SetGraphStyle(2);
                        gvc.LoadXgmml(island.documentElement.text, running);
                        
                        gvc.layout = 4;
                        return true;
                    }
                    catch(e)
                    {
                        document.getElementById('GVCLink').innerHTML='You need an ActiveX Viewer to view graphs. '+
                                   'It can be downloaded <a href="/esp/files/">here</a>.';
                        return false;
                    }
                }
                function onGVCGraphLoaded(evt)
                {
                    var obj = document.getElementById('loadingMsg');
                    if (obj) {
                        obj.style.display = 'none';
                        obj.style.visibility = 'hidden';
                    }
                }

                ]]>
                </xsl:text>
                </script>

                <script for="window" event="onload" language="Javascript">
                    setTimeout("load_graph()", 500);
                </script>

                <script for="gvc" event="OnGraphContextMenu(x, y)">   
                    var arr = new Array("Zoom In", "Zoom Out", "Mouse Wheel Zoom", "Exit Zoom Mode", "Run Layout");
                    var resp = gvc.ShowMenu(x, y, arr);
                    if(resp == 1) 
                           gvc.SetApplicationZoomMode(1);
                    else if(resp == 2)
                       gvc.SetApplicationZoomMode(2);
                    else if(resp == 3)
                       gvc.SetApplicationZoomMode(3);
                    else if(resp == 4)
                       gvc.SetApplicationZoomMode(0);
                    else
                       gvc.layout=4;
                </script>

                <script for="gvc" event="OnLayoutFinished()">
                    gvc.ScaleToFit();
                    onGVCGraphLoaded(null);
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();" onunload="onUnload()">
                <div id="pageBody">
                    <table width="100%">
                        <tbody>
                            <tr>
                                <th align="left">Query:<xsl:value-of    select="QueryName"/>
                                </th>
                                <td width="10"/>
                                <th align="left">Graph:
                                    <xsl:choose>
                                        <xsl:when test="GraphNames/Item[2]">
                                            <select 
                                            onchange="go('/WsRoxieQuery/RoxieQueryShowGVCGraph?ClusterName={ClusterName}&amp;QueryName={QueryName}&amp;GraphName='+options[selectedIndex].text)">
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
                                            title="Enter id of node to find in graph"/>
                                        <input type="button" id='findBtn' 
                                        onclick="findGraphNode(document.getElementById('findId').value)"
                                        value="Find Node"></input>
                                        <input type="button" id='resetFindBtn' disabled="true"
                                        onclick="resetFindGraphNode()" value="Reset Find"></input>
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
                    
                    <xml id="xml_xgmml">
                        <xsl:copy-of select="/RoxieQueryShowGVCGraphResponse/TheGraph[1]/Control[1]"/>
                    </xml>
                    
                    <object id="gvc" codebase="/esp/files/GraphViewCtl.cab#Version=2,0,0,0" standby="Loading Seisint GraphView Control..."
                        data="data:application/x-oleobject;base64,yxfq8b33CEGnQhvHd0OD/xAHAAA=" classid="CLSID:F1EA17CB-F7BD-4108-A742-1BC7774383FF"
                        style="width: 100%; height: 77%;">
                    </object>
                    <span id="GVCLink"/>
                    <iframe id="popupFrame" frameborder="0" scrolling="no" 
                        style="position:absolute; left:0; top:0; width:400; height:100; visibility:hidden; display:none">
                    </iframe>
                </div>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
