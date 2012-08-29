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
    <xsl:output method="html" indent="no"/>
    <!--xsl:output method="html"/-->
    
    <xsl:template match="RoxieQueryShowGraphResponse">
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
                ]]>
                </xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();return on_load()" onunload="onUnload()">
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
                                            onchange="go('/WsRoxieQuery/RoxieQueryShowGraph?ClusterName={ClusterName}&amp;QueryName={QueryName}&amp;GraphName='+options[selectedIndex].text)">
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
    type="image/svg+xml" width="99%" height="92%" style="border:lightblue 1px solid" >
      <param name="src" value="/esp/files_/empty.svg"/>
      <embed src="diagrams.svg" width="400" height="300"
        type="image/svg+xml" id="SVGGraph"
        pluginspage="http://www.adobe.com/svg/viewer/install/" ></embed>
    </object>
    <span id="SVGLink"/>
        <iframe id="popupFrame" frameborder="0" scrolling="no" 
            style="position:absolute;left:0;top:0;width:400;height:100;visibility:hidden">
      </iframe>
    </xsl:template>
</xsl:stylesheet>
