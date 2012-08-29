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

<!DOCTYPE xsl:stylesheet [ <!ENTITY nbsp "&#160;"> ]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html" indent="no"/>
    <xsl:variable name="gid" select="WUGraphInfoResponse/GID"/>
    <xsl:template match="WUGraphInfoResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>
                    <xsl:value-of select="Wuid"/>
                </title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script language="JavaScript1.2" src="/esp/files_/graph.js">0</script>
                <script id="popup_labels" language="JavaScript1.2">0</script>
                <script language="JavaScript1.2">
                var refresh=null;
                var update=false;
                var wuid = '<xsl:value-of select="Wuid"/>';
                var gid = '<xsl:value-of select="GID"/>';
                var graphName = '<xsl:value-of select="Name"/>';
                var baseUrl = '/WsWorkunits/WUProcessGraph?FileName=' + wuid + '/' + graphName;
                
                function on_load()
                {
                    var res = test_svg();
                    if (res)
                    {
                        var svg=document.getElementById('SVGGraph');
                        if (svg)
                        {
                            var url = baseUrl+'.svg';
                            if (gid != '')
                                url += '/' + gid;
                            svg.src = url;
                            
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
                    obj = document.getElementById('findNodeBlock');
                    if (obj) {
                        obj.style.display = 'inline';
                        obj.style.visibility = 'visible';
                    }
                    pause_resume();
                    <xsl:if test="number(Running)">
                    obj = document.getElementById('autoSpan');
                    if (obj) {
                        obj.style.display = 'inline';
                        obj.style.visibility = 'visible';
                    }
                    </xsl:if>
                }

                function reload_graph()
                {
                    if(!update)
                        return;
                    hide_popup();
                    var frame=document.getElementById('popupFrame');
                    var urlBase = '/WsWorkunits/WUProcessGraph?FileName=' + wuid + '/' + graphName;
                    if (frame)
                        frame.src = urlBase + '.htm';

                    var script=document.getElementById('popup_labels');
                    if (script)
                        script.src = urlBase + '.js';
                }

                function pause_resume()
                {
                <xsl:if test="number(Running)">
                    var button=document.all['auto'];
                    if(update)
                    {
                        update=false;
                        clearInterval(refresh);
                        refresh=null;
                        if(button)
                        button.checked=false;
                    }
                    else
                    {
                        update=true;
                        refresh=setInterval(reload_graph,30000);
                        if(button)
                            button.checked=true;
                    }
                    if(button)
                        button.parentNode.style.visibility="inherit";
                </xsl:if>
                }

                function open_new_window(popupId)
                {
                    showNodeOrEdgeDetails(popupId, graphName, wuid);
                }
                
                <xsl:text disable-output-escaping="yes"><![CDATA[
                var statsWnd = null;
                function showGraphStats()
                {
                                        if (statsWnd)
                                        {
                                           statsWnd.close();
                                        }
                    var link = document.getElementById('showGraphStats');
                    link.innerText = 'Loading Stats...';
                    var url = baseUrl + '.htm';
                    if (gid != '')
                        url += '/' + gid;
                    url += '&Stats=1';
                    var wnd = window.open("about:blank", "_graphStats_", 
                                            "toolbar=0,location=0,directories=0,status=0,menubar=0," + 
                                            "scrollbars=1, resizable=1, width=640, height=480", true);
                    link.innerText = 'Graph Stats...';
                    if (wnd)
                    {
                        wnd.location = url;
                        wnd.focus();
                        statsWnd = wnd;
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

                        if (tipsWnd)
                            tipsWnd.close();
                    }
                    catch (e)
                    {
                    }
                }
                ]]>
                </xsl:text>
                </script>
                <style type="text/css">
                    body {
                        margin-bottom: 0;
                    }
                </style>
            </head>
      <body class="yui-skin-sam" onload="return on_load()" onunload="onUnload()">
                <table width="100%">
                    <tbody>
                        <tr>
                            <th align="left">
                                <xsl:text disable-output-escaping="yes">Graph '</xsl:text>
                                <xsl:value-of select="Name"/>
                                <xsl:if test="$gid and string($gid) != ''">
                                    <xsl:text disable-output-escaping="yes">' -- GID '</xsl:text>
                                    <xsl:value-of select="GID"/>
                                </xsl:if>
                                <xsl:text disable-output-escaping="yes">' [Workunit: </xsl:text>
                                <xsl:choose>
                                    <xsl:when test="number(BatchWU)">
                                        <a href="javascript:go('/WsBatchWorkunits/BatchWUInfo?Wuid={Wuid}')">
                                            <xsl:value-of select="Wuid"/>
                                        </a>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <a href="javascript:go('/WsWorkunits/WUInfo?Wuid={Wuid}')">
                                            <xsl:value-of select="Wuid"/>
                                        </a>
                                    </xsl:otherwise>
                                </xsl:choose>
                                <xsl:text disable-output-escaping="yes">]</xsl:text>
                            </th>
                        </tr>
                    </tbody>
                </table>

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
                    style="position:absolute; left:0; top:0; width:400; height:100; visibility:hidden; display:none">
              </iframe>
            </body>
            <table width="100%">
                <tbody>
                    <tr>
                        <th align="left">
                            <span id="findNodeBlock" style="display:none; visibility:hidden">
                                <xsl:text disable-output-escaping="yes">Find Node Id:&nbsp;</xsl:text>
                                <input type="text" id='findId' size="8"></input>
                                <xsl:text disable-output-escaping="yes">&nbsp;</xsl:text>
                                <input type="button" id='findBtn' 
                                onclick="findGraphNode  (document.getElementById('findId').value)" value="Find"></input>
                                <xsl:text disable-output-escaping="yes">&nbsp;</xsl:text>
                                <input type="button" id='resetFindBtn' disabled="true"
                                onclick="resetFindGraphNode()" value="Reset Find"></input>
                            </span>
                        </th>
                        <th align="right">
                            <span id="loadingMsg">Loading, please wait...</span>
                            <span id="autoSpan" style="display:none; visibility:hidden">
                                <input value="1" id="auto" type="checkbox" onclick="pause_resume()">
                                    <xsl:text>Auto Refresh</xsl:text>
                                </input>
                            </span>
                            <span id="viewingTips"  style="display:none; visibility:hidden">
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
        </html>
    </xsl:template>
</xsl:stylesheet>
