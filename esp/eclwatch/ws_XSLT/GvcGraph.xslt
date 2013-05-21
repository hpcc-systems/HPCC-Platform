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
  <xsl:include href="graph_gvc_common.xslt" />
  <xsl:output method="html"/>
  <xsl:template match="GVCAjaxGraphResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <title>
          <xsl:value-of select="Name"/> GVC Graph.
        </title>
        <link type="text/css" rel="stylesheet" href="/esp/files/css/espdefault.css"/>
        <xsl:text disable-output-escaping="yes"><![CDATA[
<script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
<link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
<link rel="stylesheet" type="text/css" href="/esp/files/yui/build/reset-fonts-grids/reset-fonts-grids.css" />
<link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
<link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
<link rel="stylesheet" type="text/css" href="/esp/files/yui/build/datatable/assets/skins/sam/datatable.css" />
<link rel="stylesheet" type="text/css" href="/esp/files/yui/build/container/assets/skins/sam/container.css" />
<link rel="stylesheet" type="text/css" href="/esp/files/yui/build/slider/assets/skins/sam/slider.css">
<script type="text/javascript" src="/esp/files/yui/build/yahoo/yahoo-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/event/event-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/dom/dom-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/element/element-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/connection/connection-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/dragdrop/dragdrop-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/container/container-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/button/button-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/slider/slider-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/build/animation/animation-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/datasource/datasource-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/datatable/datatable-min.js"></script>
<script type="text/javascript" src="/esp/files/yui/build/json/json-min.js"></script>
<script type="text/javascript" src="/esp/files/scripts/graphgvc.js"></script>
<script type="text/javascript" src="/esp/files/scripts/objtree.js"></script>

<link type="text/css" rel="StyleSheet" href="/esp/files/css/graph.css" />
<link type="text/css" rel="StyleSheet" href="/esp/files/css/winclassic.css" />
                ]]>
                </xsl:text>
        
        <script type="text/javascript">
          var queryName = '<xsl:value-of select="Name"/>'; 
          var wuid = '<xsl:value-of select="Name"/>';
          var graphName = '<xsl:value-of select="GraphName"/>';
          var graphType = '<xsl:value-of select="GraphType"/>';
          var cluster = '<xsl:value-of select="Cluster"/>';
          <xsl:if test="SubGraphOnly = 1">
            subgraphOnly = true;
            subgraphId = '<xsl:value-of select="SubGraphId"/>';
          </xsl:if>
          <xsl:if test="SubGraphOnly = 0">
            gotosubgraph = '<xsl:value-of select="SubGraphId"/>';
            if (gotosubgraph == '')
            {
            gotosubgraph = '0';
            }
          </xsl:if>
          <xsl:text disable-output-escaping="yes"><![CDATA[
          var state = '';
          var stateId = 0;
          var graphloaded = '0';
          var initfind = true;
          var isrunning = '0';
          var graphsJson = null;
          isRoxieGraph = false;
          isEclWatchGraph = false;
          isWsEclGraph = false;
          suppressGvcControlLoad = false;
          
          resultsoffset = 48; 
          OffsetHeight = 105;

          var requestEnvelope = null;
          var requestWuInfoEnvelope = null;
            var requestSourceUrl = '';
          var requestWuInfoSourceUrl = '';
          var espAddressAndPort = '';
          var requestSOAPAction = '';
          var requestWuInfoSOAPAction = '';

          var requestWuInfoSOAPAction = 'WsWorkunits/WUInfo?ver_=1.18';
 
          window.onresize = function (){
            //sldr.recalculate();
            //graphResize();
          }

    function onLoad()
    {
      addEvent(pluginLHS(), 'LayoutFinished', layoutFinished);
      addEvent(pluginRHS(), 'LayoutFinished', layoutFinishedRHS);
      addEvent(pluginLHS(), 'Scaled', scaled);
      addEvent(pluginRHS(), 'Scaled', scaled2);
      addEvent(pluginLHS(), 'MouseDoubleClick', mouseDoubleClick);
      addEvent(pluginRHS(), 'MouseDoubleClick', mouseDoubleClickRHS);
      addEvent(pluginLHS(), 'SelectionChanged', selectionChanged);

      var thisEsp = getEspAddressAndPort(document.location.href);
      if (graphType == 'roxieconfig')
      {
        isRoxieGraph = true;
        getGraph(thisEsp + '/ws_roxieconfig/ShowGVCGraph?QueryName=' + queryName);
        return;
      }
      if (graphType == 'roxiequery')
      {
        isRoxieGraph = true;
        getGraph(thisEsp + '/WsRoxieQuery/ShowGVCGraph?QueryName=' + queryName);
        return;
      }
      if (graphType == 'eclwatch')
      {
        isEclWatchGraph = true;
        getGraph(thisEsp + '/WsWorkunits/WUGVCGraphInfo?Wuid=' + wuid + '&Name=' + graphName);
        return;
      }
      if (graphType == 'wsecl')
      {
        isWsEclGraph = true;
        return;
      }
      showElement('urlinput');
    }

    function showGraphStats()
    {
      if (isEclWatchGraph)
      {
        showEclWatchGraphStats();
      }
      if (isRoxieGraph)
      {
        showRoxieGraphStats();
      }
      if (isWsRoxieQueryGraph)
      {
        showRoxieQueryGraphStats();
      }
    }

      function showRoxieQueryGraphStats()
      {
          //var link = document.getElementById('StatsLink');
          //link.innerText = 'Loading Stats...';
          var url = '/WsRoxieQuery/RoxieQueryProcessGraph?Cluster=' + cluster + '&QueryId=' + queryName + '&GraphName=' + graphName;
          var wnd = window.open("about:blank", "_graphStats_", 
                                  "toolbar=0,location=0,directories=0,status=0,menubar=0," + 
                                  "scrollbars=1, resizable=1, width=640, height=480");
          //link.innerText = 'Stats...';
          if (wnd)
          {
              wnd.location = url;
              wnd.focus();
          }
          else
      {
              alert(    "Popup window could not be opened!  " + 
                          "Please disable any popup killer and try again.");
      }
      }

      function showRoxieGraphStats()
      {
      
          //var link = document.getElementById('StatsLink');
          //link.innerText = 'Loading Stats...';
          var baseUrl = '/ws_roxieconfig/ProcessGraph?FileName=' + queryName + '/' + graphName;
          var wnd = window.open("about:blank", "_graphStats_", 
                                  "toolbar=0,location=0,directories=0,status=0,menubar=0," + 
                                  "scrollbars=1, resizable=1, width=640, height=480");
          //link.innerText = 'Stats...';
          if (wnd)
          {
              wnd.location = baseUrl;
              wnd.focus();
          }
          else
      {
              alert(    "Popup window could not be opened!  " + 
                          "Please disable any popup killer and try again.");
      }
      }

      function showEclWatchGraphStats()
      {
          //var link = document.getElementById('Stats');
          //link.innerText = 'Loading Stats...';

      var baseUrl = '/WsWorkunits/WUProcessGraph?Wuid=' + wuid + '&Name=' + graphName;
          var wnd = window.open("about:blank", "_graphStats_", 
                                  "toolbar=0,location=0,directories=0,status=0,menubar=0," + 
                                  "scrollbars=1, resizable=1, width=640, height=480", true);
          //link.innerText = 'Graph Stats...';
          if (wnd)
          {
              wnd.location = baseUrl;
              wnd.focus();
              statsWnd = wnd;
          }
          else
      {
              alert(    "Popup window could not be opened!  " + 
                          "Please disable any popup killer and try again.");
      }
      }

      function showTimings()
      {
          var wnd = window.open("about:blank", "_graphStats_", 
                                  "toolbar=0,location=0,directories=0,status=1,menubar=0," + 
                                  "scrollbars=1, resizable=1, width=350, height=480", true);
          if (wnd)
          {
              wnd.location = '/WsWorkunits/WUGraphTiming?Wuid=' + wuid;
              wnd.focus();
              timingsWnd = wnd;
          }
          else
      {
              alert(    "Popup window could not be opened!  " + 
                          "Please disable any popup killer and try again.");
      }
      }

        ]]>
                </xsl:text>

      </script>


        <style type="text/css">

          #zoom-slider-bg {
            text-align:left;
            background:url(/esp/files/yui/build/slider/assets/bg-h.gif) 5px 0 no-repeat;
          }

          body {
            margin: 0px;
            padding: 0px;
          }

        </style>


      </head>
      <body class="yui-skin-sam" onload="nof5();" onunload="onUnload()">
          <div id="pageBody">
            <table width="100%">
              <tbody>
                <tr>
                  <td align="left">
                      <span id="WuidQueryName" class="header1">
                       <u>Wuid or Query Details.</u>
                      </span>
                  </td>
                  <td align="right">
                    <div name="graphselect" id="graphselect">
                      <span>Graphs:</span>
                      <select name="wugraphs" id="wugraphs" onchange="selectGraph(this.options[this.selectedIndex].value);" />
                    </div>
                  </td>
                </tr>
                <tr id="urlinput" style="display:none; visibility:none;" colspan="2">
                  <td>
                    Url:<input id="RequestId" class="input" type="text" style="width: 300px" value="" onkeypress="return checkFindEnter('RequestId')" />
                    <xsl:text disable-output-escaping="yes">&#160;</xsl:text>

                    <SELECT name="QueryList" id="QueryList" class="select" style="display:none; visibility:hidden;" onChange="SelectRoxieQuery();">
                    </SELECT>

                    <input type="button" value="Load" class="button" onclick="getGraph($('RequestId').value);" />
                    <input type="checkbox" id="xgmmlonly">
                      <span style="font-size: 9pt;">Suppress Control Loading</span>
                    </input>
                  </td>
                </tr>
              </tbody>
            </table>
            <xsl:apply-templates select="/" mode="htmlbody" />

            <div style="font-family: sans-serif; font-style: normal; font-variant: normal; font-weight: normal; font-size: smaller; display:none; visibility:none;" id="xml_xgmml">

            </div>

          </div>

        <script type="text/javascript">
            update_details();
            onLoad();
        </script>

      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>
