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

<!DOCTYPE xsl:stylesheet [ <!ENTITY nbsp "&#160;"> ]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:include href="graph_gvc_common.xslt" />
  <xsl:output method="html" indent="no"/>
    <xsl:variable name="gid" select="WUGVCGraphInfoResponse/GID"/>
    <xsl:variable name="running" select="WUGVCGraphInfoResponse/Running"/>
  <xsl:template match="WUGVCGraphInfoResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="/esp/files_/css/winclassic.css" />

        <title>
          <xsl:value-of select="Wuid"/>
        </title>

        <xsl:text disable-output-escaping="yes"><![CDATA[
<script type="text/javascript" src="/esp/files_/scripts/graphgvc.js"></script>
<script type="text/javascript" src="/esp/files_/scripts/range.js"></script>
<script type="text/javascript" src="/esp/files_/scripts/timer.js"></script>
<script type="text/javascript" src="/esp/files_/scripts/slider.js"></script>
<link type="text/css" rel="StyleSheet" href="/esp/files_/css/winclassic.css"/>
<link type="text/css" rel="stylesheet" href="/esp/files_/css/espdefault.css"/>
           <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>

                ]]>
                </xsl:text>

        <script language="JavaScript1.2">
          var wuid = '<xsl:value-of select="Wuid"/>';
          var gid = '<xsl:value-of select="GID"/>';
          var graphName = '<xsl:value-of select="Name"/>';
          var isrunning = '<xsl:value-of select="Running"/>';
          var state = 0;
          var refresh=null;
          var update=false;
          var graphloaded = 0;
          var currentgraph = '';
          var currentgraphnode = '0';
          var gotosubgraph = '<xsl:value-of select="TheGraph/Control/Endpoint/Query/Graph/Subgraph" />';
          
          var isrunningsave = '0';
          var currentgraphnodesave = '0';
          var initfind = true;
          

          var baseUrl = '/WsWorkunits/WUProcessGraph?FileName=' + wuid + '/' + graphName;
        </script>

        
        <script language="JavaScript1.2">
          <xsl:text disable-output-escaping="yes"><![CDATA[
          window.onresize = function (){
            s.recalculate();
            graphResize();
          }

          function onGVCGraphLoaded(evt)
          {
            if (window.graphloaded != '1')
            {
              gvc.ScaleToFit();
              graphloaded = '1';
              if (s)
              {
                s.setValue(gvc.Zoom);
              }
              if (gotosubgraph != '0')
              {
                selectSubGraph(gotosubgraph);
                gotosubgraph = '0';
              }
            }
          }

          function reload_graph()
          {
              var graphNameDiv = document.getElementById('GraphName');
              if (graphNameDiv)
              {
                graphNameDiv.innerText = graphName;
              }

              hideElement('autoSpan');
              showElement('loadingMsg');
              configureGraphlist();

              pause_resume();

              var frame=document.getElementById('xgmmlFrame');
              var urlBase = '/WsWorkunits/WUProcessGraph?FileName=' + wuid + '/' + graphName + '.gjs';
              if (frame)
              {
                frame.src = urlBase;
                return;
                if (frame.src)
                {
                  frame.src = 'graphupdate51.htm';
                }
                else
                {
                  frame.src = 'graphupdate5.htm';
                }
              }
          }

                  var statsWnd = null;
                  function showGraphStats()
                  {
                      var link = document.getElementById('Stats');
                      link.innerText = 'Loading Stats...';
                      var url = baseUrl + '.htm';
                      if (gid != '')
            {
                          url += '/' + gid;
            }
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
            {
                          alert(    "Popup window could not be opened!  " + 
                                      "Please disable any popup killer and try again.");
            }
                  }

                  var timingsWnd = null;

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

          function selectGraph(GraphId)
          {
            resetFind();

            graphloaded = 0;
            gvc.Clear();
            if (GraphId.toString().indexOf('graph') < 0)
            {
              graphName = 'graph' + GraphId.toString();
            }
            else
            {
              graphName = GraphId;
            }
            reload_graph();
          }

          function onUnload()
          {
            try {
                if (statsWnd)
                {
                    statsWnd.close();
                }
                if (timingsWnd)
                {
                    timingsWnd.close();
                }
            }
            catch (e)
            {
            }
          }

                ]]>
                </xsl:text>
                </script>

        <script for="window" event="onload" language="Javascript">
          setTimeout("reload_graph()", 500);
        </script>

        <script for="gvc" event="OnGraphContextMenu(x, y)">
          alert('Right Click');
        </script>

        <script for="gvc" event="OnVertexMouseOver(x, y, vertex)">
                </script>

        <script for="gvc" event="OnVertexSingleClick(x, y, vertex)">
          alert('Vertex single click');
        </script>

                <script for="gvc" event="OnVertexDoubleClick(x, y, vertex)">
                </script>

                <script for="gvc" event="OnLayoutFinished()">
                    onGVCGraphLoaded(null);
                </script>



                <style type="text/css">
                    body {
                        margin-bottom: 0;
                    }
                </style>

      </head>
      <body class="yui-skin-sam" onload="nof5();" onunload="onUnload()">
        <table width="100%">
          <tbody>
            <tr>
              <td align="left">
                <xsl:text disable-output-escaping="yes">&nbsp;&nbsp;[Workunit: </xsl:text>
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
                <xsl:text disable-output-escaping="yes">]&nbsp;&nbsp;</xsl:text>
                <span id="state">
                </span>
                <span>&nbsp;<b>Graph:</b>&nbsp;
                </span>
                <span id="GraphName"></span>
              </td>
            </tr>
          </tbody>
        </table>
        <xsl:apply-templates select="/" mode="htmlbody" />
      </body>
      </html>
    </xsl:template>
</xsl:stylesheet>
