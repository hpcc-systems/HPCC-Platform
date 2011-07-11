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
  <!ENTITY nbsp "&#160;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:include href="graph_gvc_common.xslt" />
  <xsl:output method="html" indent="no"/>

  <xsl:template match="ShowGVCGraphResponse">
    <xsl:variable name="graphName">
      <xsl:choose>
        <xsl:when test="GraphName!=''">
          <xsl:value-of select="GraphName"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="GraphNames/Item[1]"/>
        </xsl:otherwise>
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
        <style type="text/css">
          body {
          padding-bottom: 0;
          }
          div#pageBody {
          padding-bottom: 0;
          }
        </style>
        <script language="JavaScript1.2" src="/esp/files_/graph.js">0</script>

        <xsl:text disable-output-escaping="yes"><![CDATA[
<script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
<script type="text/javascript" src="/esp/files_/scripts/graphgvc.js"></script>
<script type="text/javascript" src="/esp/files_/scripts/range.js"></script>
<script type="text/javascript" src="/esp/files_/scripts/timer.js"></script>
<script type="text/javascript" src="/esp/files_/scripts/slider.js"></script>
<link type="text/css" rel="StyleSheet" href="/esp/files_/css/winclassic.css"/>
                ]]>
                </xsl:text>

        <script language="JavaScript1.2">
          var queryName = '<xsl:value-of select="QueryName"/>';
          var graphName = '<xsl:value-of select="$graphName"/>';
          var graphloaded = '0';
          var initfind = true;
          var isrunning = '0';
          isRoxieGraph = true;
          window.onresize = function (){
            s.recalculate();
            graphResize();
          }

          function loadGraphs()
          {
            removeElements('wugraphs');
          <xsl:for-each select="GraphNames/Item">
            addGraphElement('<xsl:value-of select="."/>', 1, '');
          </xsl:for-each>
          }
          <xsl:text disable-output-escaping="yes"><![CDATA[

                function loadRoxieGraph()
                {
          var graphNameDiv = document.getElementById('GraphName');
          if (graphNameDiv)
          {
            graphNameDiv.innerText = graphName;
          }
                    try 
                    {
            isRoxieGraph = true;
                        var island = document.getElementById('xml_xgmml');
                        if (!island)
                        {
                            alert('data island not loaded');
                            return false;
                        }
                        var gvc = document.getElementById('gvc');
                        gvc.SetGraphStyle(2);
                        gvc.LoadXgmml(island.documentElement.text, 1);
                        gvc.layout = 4;

            OffsetHeight = 140;
            loadGraphs();
            update_details();
                        return true;
                    }
                    catch(e)
                    {
                        document.getElementById('GVCLink').innerHTML='You need an ActiveX Viewer to view graphs. '+
                                   'It can be downloaded <a href="/esp/files/">here</a>.';
                        return false;
                    }
                }

                var statsWnd = null;

                function showGraphStats()
                {
          
                    var link = document.getElementById('StatsLink');
                    link.innerText = 'Loading Stats...';
                    var baseUrl = '/ws_roxieconfig/ProcessGraph?FileName=' + queryName + '/' + graphName;
                    var url = baseUrl + '.htm&Stats=1';
                    var wnd = window.open("about:blank", "_graphStats_", 
                                            "toolbar=0,location=0,directories=0,status=0,menubar=0," + 
                                            "scrollbars=1, resizable=1, width=640, height=480");
                    link.innerText = 'Stats...';
                    if (wnd)
                    {
                        wnd.location = url;
                        wnd.focus();
                    }
                    else
          {
                        alert(  "Popup window could not be opened!  " + 
                                    "Please disable any popup killer and try again.");
          }
                }

        function selectGraph(GraphId)
        {
          resetFind();
          go('/ws_roxieconfig/ShowGVCGraph?QueryName=' + queryName + '&amp;GraphName=graph' + GraphId);
        }


                ]]>
                </xsl:text>
        </script>

        <script for="window" event="onload" language="Javascript">
          loadRoxieGraph();
        </script>

        <script for="gvc" event="OnLayoutFinished()">
          update_details();
          onGVCGraphLoaded(null);
        </script>

      </head>
      <body class="yui-skin-sam" onload="nof5();" onunload="onUnload()">
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
                <td align="left">
                  &nbsp;<b>Query:</b>&nbsp;<xsl:value-of select="QueryName"/>&nbsp;<b>Graph:</b>&nbsp;<span id="GraphName"></span>
                </td>
              </tr>
              <tr>
                <td align="left">
                  &#160;
                </td>
              </tr>
            </tbody>
          </table>
          <xml id="xml_xgmml">
            <xsl:copy-of select="/ShowGVCGraphResponse/TheGraph[1]/Control[1]"/>
          </xml>

          <xsl:apply-templates select="/" mode="htmlbody" />

        </div>
      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>
