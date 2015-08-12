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

<!DOCTYPE xsl:stylesheet [
  <!--define the HTML non-breaking space:-->
  <!ENTITY nbsp "<xsl:text disable-output-escaping='yes'>&amp;nbsp;</xsl:text>">
]>
<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="html" indent="no"/>
  <xsl:variable name="currentgraphname" select="/Control/Endpoint/Graph"/>
  <xsl:variable name="wustate" select="/Control/Endpoint/wustate" />
  <xsl:template match="/">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script defer="defer">
          parent.window.graphName = '<xsl:value-of select="/Control/Endpoint/Graph"/>';
          parent.window.isrunning = '<xsl:value-of select="Control/Endpoint/isrunning"/>';
          parent.window.currentgraph = '<xsl:value-of select="/Control/Endpoint/currentgraph"/>';
          parent.window.currentgraphnode = '<xsl:value-of select="/Control/Endpoint/currentgraphnode"/>';
          parent.window.state = <xsl:value-of select="/Control/Endpoint/wustate"/>;
          parent.window.baseUrl = '/WsWorkunits/WUProcessGraph?FileName=' + parent.window.wuid + '/' + parent.window.graphName;

          function page_loaded()
          {
          try
          {
          var island = document.getElementById('xml_xgmml');
          if (!island)
          {
          alert('data island not loaded');
          return false;
          }

          var gvc = parent.document.getElementById('gvc');
          if (gvc)
          {
          <xsl:apply-templates select="/Control/Endpoint/wugraphs" mode="graphstate" />
                   if (parent.window.graphloaded == '1')
                   {
                     gvc.MergeXgmml(island.documentElement.text, graphstate);
                   }
                   else
                   {
                     gvc.SetGraphStyle(2);
                     gvc.LoadXgmml(island.documentElement.text, graphstate);
                     gvc.layout = 4;
                   }
                   updateGraphList();
                   parent.window.setStateDescription();
                       parent.window.update_details();
                   parent.window.pause_resume();
                   
                }
                else
                {
                  alert('GraphView Control not found');
                }
                return true;
            }
            catch(e)
            {
              document.getElementById('GVCLink').innerHTML='You need an ActiveX Viewer to view graphs. '+
              'It can be downloaded <a href="/esp/files/">here</a>.';
              return false;
            }
          }
          
             <xsl:apply-templates select="/Control/Endpoint/wugraphs"/>
        </script>
        
      </head>
      <body class="yui-skin-sam" onload="page_loaded()">
        <b>Right click and View Source.</b>
        <xml id="xml_xgmml">
          <xsl:copy-of select="/_Probe"/>
        </xml>
        <xml id="xml_wugraphs">
          <xsl:copy-of select="/Control/Endpoint/wugraphs"/>
        </xml>

      </body>
    </html>
  </xsl:template>
  
  <xsl:template match="wugraphs">
    function updateGraphList()
    {
      parent.window.removeElements('wugraphs');
      <xsl:variable name="single-quote">'</xsl:variable>
    <xsl:for-each select="wugraph">
      parent.window.addGraphElement('<xsl:value-of select="@id"/>', <xsl:value-of select="graphstate"/>, '<xsl:value-of select="translate(@name, $single-quote, ' ')" />');
    </xsl:for-each>
    }
  </xsl:template>
  <xsl:template match="wugraphs" mode="graphstate">
    var graphstate = 0;
    <xsl:if test="$wustate = 4 or $wustate = 7 or $wustate = 8">
    graphstate = 3; // WU Failed, Aborted or Blocked.
    </xsl:if>
    <xsl:if test="$wustate = 3">
    graphstate = 2; // WU Completed.
    </xsl:if>
    <xsl:if test="$wustate = 1">
    graphstate = 2; // WU Failed or Aborted.
    </xsl:if>
    <xsl:if test="$wustate = 2">
    graphstate = 1; // WU Failed or Aborted.
    </xsl:if>
  </xsl:template>

</xsl:stylesheet>
