<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
-->
<!DOCTYPE xsl:stylesheet [
    <!ENTITY nbsp "&#160;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html" indent="yes"/>
    <xsl:variable name="pathval" select="/tabview/path"/>
    <xsl:variable name="queryname" select="/tabview/query"/>
    <xsl:template match="tabview">
        <html>
            <head>
                <title>WS-ECL2 Tab Menu</title>
                <style type="text/css">
          /*margin and padding on body element
          can introduce errors in determining
          element position and are not recommended;
          we turn them off as a foundation for YUI
          CSS treatments. */
          body {
          margin:0;
          padding:0;
          }
          div
          {
          border:0;
          }
          #toggle {
          text-align: center;
          padding: 1em;
          }
          #toggle a {
          padding: 0 5px;
          border-left: 1px solid black;
          }
          #tRight {
          border-left: none !important;
          }
  
        .yui-navset  {
            height:100%;   
        }
        
        .yui-content {
            height:100%;   
        }  

        </style>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/reset-fonts-grids/reset-fonts-grids.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/resize/assets/skins/sam/resize.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/tabview/assets/skins/sam/tabview.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/paginator/assets/skins/sam/paginator.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/datatable/assets/skins/sam/datatable.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/container/assets/skins/sam/container.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/autocomplete/assets/skins/sam/autocomplete.css"/>
                <script type="text/javascript" src="/esp/files/yui/build/yahoo/yahoo-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/yuiloader/yuiloader-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/event/event-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/dom/dom-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/element/element-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/connection/connection-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/dragdrop/dragdrop-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/container/container-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/resize/resize-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/animation/animation-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/button/button-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/animation/animation-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/datasource/datasource-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/paginator/paginator-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/datatable/datatable-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/json/json-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/tabview/tabview-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/treeview/treeview.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/autocomplete/autocomplete-min.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/hpcc-ext/DataView.js"/>
                <script type="text/javascript" src="/esp/files/yui/build/hpcc-ext/RowFilter.js"/>
            </head>
            <body class="yui-skin-sam">
                <div id="tvcontainer" class="yui-navset" style="font-size: 0.90em; height:100%; text-align:left">
                    <ul class="yui-nav">
                        <xsl:if test="number(version)>=2">
                            <li class="selected"><a href="#tab1"><em>ECL</em></a></li>
                        </xsl:if>
                        <li>
                                <xsl:if test="number(version)&lt;2">
                                    <xsl:attribute name="class">selected</xsl:attribute>
                                </xsl:if>
                        <a href="#tab2"><em>XSLT</em></a>
                        </li>
                        <li><a href="#tab3"><em>Links</em></a></li>
                    </ul>
                    <div class="yui-content"  style="height:94%">
                            <xsl:if test="number(version)>=2">
                            <div id="tab1" style="height:100%">
                                <iframe src="/WsEcl2/forms/query/{$pathval}/{$queryname}" width="100%" height="100%" frameborder="0" border="0">
                                    <p>Your browser does not support iframes.</p>
                                </iframe>
                        </div>
                            </xsl:if>
                            <div id="tab2" style="height:100%">
                                <iframe src="/{$pathval}/{$queryname}?form" width="100%" height="100%" frameborder="0"  border="0">
                                    <p>Your browser does not support iframes.</p>
                                </iframe>
                            </div>
                            <div id="tab3" style="height:94%">
                                <iframe name="links" src="/WsEcl2/links/query/{$pathval}/{$queryname}" width="100%" height="100%" frameborder="0" border="0">
                                    <p>Your browser does not support iframes.</p>
                                </iframe>
                            </div>
                    </div>
                </div>
    <script> 
        (function() {    var tabView = new YAHOO.widget.TabView('tvcontainer');})();
    </script>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
