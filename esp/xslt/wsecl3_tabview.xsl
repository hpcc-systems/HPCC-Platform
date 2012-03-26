<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
##############################################################################
-->
<!DOCTYPE xsl:stylesheet [
    <!ENTITY nbsp "&#160;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html" indent="yes"/>
    <xsl:variable name="wuid" select="/tabview/wuid"/>
    <xsl:variable name="qset" select="/tabview/qset"/>
    <xsl:variable name="qname" select="/tabview/qname"/>
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
                <link rel="shortcut icon" href="/esp/files/img/affinity_favicon_1.ico" />
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/reset-fonts-grids/reset-fonts-grids.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/resize/assets/skins/sam/resize.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/tabview/assets/skins/sam/tabview.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/treeview/assets/skins/sam/treeview.css"/>
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
                        <li class="selected"><a href="#tab1"><em>Form</em></a></li>
                        <!--li>
                                <xsl:if test="number(version)&lt;2">
                                    <xsl:attribute name="class">selected</xsl:attribute>
                                </xsl:if>
                        <a href="#tab2"><em>Custom</em></a>
                        </li-->
                        <li><a href="#tab2"><em>Links</em></a></li>
                    </ul>
                    <div class="yui-content"  style="height:94%">
                            <div id="tab1" style="height:100%">
                                <iframe src="/WsEcl/forms/ecl/query/{$qset}/{$qname}" width="100%" height="100%" frameborder="0" border="0">
                                    <p>Your browser does not support iframes.</p>
                                </iframe>
                        </div>
                            <div id="tab2" style="height:94%">
                                <iframe name="links" src="/WsEcl/links/query/{$qset}/{$qname}" width="100%" height="100%" frameborder="0" border="0">
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
