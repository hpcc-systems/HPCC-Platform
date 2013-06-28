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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema">
<xsl:output method="html" encoding="utf-8"/>
  <xsl:variable name="Component" select="/*[1]/Component/text()"/>
  <xsl:variable name="CompDefn" select="/*[1]/CompDefn/text()"/>
  <xsl:variable name="debugMode" select="0"/>
  <xsl:variable name="filePath">
    <xsl:choose>
      <xsl:when test="$debugMode">c:/development/bin/debug/files</xsl:when>
      <xsl:otherwise>/esp/files_</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

<xsl:template name="string-replace-all">
  <xsl:param name="text" />
  <xsl:param name="replace" />
  <xsl:param name="by" />
  <xsl:choose>
    <xsl:when test="contains($text, $replace)">
      <xsl:value-of select="substring-before($text,$replace)" />
      <xsl:value-of select="$by" />
      <xsl:call-template name="string-replace-all">
        <xsl:with-param name="text" select="substring-after($text,$replace)" />
        <xsl:with-param name="replace" select="$replace" />
        <xsl:with-param name="by" select="$by" />
      </xsl:call-template>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="$text" />
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

  <xsl:template match="/*[1]/XmlArgs/*">
          <html>
            <head>
            <!--<script type="text/javascript" src="{$filePath}/scripts/configmgr/generated/{$Component}.js"></script>-->
              <script type="text/javascript">
                <xsl:value-of select="$CompDefn"/>
              </script>
              <script type="text/javascript" src="{$filePath}/yui/build/yahoo/yahoo-min.js"></script>
              <script type="text/javascript" src="{$filePath}/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
              <script type="text/javascript" src="{$filePath}/yui/build/event/event-min.js"></script>
              <script type="text/javascript" src="{$filePath}/yui/build/dragdrop/dragdrop-min.js"></script>
            <link rel="stylesheet" type="text/css" href="{$filePath}/yui/build/fonts/fonts.css" />
            <link rel="stylesheet" type="text/css" href="{$filePath}/yui/build/layout/assets/skins/sam/layout.css" />
            <link rel="stylesheet" type="text/css" href="{$filePath}/yui/build/datatable/assets/skins/sam/datatable.css" />
            <link rel="stylesheet" type="text/css" href="{$filePath}/yui/build/tabview/assets/skins/sam/tabview.css" />
            <link rel="stylesheet" type="text/css" href="{$filePath}/yui/build/menu/assets/skins/sam/menu.css" />
            <link rel="stylesheet" type="text/css" href="{$filePath}/yui/build/container/assets/skins/sam/container.css" />
            <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
            <script type="text/javascript" src="{$filePath}/yui/build/yuiloader/yuiloader-min.js" ></script>
            <script type="text/javascript" src="{$filePath}/yui/build/element/element-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/datasource/datasource-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/datatable/datatable.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/tabview/tabview-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/connection/connection-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/container/container-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/layout/layout-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/menu/menu-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/json/json-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/animation/animation-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/dom/dom-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/resize/resize-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/progressbar/progressbar-min.js"></script>
            <script type="text/javascript" src="{$filePath}/yui/build/event-simulate/event-simulate-min.js"></script>
            <script type="text/javascript" src="/esp/files/scripts/configmgr/common.js"></script>
            <script type="text/javascript" src="{$filePath}/scripts/configmgr/configmgr.js"/>

              <!--begin custom header content for this example-->
            <style type="text/css">
              /* custom styles for this example */
              .yui-skin-sam .yui-dt-liner { white-space:nowrap; }

              .not_in_env  { color: #FF0000;}
            </style>

            <style type="text/css">
              .yui-dt-liner #depth-1 {
              padding-left:0em !important;
              }

              .yui-dt-liner #depth0 {
              padding-left:2em !important;
              }

              .yui-dt-liner #depth1 {
              padding-left:4em !important;
              background-position:1em 0  !important;
              }
              .yui-dt-liner #depth2 {
              padding-left:6em !important;
              background-position:2em 0  !important;
              }
              .yui-dt-liner #depth3 {
              padding-left:8em !important;
              background-position:3em 0  !important;
              }
              .yui-dt-liner #depth4 {
              padding-left:10em !important;
              background-position:4em 0  !important;
              }
              .yui-dt-liner #depth5 {
              padding-left:12em !important;
              background-position:5em 0  !important;
              }
              .yui-dt-liner #depth6 {
              padding-left:14em !important;
              background-position:6em 0  !important;
              }
              .yui-dt-liner #depth7 {
              padding-left:16em !important;
              background-position:7em 0  !important;
              }
              .yui-dt-liner #depth8 {
              padding-left:18em !important;
              background-position:8em 0  !important;
              }
              .yui-dt-liner #depth9 {
              padding-left:20em !important;
              background-position:9em 0  !important;
              }

              .hidden {
              display:none;
              }
              .expanded .yui-dt-col-icon .yui-dt-liner{
              background: url(<xsl:value-of select="$filePath"/>/img/collapse.gif) no-repeat;
              }
              .collapsed .yui-dt-col-icon .yui-dt-liner{
              background: url(<xsl:value-of select="$filePath"/>/img/expand.gif) no-repeat;
              }
              .buttoncollapsed#pushbutton {

              background: url(<xsl:value-of select="$filePath"/>/img/expand.gif) center center no-repeat;
              border: none;
              text-indent: -4em;
              text-align:justify;
              overflow: hidden;
              padding: 0 .75em;
              width: 1.75em;
              height: 1em;
              margin-left: 4em;   /* IE only */
              padding: 0 1.75em;  /* IE only */
              }

              div>.buttoncollapsed#pushbutton {
              margin-left: 0em;   /* non-IE only */
              padding: 0 0em;  /* non-IE only */
            }

            .buttonexpanded#pushbutton {

            background: url(<xsl:value-of select="$filePath"/>/img/collapse.gif) center center no-repeat;
              border: none;
              text-indent: -4em;
              text-align:justify;
              overflow: hidden;
              padding: 0 .75em;
              width: 1.75em;
              height: 1em;
              margin-left: 4em;   /* IE only */
              padding: 0 1.75em;  /* IE only */
              }
              div>.buttonexpanded#pushbutton {
              margin-left: 0em;   /* non-IE only */
              padding: 0 0em;  /* non-IE only */
              }

              /* Class for displaying out of focus tables */
              .yui-skin-sam .yui-dt tr.outoffocus td {
              background-color: #D0D0D0;
              }

              .yui-skin-sam .yui-tt .bd
              {
              BORDER-RIGHT: #808080 1px solid;
              BORDER-TOP: #808080 1px solid;
              BORDER-LEFT: #808080 1px solid;
              BORDER-BOTTOM: #808080 1px solid;
              BACKGROUND-COLOR: #FAFAD2;
              }

            </style>
          </head>

      <body class=" yui-skin-sam" onmousedown="handlemousedown(event)" onkeydown="handlekeydown(event)" onunload="handleunload(true)">
        <xsl:choose>
          <xsl:when test="($Component='Refresh')">
            <script type="text/javascript">
              doPageRefresh("Configmgr has been restarted. Press ok to refresh the page");
            </script>
          </xsl:when>
          <xsl:when test="($Component='Deploy')">
              <div id="DeployTab">
              </div>
          </xsl:when>
          <xsl:when test="not($Component='BuildSet') and not($Component='Deploy')">
            <div>
              <i>
                <h3>
                  <script type="text/javascript">
                    if (top.document.navDT.getRecordIndex(top.document.navDT.getSelectedRows()[0]) == 0)
                      document.write("XML View");
                    else
                      document.write('<xsl:value-of select="$Component"/>');
                  </script>
                </h3>
              </i>
            </div>

            <div id="tabviewcontainer" class="yui-navset">
              <ul id="tabviewcontainerul" class="yui-nav">
                  <script type="text/javascript">
                  createTabDivsForComp('<xsl:value-of select="$Component"/>');
                </script>
              </ul>
              <div id ="tabviewcontainercontent" class="yui-content">
                <script type="text/javascript">
                </script>
              </div>
            </div>
        
            <br>
              <div id="tabviewcontainer1" class="hidden yui-navset">
                <ul id="tabviewcontainerul1" class="yui-nav">
                  <script type="text/javascript">
                    createTabDivsForComp('BuildSet');
                  </script>
                </ul>
                <div id ="tabviewcontainercontent1" class="yui-content">
                  <div id="BuildSetTab"></div>
                </div>
              </div>
              </br>
              </xsl:when>
            </xsl:choose>
            <script type="text/javascript">
              var rows = new Array();
              var rowsServers = new Array();
              var rowsSlaves = new Array();
              var rowsTopology = new Array();
              var menuEnabled = new Array();
              var viewChildNodes = new Array();
              var multiRowNodes = new Array();
              initRowsForComplexComps(rowsServers, "RoxieServers");
              initRowsForComplexComps(rowsSlaves, "RoxieSlaves");
              initRowsForComplexComps(rowsTopology, "Topology");
              <xsl:call-template name="fillenableMenu"/>
              createDivInTabsForComp('<xsl:value-of select="$Component"/>');
              <xsl:choose>
              <xsl:when test="($Component='Refresh')"></xsl:when>
              <xsl:when test="($Component='Programs')">
                  var id = 0;
                  <xsl:for-each select="child::*">
                    var i = {};
                    i.compType = '<xsl:value-of select="$Component"/>';
                    var subRecordIndex = 0;
                    i.depth = 0;
                    var parent = 0;
                    var eN = '<xsl:value-of select="name()"/>'
                    var subTypeKey = '<xsl:value-of select="@name"/>';
                    var cN = '<xsl:value-of select="@name"/>';
                    <xsl:for-each select="@*">
                      var aS = cS['<xsl:value-of select="name()"/>'+'<xsl:value-of select="$Component"/>'];
                      if ( aS.hidden != 1) {
                        i.<xsl:value-of select="name()"/> = "<xsl:value-of select="."/>";
                        i.<xsl:value-of select="name()"/>_extra = aS.extra;
                        i.<xsl:value-of select="name()"/>_ctrlType = aS.ctrlType;
                      }
                    </xsl:for-each>
                    i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN + "::subType=" + eN;
                    i.params +="::subTypeKey=" + subTypeKey;
                    i.parent = -1;
                    parent = id;
                    i.id = id++;
                    rows[rows.length] = i;
                    <xsl:for-each select="*">
                      var i = {};
                      var eN = '<xsl:value-of select="name()"/>';
                      i.compType = '<xsl:value-of select="$Component"/>';
                      <xsl:for-each select="@*">
                        var aS = cS['<xsl:value-of select="name()"/>'+'<xsl:value-of select="$Component"/>'];
                        if(aS.hidden != 1) {
                          i.<xsl:value-of select="name()"/> = "<xsl:value-of select="."/>";
                          i.<xsl:value-of select="name()"/>_extra = aS.extra;
                          i.<xsl:value-of select="name()"/>_ctrlType = aS.ctrlType;
                        }
                    </xsl:for-each>
                      i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN + "::subType=" + eN;
                      i.params +="::subTypeKey=";
                      subRecordIndex = id;
                      i.id = id++;
                      i.depth = 1;
                      i.parent = parent;
                      rows[rows.length] = i;
                    </xsl:for-each>
                </xsl:for-each>
                createMultiColTreeCtrlForComp(rows, "Programs", subRecordIndex);
              </xsl:when>
              <xsl:when test="($Component='Topology')">
                var id = 0;
                var i = {};
                i.compType = '<xsl:value-of select="$Component"/>';
                i.depth = 0;
                i.name = "<xsl:value-of select="name()"/>";
                i.value = "";
                i.name_ctrlType = 0;
                i.value_extra = "";
                i.value_ctrlType = 0;
                i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=::subType=";
                i.params +="::subTypeKey=";
                i.parent = -1;
                parent = id;
                i.id = id++;
                rows[rows.length] = i;
                var subRecordIndex = 0;
                var parent = 0;
                var eN = '<xsl:value-of select="name()"/>'
                var subTypeKey = '<xsl:value-of select="@name"/>';
                var cN = '<xsl:value-of select="@name"/>';

                <xsl:for-each select="@*">
                  var aS = cS['<xsl:value-of select="name()"/>'];
                  if(typeof(aS) != 'undefined') {
                    if ( aS.hidden != 1) {
                      var i = {};
                      i.compType = '<xsl:value-of select="$Component"/>';
                      i.depth = 1;
                      i.name = "<xsl:value-of select="name()"/>";
                      if ( i.name != 'build') {
                        i.value = "<xsl:value-of select="."/>";
                        i.name_extra = "";
                        i.name_ctrlType = 0;
                        i.value_extra = aS.extra;
                        i.value_ctrlType = aS.ctrlType;
                        i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN + "::subType=" + eN;
                        i.params +="::subTypeKey=" + subTypeKey;
                        i.parent = parent;
                        i.id = id++;
                      }
                      rows[rows.length] = i;
                    }
                  }
                </xsl:for-each>
                var parentIds = new Array();
                parentIds[parentIds.length] = parent;
                <xsl:for-each select="descendant::*">
                  var cN = '<xsl:value-of select="name()"/>';
                  var i = {};
                  i.compType = '<xsl:value-of select="$Component"/>';
                  i.depth = <xsl:value-of select="count(ancestor::*) - 2"/>;
                  i.name = '<xsl:value-of select="name()"/>';
                  if ('<xsl:value-of select="@name"/>' != '')
                    i.name += ' - ' + '<xsl:value-of select="@name"/>';
                  else
                    i.name += ' - ' + '<xsl:value-of select="@process"/>';
                  i.value = "";
                  i.name_extra = "";
                  i.name_ctrlType = 0;
                  i.value_extra = "";
                  i.value_ctrlType = 0;
                  i.parent = parentIds[parentIds.length-1];
                  i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + rows[i.parent].name + "::subType=";
                  i.params +="::subTypeKey=" + rows[i.parent].value;
                  parent = id;
                  i.id = id++;
                  rows[rows.length] = i;
                  var eN = '<xsl:value-of select="name()"/>'
                  setParentIds(i, rows, parentIds);
                  <xsl:for-each select="@*">
                    var aS = cS['<xsl:value-of select="name()"/>'+cN];
                    if(aS.hidden != 1) {
                      var i = {};
                      i.compType = '<xsl:value-of select="$Component"/>';
                      i.depth = <xsl:value-of select="count(ancestor::*) - 1"/>;
                      i.name = "<xsl:value-of select="name()"/>";
                      i.value = "<xsl:value-of select="."/>";
                      i.name_ctrlType = 0;
                      i.value_extra = aS.extra;
                      i.value_ctrlType = aS.ctrlType;
                      i.parent = parentIds[parentIds.length-1];
                      i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN + "::subType=" + eN;
                    i.params +="::subTypeKey=" + rows[i.parent].name;
                    i.id = id++;
                    rows[rows.length] = i;
                    }
                  </xsl:for-each>
                </xsl:for-each>
                createMultiColTreeCtrlForComp(rows, "<xsl:value-of select="$Component"/>", 0);
              </xsl:when>
                <xsl:when test="($Component='Environment')">
                  var id = 0;
                  var i = {};
                  initEnvXmlType(i);
                  i.compType = '<xsl:value-of select="$Component"/>';
                  i.depth = 0;
                  i.name = "<xsl:value-of select="name()"/>";
                  i.value = "";
                  i.params = "pcType=<xsl:value-of select="$Component"/>";
                  i.parent = -1;
                  parent = id;
                  i.id = id++;
                  rows[rows.length] = i;
                  var parent = 0;
                  var cN = '<xsl:value-of select="@name"/>';

                  <xsl:for-each select="@*">
                    var i = {};
                    initEnvXmlType(i);
                    i.compType = '<xsl:value-of select="$Component"/>';
                    i.depth = 1;
                    i.name = "<xsl:value-of select="name()"/>";
                    i.value = "<xsl:value-of select="."/>";
                    i.params = "isAttr=no:parentParams" + i.depth + "=" + rows[i.parent].params;
                    i.parent = parent;
                    i.hasChildren = false;
                    i.id = id++;
                    rows[rows.length] = i;
                    rows[i.parent].hasChildren = true;
                  </xsl:for-each>
                  var parentIds = new Array();
                  parentIds[parentIds.length] = parent;
                  <xsl:for-each select="descendant::*">
                    var i = {};
                    initEnvXmlType(i);
                    i.compType = '<xsl:value-of select="@name"/>';
                    i.depth = <xsl:value-of select="count(ancestor::*) - 2"/>;
                    i.name = '<xsl:value-of select="name()"/>';
                    <xsl:variable name="value_quote">
                      <xsl:call-template name="string-replace-all">
                        <xsl:with-param name="text" select="normalize-space(text())"/>
                        <xsl:with-param name="replace" select="'&quot;'" />
                        <xsl:with-param name="by" select="'\&quot;'" />
                      </xsl:call-template>
                    </xsl:variable>
                    i.value = "<xsl:value-of select="$value_quote"/>";
                    i.parent = parentIds[parentIds.length-1];
                    parent = id;
                    i.id = id++;
                    rows[rows.length] = i;
                    setParentIds(i, rows, parentIds);
                    rows[i.parent].hasChildren = true;
                    i.params = "isAttr=no:pcType=" + '<xsl:value-of select="name()"/>' + ":pcName=" + '<xsl:value-of select="@name"/>' + "::parentParams" + i.depth + "=" + rows[i.parent].params;
                    <xsl:for-each select="@*">
                      var i = {};
                      initEnvXmlType(i);
                      i.compType = '<xsl:value-of select="$Component"/>';
                      i.depth = <xsl:value-of select="count(ancestor::*) - 1"/>;
                      i.name = "<xsl:value-of select="name()"/>";
                      <xsl:variable name="value">
                        <xsl:call-template name="string-replace-all">
                          <xsl:with-param name="text" select="."/>
                          <xsl:with-param name="replace" select="'\'" />
                          <xsl:with-param name="by" select="'\\'" />
                        </xsl:call-template>
                      </xsl:variable>
                      i.value = "<xsl:value-of select="$value" />";
                      i.parent = parentIds[parentIds.length-1];
                      i.params = "parentParams" + i.depth + "=" + rows[i.parent].params;
                      i.id = id++;
                      rows[i.parent].hasChildren = true;
                      i.hasChildren = false;
                      rows[rows.length] = i;
                    </xsl:for-each>
                  </xsl:for-each>
                  createEnvXmlView(rows, "<xsl:value-of select="$Component"/>", 0);
                </xsl:when>                
              <xsl:when test="($Component='Deploy')">
                var id = 0;
                var i = {};
                i.compType = '<xsl:value-of select="$Component"/>';
                i.depth = 0;
                i.name = "<xsl:value-of select="name()"/>";
                i.value = "";
                i.name_ctrlType = 0;
                i.value_extra = "";
                i.value_ctrlType = 1;
                i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=::subType=";
                i.params +="::subTypeKey=";
                i.parent = -1;
                i.instanceName = '';
                i.build = '';
                i.buildSet = '';
                parent = id;
                i.id = id++;
                rows[rows.length] = i;
                var subRecordIndex = 0;
                var parent = 0;
                var eN = '<xsl:value-of select="name()"/>'
                var subTypeKey = '<xsl:value-of select="@name"/>';
                var cN = '<xsl:value-of select="@name"/>';
                var parentIds = new Array();
                parentIds[parentIds.length] = parent;
                <xsl:for-each select="descendant::*">
                    var cN = '<xsl:value-of select="name()"/>';
                    var i = {};
                    i.compType = '<xsl:value-of select="$Component"/>';
                    i.depth = <xsl:value-of select="count(ancestor::*) - 2"/>;
                    if ('<xsl:value-of select="@name"/>' != '')
                      i.name = '<xsl:value-of select="@name"/>';
                    else
                      i.name = '<xsl:value-of select="@nodeName"/>';
                    i.parent = parentIds[parentIds.length-1];
                    parent = id;
                    i.id = id++;
                    var eN = '<xsl:value-of select="name()"/>'
                    setParentIds(i, rows, parentIds);
                    <xsl:for-each select="@*">
                      i.<xsl:value-of select="name()"/> = "<xsl:value-of select="."/>";
                    </xsl:for-each>
                    
                    rows[rows.length] = i;
                  </xsl:for-each>
                  createMultiColTreeCtrlForComp(rows, "Deploy", 0);
                </xsl:when>
              <xsl:otherwise>
                createRowArraysForComp('<xsl:value-of select="$Component"/>', rows);
                var cN = '<xsl:value-of select="@name"/>';
                var notInEnv = '<xsl:value-of select="@_notInEnv"/>';
                var aS;
                var tN;
                <xsl:for-each select="@*">
                  aS = cS['<xsl:value-of select="name()"/>'];
                  if(typeof(aS) != 'undefined') {
                    if (typeof(aS.tab) != 'undefined') {
                      tN = 'Attributes';

                      if (aS.tab.length > 0)
                        tN = aS.tab;

                      if ( aS.hidden != 1) {
                        if ((top.document.forms['treeForm'].displayMode.value !== '1') || (aS.displayMode === 1)) {
                          var i = {};
                          i.compType = '<xsl:value-of select="$Component"/>';
                          i.name = "<xsl:value-of select="name()"/>";
                          i._key = "<xsl:value-of select="name()"/>";
                          if (aS.caption)
                            i.name = aS.caption;
                          i.value = "<xsl:value-of select="."/>";
                          i.name_ctrlType = 0;
                          i.value_extra = aS.extra;
                          i.value_ctrlType = aS.ctrlType;
                          i.value_required = aS.required;
                          i.value_onChange = aS.onChange;
                          i.value_onChangeMsg = aS.onChangeMsg;
                          i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN;
                          if (isNotInEnv(notInEnv, "<xsl:value-of select="name()"/>"))
                            i._not_in_env = 1;
                          rows[tN][rows[tN].length] = i;
                        }
                      }
                    }
                  }
                </xsl:for-each>
                var id = 0;
                var parent = 0;
                var parentIds = new Array();
                <xsl:for-each select="*">
                  <xsl:choose>
                    <xsl:when test="(name() = 'Topology')">
                      id = rowsTopology.length;
                      parent = 0;
                      parentIds.splice(0, parentIds.length);
                      parentIds[parentIds.length] = parent;
                      <xsl:for-each select="descendant::*">
                        var i = {};
                        i.compType = '<xsl:value-of select="$Component"/>';
                        i.depth = <xsl:value-of select="count(ancestor::*) - 2"/>;
                        i.parent = parentIds[parentIds.length-1];
                        parent = id;
                        i.id = id++;
                        setParentIds(i, rowsTopology, parentIds);
                        <xsl:for-each select="@*">
                          i.<xsl:value-of select="name()"/> = "<xsl:value-of select="."/>";
                        </xsl:for-each>
                        rowsTopology[rowsTopology.length] = i;
                      </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="(name() = 'RoxieFarmProcess')">
                    id = rowsServers.length;
                    parent = 0;
                    parentIds.splice(0, parentIds.length);
                    parentIds[parentIds.length] = parent;
                    <xsl:for-each select="descendant-or-self::*">
                      var subCompType = '<xsl:value-of select="name()"/>';
                      var i = {};
                      i.compType = subCompType;
                      i.depth = <xsl:value-of select="count(ancestor::*) - 2"/>;
                      i.parent = parentIds[parentIds.length-1];
                      parent = id;
                      i.id = id++;
                      setParentIds(i, rowsServers, parentIds);
                      var subTypeKey = '<xsl:value-of select="@name"/>';
                      <xsl:for-each select="@*">
                        i.<xsl:value-of select="name()"/> = "<xsl:value-of select="."/>";
                        i.<xsl:value-of select="name()"/>_extra = cS['<xsl:value-of select="name()"/>'+subCompType].extra;
                        i.<xsl:value-of select="name()"/>_ctrlType = cS['<xsl:value-of select="name()"/>'+subCompType].ctrlType;
                      </xsl:for-each>
                      i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN;
                      i.params +=  "::subType=" + subCompType + "::subTypeKey=" + subTypeKey;
                      rowsServers[rowsServers.length] = i;
                    </xsl:for-each>
                  </xsl:when>
                    <xsl:when test="(name() = 'RoxieServerProcess')"/>
                    <xsl:when test="(name() = 'RoxieSlave')">
                      id = rowsSlaves.length;
                      parent = 0;
                      parentIds.splice(0, parentIds.length);
                      parentIds[parentIds.length] = parent;
                      <xsl:for-each select="descendant-or-self::*">
                        var i = {};
                        i.compType = 'RoxieSlave';
                        i.depth = <xsl:value-of select="count(ancestor::*) - 2"/>;
                        i.name_ctrlType = 0;
                        i.parent = parentIds[parentIds.length-1];
                        parent = id;
                        i.id = id++;
                        setParentIds(i, rowsSlaves, parentIds);
                        <xsl:for-each select="@*">
                          i.<xsl:value-of select="name()"/> = "<xsl:value-of select="."/>";
                        </xsl:for-each>
                        if (i.depth > 1)
                          i.name = '<xsl:value-of select="@number"/>';
                        else
                          i.name = '<xsl:value-of select="@computer"/>';
                          
                        rowsSlaves[rowsSlaves.length] = i;
                      </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="name() = /*[1]/ViewChildNodes/viewChildNodes/*">
                        var i = {};
                        i.compType = '<xsl:value-of select="$Component"/>';
                      var eN = '<xsl:value-of select="name()"/>'
                      var subTypeKey="";

                      <xsl:for-each select="@*">
                        aS = cS['<xsl:value-of select="name()"/>'+eN];
                        if(typeof(aS) != 'undefined') {
                          if (typeof(aS.tab) != 'undefined') {
                            if(aS.hidden != 1) {
                              if ((top.document.forms['treeForm'].displayMode.value !== 1) || (aS.displayMode === 1)) {
                                i.<xsl:value-of select="name()"/> = "<xsl:value-of select="."/>";
                                i.<xsl:value-of select="name()"/>_extra = aS.extra;
                                i.<xsl:value-of select="name()"/>_ctrlType = aS.ctrlType;
                                i.<xsl:value-of select="name()"/>_required = aS.required;
                                i.<xsl:value-of select="name()"/>_onChange = aS.onChange;
                                i.<xsl:value-of select="name()"/>_onChangeMsg = aS.onChangeMsg;
                                if ("<xsl:value-of select="."/>"!== "")
                                  subTypeKey += "[@" + "<xsl:value-of select="name()"/>" + "='" + "<xsl:value-of select="."/>" + "']";
                                if (aS.caption)
                                  i.<xsl:value-of select="name()"/>_caption = aS.caption;
                              }
                            }
                          }
                        }
                      </xsl:for-each>
                      i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN + "::subType=" + eN;
                      i.params +="::subTypeKey=" + subTypeKey;

                      if (typeof(aS) != 'undefined')
                      rows[aS.tab][rows[aS.tab].length] = i;

                      <xsl:for-each select="*">
                        var subi = {};
                        subi.compType = '<xsl:value-of select="$Component"/>';
                        var subeN = '<xsl:value-of select="name()"/>'
                        var subsubTypeKey="";

                        if (typeof(i._<xsl:value-of select="name()"/>) === 'undefined')
                          i._<xsl:value-of select="name()"/> = new Array();

                        <xsl:for-each select="@*">
                          aS = cS['<xsl:value-of select="name()"/>'+ subeN];
                          if(typeof(aS) != 'undefined') {
                            if (typeof(aS.tab) != 'undefined') {
                              if(aS.hidden != 1) {
                                if ((top.document.forms['treeForm'].displayMode.value !== 1) || (aS.displayMode === 1)) {
                                  subi.<xsl:value-of select="name()"/> = "<xsl:value-of select="."/>";
                                  subi.<xsl:value-of select="name()"/>_extra = aS.extra;
                                  subi.<xsl:value-of select="name()"/>_ctrlType = aS.ctrlType;
                                  subi.<xsl:value-of select="name()"/>_required = aS.required;
                                  subi.<xsl:value-of select="name()"/>_onChange = aS.onChange;
                                  subi.<xsl:value-of select="name()"/>_onChangeMsg = aS.onChangeMsg;
                                  if ("<xsl:value-of select="."/>"!== "")
                                    subsubTypeKey += "[@" + "<xsl:value-of select="name()"/>" + "='" + "<xsl:value-of select="."/>" + "']";
                                  if (aS.caption)
                                    subi.<xsl:value-of select="name()"/>_caption = aS.caption;
                                }
                              }
                            }
                          }
                        </xsl:for-each>
                        
                        if (<xsl:value-of select="count(@*)"/> === 0) {
                          subi.<xsl:value-of select="name()"/> = '<xsl:value-of select="."/>';
                          subi.<xsl:value-of select="name()"/>_ctrlType = cS['<xsl:value-of select="name()"/>'+ subeN].ctrlType;
                        }

                        subi.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN + "::subType=" + eN;
                        subi.params += subTypeKey + "/" + subeN;
                        subi.params +="::subTypeKey=" + subsubTypeKey;
                        i._<xsl:value-of select="name()"/>[i._<xsl:value-of select="name()"/>.length] = subi;
                      </xsl:for-each>
                    </xsl:when>
                    <xsl:when test="name() = /*[1]/MultiRowNodes/multiRowNodes/*">
                      var i = {};
                      i.compType = '<xsl:value-of select="$Component"/>';
                      var eN = '<xsl:value-of select="name()"/>'
                      var subTypeKey="";
                      var tN;

                      <xsl:for-each select="@*">
                        aS = cS['<xsl:value-of select="name()"/>'+eN];
                        if(typeof(aS) != 'undefined') {
                          if (typeof(aS.tab) != 'undefined') {
                            tN = aS.tab
                            if(aS.hidden != 1) {
                              if ((top.document.forms['treeForm'].displayMode.value !== 1) || (aS.displayMode === 1)) {
                                i.<xsl:value-of select="name()"/> = "<xsl:value-of select="."/>";
                                i.<xsl:value-of select="name()"/>_extra = aS.extra;
                                i.<xsl:value-of select="name()"/>_ctrlType = aS.ctrlType;
                                i.<xsl:value-of select="name()"/>_required = aS.required;
                                i.<xsl:value-of select="name()"/>_onChange = aS.onChange;
                                i.<xsl:value-of select="name()"/>_onChangeMsg = aS.onChangeMsg;
                                subTypeKey += "[@" + "<xsl:value-of select="name()"/>" + "='" + "<xsl:value-of select="."/>" + "']";
                                if (aS.caption)
                                  i.<xsl:value-of select="name()"/>_caption = aS.caption;
                              }
                            }
                          }
                        }
                      </xsl:for-each>
                      i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN + "::subType=" + eN;
                      i.params +="::subTypeKey=" + subTypeKey;

                      if (typeof(tN) != 'undefined')
                        rows[tN][rows[tN].length] = i;
                    </xsl:when>

                    <xsl:otherwise>
                    var eN = '<xsl:value-of select="name()"/>'
                    var subTypeKey;
                    if (eN == 'Instance')
                      subTypeKey = '<xsl:value-of select="@name"/>';
                    else if (eN == 'Notes')
                      subTypeKey = '<xsl:value-of select="@date"/>';
                    else
                      subTypeKey = '';

                    <xsl:for-each select="@*">
                      aS = cS['<xsl:value-of select="name()"/>'+eN];
                      if(typeof(aS) != 'undefined') {
                        if (typeof(aS.tab) != 'undefined') {
                          if(aS.hidden != 1) {
                            if ((top.document.forms['treeForm'].displayMode.value !== 1) || (aS.displayMode === 1)) {
                              var i = {};
                              i.compType = '<xsl:value-of select="$Component"/>';
                              i.name = "<xsl:value-of select="name()"/>";
                              i._key = "<xsl:value-of select="name()"/>";
                              if (aS.caption)
                                i.name = aS.caption;
                              i.value = "<xsl:value-of select="."/>";
                              i.name_ctrlType = 0;
                              i.value_extra = aS.extra;
                              i.value_ctrlType = aS.ctrlType;
                              i.value_required = aS.required;
                              i.value_onChange = aS.onChange;
                              i.value_onChangeMsg = aS.onChangeMsg;
                              i.params = "pcType=<xsl:value-of select="$Component"/>::pcName=" + cN + "::subType=" + eN;
                              i.params +="::subTypeKey=" + subTypeKey;
                              rows[aS.tab][rows[aS.tab].length] = i;
                            }
                          }
                        }
                      }
                    </xsl:for-each>
                  </xsl:otherwise>
                </xsl:choose>
                </xsl:for-each>
                createTablesForComp('<xsl:value-of select="$Component"/>', rows);
                if ('<xsl:value-of select="$Component"/>'==='RoxieCluster') {
                  createMultiColTreeCtrlForComp(rowsServers, "Servers", 0);
                  createMultiColTreeCtrlForComp(rowsSlaves, "Farms", 0);
                }
                else if ('<xsl:value-of select="$Component"/>'==='ThorCluster')
                  createMultiColTreeCtrlForComp(rowsTopology, "Topology", 0);
              </xsl:otherwise>
            </xsl:choose>
              selectLastActiveTab();
              top.window.document.body.style.cursor='auto';
            </script>
          </body>
        </html>
    </xsl:template>
  <xsl:template match="/*[1]/Component" />
  <xsl:template match="/*[1]/CompDefn" />
  <xsl:template match="/*[1]/ViewChildNodes" />
  <xsl:template match="/*[1]/MultiRowNodes" />

  <xsl:template name="fillenableMenu">
    <xsl:for-each select="/*[1]/ViewChildNodes/viewChildNodes/*">
        addUniqueToArray(menuEnabled, '<xsl:value-of select="."/>');
        addUniqueToArray(viewChildNodes, '<xsl:value-of select="."/>');
    </xsl:for-each>
    <xsl:for-each select="/*[1]/MultiRowNodes/multiRowNodes/*">
      addUniqueToArray(menuEnabled, '<xsl:value-of select="."/>');
      addUniqueToArray(multiRowNodes, '<xsl:value-of select="."/>');
    </xsl:for-each>
  </xsl:template>

</xsl:stylesheet>
