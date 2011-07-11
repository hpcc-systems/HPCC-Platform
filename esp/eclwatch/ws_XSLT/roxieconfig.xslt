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
  <xsl:output method="html"/>
  <xsl:template match="/">
    <html>
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1" />
        <title>HPCC Platform</title>
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

        </style>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/reset-fonts-grids/reset-fonts-grids.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/resize/assets/skins/sam/resize.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/tabview/assets/skins/sam/tabview.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/treeview/assets/skins/sam/treeview.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/paginator/assets/skins/sam/paginator.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/datatable/assets/skins/sam/datatable.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/container/assets/skins/sam/container.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/autocomplete/assets/skins/sam/autocomplete.css" />
        <script type="text/javascript" src="/esp/files/yui/build/yahoo/yahoo-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/yuiloader/yuiloader-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/event/event-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/dom/dom-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/element/element-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/connection/connection-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/dragdrop/dragdrop-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/container/container-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/resize/resize-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/animation/animation-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/button/button-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/animation/animation-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/datasource/datasource-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/paginator/paginator-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/datatable/datatable-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/json/json-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/tabview/tabview-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/treeview/treeview.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/autocomplete/autocomplete-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/hpcc-ext/DataView.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/hpcc-ext/RowFilter.js"></script>


        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script type="text/javascript" src="/esp/files/scripts/ws_roxieconfig_deploytab.js"></script>
        <script type="text/javascript" src="/esp/files/scripts/ws_roxieconfig.js"></script>

        <script type="text/javascript">
          <xsl:choose>
            <xsl:when test="//ShowDeployTab='1'">
              ShowDeployTab = 6;
            </xsl:when>
            <xsl:otherwise>
              ShowDeployTab = 0;
            </xsl:otherwise>
          </xsl:choose>
        </script>

        <style type="text/css">
          #left1 {background: #fff;}


          .yui-skin-sam div.loading div {
          background:url(/esp/files/img/loading.gif) no-repeat center center;
          height:8em; /* hold some space while loading */
          }

          .yui-skin-sam .yui-dt-body { cursor:pointer; } /* when rows are selectable */


          #single { margin-top:2em; }

          .graphlink
          {
          PADDING-LEFT: 20px;
          BACKGROUND: url(/esp/files_/img/outlet.png) no-repeat;
          text-align: center;
          TEXT-DECORATION: none;
          }

          .cellstatus3
          {
          PADDING-LEFT: 20px;
          BACKGROUND: url(/esp/files/img/warning.png) no-repeat;
          text-align: center;
          TEXT-DECORATION: none;
          }

          .cellstatus1
          {
          PADDING-LEFT: 20px;
          BACKGROUND: url(/esp/files/img/accept-icon.png) no-repeat;
          text-align: center;
          TEXT-DECORATION: none;
          }

          .cellstatus2
          {
          PADDING-LEFT: 20px;
          BACKGROUND: url(/esp/files/img/error-icon.png) no-repeat;
          text-align: center;
          TEXT-DECORATION: none;
          }

          .cellstatusinformation
          {
          PADDING-LEFT: 20px;
          BACKGROUND: url(/esp/files/img/information.png) no-repeat;
          text-align: center;
          TEXT-DECORATION: none;
          }
          .list-checked {
          background: url(/esp/files/yui/build/menu/assets/menuitem_checkbox.png) left center no-repeat;
          }

          .yui-skin-sam .yui-layout #bd1 {
          background-color: White;
          border: 1px solid #808080;
          border-bottom: none;
          border-top: none;
          *border-bottom-width: 0;
          *border-top-width: 0;
          }

          .spanleft
          {
          float: left;
          }

          .spanright
          {
          float: right;
          }

          div.fileinputs {
          position: relative;
          }

          div.fakefile {
          position: absolute;
          top: 0px;
          left: 0px;
          z-index: 1;
          }

          input.file {
          position: relative;
          text-align: right;
          width: 350px;
          -moz-opacity:0 ;
          filter:alpha(opacity: 0);
          opacity: 0;
          z-index: 2;
          }

        </style>


      </head>

      <body class=" yui-skin-sam" onload="setReloadFunction('refreshCurrentTab(false)');">
        <xsl:variable name="tab1class">
          <xsl:choose>
            <xsl:when test="//ShowDeployTab='1'">
            </xsl:when>
            <xsl:otherwise>selected</xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:variable name="tab7class">
          <xsl:choose>
            <xsl:when test="//ShowDeployTab='1'">selected</xsl:when>
            <xsl:otherwise>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <div id="bd1" style="height:auto; text-align:left; ">
          <div style="padding: 3px;">
            <div id="tvcontainer" class="yui-navset" style="font-size: 0.90em; display:none; width:100%; height:100%;">
              <ul class="yui-nav">
                <li class="{$tab1class}">
                  <a href="#tab1">
                    <em>Queries</em>
                  </a>
                </li>
                <li>
                  <a href="#tab2">
                    <em>Data Queries</em>
                  </a>
                </li>
                <li>
                  <a href="#tab3">
                    <em>Library Queries</em>
                  </a>
                </li>
                <li>
                  <a href="#tab4">
                    <em>Aliases</em>
                  </a>
                </li>
                <li>
                  <a href="#tab5">
                    <em>Files</em>
                  </a>
                </li>
                <li>
                  <a href="#tab6">
                    <em>Super Files</em>
                  </a>
                </li>
                <li class="{$tab7class}">
                  <a href="#tab7">
                    <em>Deployments</em>
                  </a>
                </li>
              </ul>
              <div class="yui-content">
                <div>
                  <div id="ListDeployedQueriesFilterGroup" style="padding-bottom:5px">
                    Query search:
                    <input id="ListDeployedQueriesFilter" type="text" onkeypress="checkForEnter(event);" onchange="doFilter(event);" />&#160;
                    <input id="ListDeployedQueriesFilterSuspended" type="checkbox" onclick="doFilter(event);" />&#160;Suspended
                    <input id="ListDeployedQueriesFilterAliases" type="checkbox" onclick="doFilter(event);" />&#160;Aliases
                    <input id="ListDeployedQueriesFilterLibrary" type="checkbox" onclick="doFilter(event);" />&#160;Library
                    <input id="ListDeployedQueriesFilterUsesLibrary" type="checkbox" onclick="doFilter(event);" />&#160;Uses Library
                  </div>
                  <div id="dt_ListDeployedQueries" style="height: 100px; width: 90%"></div>
                  <div style="text-align:left; width:98%; padding-top:5px; padding-bottom:5px">
                    <span id="deletequery1" class="yui-button yui-push-button" style="font-size: 1em">
                      <span class="first-child">
                        <button type="button" id="buttonDeleteQuery1" name="buttonDeleteQuery1" disabled="true" onclick="deleteQueries();">Delete</button>
                      </span>
                    </span>
                    <!--
                         <span id="activatequery1" class="yui-button yui-push-button" style="font-size: 1em">   
                             <span class="first-child">   
                                <button type="button" name="buttonActivateQuery" onclick="activateQueries(true);">Activate</button> 
                             </span>  
                         </span> 
                         -->
                    <span id="addalias1" class="yui-button yui-push-button" style="font-size: 1em">
                      <span class="first-child">
                        <button type="button" id="buttonAddAlias1" name="buttonAddAlias1" disabled="true" onclick="activateQueries(false);">Add Alias</button>
                      </span>
                    </span>
                    <span id="suspendqueries1" class="yui-button yui-push-button" style="font-size: 1em">
                      <span class="first-child">
                        <button type="button" id="buttonSuspendQueries1" name="buttonSuspendQueries1" disabled="true" onclick="toggleQueries();">Toggle Suspend</button>
                      </span>
                    </span>
                    <input type="checkbox" name="checkNotifyRoxie1" id="checkNotifyRoxie1" checked="true" onclick="setNotifyRoxie(this.checked);">Notify Roxie?</input>
                  </div>
                </div>
                <div>
                  <div id="Div1" style="padding-bottom:5px">
                    Query search:
                    <input id="ListDeployedDataOnlyQueriesFilter" type="text" onkeypress="checkForEnter(event);" onchange="filterDeployedQueries();" />
                    <input id="ListDeployedDataOnlyQueriesFilterSuspended" type="checkbox" onclick="doFilter(event);" />&#160;Suspended
                    <input id="ListDeployedDataOnlyQueriesFilterAliases" type="checkbox" onclick="doFilter(event);" />&#160;Aliases
                  </div>
                  <div id="dt_ListDeployedDataOnlyQueries"></div>
                  <div style="text-align:left; width:98%; padding-top:5px">
                    <span id="deletequery2" class="yui-button yui-push-button" style="font-size: 1em" onclick="deleteQueries();">
                      <span class="first-child">
                        <button type="button" id="buttonDeleteQuery2" name="buttonDeleteQuery2" disabled="true">Delete</button>
                      </span>
                    </span>
                    <!--
                         <span id="activatequery2" class="yui-button yui-push-button" style="font-size: 1em" onclick="activateQueries(true);">   
                             <span class="first-child">   
                                <button type="button" name="buttonActivateQuery">Activate</button> 
                             </span>  
                         </span> 
                         -->
                    <span id="addalias2" class="yui-button yui-push-button" style="font-size: 1em" onclick="activateQueries(false);">
                      <span class="first-child">
                        <button type="button" id="buttonAddAlias2" name="buttonAddAlias2" disabled="true">Add Alias</button>
                      </span>
                    </span>
                    <input type="checkbox" name="checkNotifyRoxie2" id="checkNotifyRoxie2" checked="true" onclick="setNotifyRoxie(this.checked);">Notify Roxie?</input>
                  </div>
                </div>
                <div>
                  <div id="Div2" style="padding-bottom:5px">
                    Query search:
                    <input id="ListDeployedLibraryQueriesFilter" type="text" onkeypress="checkForEnter(event);" onchange="filterDeployedQueries();" />
                    <input id="ListDeployedLibraryQueriesFilterSuspended" type="checkbox" onclick="doFilter(event);" />&#160;Suspended
                    <input id="ListDeployedLibraryQueriesFilterAliases" type="checkbox" onclick="doFilter(event);" />&#160;Aliases
                    <input id="ListDeployedLibraryQueriesFilterUsesLibrary" type="checkbox" onclick="doFilter(event);" />&#160;Uses Library
                  </div>
                  <div id="dt_ListDeployedLibraryQueries"></div>
                  <div style="text-align:left; width:98%; padding-top:5px">
                    <span id="deletequery3" class="yui-button yui-push-button" style="font-size: 1em" onclick="deleteQueries();">
                      <span class="first-child">
                        <button type="button" id="buttonDeleteQuery3" name="buttonDeleteQuery3" disabled="true">Delete</button>
                      </span>
                    </span>
                    <!--                         
                         <span id="activatequery3" class="yui-button yui-push-button" style="font-size: 1em" onclick="activateQueries(true);">   
                             <span class="first-child">   
                                <button type="button" name="buttonActivateQuery">Activate</button> 
                             </span>  
                         </span>
                         -->
                    <span id="addalias3" class="yui-button yui-push-button" style="font-size: 1em" onclick="activateQueries(false);">
                      <span class="first-child">
                        <button type="button" id="buttonAddAlias3" name="buttonAddAlias3" disabled="true">Add Alias</button>
                      </span>
                    </span>
                    <span id="suspendqueries3" class="yui-button yui-push-button" style="font-size: 1em">
                      <span class="first-child">
                        <button type="button" id="buttonSuspendQueries3" name="buttonSuspendQueries3" disabled="true" onclick="toggleQueries();">Toggle Suspend</button>
                      </span>
                    </span>
                    <input type="checkbox" name="checkNotifyRoxie3" id="checkNotifyRoxie3" checked="true" onclick="setNotifyRoxie(this.checked);">Notify Roxie?</input>
                  </div>
                </div>
                <div>
                  <div id="AliasFilterGroup" style="padding-bottom:5px">
                    Query search:
                    <input id="AliasFilter" type="text" onkeypress="checkForEnter(event);" onchange="doFilter(event);" />
                  </div>
                  <div id="dtaliases"></div>
                  <div id="aliasactions" style="text-align:left; width:98%; padding-top:5px">
                    <span id="deletealiases" class="yui-button yui-push-button" style="font-size: 1em">
                      <span class="first-child">
                        <button type="button" id="buttonDeleteAliases" name="buttonDeleteAliases" disabled="true" onclick="deleteAliases();">Delete</button>
                      </span>
                    </span>
                  </div>
                </div>
                <div>
                  <div id="tvdatafiles" class="yui-navset">
                    <ul class="yui-nav">
                      <li class="selected">
                        <a href="#tab11">
                          <em>Index Files</em>
                        </a>
                      </li>
                      <li>
                        <a href="#tab12">
                          <em>Data Files</em>
                        </a>
                      </li>
                    </ul>
                    <div class="yui-content">
                      <div>
                        <div id="IndexFilterGroup" style="padding-bottom:5px">
                          Index search:
                          <input id="IndexFilesFilter" type="text" onkeypress="checkForEnter(event);" onchange="doFilter(event);" />
                        </div>
                        <div id="dtindexes">
                          <p>Index Files</p>
                        </div>
                        <span id="queriesusingindex" class="yui-button yui-push-button" style="font-size: 1em; padding-top: 5px;" onclick="listQueriesUsingFile(dt_IndexFiles);">
                          <span class="first-child">
                            <button type="button" name="buttonQueriesUsingIndex">Queries using index</button>
                          </span>
                        </span>
                      </div>
                      <div>
                        <div id="DataFileFilterGroup" style="padding-bottom:5px">
                          Data file search:
                          <input id="DataFilesFilter" type="text" onkeypress="checkForEnter(event);" onchange="doFilter(event);" />
                        </div>
                        <div id="dtindexes2">
                          <p>Data Files</p>
                        </div>
                        <span id="queriesusingdatafile" class="yui-button yui-push-button" style="font-size: 1em; padding-top: 5px;" onclick="listQueriesUsingFile(dt_DataFiles);">
                          <span class="first-child">
                            <button type="button" name="buttonQueriesUsingDataFile">Queries using File</button>
                          </span>
                        </span>
                      </div>
                    </div>
                  </div>
                  <div>
                    <b>Total Index/Data File Records :</b>
                    <span id="dtindexesRecords"></span>
                  </div>
                  <div>
                    <b>Total Index/Data File Size :</b>
                    <span id="dtindexesSize"></span>
                  </div>
                </div>
                <div>
                  <div id="FilterSuperFilesGroup" style="padding-bottom:5px">
                    Super File search:
                    <input id="SuperFilesFilter" type="text" onkeypress="checkForEnter(event);" onchange="doFilter(event);" />
                  </div>
                  <div id="dtsuperfiles">Super Files.</div>
                  <span id="queriesusingsuperfile" class="yui-button yui-push-button" style="font-size: 1em; padding-top: 5px;" onclick="listQueriesUsingFile(dt_SuperFiles);">
                    <span class="first-child">
                      <button type="button" name="buttonQueriesUsingDataFile">Queries using Superfile</button>
                    </span>
                  </span>
                  <!--
                     <span id="deployrelatedqueries" class="yui-button yui-push-button" style="font-size: 1em; padding-top: 5px;" onclick="redeployRelatedQueries();">   
                         <span class="first-child">   
                            <button type="button" name="buttonDeployRelatedQueries">Redepoy related Queries</button> 
                         </span>  
                     </span> 
                     -->
                </div>
                <div id="tb_deployments">
                  <div id="dt_deployments"></div>
                  <br />
                  <div id="btnDeployments"></div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div id="QueryPanel" style="text-align:left; display:none;">
          <div id="qhd" class="hd">Query Panel Header.</div>
          <div id="qbd" class="bd">
            <div id="tvquery" class="yui-navset" style="font-size: 0.90em">
              <ul class="yui-nav">
                <li class="selected">
                  <a href="#tab1">
                    <em>Details</em>
                  </a>
                </li>
                <li>
                  <a href="#tab2">
                    <em>Aliases</em>
                  </a>
                </li>
                <li>
                  <a href="#tab3">
                    <em>Files</em>
                  </a>
                </li>
                <li>
                  <a href="#tab4">
                    <em>Super Files</em>
                  </a>
                </li>
                <li>
                  <a href="#tab6">
                    <em>Queries using Library</em>
                  </a>
                </li>
              </ul>
              <div class="yui-content">
                <div>
                  <p id="qdetails" ></p>
                  <div id="dtlibrariesused"></div>
                </div>
                <div id="dtqueryaliases"></div>
                <div>
                  <div id="tvquerydata" class="yui-navset" style="font-size: 0.90em">
                    <ul class="yui-nav">
                      <li class="selected">
                        <a href="#tab11">
                          <em>Index Files</em>
                        </a>
                      </li>
                      <li>
                        <a href="#tab12">
                          <em>Data Files</em>
                        </a>
                      </li>
                    </ul>
                    <div class="yui-content">
                      <div id="dtqueryfiles">
                        <p>Data Files</p>
                      </div>
                      <div id="dtqueryfiles2">
                        <p>Data Files</p>
                      </div>
                    </div>
                  </div>
                  <div>
                    <b>Total Index/Data File Records :</b>
                    <span id="dtqueryfilesRecords"></span>
                  </div>
                  <div>
                    <b>Total Index/Data File Size :</b>
                    <span id="dtqueryfilesSize"></span>
                  </div>
                </div>
                <div id="dtquerysuperfiles"></div>
                <div id="dtlibqueries"></div>
              </div>
            </div>
          </div>
        </div>

        <div id="ConfirmationPanel" style="font-size: 0.85em;  display:none;">
          <div id="ConfirmationHeader" class="hd">Query Panel Header.</div>
          <div id="ConfirmationBody" class="bd">
            <div id="ConfirmationList" style="overflow:scroll; width:290px; height:240px; border:1 #000000 solid; text-align: left;  padding: 2px; text-align:center">
            </div>
            <div id="ConfirmationButtons" style="text-align:center;">
              <span id="ConfirmationOk" class="yui-button yui-push-button" style="font-size: 1em">
                <span class="first-child">
                  <button type="button" id="buttonConfirmationOk" name="buttonConfirmationOk">Ok</button>
                </span>
              </span>
              <span id="ConfirmationCancel" class="yui-button yui-push-button" style="font-size: 1em">
                <span class="first-child">
                  <button type="button" name="buttonConfirmationCancel" onclick="hideConfirmationPanel();">Cancel</button>
                </span>
              </span>
            </div>
          </div>
        </div>
        <div id="ActionPanel" style="font-size: 0.85em; display:none;">
          <div id="ahd" class="hd">Action Header.</div>
          <div id="abd" class="bd">
            <div id="dt_Action">
            </div>
            <div style="height: 20px; padding-top: 5px;">
              <span id="ActionProgress"></span>
              <span id="ActionButtons">
                <span id="ActionOk" class="yui-button yui-push-button">
                  <span class="first-child">
                    <button type="button" id="buttonActionOk" name="buttonActionOk">Apply</button>
                  </span>
                </span>
                <span id="ActionCancel" class="yui-button yui-push-button">
                  <span class="first-child">
                    <button type="button" id="buttonActionCancel" name="buttonActionCancel" onclick="hideActionPanel();">Cancel</button>
                  </span>
                </span>
              </span>
            </div>
          </div>
        </div>
        <div id="statusdiv"></div>
        <div id="AddEclPanel" style="display:none">
          <div id="eclhd" class="hd">Add ECL Query from File.</div>
          <div id="eclbd" class="bd">
            <div id="AddEclForm" style="height:250px;">
              <FORM encType="multipart/form-data" name="EclFileForm" id="EclFileForm" onsubmit="return onSubmit()" method="post" action="/ws_roxieconfig/NavMenuEvent?Cmd=NewDeployECLFile">
                <INPUT id="component" value="ws_roxieconfig" type="hidden" name="comp" />
                <INPUT id="command" value="DeployECLFileForm" type="hidden" name="command" />
                <div id="EclFiles.EclFile.0.div">
                </div>
                <div id="EclFiles.EclFile.1.div">
                </div>
                <div id="EclFiles.EclFile.2.div">
                </div>
                <div id="EclFiles.EclFile.3.div">
                </div>
                <div id="EclFiles.EclFile.4.div">
                </div>
                <div id="EclFiles.EclFile.5.div">
                </div>
                <div id="EclFiles.EclFile.6.div">
                </div>
                <div id="EclFiles.EclFile.7.div">
                </div>
                <div id="EclFiles.EclFile.8.div">
                </div>
                <div id="EclFiles.EclFile.9.div">
                </div>
                <INPUT id="EclFiles.EclFile.itemlist" value="+0+" type="hidden" name="EclFiles.EclFile.itemlist" />
              </FORM>
            </div>
            <div style="height: 20px; padding-top: 5px;">
            <span id="AddEclButtons">
              <span id="AddEclOk" class="yui-button yui-push-button">
                <span class="first-child">
                  <button type="button" id="buttonAddEclOk" name="buttonAddEclOk" onclick="submitAddEclPanel();">Apply</button>
                </span>
              </span>
              <span id="AddEclCancel" class="yui-button yui-push-button">
                <span class="first-child">
                  <button type="button" id="buttonAddEclCancel" name="buttonAddEclCancel" onclick="hideAddEclPanel();">Cancel</button>
                </span>
              </span>
            </span>
          </div>
          </div>
        </div> 
      </body>
    </html>

  </xsl:template>


</xsl:stylesheet>
