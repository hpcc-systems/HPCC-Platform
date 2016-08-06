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
    <xsl:variable name="clusterselected" select="WUQuerySetDetailsResponse/ClusterName"/>
    <xsl:variable name="filtertype" select="WUQuerySetDetailsResponse/FilterType"/>
    <xsl:variable name="filter" select="WUQuerySetDetailsResponse/Filter"/>
    <xsl:template match="/WUQuerySetDetailsResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <xsl:text disable-output-escaping="yes"><![CDATA[
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/tabview/assets/skins/sam/tabview.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/treeview/assets/skins/sam/treeview.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/paginator/assets/skins/sam/paginator.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/datatable/assets/skins/sam/datatable.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/container/assets/skins/sam/container.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
          <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/yahoo/yahoo-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/event/event-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/dom/dom-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/element/element-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/connection/connection-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/datasource/datasource-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/datatable/datatable-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/json/json-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/tabview/tabview-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/button/button-min.js"></script>
        ]]></xsl:text>

        <script language="JavaScript1.2">
            var querySet = '<xsl:value-of select="QuerySetName"/>';
            var clusterName = '<xsl:value-of select="ClusterName"/>';
            var countClusters = '<xsl:value-of select="count(ClusterNames/Item)"/>';
            <xsl:text disable-output-escaping="yes"><![CDATA[
              function onLoad() {
                if ((countClusters > 0) && ((clusterName == '') || (clusterName == '--')))
                {
                    document.getElementById('divStatus').style.visibility = "hidden";
                }
              }

              var selectedRows = 0;
              var sortableTable = null;

              function deleteQueries() {
                actionWorkunits('Delete');
              }

              function toggleQueries() {
                actionWorkunits('ToggleSuspend');
              }
              function activateQueries() {
                actionWorkunits('Activate');
              }

              function actionWorkunits(Action) {
                  var connectionCallback = {
                      success: function(o) {
                          var xmlDoc = o.responseXML;
                          document.location.replace( document.location.href );

                      },
                      failure: function(o) {
                          alert('Failure:' + o.statusText);

                      }
                  };

                  var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns="http://webservices.seisint.com/WsWorkunits"><soap:Body>' + getQueryActions(Action) + '</soap:Body></soap:Envelope>';

                  YAHOO.util.Connect.initHeader("SOAPAction", "/WsWorkunits/WUQuerysetQueryAction?");
                  YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
                  YAHOO.util.Connect._use_default_post_header = false;

                  var getXML = YAHOO.util.Connect.asyncRequest("POST",
                          "/WsWorkunits/WUQuerysetQueryAction",
                          connectionCallback, postBody);
                  return;

              }

              function getQueryActions(Action) {
                  var soapXML = '<WUQuerysetQueryAction><QuerySetName>' + querySet + '</QuerySetName><Action>' + Action + '</Action><Queries>';

                  // get records and iterate.

                  var selectedRows = queryDataTable.getSelectedRows();
                  for (var i = 0; i < selectedRows.length; i++) {
                     var record = queryDataTable.getRecord(selectedRows[i]);
                     soapXML += '<QuerySetQueryActionItem><QueryId>' + record.getData('Id') + '</QueryId><ClientState><Suspended>' + record.getData('Suspended') + '</Suspended></ClientState></QuerySetQueryActionItem>';
                  }
                  soapXML += '</Queries></WUQuerysetQueryAction>';
                  return soapXML;
              }

              function deleteAliases() {
                actionAliases('Deactivate');
              }

              function actionAliases(Action) {
                  var connectionCallback = {
                      success: function(o) {
                          var xmlDoc = o.responseXML;
                          document.location.replace( document.location.href );

                      },
                      failure: function(o) {
                          alert('Failure:' + o.statusText);

                      }
                  };

                  var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body>' + getAliasActions(Action) + '</soap:Body></soap:Envelope>';

                  YAHOO.util.Connect.initHeader("SOAPAction", "/WsWorkunits/WUQuerysetActionAliases?");
                  YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
                  YAHOO.util.Connect._use_default_post_header = false;

                  var getXML = YAHOO.util.Connect.asyncRequest("POST",
                          "/WsWorkunits/WUQuerysetActionAliases",
                          connectionCallback, postBody);
                  return;

              }

              function getAliasActions(Action) {
                  var soapXML = '<WUQuerysetAliasAction><QuerySetName>' + querySet + '</QuerySetName><Action>' + Action + '</Action><Aliases>';

                  // get records and iterate.

                  var selectedRows = aliasDataTable.getSelectedRows();
                  for (var i = 0; i < selectedRows.length; i++) {
                     var record = aliasDataTable.getRecord(selectedRows[i]);
                     soapXML += '<Alias><Name>' + record.getData('Name') + '</Name></Alias>';
                  }
                  soapXML += '</Alias></WUQuerysetAliasAction>';
                  return soapXML;
              }



              var formatCheckColumn = function(elCell, oRecord, oColumn, sData) {
                  if (sData == "1") {
                      elCell.innerHTML = '<img src="/esp/files/yui/build/assets/skins/sam/menuitem_checkbox.png" />';
                  }
              };

              function reloadPage()
              {
                  var clusterSelect = document.getElementById('Clusters');
                  if (clusterSelect.options[clusterSelect.selectedIndex].value == '--')
                  {
                      document.location.href='/WsWorkunits/WUQuerysetDetails?QuerySetName=' + querySet;
                      return;
                  }

                  var optionSelect = document.getElementById('Status');
                  var opt = ('&ClusterName='+ clusterSelect.options[clusterSelect.selectedIndex].value);
                  opt += ('&FilterType=Status&Filter='+ optionSelect.options[optionSelect.selectedIndex].value);
                  document.location.href='/WsWorkunits/WUQuerysetDetails?QuerySetName=' + querySet + opt;
              }

              var queryDataTable;
              var aliasDataTable;

              YAHOO.util.Event.addListener(window, "load", function() {
                LoadQueries = function() {
                  var queryColumnDefs = [
                    {key:"Id", sortable:true, resizeable:true},
                    {key:"Name", sortable:true, resizeable:true},
                    {key:"Wuid", sortable:true, resizeable:true},
                    {key:"Dll", sortable:true, resizeable:true},
                    {key:"Suspended", formatter: formatCheckColumn, sortable:true, resizeable:true}
                  ];
                  if (clusterName != '')
                    queryColumnDefs = [
                        {key:"Id", sortable:true, resizeable:true},
                        {key:"Name", sortable:true, resizeable:true},
                        {key:"Wuid", sortable:true, resizeable:true},
                        {key:"Dll", sortable:true, resizeable:true},
                        {key:"Suspended", formatter: formatCheckColumn, sortable:true, resizeable:true},
                        {key:"Status", sortable:true, resizeable:true}
                      ];

                  var queryDataSource = new YAHOO.util.DataSource(querysetQueries);
                  queryDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
                  if (clusterName == '')
                  queryDataSource.responseSchema = {
                    fields: ["Id","Name","Wuid","Dll","Suspended"]
                  };
                  else
                  queryDataSource.responseSchema = {
                    fields: ["Id","Name","Wuid","Dll","Suspended","Status"]
                  };

                  queryDataTable = new YAHOO.widget.DataTable("querydiv",
                  queryColumnDefs, queryDataSource);

                  queryDataTable.subscribe("rowMouseoverEvent", queryDataTable.onEventHighlightRow);   
                  queryDataTable.subscribe("rowMouseoutEvent", queryDataTable.onEventUnhighlightRow);   
                  queryDataTable.subscribe("rowClickEvent", queryDataTable.onEventSelectRow);   

                  return {
                    oqDS: queryDataSource,
                    oqDT: queryDataTable
                  };
                }();

                LoadAliases = function() {
                  var aliasColumnDefs = [
                    {key:"Name", sortable:true, resizeable:true},
                    {key:"Id", sortable:true, resizeable:true}
                  ];

                  var aliasDataSource = new YAHOO.util.DataSource(querysetAliases);
                  aliasDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
                  aliasDataSource.responseSchema = {
                    fields: ["Name","Id"]
                  };

                  aliasDataTable = new YAHOO.widget.DataTable("aliasdiv",
                  aliasColumnDefs, aliasDataSource);

                  aliasDataTable.subscribe("rowMouseoverEvent", aliasDataTable.onEventHighlightRow);   
                  aliasDataTable.subscribe("rowMouseoutEvent", aliasDataTable.onEventUnhighlightRow);   
                  aliasDataTable.subscribe("rowClickEvent", aliasDataTable.onEventSelectRow);   

                  return {
                    oqDS: aliasDataSource,
                    oqDT: aliasDataTable
                  };
                }();

                LoadTabs = function() {
                  var myTabs = new YAHOO.widget.TabView("QuerysetTab");
                }();

              });

                ]]></xsl:text>

            var querysetQueries = [ <xsl:apply-templates select="QuerysetQueries"/>
            ];

            var querysetAliases = [ <xsl:apply-templates select="QuerysetAliases"/>
            ];
            

        </script>
      </head>
      <body onload="nof5();onLoad()" class="yui-skin-sam">
        <h3><xsl:value-of select="QuerySetName"/> Queryset </h3>
        <div id="QuerysetTab" class="yui-navset">
          <ul class="yui-nav">
            <li class="selected">
              <a href="#Queries">
                <em>Queries</em>
              </a>
            </li>
            <li>
              <a href="#tab2">
                <em>Aliases</em>
              </a>
            </li>
          </ul>
          <div class="yui-content">
            <div>
                <table>
                    <xsl:if test="count(ClusterNames/Item)">
                        <tr>
                            <td align="left">
                                Cluster:
                                <select id="Clusters" name="Clusters" size="1" onchange="reloadPage()">
                                    <option>--</option>
                                    <xsl:for-each select="ClusterNames/Item">
                                        <xsl:choose>
                                            <xsl:when test="$clusterselected=.">
                                                <option value="{.}" selected="selected">
                                                    <xsl:value-of select="."/>
                                                </option>
                                            </xsl:when>
                                            <xsl:otherwise>
                                                <option value="{.}">
                                                    <xsl:value-of select="."/>
                                                </option>
                                            </xsl:otherwise>
                                        </xsl:choose>
                                    </xsl:for-each>
                                </select>
                                <span id="divStatus">
                                    <xsl:text disable-output-escaping="yes">&amp;nbsp;&amp;nbsp;&amp;nbsp;</xsl:text>
                                    Query Status:
                                    <select id="Status" name="Status" size="1" onchange="reloadPage()">
                                        <option value="All">All</option>
                                        <xsl:choose>
                                            <xsl:when test="$filtertype='Status' and $filter='Available'">
                                                <option value="Available" selected="selected">Available</option>
                                            </xsl:when>
                                            <xsl:otherwise>
                                                <option value="Available">Available</option>
                                            </xsl:otherwise>
                                        </xsl:choose>
                                        <xsl:choose>
                                            <xsl:when test="$filtertype='Status' and $filter='Suspended'">
                                                <option value="Suspended" selected="selected">Suspended</option>
                                            </xsl:when>
                                            <xsl:otherwise>
                                                <option value="Suspended">Suspended</option>
                                            </xsl:otherwise>
                                        </xsl:choose>
                                        <xsl:choose>
                                            <xsl:when test="$filtertype='Status' and $filter='NotFound'">
                                                <option value="NotFound" selected="selected">Not Found</option>
                                            </xsl:when>
                                            <xsl:otherwise>
                                                <option value="NotFound">Not Found</option>
                                            </xsl:otherwise>
                                        </xsl:choose>
                                    </select>
                                </span>
                            </td>
                        </tr>
                        <tr>
                            <td>
                            </td>
                        </tr>
                    </xsl:if>
                    <tr>
                        <td>
                            <div id="querydiv">&#160;</div>
                        </td>
                    </tr>
                    <tr>
                        <td>
                          <br/>
                          <span id="DeleteQueryButton1" class="yui-button yui-push-button">
                            <em class="first-child">
                                <button type="button" name="DeleteQueryButton1" onclick="deleteQueries();">Delete</button>
                            </em>
                          </span>
                          <span id="ToggleSuspendButton" class="yui-button yui-push-button">
                            <em class="first-child">
                              <button type="button" name="ToggleSuspendButton" onclick="toggleQueries();">Toggle Suspend</button>
                            </em>
                          </span>
                          <span id="ActivateButton" class="yui-button yui-push-button">
                            <em class="first-child">
                                <button type="button" name="ActivateButton" onclick="activateQueries();">Activate</button>
                            </em>
                          </span>
                        </td>
                    </tr>
                </table>
            </div>
            <div>
              <div id="aliasdiv">&#160;</div>
              <br/>
              <span id="DeleteAliasButton" class="yui-button yui-push-button">
                <em class="first-child">
                  <button type="button" name="DeleteAliasButton" onclick="deleteAliases();">Delete</button>
                </em>
              </span>
            </div>
          </div>
        </div>


      </body>
        </html>
    </xsl:template>


  <xsl:template match="QuerysetQueries">

                <xsl:apply-templates select="QuerySetQuery"/>

  </xsl:template>

  <xsl:template match="QuerySetQuery">
      <xsl:choose>
          <xsl:when test="$clusterselected = ''">
              {Id:'<xsl:value-of select="Id"/>', Name:'<xsl:value-of select="Name"/>', Wuid:'<xsl:value-of select="Wuid"/>', Dll:'<xsl:value-of select="Dll"/>', Suspended:'<xsl:value-of select="Suspended"/>'}<xsl:if test="position()!=last()">,</xsl:if>
          </xsl:when>
          <xsl:otherwise>
              {Id:'<xsl:value-of select="Id"/>', Name:'<xsl:value-of select="Name"/>', Wuid:'<xsl:value-of select="Wuid"/>', Dll:'<xsl:value-of select="Dll"/>', Suspended:'<xsl:value-of select="Suspended"/>', Status:'<xsl:value-of select="Clusters/ClusterQueryState[1]/State"/>'}<xsl:if test="position()!=last()">,</xsl:if>
          </xsl:otherwise>
      </xsl:choose>
  </xsl:template>


  <xsl:template match="QuerysetAliases">

    <xsl:apply-templates select="QuerySetAlias"/>

  </xsl:template>

  <xsl:template match="QuerySetAlias">
    {Name:'<xsl:value-of select="Name"/>', Id:'<xsl:value-of select="Id"/>'}<xsl:if test="position()!=last()">,</xsl:if>
  </xsl:template>

  <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
