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
        ]]></xsl:text>

        <script language="JavaScript1.2">
          var querySet = '<xsl:value-of select="QuerySetName"/>';
          function onLoad() {
          }
          <xsl:text disable-output-escaping="yes"><![CDATA[
              var selectedRows = 0;
                            var sortableTable = null;

              function deleteQueries() {
                actionWorkunits('Remove');
              }

              function toggleQueries() {
                actionWorkunits('ToggleSuspend');
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

                  var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body>' + getQueryActions(Action) + '</soap:Body></soap:Envelope>';

                  YAHOO.util.Connect.initHeader("SOAPAction", "/WsWorkunits/WUQuerysetActionQueries?");
                  YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
                  YAHOO.util.Connect._use_default_post_header = false;

                  var getXML = YAHOO.util.Connect.asyncRequest("POST",
                          "/WsWorkunits/WUQuerysetActionQueries",
                          connectionCallback, postBody);
                  return;

              }

              function getQueryActions(Action) {
                  var remove = Action == 'Remove' ? 1 : 0;
                  var toggleSuspend = Action == 'ToggleSuspend' ? 1 : 0;
                  var soapXML = '<WUQuerysetActionQueries><QuerySetName>' + querySet + '</QuerySetName><Remove>' + remove + '</Remove><ToggleSuspend>' + toggleSuspend + '</ToggleSuspend><QuerysetQueryActions>';

                  // get records and iterate.

                  var selectedRows = queryDataTable.getSelectedRows();
                  for (var i = 0; i < selectedRows.length; i++) {
                     var record = queryDataTable.getRecord(selectedRows[i]);
                     soapXML += '<QuerysetQueryAction><Id>' + record.getData('Id') + '</Id><Suspended>' + record.getData('Suspended') + '</Suspended></QuerysetQueryAction>';
                  }
                  soapXML += '</QuerysetQueryActions></WUQuerysetActionQueries>';
                  return soapXML;
              }

              function deleteAliases() {
                actionAliases('Remove');
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

                  var postBody = '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/ws_roxieconfig"><soap:Body>' + getAliasActions(Action) + '</soap:Body></soap:Envelope>';

                  YAHOO.util.Connect.initHeader("SOAPAction", "/WsWorkunits/WUQuerysetActionAliases?");
                  YAHOO.util.Connect.initHeader("Content-Type", "text/xml");
                  YAHOO.util.Connect._use_default_post_header = false;

                  var getXML = YAHOO.util.Connect.asyncRequest("POST",
                          "/WsWorkunits/WUQuerysetActionAliases",
                          connectionCallback, postBody);
                  return;

              }

              function getAliasActions(Action) {
                  var remove = Action == 'Remove' ? 1 : 1;
                  var soapXML = '<WUQuerysetActionAliases><QuerySetName>' + querySet + '</QuerySetName><Remove>' + remove + '</Remove><QuerysetAliasActions>';

                  // get records and iterate.

                  var selectedRows = aliasDataTable.getSelectedRows();
                  for (var i = 0; i < selectedRows.length; i++) {
                     var record = aliasDataTable.getRecord(selectedRows[i]);
                     soapXML += '<QuerysetAliasAction><Id>' + record.getData('Id') + '</Id></QuerysetAliasAction>';
                  }
                  soapXML += '</QuerysetAliasActions></WUQuerysetActionAliases>';
                  return soapXML;
              }



              var formatCheckColumn = function(elCell, oRecord, oColumn, sData) {
                  if (sData == "1") {
                      elCell.innerHTML = '<img src="/esp/files/yui/build/assets/skins/sam/menuitem_checkbox.png" />';
                  }
              };

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

                  var queryDataSource = new YAHOO.util.DataSource(querysetQueries);
                  queryDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
                  queryDataSource.responseSchema = {
                    fields: ["Id","Name","Wuid","Dll","Suspended"]
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
              <div id="querydiv">&#160;</div>
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
    {Id:'<xsl:value-of select="Id"/>', Name:'<xsl:value-of select="Name"/>', Wuid:'<xsl:value-of select="Wuid"/>', Dll:'<xsl:value-of select="Dll"/>', Suspended:'<xsl:value-of select="Suspended"/>'}<xsl:if test="position()!=last()">,</xsl:if>
  </xsl:template>


  <xsl:template match="QuerysetAliases">

    <xsl:apply-templates select="QuerySetAlias"/>

  </xsl:template>

  <xsl:template match="QuerySetAlias">
    {Name:'<xsl:value-of select="Name"/>', Id:'<xsl:value-of select="Id"/>'}<xsl:if test="position()!=last()">,</xsl:if>
  </xsl:template>

  <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
