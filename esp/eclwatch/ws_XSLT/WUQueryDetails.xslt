<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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
    <xsl:output method="html"/>
    <xsl:template match="/WUQueryDetailsResponse">
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
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
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
                <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                ]]></xsl:text>
                <script language="JavaScript1.2">
                    var querySet = '<xsl:value-of select="QuerySet"/>';
                    var queryId = '<xsl:value-of select="QueryId"/>';
                    var queryName = '<xsl:value-of select="QueryName"/>';
                    var suspended = '<xsl:value-of select="Suspended"/>';
                    var activated = '<xsl:value-of select="Activated"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                      function deleteQuery() {
                        actionWorkunits('Delete');
                      }

                      function toggleQuery() {
                        actionWorkunits('ToggleSuspend');
                      }

                      function toggleActivated() {
                        if (activated == 1)
                          actionAliases('Deactivate');
                        else
                          actionWorkunits('Activate');
                      }

                      function getQueryActions(Action) {
                          var soapXML = '<WUQuerysetQueryAction><QuerySetName>' + querySet + '</QuerySetName><Action>' + Action + '</Action><Queries>';
                          soapXML += '<QuerySetQueryActionItem><QueryId>' + queryId + '</QueryId><ClientState><Suspended>' + suspended + '</Suspended></ClientState></QuerySetQueryActionItem>';
                          soapXML += '</Queries></WUQuerysetQueryAction>';
                          return soapXML;
                      }

                      function actionWorkunits(Action) {
                          var connectionCallback = {
                              success: function(o) {
                                  var xmlDoc = o.responseXML;
                                  if (Action == 'Delete') {
                                    document.location.replace( "/WsWorkunits/WUQuerysetDetails?QuerySetName=" + querySet);
                                  } else {
                                    document.location.replace(document.location.href);
                                  }
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

                      function getAliasActions(Action) {
                          var soapXML = '<WUQuerysetAliasAction><QuerySetName>' + querySet + '</QuerySetName><Action>' + Action + '</Action><Aliases>';
                          soapXML += '<Alias><Name>' + queryName + '</Name></Alias>';
                          soapXML += '</Aliases></WUQuerysetAliasAction>';
                          return soapXML;
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

                      function DFUFileDetails(fileName) {
                          document.location.href='/WsDfu/DFUInfo?Name='+escape(fileName);
                      }
                      function WUDetails(WUID) {
                          document.location.href='/WsWorkunits/WUInfo?Wuid='+escape(WUID);
                      }
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam">
                <xsl:variable name="suspendedOnClusters">
                    <xsl:for-each select="Clusters/ClusterQueryState[State='Suspended']">
                        <xsl:if test="position() > 1"><xsl:text>, </xsl:text></xsl:if><xsl:value-of select="Cluster"/><xsl:if test="MixedNodeStates=1"><xsl:text>(some nodes)</xsl:text></xsl:if>
                    </xsl:for-each>
                </xsl:variable>
                <h3>Query Details for <xsl:value-of select="QueryId"/></h3>
                <form>
                    <table style="text-align:left;" cellspacing="10">
                        <colgroup style="vertical-align:top;padding-right:10px;" span="2"/>
                        <tr>
                            <th>Name:</th>
                            <td><xsl:value-of select="QueryName"/></td>
                        </tr>
                        <tr>
                            <th>Query Set:</th>
                            <td><xsl:value-of select="QuerySet"/></td>
                        </tr>
                        <tr>
                            <th>WUID:</th>
                            <td>
                                <a href="javascript:void(0)" onclick="WUDetails('{Wuid}');">
                                    <xsl:value-of select="Wuid"/>
                                </a>
                            </td>
                        </tr>
                        <tr>
                            <th>Dll:</th>
                            <td>
                                <xsl:value-of select="Dll"/>
                            </td>
                        </tr>
                        <xsl:if test="string-length(PublishedBy)">
                            <tr>
                                <th>Published By:</th>
                                <td><xsl:value-of select="PublishedBy"/></td>
                            </tr>
                        </xsl:if>
                        <tr>
                            <th>Suspended By User:
                        </th>
                            <td>
                                <input type="checkbox" onclick="toggleQuery();">
                                    <xsl:if test="Suspended=1">
                                        <xsl:attribute name="checked"/>
                                    </xsl:if>
                                </input>
                            </td>
                        </tr>
                        <xsl:if test="string-length(SuspendedBy)">
                            <tr>
                                <th>Suspended By:</th>
                                <td><xsl:value-of select="SuspendedBy"/></td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="string-length($suspendedOnClusters)">
                            <tr>
                                <th>Suspended On Cluster(s):</th>
                                <td>
                                    <xsl:value-of select="$suspendedOnClusters"/>
                                </td>
                            </tr>
                        </xsl:if>
                        <tr>
                            <th>
                                Activated:
                            </th>
                            <td>
                                <input type="checkbox" onclick="toggleActivated();">
                                    <xsl:if test="Activated=1">
                                        <xsl:attribute name="checked"/>
                                    </xsl:if>
                                </input>
                            </td>
                        </tr>
                        <xsl:if test="string-length(Label)">
                            <tr>
                                <th>Label:</th>
                                <td><xsl:value-of select="Label"/></td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="string-length(Error)">
                            <tr>
                                <th>Error:</th>
                                <td><xsl:value-of select="Error"/></td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="string-length(Comment)">
                            <tr>
                                <th>Comment:</th>
                                <td><xsl:value-of select="Comment"/></td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="count(LogicalFiles/*)">
                            <tr>
                                <th valign="top">Files Used:</th>
                                <td>
                                    <table id="FileTable">
                                        <xsl:for-each select="LogicalFiles/Item">
                                            <tr>
                                                <td>
                                                    <a href="javascript:void(0)" onclick="DFUFileDetails('{.}');">
                                                        <xsl:value-of select="."/>
                                                    </a>
                                                </td>
                                            </tr>
                                        </xsl:for-each>
                                    </table>
                                </td>
                            </tr>
                        </xsl:if>
                      <xsl:if test="count(SuperFiles/SuperFile)">
                        <tr>
                          <th valign="top">SuperFiles<br/>
                            <img src="/esp/files/img/expand.gif" alt=" >> "/>SubFiles:
                            </th>
                          <td>
                            <table id="SuperFileTable">
                              <xsl:for-each select="SuperFiles/SuperFile">
                                <tr>
                                  <td>
                                    <a href="javascript:void(0)" onclick="DFUFileDetails('{.}');">
                                      <xsl:value-of select="Name"/>
                                    </a>
                                  </td>
                                </tr>
                                <tr>
                                  <td>
                                    <table>
                                      <xsl:for-each select="SubFiles/File">
                                        <tr>
                                          <td>
                                            <img src="/esp/files/img/expand.gif" alt=" >> "/>
                                            <a href="javascript:void(0)" onclick="DFUFileDetails('{.}');">
                                              <xsl:value-of select="."/>
                                            </a>
                                          </td>
                                        </tr>
                                      </xsl:for-each>
                                    </table>
                                  </td>
                                </tr>
                              </xsl:for-each>
                            </table>
                          </td>
                        </tr>
                      </xsl:if>
                    </table>
                </form>
                <input id="deleteBtn" type="button" value="Delete" onclick="deleteQuery();"> </input>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>

</xsl:stylesheet>
