<?xml version="1.0" encoding="utf-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
  <!--uncomment these for production-->
  <!ENTITY filePathEntity "/esp/files_">
  <!ENTITY xsltPathEntity "/esp/xslt">

  <!--uncomment these for debugging and change it to valid path-->
  <!--!ENTITY filePathEntity "d:/development/esp/files">
    <!ENTITY xsltPathEntity "d:/development/esp/services"-->
]>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
                              >
  <xsl:output method="html"/>

  <xsl:variable name="autoRefresh" select="5"/>

  <xsl:variable name="memThreshold" select="/TpTargetClusterQueryResponse/MemThreshold"/>
  <xsl:variable name="diskThreshold" select="/TpTargetClusterQueryResponse/DiskThreshold"/>
  <xsl:variable name="cpuThreshold" select="/TpTargetClusterQueryResponse/CpuThreshold"/>
  <xsl:variable name="memThresholdType" select="/TpTargetClusterQueryResponse/MemThresholdType"/>
  <!-- % -->
  <xsl:variable name="diskThresholdType" select="/TpTargetClusterQueryResponse/DiskThresholdType"/>
  <!-- % -->
  <!--xsl:variable name="encapsulatedSystem" select="/TpServiceQueryResponse/EncapsulatedSystem"/-->
  <xsl:variable name="addProcessesToFilter" select="/TpTargetClusterQueryResponse/PreflightProcessFilter"/>
  <xsl:variable name="enableSNMP" select="0"/>
  <xsl:variable name="showDetails" select="/TpTargetClusterQueryResponse/ShowDetails"/>
  <xsl:variable name="fromTargetClusterPage" select="1"/>
  <xsl:variable name="countTargetClusters" select="count(/TpTargetClusterQueryResponse/TpTargetClusters/TpTargetCluster)"/>

  <xsl:include href="&xsltPathEntity;/ws_machine/preflightControls.xslt"/>
  
  <xsl:template match="TpTargetClusterQueryResponse">
        <html>
            <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/default.css"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="&filePathEntity;/css/sortabletable.css"/>
        <style type="text/css">
          table.C { border: 0; cellpadding:0; cellspacing:0}
          table.C th, table.C td {border: 0; }
          .grey { background-color: #eee}
          .blueline {
          border-collapse: collapse;
          text-align: left;
          }
          .blueline thead {
          background-color: #eee;
          }
          .blueline thead tr th {
          border: gray 1px solid;
          border-collapse: collapse;
          }
          .content th {
          border: lightgrey 1px solid;
          border-top:0;
          text-align:left;
          }
        </style>
        <title>EclWatch</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/container/assets/skins/sam/container.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link type="text/css" rel="StyleSheet" href="/esp/files/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/animation/animation-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/container/container-min.js"></script>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
          <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
        </script>
        <script type="text/javascript" src="files_/scripts/sortabletable.js">
          <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
        </script>
        <script language="JavaScript1.2">
          var countTCs=<xsl:value-of select="$countTargetClusters"/>;
          <xsl:text disable-output-escaping="yes"><![CDATA[
          var fromTargetClusterPage = true;
          var allowReloadPage = false;
          var clusterChecked = 0;

          function onLoad()
          {
            document.getElementsByName('TargetClusters.itemcount')[0].value = countTCs;
            initSelection('resultsTable');

            if (countTCs > 1)
            { //no target cluster is selected when the page is loaded.
              document.getElementById( 'TargetClusters.All' ).checked = false;
              document.getElementById( 'TargetClusters.All2' ).checked = false;
            }

            initPreflightControls();
          }

          function showDetails(type, name)
          {
            var url = "/WsTopology/TpTargetClusterQuery?ShowDetails=true";
            url = url + "&Type=" + type + "&Name=" + name;
            document.location.href = url;
          }

          function toggleTargetCluster(ElementId)
          {
            var obj = document.getElementById('div_' + ElementId);
            explink = document.getElementById('explink_' + ElementId.toLowerCase());
            if (obj) 
            {
              if (obj.style.visibility == 'visible')
              {
                obj.style.display = 'none';
                obj.style.visibility = 'hidden';
                if (explink)
                {
                  explink.className = 'wusectionexpand';
                }
              }
              else
              {
                obj.style.display = 'inline';
                obj.style.visibility = 'visible';
                if (explink)
                {
                  explink.className = 'wusectioncontract';
                }
                else
                {
                  alert('could not find: explink' + ElementId);
                }
                reloadSection(ElementId);
              }
            }
          }

          function toggleElement(id)
          {
            span = document.getElementById( 'div_' + id );
            img  = document.getElementById( 'toggle_' + id );
            var show = span.style.display == 'none';
            span.style.display = show ? 'block' : 'none';
            img.src = '/esp/files_/img/folder' + (show?'open':'') + '.gif';
          }

          function getConfigXML(url) 
          {
            document.location.href = url;
          }   

          var browseUrl = null;
          var browsePath = null;
          var browseCaption = null;

          function popup(ip, path, url, caption, os)
          {
            browseUrl = url;
            browsePath = path;
            browseCaption = caption;
            mywindow = window.open ("/FileSpray/FileList?Mask=*.log&Netaddr="+ip+"&OS="+os+"&Path="+path, 
              "mywindow", "location=0,status=1,scrollbars=1,resizable=1,width=500,height=600");
            if (mywindow.opener == null)
              mywindow.opener = window;
            mywindow.focus();
            return false;
          } 

          //note that the following function gets invoked from the file selection window
          //
          function setSelectedPath(path)
          {
            //document.location = browseUrl + path + browseCaption;
            document.location = "/esp/iframe?" + browseCaption + "&inner=" + browseUrl + path;
          }

          function clickTCCheckbox(type, name, o) 
          {
            if (countTCs < 1)
              return;

            if ((o.id == 'TargetClusters.All') || (o.id == 'TargetClusters.All2'))
            {
              for (i=0; i<countTCs; i++)
                document.getElementById( 'TargetClusters.' + i ).checked = o.checked;
              clusterChecked = o.checked? countTCs:0;

              if (o.id == 'TargetClusters.All')
                document.getElementById( 'TargetClusters.All2' ).checked = o.checked;
              else
                document.getElementById( 'TargetClusters.All' ).checked = o.checked;
            }
            else
            {
              if (o.checked)
                clusterChecked++;
              else
                clusterChecked--;

              if (countTCs > 1)
              {
                document.getElementById( 'TargetClusters.All' ).checked = (clusterChecked == countTCs)? true: false;
                document.getElementById( 'TargetClusters.All2' ).checked = (clusterChecked == countTCs)? true: false;
              }
            }

            if (clusterChecked > 0)
              document.forms[0].submitBtn.disabled = false;
            else
              document.forms[0].submitBtn.disabled = true;
          }

          ]]></xsl:text>
        </script>
        <xsl:call-template name="GenerateScriptForPreflightControls"/>
      </head>
            <body class="yui-skin-sam" onload="nof5();onLoad();">
                <h3>Target Clusters:</h3>
                <form id="listitems" action="/ws_machine/GetTargetClusterInfo" method="post">
                    <input type="hidden" name="TargetClusters.itemcount" value=""/>
                    <xsl:if test="TpTargetClusters/TpTargetCluster[2]">
                        <table cellpadding="0" width="100%">
                            <tr>
                                <th id="selectAll" width="1%" style="padding-left:4px">
                                    <input type="checkbox" id="TargetClusters.All" name="TargetClusters.ALL"
                                           title="Select all target clusters" onclick="return clickTCCheckbox('', '', this);"></input>
                                </th>
                                <th colspan="5" align="left">Select All / None</th>
                            </tr>
                        </table>
                    </xsl:if>
                    <xsl:for-each select="TpTargetClusters/TpTargetCluster">
                        <xsl:call-template name="show-cluster">
                            <xsl:with-param name="type" select="Type"/>
                            <xsl:with-param name="name" select="Name"/>
                        </xsl:call-template>
                    </xsl:for-each>
                    <xsl:if test="TpTargetClusters/TpTargetCluster[2]">
                        <table cellpadding="0" width="100%">
                            <tr>
                                <th id="selectAll2" width="1%" style="padding-left:4px">
                                    <input type="checkbox" id="TargetClusters.All2" name="TargetClusters.ALL2"
                                           title="Select all target clusters" onclick="return clickTCCheckbox('', '', this);"></input>
                                </th>
                                <th colspan="5" align="left">Select All / None</th>
                            </tr>
                            <tr>
                                <td height="20"/>
                            </tr>
                        </table>
                    </xsl:if>
          <xsl:call-template name="ShowPreflightControls">
            <xsl:with-param name="method" select="'GetMachineInfo'"/>
            <xsl:with-param name="getProcessorInfo" select="1"/>
            <xsl:with-param name="getSoftwareInfo" select="1"/>
            <xsl:with-param name="getStorageInfo" select="1"/>
            <xsl:with-param name="applyProcessFilter" select="1"/>
            <xsl:with-param name="addProcessesToFilter" select="$addProcessesToFilter"/>
            <xsl:with-param name="enableSNMP" select="$enableSNMP"/>
            <xsl:with-param name="securityString" select="''"/>
            <xsl:with-param name="command" select="''"/>
            <xsl:with-param name="userid" select="''"/>
            <xsl:with-param name="password" select="''"/>
            <xsl:with-param name="isLinux" select="0"/>
            <xsl:with-param name="waitUntilDone" select="1"/>
            <xsl:with-param name="autoRefresh" select="0"/>
            <xsl:with-param name="targetCluster" select="1"/>
          </xsl:call-template>
                </form>
            </body>

        </html>
    </xsl:template>

    <xsl:template name="show-cluster">
        <xsl:param name="type"/>
    <xsl:param name="name"/>
        <table id="resultsTable" class="sort-table" width="100%">
            <tr class="grey">
                <td valign="top" width="20">
          <input type="checkbox" id="TargetClusters.{count(preceding::TpTargetCluster)}" name="TargetClusters.{count(preceding::TpTargetCluster)}"
                                value="{$type}:{$name}" title="Select this target cluster" onclick="return clickTCCheckbox('{$type}', '{$name}', this);"></input>
        </td>
        <td align="left" width="20">
          <a href="javascript:showDetails('{$type}', '{$name}');">
            <img id="view_details_{$name}" border="0" src="&filePathEntity;/img/keyfile.png" alt="View details" title="View details" align="middle"/>
          </a>
        </td>
        <!--xsl:if test="type='RoxieCluster'">
          <td align="left" width="20">
            <a href="javascript:showMetrix('{$name}');">
              <img id="view_details_{$name}" border="0" src="&filePathEntity;/img/gal.gif" align="middle"/>
            </a>
          </td>
        </xsl:if-->
          <td width="1000">
          <div class="grey">
            <div class="WuGroupHdrLeft">
              <A href="javascript:void(0)" id="explink_{$name}_{position()}" class="wusectioncontract" 
                 onclick="toggleTargetCluster('{$name}_{position()}');" >
                <xsl:value-of select="$name"/>
              </A>
            </div>
          </div>                            
                </td>
            </tr>
      <tr>
        <td/>
        <td colspan="7" align="left" style="padding-left=30px">
          <span id="div_{$name}_{position()}" style="display:inline;visibility:visible">
                <table class="blueline" border="2" frame="box" rules="groups">
                  <colgroup>
                    <col width="2%"/>
                    <col width="20%"/>
                    <col width="20%"/>
                    <col width="10%"/>
                    <col width="8%"/>
                    <col width="25%"/>
                    <col width="20%"/>
                  </colgroup>
                  <tr bgcolor="#C0C0C0">
                    <th/>
                    <th align="center">Name</th>
                    <th align="center">Component</th>
                    <th align="center">Computer</th>
                    <th align="center">Platform</th>
                    <th align="center">Network Address</th>
                    <th align="center">Directory</th>
                  </tr>

                  <xsl:if test="count(TpClusters/TpCluster)">
                    <xsl:call-template name="showMachines">
                      <xsl:with-param name="caption" select="'Cluster Processes'"/>
                      <xsl:with-param name="nodes" select="TpClusters/TpCluster"/>
                      <xsl:with-param name="compType" select="'ClusterProcess'"/>
                      <xsl:with-param name="cluster" select="$name"/>
                    </xsl:call-template>
                  </xsl:if>
                  <xsl:if test="count(TpEclCCServers/TpEclServer)">
                    <xsl:call-template name="showMachines">
                      <xsl:with-param name="caption" select="'ECL CC Servers'"/>
                      <xsl:with-param name="nodes" select="TpEclCCServers/TpEclServer"/>
                      <xsl:with-param name="compType" select="'EclCCServerProcess'"/>
                      <xsl:with-param name="cluster" select="$name"/>
                    </xsl:call-template>
                  </xsl:if>
                  <xsl:if test="count(TpEclServers/TpEclServer)">
                    <xsl:call-template name="showMachines">
                      <xsl:with-param name="caption" select="'ECL Servers'"/>
                      <xsl:with-param name="nodes" select="TpEclServers/TpEclServer"/>
                      <xsl:with-param name="compType" select="'EclServerProcess'"/>
                      <xsl:with-param name="cluster" select="$name"/>
                    </xsl:call-template>
                  </xsl:if>
                  <xsl:if test="count(TpEclAgents/TpEclAgent)">
                    <xsl:call-template name="showMachines">
                      <xsl:with-param name="caption" select="'ECL Agents'"/>
                      <xsl:with-param name="showAgentExec" select="1"/>
                      <xsl:with-param name="nodes" select="TpEclAgents/TpEclAgent"/>
                      <xsl:with-param name="compType" select="'EclAgentProcess'"/>
                      <xsl:with-param name="cluster" select="$name"/>
                    </xsl:call-template>
                  </xsl:if>
                  <xsl:if test="count(TpEclSchedulers/TpEclScheduler)">
                    <xsl:call-template name="showMachines">
                      <xsl:with-param name="caption" select="'ECL Schedulers'"/>
                      <xsl:with-param name="nodes" select="TpEclSchedulers/TpEclScheduler"/>
                      <xsl:with-param name="compType" select="'EclSchedulerProcess'"/>
                      <xsl:with-param name="cluster" select="$name"/>
                    </xsl:call-template>
                  </xsl:if>
                </table>
          </span>
        </td>
      </tr>
        </table>
    </xsl:template>



  <xsl:template name="showMachines">
    <xsl:param name="caption"/>
    <xsl:param name="cluster"/>
    <xsl:param name="nodes"/>
    <xsl:param name="compType"/>
    <xsl:param name="showAgentExec" select="0"/>

    <xsl:if test="$nodes/TpMachines/*">
      <xsl:for-each select="$nodes">
        <xsl:sort select="Name"/>
        <xsl:choose>
          <xsl:when test="$compType = 'ClusterProcess'">
            <xsl:variable name="type2">
              <xsl:choose>
                <xsl:when test="Type='RoxieCluster'">Roxie</xsl:when>
                <xsl:when test="Type='ThorCluster'">Thor</xsl:when>
                <xsl:when test="Type='HoleCluster'">Hole</xsl:when>
              </xsl:choose>
            </xsl:variable>
            <xsl:variable name="type4">
              <xsl:choose>
                <xsl:when test="Type='RoxieCluster'">Roxie Cluster Process</xsl:when>
                <xsl:when test="Type='ThorCluster'">Thor Cluster Process</xsl:when>
                <xsl:when test="Type='HoleCluster'">Hole Cluster Process</xsl:when>
              </xsl:choose>
            </xsl:variable>
            <xsl:variable name="type3" select="translate($type2, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')"/>
            <xsl:variable name="absolutePath">
              <xsl:call-template name="makeAbsolutePath">
                <xsl:with-param name="path" select="Directory"/>
                <xsl:with-param name="isLinuxInstance" select="OS!=0"/>
              </xsl:call-template>
            </xsl:variable>
            <tr onmouseenter="this.bgColor = '#F0F0FF'">
              <xsl:variable name="iconName">
                <xsl:choose>
                  <xsl:when test="number($showDetails) = 1">folderopen</xsl:when>
                  <xsl:otherwise>folder</xsl:otherwise>
                </xsl:choose>
              </xsl:variable>
              <xsl:choose>
                <xsl:when test="position() mod 2">
                  <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                  <xsl:attribute name="onmouseleave">this.bgColor = '#FFFFFF'</xsl:attribute>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                  <xsl:attribute name="onmouseleave">this.bgColor = '#F0F0F0'</xsl:attribute>
                </xsl:otherwise>
              </xsl:choose>
                <xsl:variable name="compName" select="../../Name"/>
              <td width="1%" valign="top">
              </td>
              <td align="left" padding="0" margin="0">
                <a href="javascript:toggleElement('{$cluster}_{Name}_{position()}')">
                  <img id="toggle_{$cluster}_{Name}_{position()}" border="0" src="&filePathEntity;/img/{$iconName}.gif" align="middle"/>
                  <xsl:value-of select="Name"/>
                </a>
              </td>
              <td align="center">
                <xsl:value-of select="$type4"/>
              </td>
              <td>
              </td>
              <td align="center">
                <xsl:choose>
                  <xsl:when test="OS=0">Windows</xsl:when>
                  <xsl:when test="OS=2">Linux</xsl:when>
                  <xsl:when test="OS=1">Solaris</xsl:when>
                  <xsl:otherwise>Unknown</xsl:otherwise>
                </xsl:choose>
              </td>
              <td/>
              <td>
                <xsl:variable name="href0">
                  <xsl:text disable-output-escaping="yes">/esp/iframe?esp_iframe_title=Configuration file for </xsl:text>
                  <xsl:value-of select="concat($type2, ' cluster - ', Name)"/>
                  <xsl:text disable-output-escaping="yes">&amp;inner=/WsTopology/TpGetComponentFile%3fFileType%3dcfg%26CompType%3d</xsl:text>
                  <xsl:value-of select="concat($type2, 'Cluster%26CompName%3d', Name, '%26Directory%3d', $absolutePath, '%26OsType%3d', OS)"/>
                </xsl:variable>
                <img onclick="getConfigXML('{$href0}')" border="0" src="&filePathEntity;/img/config.png" alt="View configuration file..." 
                  title="View configuration file..." width="14" height="14"/>
                <xsl:value-of select="Directory"/>
              </td>
            </tr>
            <tr>
              <xsl:variable name="spanStyle">
                <xsl:choose>
                  <xsl:when test="number($showDetails) = 1">display:inline;visibility:visible</xsl:when>
                  <xsl:otherwise>display:none;</xsl:otherwise>
                </xsl:choose>
              </xsl:variable>
              <td/>
              <td/>
              <td colspan="6" align="left" style="padding-left=30px">
                <span id="div_{$cluster}_{Name}_{position()}" style="{$spanStyle}">
                  <table class="blueline" border="2" frame="box" rules="groups">
                      <colgroup>
                        <col width="100" align="left"/>
                        <col width="150"/>
                        <xsl:if test="Type='ThorCluster' and HasThorSpareProcess/text()='1'">
                          <col width="100"/>
                        </xsl:if>
                        <col width="150"/>
                        <col width="100"/>
                        <col width="100"/>
                        <col width="300"/>
                      </colgroup>
                      <thead>
                        <tr>
                          <th align="center">Name</th>
                          <th align="center">Component</th>
                          <xsl:if test="Type='ThorCluster' and HasThorSpareProcess/text()='1'">
                            <th>Action</th>
                          </xsl:if>
                          <th align="center">Network Address</th>
                          <th align="center">Domain</th>
                          <th align="center">Platform</th>
                        </tr>
                      </thead>
                      <tbody>
                        <xsl:apply-templates select="TpMachines/TpMachine"/>
                      </tbody>
                  </table>
                </span>
              </td>
            </tr>
          </xsl:when>
          <xsl:otherwise>
            <xsl:for-each select="TpMachines/*">
              <xsl:variable name="showDir">
                <xsl:choose>
                  <xsl:when test="$compType='EclCCServerProcess' or $compType='EclServerProcess' or $compType='EclSchedulerProcess'">
                    <xsl:value-of select="Directory"/>
                  </xsl:when>
                  <xsl:when test="$compType='EclAgentProcess'">
                    <xsl:value-of select="../../LogDir"/>
                  </xsl:when>
                </xsl:choose>
              </xsl:variable>
              <xsl:variable name="type4">
                <xsl:choose>
                  <xsl:when test="$compType='EclCCServerProcess'">Ecl CC Server Process</xsl:when>
                  <xsl:when test="$compType='EclServerProcess'">Ecl Server Process</xsl:when>
                  <xsl:when test="$compType='EclAgentProcess'">Ecl Agent Process</xsl:when>
                  <xsl:when test="$compType='EclSchedulerProcess'">Ecl Scheduler Process</xsl:when>
                </xsl:choose>
              </xsl:variable>              <xsl:variable name="compName" select="../../Name"/>
              <tr onmouseenter="this.bgColor = '#F0F0FF'">
                <xsl:choose>
                  <xsl:when test="position() mod 2">
                    <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                    <xsl:attribute name="onmouseleave">this.bgColor = '#FFFFFF'</xsl:attribute>
                  </xsl:when>
                  <xsl:otherwise>
                    <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                    <xsl:attribute name="onmouseleave">this.bgColor = '#F0F0F0'</xsl:attribute>
                  </xsl:otherwise>
                </xsl:choose>
            <td width="1%" valign="top">
            </td>
            <td align="left" padding="0" margin="0">
              <table class="C" width="100%">
                <tbody>
                  <tr>
                    <xsl:variable name="iconName">
                      <xsl:choose>
                        <xsl:when test="number($showDetails) = 1">folderopen</xsl:when>
                        <xsl:otherwise>folder</xsl:otherwise>
                      </xsl:choose>
                    </xsl:variable>
                    <td colspan="2" width="100%">
                        <xsl:choose>
                          <xsl:when test="$showAgentExec">
                            <a href="javascript:toggleElement('{../../../../Name}_{../../Name}_{position()}')">
                              <img id="toggle_{../../../../Name}_{../../Name}_{position()}" border="0" src="&filePathEntity;/img/{$iconName}.gif" align="middle"/>
                              <xsl:value-of select="../../Name"/>
                              <xsl:if test="count(../TpMachine) &gt; 1">
                                (Instance <xsl:value-of select="position()"/>)
                              </xsl:if>
                            </a>
                          </xsl:when>
                          <xsl:otherwise>
                            <xsl:value-of select="../../Name"/>
                            <xsl:if test="count(../TpMachine) &gt; 1">
                              (Instance <xsl:value-of select="position()"/>)
                            </xsl:if>
                          </xsl:otherwise>
                        </xsl:choose>
                      </td>
                  </tr>
                </tbody>
              </table>
            </td>
            <td align="center">
              <xsl:value-of select="$type4"/>
            </td>
            <td align="center">
              <xsl:value-of select="Name"/>
            </td>
            <td align="center">
              <xsl:choose>
                <xsl:when test="OS=0">Windows</xsl:when>
                <xsl:when test="OS=2">Linux</xsl:when>
                <xsl:when test="OS=1">Solaris</xsl:when>
                <xsl:otherwise>Unknown</xsl:otherwise>
              </xsl:choose>
            </td>
            <td align="center">
              <xsl:value-of select="Netaddress"/>
              <xsl:if test="not(Port=0)">
                :<xsl:value-of select="Port"/>
              </xsl:if>
            </td>
            <td>
                <xsl:variable name="absolutePath">
                  <xsl:call-template name="makeAbsolutePath">
                    <xsl:with-param name="path" select="Directory"/>
                    <xsl:with-param name="isLinuxInstance" select="OS!=0"/>
                  </xsl:call-template>
                </xsl:variable>
                <table width="100%" cellpadding="0" cellspacing="0" class="C">
                  <tbody>
                    <tr>
                      <td align="left" width="19">
                        <xsl:if test="$compType!=''">
                          <xsl:variable name="logDir">
                            <xsl:choose>
                              <xsl:when test="$compType!='EclAgentProcess'">
                                <xsl:value-of  select="string(../../LogDirectory)"/>
                              </xsl:when>
                              <xsl:otherwise>
                                <xsl:value-of  select="string(../../LogDir)"/>
                              </xsl:otherwise>
                            </xsl:choose>
                          </xsl:variable>
                          <xsl:variable name="logPath">
                            <xsl:choose>
                              <xsl:when test="$logDir!='' and $logDir!='.'">
                                <xsl:call-template name="makeAbsolutePath">
                                  <xsl:with-param name="path">
                                    <xsl:choose>
                                      <xsl:when test="starts-with($logDir, '.')">
                                        <xsl:value-of select="Directory"/>
                                        <xsl:value-of select="substring($logDir, 2)"/>
                                      </xsl:when>
                                      <xsl:when test="starts-with($logDir, '\') and string(OS)='0'">
                                        <xsl:value-of select="substring(Directory, 1, 2)"/>
                                        <xsl:value-of select="$logDir"/>
                                      </xsl:when>
                                      <xsl:otherwise>
                                        <xsl:value-of select="$logDir"/>
                                      </xsl:otherwise>
                                    </xsl:choose>
                                  </xsl:with-param>
                                  <xsl:with-param name="isLinuxInstance" select="OS!=0"/>
                                </xsl:call-template>
                              </xsl:when>
                              <xsl:otherwise>
                                <xsl:value-of select="$absolutePath"/>
                              </xsl:otherwise>
                            </xsl:choose>
                          </xsl:variable>
                          <a style="padding-right:2">
                            <xsl:variable name="url">
                              <xsl:text disable-output-escaping="yes">/WsTopology/TpGetComponentFile%3fNetAddress%3d</xsl:text>
                              <xsl:value-of select="Netaddress"/>
                              <xsl:text disable-output-escaping="yes">%26FileType%3dlog%26CompType%3d</xsl:text>
                              <xsl:value-of select="$compType"/>
                              <xsl:text disable-output-escaping="yes">%26OsType%3d</xsl:text>
                              <xsl:value-of select="OS"/>
                              <xsl:text disable-output-escaping="yes">%26Directory%3d</xsl:text>
                            </xsl:variable>
                            <xsl:variable name="pageCaption">
                              <xsl:variable name="captionLen" select="string-length($caption)-1"/>
                              <!--xsl:text>&amp;esp_iframe_title=Log file for </xsl:text-->
                              <xsl:text disable-output-escaping="yes">esp_iframe_title=Log file for </xsl:text>
                              <xsl:value-of select="substring($caption, 1, $captionLen)"/>
                              <xsl:text disable-output-escaping="yes"> '</xsl:text>
                              <xsl:value-of select="../../Name"/>
                              <xsl:text disable-output-escaping="yes">'</xsl:text>
                            </xsl:variable>
                            <xsl:attribute name="href">
                              <xsl:if test="$compType!='EclAgentProcess'">
                                <xsl:text disable-output-escaping="yes">/esp/iframe?</xsl:text>
                                <xsl:value-of select="$pageCaption"/>
                                <xsl:text disable-output-escaping="yes">&amp;inner=</xsl:text>
                                <xsl:value-of select="$url"/>
                                <xsl:value-of select="$logPath"/>
                              </xsl:if>
                            </xsl:attribute>
                            <xsl:attribute name="onclick">
                              <xsl:text disable-output-escaping="yes">return popup('</xsl:text>
                              <xsl:value-of select="Netaddress"/>
                              <xsl:text disable-output-escaping="yes">', '</xsl:text>
                              <xsl:value-of select="translate($logPath, '\', '/')"/>
                              <xsl:text disable-output-escaping="yes">', '</xsl:text>
                              <xsl:value-of select="$url"/>
                              <xsl:text disable-output-escaping="yes">', "</xsl:text>
                              <xsl:value-of select="$pageCaption"/>
                              <xsl:text disable-output-escaping="yes">", </xsl:text>
                              <xsl:value-of select="OS"/>
                              <xsl:text disable-output-escaping="yes">);</xsl:text>
                            </xsl:attribute>
                            <img border="0" src="&filePathEntity;/img/base.gif" alt="View log file" title="View log file" width="19" height="16"/>
                          </a>
                        </xsl:if>
                      </td>
                      <td width="14">
                        <xsl:choose>
                          <xsl:when test="$compType!='' and ($compType!='EclAgentProcess' or OS=0)">
                            <xsl:variable name="captionLen" select="string-length($caption)-1"/>
                            <xsl:variable name="href0">
                              <xsl:text disable-output-escaping="yes">/esp/iframe?esp_iframe_title=Configuration file for </xsl:text>
                              <xsl:value-of select="substring($caption, 1, $captionLen)"/>
                              <xsl:text disable-output-escaping="yes"> - </xsl:text>
                              <xsl:value-of select="../../Name"/>
                              <xsl:text disable-output-escaping="yes"></xsl:text>
                              <xsl:text disable-output-escaping="yes">&amp;inner=/WsTopology/TpGetComponentFile%3fNetAddress%3d</xsl:text>
                              <xsl:value-of select="Netaddress"/>
                              <xsl:text disable-output-escaping="yes">%26FileType%3dcfg%26Directory%3d</xsl:text>
                              <xsl:value-of select="$absolutePath"/>
                              <xsl:text disable-output-escaping="yes">%26CompType%3d</xsl:text>
                              <xsl:value-of select="$compType"/>
                              <xsl:text disable-output-escaping="yes">%26OsType%3d</xsl:text>
                              <xsl:value-of select="OS"/>
                            </xsl:variable>
                            <img onclick="getConfigXML('{$href0}')" border="0" src="/esp/files_/img/config.png" alt="View deployed configuration file"
                              title="View configuration file..." width="14" height="14"/>
                          </xsl:when>
                          <xsl:otherwise>
                            <img border="0" src="/esp/files_/img/blank.png" width="14" height="14"/>
                          </xsl:otherwise>
                        </xsl:choose>
                      </td>
                      <td align="left">
                        <xsl:value-of select="$showDir"/>
                      </td>
                    </tr>
                  </tbody>
                </table>
            </td>
          </tr>
          <xsl:if test="$showAgentExec">
            <xsl:variable name="absolutePath">
              <xsl:call-template name="makeAbsolutePath">
                <xsl:with-param name="path" select="string(Directory)"/>
                <xsl:with-param name="isLinuxInstance" select="OS!=0"/>
              </xsl:call-template>
            </xsl:variable>
            <xsl:variable name="logDir" select="string(../../LogDir)"/>
            <xsl:variable name="logPath">
              <xsl:choose>
                <xsl:when test="$logDir!='' and $logDir!='.'">
                  <xsl:call-template name="makeAbsolutePath">
                    <xsl:with-param name="path">
                      <xsl:choose>
                        <xsl:when test="starts-with($logDir, '.')">
                          <xsl:value-of select="Directory"/>
                          <xsl:value-of select="substring($logDir, 2)"/>
                        </xsl:when>
                        <xsl:when test="starts-with($logDir, '\') and string(OS)='0'">
                          <xsl:value-of select="substring(Directory, 1, 2)"/>
                          <xsl:value-of select="$logDir"/>
                        </xsl:when>
                        <xsl:otherwise>
                          <xsl:value-of select="$logDir"/>
                        </xsl:otherwise>
                      </xsl:choose>
                    </xsl:with-param>
                    <xsl:with-param name="isLinuxInstance" select="OS!=0"/>
                  </xsl:call-template>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:value-of select="$absolutePath"/>
                </xsl:otherwise>
              </xsl:choose>
            </xsl:variable>
            <tr>
              <xsl:variable name="spanStyle">
                <xsl:choose>
                  <xsl:when test="number($showDetails) = 1">display:inline;visibility:visible</xsl:when>
                  <xsl:otherwise>display:none;</xsl:otherwise>
                </xsl:choose>
              </xsl:variable>
              <td/>
              <td/>
              <td colspan="6" align="left" style="padding-left=30px">
                <span id="div_{../../../../Name}_{../../Name}_{position()}" style="{$spanStyle}">
                  <table class="blueline" border="2" frame="box" rules="groups">
                    <thead>
                      <tr>
                        <th align="center">Component</th>
                        <th align="center">Dali Server</th>
                        <!--th>WUQueueName</th-->
                        <th align="center">Configuration</th>
                      </tr>
                    </thead>
                    <tbody>
                      <xsl:variable name="netAddress" select="Netaddress"/>
                      <tr>
                        <td align="center">AgentExec</td>
                        <td align="center">
                          <xsl:value-of select="../../DaliServer"/>
                        </td>
                        <td>
                          <a>
                            <xsl:attribute name="href">
                              <xsl:variable name="captionLen" select="string-length($caption)-1"/>
                              <xsl:text disable-output-escaping="yes">/esp/iframe?esp_iframe_title=Configuration file for </xsl:text>
                              <xsl:value-of select="substring($caption, 1, $captionLen)"/>
                              <xsl:text disable-output-escaping="yes"> '</xsl:text>
                              <xsl:value-of select="../../Name"/>
                              <xsl:text disable-output-escaping="yes">'</xsl:text>
                              <xsl:text disable-output-escaping="yes">&amp;inner=/WsTopology/TpGetComponentFile%3fNetAddress%3d</xsl:text>
                              <xsl:value-of select="Netaddress"/>
                              <xsl:text disable-output-escaping="yes">%26FileType%3dcfg%26Directory%3d</xsl:text>
                              <xsl:value-of select="$absolutePath"/>
                              <xsl:text disable-output-escaping="yes">%26CompType%3dAgentExecProcess</xsl:text>
                              <xsl:text disable-output-escaping="yes">%26OsType%3d</xsl:text>
                              <xsl:value-of select="OS"/>
                            </xsl:attribute>
                            <img border="0" src="/esp/files_/img/config.png" alt="View deployed configuration file"
                              title="View configuration file..." width="14" height="14"/>
                          </a>
                          <xsl:value-of select="Directory"/>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </span>
              </td>
            </tr>
          </xsl:if>
        </xsl:for-each>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:for-each>
    </xsl:if>
  </xsl:template>

  <xsl:template match="TpMachine">
    <xsl:variable name="clusterName">
      <xsl:value-of  select="string(../../Name)"/>
    </xsl:variable>
    <xsl:variable name="clusterType">
      <xsl:value-of  select="string(../../Type)"/>
    </xsl:variable>
    <tr onmouseenter="this.bgColor = '#F0F0FF'">
        <xsl:choose>
          <xsl:when test="position() mod 2">
            <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
            <xsl:attribute name="onmouseleave">this.bgColor = '#FFFFFF'</xsl:attribute>
          </xsl:when>
          <xsl:otherwise>
            <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
            <xsl:attribute name="onmouseleave">this.bgColor = '#F0F0F0'</xsl:attribute>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:variable name="displayType">
          <xsl:choose>
            <xsl:when test="Type='ThorMasterProcess'">Thor Master</xsl:when>
            <xsl:when test="Type='ThorSlaveProcess'">Thor Slave</xsl:when>
            <xsl:when test="Type='ThorSpareProcess'">Thor Spare</xsl:when>
            <xsl:when test="Type='HoleSocketProcess'">Hole Socket Node</xsl:when>
            <xsl:when test="Type='HoleProcessorProcess'">Hole Processor</xsl:when>
            <xsl:when test="Type='HoleControlProcess'">Hole Control</xsl:when>
            <xsl:when test="Type='HoleCollatorProcess'">Hole Collator</xsl:when>
            <xsl:when test="Type='HoleStandbyProcess'">Hole Standby</xsl:when>
            <xsl:when test="Type='RoxieServerProcess'">Roxie Server</xsl:when>
            <xsl:when test="Type='DropZone'">Drop Zone</xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="Type"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <td align="center">
          <xsl:value-of select="Name"/>
        </td>
      <td align="center">
        <xsl:value-of select="$displayType"/>
          <xsl:if test="Type='ThorSlaveProcess'">
              <br/>
              <xsl:value-of select="concat('[', $clusterName, ', ', ProcessNumber, ']')"/>
          </xsl:if>
      </td>
      <xsl:if test="$clusterType='ThorCluster' and ../../HasThorSpareProcess/text()='1'">
          <td align="center">
            <xsl:if test="Type='ThorSlaveProcess'">
              <a href="/WsTopology/TpMachineQuery?Type=THORSPARENODES&amp;Cluster={$clusterName}&amp;OldIP={Netaddress}&amp;Path={../../Path}"
                title="Swap node...">
                Swap Node
              </a>
            </xsl:if>
          </td>
      </xsl:if>
      <td align="center">
          <xsl:value-of select="Netaddress"/>
      </td>
      <td align="center">
          <xsl:value-of select="Domain"/>
      </td>
      <td align="center">
          <xsl:choose>
            <xsl:when test="OS=0">Windows</xsl:when>
            <xsl:when test="OS=1">Solaris</xsl:when>
            <xsl:when test="OS=2">Linux</xsl:when>
            <xsl:otherwise>Unknown</xsl:otherwise>
          </xsl:choose>
      </td>
    </tr>
    </xsl:template>

  <xsl:template name="makeAbsolutePath">
    <xsl:param name="path"/>
    <xsl:param name="isLinuxInstance"/>
    <xsl:variable name="oldPathSeparator">
      <xsl:choose>
        <xsl:when test="$isLinuxInstance">'\:'</xsl:when>
        <xsl:otherwise>'/$'</xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <xsl:variable name="newPathSeparator">
      <xsl:choose>
        <xsl:when test="$isLinuxInstance">'/$'</xsl:when>
        <xsl:otherwise>'\:'</xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <xsl:variable name="newPath" select="translate($path, $oldPathSeparator, $newPathSeparator)"/>
    <xsl:if test="$isLinuxInstance and not(starts-with($newPath, '/'))">/</xsl:if>
    <xsl:value-of select="$newPath"/>
  </xsl:template>

  <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
