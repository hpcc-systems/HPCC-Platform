<?xml version="1.0" encoding="UTF-8"?>
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
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="html"/>
    
<xsl:include href="&xsltPathEntity;/ws_machine/preflightControls.xslt"/>

<xsl:variable name="autoRefresh" select="5"/>

<xsl:variable name="memThreshold" select="/TpServiceQueryResponse/MemThreshold"/>
<xsl:variable name="diskThreshold" select="/TpServiceQueryResponse/DiskThreshold"/>
<xsl:variable name="cpuThreshold" select="/TpServiceQueryResponse/CpuThreshold"/>
<xsl:variable name="memThresholdType" select="/TpServiceQueryResponse/MemThresholdType"/><!-- % -->
<xsl:variable name="diskThresholdType" select="/TpServiceQueryResponse/DiskThresholdType"/><!-- % -->
<xsl:variable name="encapsulatedSystem" select="/TpServiceQueryResponse/EncapsulatedSystem"/>
<xsl:variable name="enableSNMP" select="/TpServiceQueryResponse/EnableSNMP"/>
<xsl:variable name="addProcessesToFilter" select="/TpServiceQueryResponse/PreflightProcessFilter"/>
<xsl:variable name="numFTSlaves" select="count(/TpServiceQueryResponse/ServiceList/TpFTSlaves/TpFTSlave/TpMachines/TpMachine)"/>
<xsl:variable name="acceptLanguage" select="/TpServiceQueryResponse/AcceptLanguage"/>
<xsl:variable name="localiseFile"><xsl:value-of select="concat('nls/', $acceptLanguage, '/hpcc.xml')"/></xsl:variable>
<xsl:variable name="hpccStrings" select="document($localiseFile)/hpcc/strings"/>

  <xsl:template match="/TpServiceQueryResponse">
    <script type="text/javascript" language="javascript">
      <![CDATA[
      function getConfigXML(url) {
          document.location.href = url;
      }                     
    ]]>
    </script>
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <xsl:choose>
            <xsl:when test="not(ServiceList/*)">
                <head>
                    <title>System Services</title>
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        </head>
        <body class="yui-skin-sam" onload="nof5();">
                    <h3><xsl:value-of select="$hpccStrings/st[@id='NoSystemServicesDefined']"/></h3>
                </body>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates select="ServiceList"/>
            </xsl:otherwise>
        </xsl:choose>
    </html>
</xsl:template>

<xsl:template match="ServiceList">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <title>System Servers</title>
    <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
    <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
    <link type="text/css" rel="StyleSheet" href="&filePathEntity;/css/sortabletable.css"/>
    <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
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
        <script language="javascript" src="&filePathEntity;/scripts/multiselect.js">
        </script>
        <script language="javascript">
            var numFTSlaves=<xsl:value-of select="$numFTSlaves"/>;
            <xsl:text disable-output-escaping="yes"><![CDATA[
      var fromTargetClusterPage = false;
            function onRowCheck(checked)
            {
                document.forms[0].submitBtn.disabled = checkedCount == 0;
            }
            function onLoad()
            {
                initSelection('resultsTable');
                document.getElementsByName('Addresses.itemcount')[0].value = totalItems;
                initPreflightControls();
                onRowCheck(true);
                toggleComponent('FTSlave', true);
            }

            function toggleComponent(ComponentType, onload)
            {
                var obj = document.getElementById('row_'+ComponentType+'_1');
                if (null == obj)
                    return;

                var display = '';
                var visibility = 'visible';
                var src = '/esp/files_/img/folderopen.gif';
                if (onload || obj.style.visibility == 'visible')
                {
                    display = 'none';
                    visibility = 'hidden';
                    src = '/esp/files_/img/folder.gif';
                }

                img  = document.getElementById( ComponentType + 'ExpLink' );
                if (img)
                    img.src = src;
                for (i = 1; i <=numFTSlaves; i++)
                {
                    obj = document.getElementById('row_'+ComponentType+'_'+i);
                    if (null == obj)
                        return;
                    obj.style.display = display;
                    obj.style.visibility = visibility;
                }
            }
            function toggleDetails(id)
            {
                span = document.getElementById( 'div_' + id );
                img  = document.getElementById( 'toggle_' + id );
                var show = span.style.display == 'none';
                var row = document.getElementById('row_' + id);
                row.vAlign = show ? 'top' : 'middle';
                span.style.display = show ? 'block' : 'none';
                img.src = '/esp/files_/img/folder' + (show?'open':'') + '.gif';
            }
            function toggleEclAgent(id)
            {
                span = document.getElementById( 'div_' + id );
                img  = document.getElementById( 'toggle_' + id );
                var show = span.style.display == 'none';
                span.style.display = show ? 'block' : 'none';
                img.src = '/esp/files_/img/folder' + (show?'open':'') + '.gif';
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

            function setPath(protocol, port)
            {
                var url = protocol + "://" + window.location.hostname + ":" + port;
                parent.document.location.href = url;
            }
            allowReloadPage = false;
            ]]>
        </xsl:text>
        </script>
     <xsl:call-template name="GenerateScriptForPreflightControls"/>
    </head>
  <body class="yui-skin-sam" onload="nof5();onLoad()">
        <form id="listitems" action="/ws_machine/GetMachineInfo" method="post">
            <h2><xsl:value-of select="$hpccStrings/st[@id='SystemServers']"/></h2>

            <table id="resultsTable" class="sort-table" width="100%">
            <colgroup>
                <col width="1%"/>
                <col width="25%"/>
                <col width="18%"/>
                <col width="18%"/>
                <col width="19%"/>
                <col width="19%"/>
            </colgroup>
            <tr class="grey">
                <th id="selectAll1" align="left" width="1%">
                    <input type="checkbox" title="{$hpccStrings/st[@id='SelectDeselectAllMachines']}" onclick="selectAll(this.checked)"/>
                </th>
                <th width="20%"><xsl:value-of select="$hpccStrings/st[@id='Name']"/></th>
                <th width="15%"><xsl:value-of select="$hpccStrings/st[@id='Queue']"/></th>
                <th><xsl:value-of select="$hpccStrings/st[@id='Computer']"/></th>
                <th><xsl:value-of select="$hpccStrings/st[@id='NetworkAddress']"/></th>
                <th><xsl:value-of select="$hpccStrings/st[@id='Directory']"/></th>
            </tr>

            <input type="hidden" name="Addresses.itemcount" value=""/>
            <xsl:call-template name="showMachines">
                <xsl:with-param name="caption" select="$hpccStrings/st[@id='DaliServers']"/>
                <xsl:with-param name="nodes" select="TpDalis/TpDali"/>
                <xsl:with-param name="compType" select="'DaliServerProcess'"/>
            </xsl:call-template>

            <xsl:call-template name="showMachines">
                <xsl:with-param name="caption" select="$hpccStrings/st[@id='DFUServers']"/>
                <xsl:with-param name="showQueue" select="1"/>
                <xsl:with-param name="nodes" select="TpDfuServers/TpDfuServer"/>
                <xsl:with-param name="compType" select="'DfuServerProcess'"/>
            </xsl:call-template>
            
            <xsl:call-template name="showMachines">
                <xsl:with-param name="caption" select="$hpccStrings/st[@id='DropZones']"/>
                <xsl:with-param name="showCheckbox" select="1"/>
                <xsl:with-param name="nodes" select="TpDropZones/TpDropZone"/>
                <xsl:with-param name="checked" select="0"/>
            </xsl:call-template>                    
            
                        <xsl:call-template name="showMachines">
                          <xsl:with-param name="caption" select="$hpccStrings/st[@id='ECLAgents']"/>
                          <xsl:with-param name="showAgentExec" select="1"/>
                          <xsl:with-param name="nodes" select="TpEclAgents/TpEclAgent"/>
                          <xsl:with-param name="compType" select="'EclAgentProcess'"/>
                        </xsl:call-template>

                        <xsl:call-template name="showMachines">
                          <xsl:with-param name="caption" select="$hpccStrings/st[@id='ECLServers']"/>
                          <xsl:with-param name="showQueue" select="1"/>
                          <xsl:with-param name="nodes" select="TpEclServers/TpEclServer"/>
                          <xsl:with-param name="compType" select="'EclServerProcess'"/>
                        </xsl:call-template>

                        <xsl:call-template name="showMachines">
                          <xsl:with-param name="caption" select="$hpccStrings/st[@id='ECLCCServers']"/>
                          <xsl:with-param name="showQueue" select="1"/>
                          <xsl:with-param name="nodes" select="TpEclCCServers/TpEclServer"/>
                          <xsl:with-param name="compType" select="'EclCCServerProcess'"/>
                        </xsl:call-template>

                        <xsl:call-template name="showMachines">
                          <xsl:with-param name="caption" select="$hpccStrings/st[@id='ECLSchedulers']"/>
                          <xsl:with-param name="showQueue" select="1"/>
                          <xsl:with-param name="nodes" select="TpEclSchedulers/TpEclScheduler"/>
                          <xsl:with-param name="compType" select="'EclSchedulerProcess'"/>
                        </xsl:call-template>

                        <xsl:call-template name="showMachines">
                <xsl:with-param name="caption" select="$hpccStrings/st[@id='ESPServers']"/>
                <xsl:with-param name="showBindings" select="1"/>
                <xsl:with-param name="nodes" select="TpEspServers/TpEspServer"/>
                <xsl:with-param name="compType" select="'EspProcess'"/>
            </xsl:call-template>
            
            <xsl:call-template name="showMachines">
                <xsl:with-param name="caption" select="$hpccStrings/st[@id='FTSlaves']"/>
                <xsl:with-param name="nodes" select="TpFTSlaves/TpFTSlave"/>
                <xsl:with-param name="checked" select="0"/>
            </xsl:call-template>                    
            
            <xsl:call-template name="showMachines">
                <xsl:with-param name="caption" select="$hpccStrings/st[@id='GenesisServers']"/>
                <xsl:with-param name="nodes" select="TpGenesisServers/TpGenesisServer"/>
            </xsl:call-template>                    
            
            <xsl:call-template name="showMachines">
                <xsl:with-param name="caption" select="$hpccStrings/st[@id='LDAPServers']"/>
                <xsl:with-param name="nodes" select="TpLdapServers/TpLdapServer"/>
                <xsl:with-param name="showDirectory" select="0"/>
                <xsl:with-param name="checked" select="0"/>
            </xsl:call-template>                    
            
            <xsl:call-template name="showMachines">
                <xsl:with-param name="caption" select="$hpccStrings/st[@id='MySQLServers']"/>
                <xsl:with-param name="nodes" select="TpMySqlServers/TpMySqlServer"/>
                <xsl:with-param name="showDirectory" select="0"/>
                    <xsl:with-param name="checked" select="0"/>
            </xsl:call-template>                    
            
            <xsl:call-template name="showMachines">
                <xsl:with-param name="caption" select="$hpccStrings/st[@id='SashaServers']"/>
                <xsl:with-param name="nodes" select="TpSashaServers/TpSashaServer"/>
                <xsl:with-param name="compType" select="'SashaServerProcess'"/>
            </xsl:call-template>
            <tr class="content">
                <th colspan="7">
                    <br/>
                </th>
            </tr>
        </table>

        <table cellpadding="0" width="100%">
            <tr>
                <th id="selectAll2" width="1%" style="padding-left:4px">
                    <input type="checkbox" title="{$hpccStrings/st[@id='SelectDeselectAllMachines']}" onclick="selectAll(this.checked)"></input>
                </th>
                <th colspan="5" align="left"><xsl:value-of select="$hpccStrings/st[@id='SelectAllOrNone']"/></th>
            </tr>
            <tr><td height="20"/></tr>
        </table>
        <xsl:call-template name="ShowPreflightControls">
          <xsl:with-param name="method" select="'GetMachineInfo'"/>
          <xsl:with-param name="getProcessorInfo" select="1"/>
          <xsl:with-param name="getSoftwareInfo" select="1"/>
          <xsl:with-param name="getStorageInfo" select="1"/>
          <xsl:with-param name="applyProcessFilter" select="1"/>
          <xsl:with-param name="securityString" select="''"/>
          <xsl:with-param name="command" select="''"/>
          <xsl:with-param name="userid" select="''"/>
          <xsl:with-param name="password" select="''"/>
          <xsl:with-param name="isLinux" select="0"/>
          <xsl:with-param name="waitUntilDone" select="1"/>
          <xsl:with-param name="autoRefresh" select="1"/>
          <xsl:with-param name="clusterType" select="''"/>
          <xsl:with-param name="addProcessesToFilter" select="$addProcessesToFilter"/>
          <xsl:with-param name="enableSNMP" select="$enableSNMP"/>
        </xsl:call-template>
    </form>
    </body>
</xsl:template>
        
<xsl:template name="showMachines">
    <xsl:param name="caption"/>
    <xsl:param name="showCheckbox" select="1"/>
    <xsl:param name="checked" select="1"/>
    <xsl:param name="showDirectory" select="1"/>
    <xsl:param name="showQueue" select="0"/>
    <xsl:param name="showBindings" select="0"/>
  <xsl:param name="showAgentExec" select="0"/>
  <xsl:param name="nodes"/>
    <xsl:param name="compType"/>
    
    <xsl:if test="$nodes/TpMachines/*">
        <tr class="content">
            <th colspan="7">
                <br/>
                <xsl:choose>
                    <xsl:when test="$caption=$hpccStrings/st[@id='FTSlaves']">
                        <a href="javascript:toggleComponent('FTSlave', false)">
                            <img id="FTSlaveExpLink" border="0" src="&filePathEntity;/img/folder.gif" align="middle"/>
                                <xsl:value-of select="$caption"/>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="$caption"/>
                    </xsl:otherwise>
                </xsl:choose>
            </th>
        </tr>
        <xsl:for-each select="$nodes">
            <xsl:sort select="Name"/>
            <xsl:for-each select="TpMachines/*">
                <tr>
                    <xsl:variable name="compName" select="../../Name"/>
                    <xsl:if test="$showBindings">
                        <xsl:attribute name="id">
                            <xsl:value-of select="concat('row_', $compName, '_', position())"/>
                        </xsl:attribute>
                    </xsl:if>
                    <xsl:if test="$caption=$hpccStrings/st[@id='FTSlaves']">
                        <xsl:attribute name="id">
                            <xsl:value-of select="concat('row_FTSlave_', position())"/>
                        </xsl:attribute>
                    </xsl:if>
                    <td width="1%" valign="top">
                        <xsl:if test="$showCheckbox">
                            <input type="checkbox" name="Addresses.{count(preceding::TpMachine)}"
                                value="{Netaddress}|{ConfigNetaddress}:{Type}:{$compName}:{OS}:{translate(Directory, ':', '$')}" onclick="return clicked(this, event)">
                                <xsl:if test="$checked">
                                    <xsl:attribute name="checked">true</xsl:attribute>
                                </xsl:if>
                            </input>
                        </xsl:if>
                    </td>
                    <td align="left" colspan="2" padding="0" margin="0">
                        <table class="C" width="100%">
                        <tbody>
                            <tr>
                                <xsl:choose>
                                    <xsl:when test="$showQueue">
                                        <td width="54%">
                                                <xsl:value-of select="../../Name"/>
                                                <xsl:if test="count(../TpMachine) &gt; 1"> (<xsl:value-of select= "$hpccStrings/st[@id='Instance']"/> <xsl:value-of select="position()"/>)</xsl:if>
                                            </td>
                                        <td width="45%" align="center"><xsl:value-of select="../../Queue"/></td>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <td colspan="2" width="100%">
                                            <xsl:choose>
                                                <xsl:when test="$showBindings">
                                                    <a href="javascript:toggleDetails('{../../Name}_{position()}')">
                                                        <img id="toggle_{../../Name}_{position()}" border="0" src="&filePathEntity;/img/folder.gif" align="middle"/>
                                                        <xsl:value-of select="../../Name"/>
                                                        <xsl:if test="count(../TpMachine) &gt; 1"> (<xsl:value-of select= "$hpccStrings/st[@id='Instance']"/> <xsl:value-of select="position()"/>)</xsl:if>
                                                    </a>
                                                </xsl:when>
                        <xsl:when test="$showAgentExec">
                          <a href="javascript:toggleEclAgent('{../../Name}_{position()}')">
                            <img id="toggle_{../../Name}_{position()}" border="0" src="&filePathEntity;/img/folder.gif" align="middle"/>
                            <xsl:value-of select="../../Name"/>
                            <xsl:if test="count(../TpMachine) &gt; 1">
                              (Instance <xsl:value-of select="position()"/>)
                            </xsl:if>
                          </a>
                        </xsl:when>
                        <xsl:otherwise>
                                                    <xsl:value-of select="../../Name"/>
                                                    <xsl:if test="count(../TpMachine) &gt; 1"> (Instance <xsl:value-of select="position()"/>)</xsl:if>
                                                </xsl:otherwise>
                                            </xsl:choose>
                                        </td>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </tr>
                        </tbody>
                     </table>
                    </td>
                    <td>
                        <xsl:value-of select="Name"/>
                    </td>
                    <td>
                        <xsl:value-of select="Netaddress"/>
                        <xsl:if test="not(Port=0)">:<xsl:value-of select="Port"/></xsl:if>
                    </td>
                    <td>
                        <xsl:if test="$showDirectory">
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
                                                        <xsl:text>/WsTopology/TpGetComponentFile%3fNetAddress%3d</xsl:text>
                                                        <xsl:value-of select="Netaddress"/>
                                                        <xsl:text>%26FileType%3dlog%26CompType%3d</xsl:text>
                                                        <xsl:value-of select="$compType"/>
                                                        <xsl:text>%26OsType%3d</xsl:text>
                                                        <xsl:value-of select="OS"/>
                                                        <xsl:text>%26Directory%3d</xsl:text>
                                                    </xsl:variable>
                                                    <xsl:variable name="pageCaption">
                                                        <xsl:value-of select="concat('esp_iframe_title=', $caption, ' ', ../../Name, ' -- ', $hpccStrings/st[@id='LogFile'])"/>
                                                    </xsl:variable>
                                                    <xsl:attribute name="href">
                                                        <xsl:if test="$compType!='EclAgentProcess'">
                                                            <xsl:text>/esp/iframe?</xsl:text>
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
                                                        <xsl:text>);</xsl:text>
                                                    </xsl:attribute>
                                                    <img border="0" src="&filePathEntity;/img/base.gif" alt="{$hpccStrings/st[@id='ViewLogFile']}" title="{$hpccStrings/st[@id='ViewLogFile']}" width="19" height="16"/>
                                                </a>
                                            </xsl:if>
                                        </td>
                                        <td width="14">
                                            <xsl:choose>
                                                <xsl:when test="$compType!='' and ($compType!='EclAgentProcess' or OS=0)">
                          <xsl:variable name="captionLen" select="string-length($caption)-1"/>
                          <xsl:variable name="href0">
                              <xsl:value-of select="concat('/esp/iframe?esp_iframe_title=', $caption, ' ', ../../Name, ' -- ', $hpccStrings/st[@id='Configuration'])"/>
                              <xsl:text disable-output-escaping="yes">&amp;inner=/WsTopology/TpGetComponentFile%3fNetAddress%3d</xsl:text>
                              <xsl:value-of select="Netaddress"/>
                              <xsl:text>%26FileType%3dcfg%26Directory%3d</xsl:text>
                              <xsl:value-of select="$absolutePath"/>
                              <xsl:text>%26CompType%3d</xsl:text>
                              <xsl:value-of select="$compType"/>
                              <xsl:text>%26OsType%3d</xsl:text>
                              <xsl:value-of select="OS"/>
                          </xsl:variable>
                          <img onclick="getConfigXML('{$href0}')" border="0" src="/esp/files_/img/config.png" alt="{$hpccStrings/st[@id='ViewConfigurationFile']}" title="{$hpccStrings/st[@id='ViewConfigurationFile']}" width="14" height="14"/>
                        </xsl:when>
                                                <xsl:otherwise>
                                                    <img border="0" src="/esp/files_/img/blank.png" width="14" height="14"/>
                                                </xsl:otherwise>
                                            </xsl:choose>
                                        </td>
                                        <td align="left">
                                            <xsl:value-of select="$absolutePath"/>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </xsl:if>
                    </td>
                </tr>               
                <xsl:if test="$showBindings">
                    <tr>
                        <td/>
                        <td colspan="6" align="left" style="padding-left=30px">
                            <span id="div_{../../Name}_{position()}" style="display:none">
                                <xsl:choose>
                                    <xsl:when test="../../TpBindings/TpBinding">
                                        <table class="blueline" cellspacing="0">
                                            <thead>
                                                <tr>
                                                    <th><xsl:value-of select="$hpccStrings/st[@id='ServiceName']"/></th>
                                                    <th><xsl:value-of select="$hpccStrings/st[@id='ServiceType']"/></th>
                                                    <th><xsl:value-of select="$hpccStrings/st[@id='Protocol']"/></th>
                                                    <th><xsl:value-of select="$hpccStrings/st[@id='Port']"/></th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                <xsl:variable name="netAddress" select="Netaddress"/>
                                                <xsl:for-each select="../../TpBindings/TpBinding">
                                                    <xsl:sort select="Service"/>
                                                    <tr>
                                                        <td>
                                                            <xsl:choose>
                                                                <xsl:when test="substring(Protocol, 1, 4)='http' and (not($encapsulatedSystem) or $encapsulatedSystem != 1)">
                                                                    <a href="{Protocol}://{$netAddress}:{Port}" target="_top">
                                                                        <xsl:value-of select="Service"/>
                                                                    </a>
                                                                </xsl:when>
                                                                <xsl:when test="substring(Protocol, 1, 4)='http'">
                                                                    <a href="javascript:void(0)" onclick="setPath('{Protocol}', '{Port}')" target="_top">
                                                                        <xsl:value-of select="Service"/>
                                                                    </a>
                                                                </xsl:when>
                                                                <xsl:otherwise>
                                                                    <xsl:value-of select="Service"/>
                                                                </xsl:otherwise>
                                                            </xsl:choose>
                                                        </td>
                                                        <td>
                                                            <xsl:value-of select="ServiceType"/>
                                                        </td>
                                                        <td>
                                                            <xsl:value-of select="Protocol"/>
                                                        </td>
                                                        <td>
                                                            <xsl:value-of select="Port"/>
                                                        </td>
                                                    </tr>
                                                </xsl:for-each>
                                            </tbody>
                                        </table>
                                    </xsl:when>
                                    <xsl:otherwise>No bindings defined!</xsl:otherwise>
                                </xsl:choose>
                            </span>
                        </td>
                    </tr>
                </xsl:if>
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
            <td/>
            <td colspan="6" align="left" style="padding-left=30px">
              <span id="div_{../../Name}_{position()}" style="display:none;">
                  <table class="blueline" cellspacing="0">
                    <thead>
                      <tr>
                        <th><xsl:value-of select="$hpccStrings/st[@id='Component']"/></th>
                        <th><xsl:value-of select="$hpccStrings/st[@id='DaliServer']"/></th>
                        <th><xsl:value-of select="$hpccStrings/st[@id='Configuration']"/></th>
                      </tr>
                    </thead>
                    <tbody>
                      <xsl:variable name="netAddress" select="Netaddress"/>
                      <tr>
                        <td>AgentExec</td>
                        <td>
                          <xsl:value-of select="../../DaliServer"/>
                        </td>
                        <!--td>
                          <xsl:value-of select="../../WUQueueName"/>
                        </td-->
                        <td width="14">
                          <a>
                            <xsl:attribute name="href">
                              <xsl:value-of select="concat('/esp/iframe?esp_iframe_title=', $caption, ' ', ../../Name, ' -- ', $hpccStrings/st[@id='Configuration'])"/>
                              <xsl:text disable-output-escaping="yes">&amp;inner=/WsTopology/TpGetComponentFile%3fNetAddress%3d</xsl:text>
                              <xsl:value-of select="Netaddress"/>
                              <xsl:text>%26FileType%3dcfg%26Directory%3d</xsl:text>
                              <xsl:value-of select="$absolutePath"/>
                              <xsl:text>%26CompType%3dAgentExecProcess</xsl:text>
                              <xsl:text>%26OsType%3d</xsl:text>
                              <xsl:value-of select="OS"/>
                            </xsl:attribute>
                            <img border="0" src="/esp/files_/img/config.png" alt="{$hpccStrings/st[@id='ViewConfigurationFile']}"
                              title="{$hpccStrings/st[@id='ViewConfigurationFile']}" width="14" height="14"/>
                          </a>
                        </td>
                      </tr>
                    </tbody>
                  </table>
              </span>
            </td>
          </tr>
        </xsl:if>
      </xsl:for-each>
        </xsl:for-each>
    </xsl:if>
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


</xsl:stylesheet>
