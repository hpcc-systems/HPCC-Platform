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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                >
    <xsl:output method="html"/>
    
    <xsl:variable name="autoRefresh" select="0"/>
    
    <!--uncomment this out for production-->
    <xsl:include href="/esp/xslt/ws_machine/preflightControls.xslt"/>
    <!--uncomment this for debugging-->
    <!--xsl:include href="\development\esp\services\ws_machine\preflightControls.xslt"/-->
    <xsl:variable name="ShowPreflightInfo" select="/TpMachineQueryResponse/EnablePreflightInfo/text()='1'"/>
    <xsl:variable name="SwapNode" select="/TpMachineQueryResponse/OldIP/text() and /TpMachineQueryResponse/Type='THORSPARENODES'"/>
    <xsl:variable name="clusterName" select="string(/TpMachineQueryResponse/Cluster)"/>
    <xsl:variable name="clusterType" select="string(/TpMachineQueryResponse/Type)"/>
    
    <xsl:variable name="memThreshold" select="/TpMachineQueryResponse/MemThreshold"/>
    <xsl:variable name="diskThreshold" select="/TpMachineQueryResponse/DiskThreshold"/>
    <xsl:variable name="cpuThreshold" select="/TpMachineQueryResponse/CpuThreshold"/>
    <xsl:variable name="memThresholdType" select="/TpMachineQueryResponse/MemThresholdType"/><!-- % -->
    <xsl:variable name="diskThresholdType" select="/TpMachineQueryResponse/DiskThresholdType"/><!-- % -->
    <xsl:variable name="enableSNMP" select="/TpMachineQueryResponse/EnableSNMP"/>
    <xsl:variable name="addProcessesToFilter" select="/TpMachineQueryResponse/PreflightProcessFilter"/>
    <xsl:variable name="numSlaveNodes" select="count(/TpMachineQueryResponse/TpMachines/TpMachine/Type[text()='ThorSlaveProcess'])"/>

  <xsl:template match="/TpMachineQueryResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <xsl:choose>
                <xsl:when test="not(TpMachines)">
                    <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <meta http-equiv="Refresh" content="100"/>
            <title>Computers</title>
            <link rel="stylesheet" type="text/css" href="/esp/files/default.css"/>
            <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
            <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
            <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
            <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
            <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
            <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
            <xsl:text disable-output-escaping="yes"><![CDATA[
            <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
            <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
            <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
            <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
            ]]></xsl:text>
          </head>
          <body class="yui-skin-sam" onload="nof5()">
                        <h3>No machines!</h3>
                    </body>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:apply-templates select="TpMachines"/>
                </xsl:otherwise>
            </xsl:choose>
        </html>
    </xsl:template>
    
    
    <xsl:template match="TpMachines">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
      <meta http-equiv="Refresh" content="100"/>
      <title>Computers</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/default.css"/>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
      <xsl:text disable-output-escaping="yes"><![CDATA[
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
        ]]></xsl:text>

      <script type="text/javascript" src="files_/scripts/sortabletable.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="javascript" src="files_/scripts/multiselect.js">
                <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
            </script>
            <script language="javascript">
                var sortableTable = null;
                var ClusterName = '<xsl:value-of select="$clusterName"/>';
                var allowReloadPage = false;
        var fromTargetClusterPage = false;
                function onRowCheck(checked)
                {
          var obj = document.forms[0].submitBtn;
          if (obj)
            obj.disabled = checkedCount == 0;
                }

            <![CDATA[
                function setSortableTable(tableId)
                {
                    var table = document.getElementById(tableId);
                    if (table == null)
                        return;

                    //dynamically create a sort list since our table is defined at run time based on info returned
                    var cells = table.tHead.rows[0].cells;
                    var nCols = cells.length;
                    var sortCriteria = new Array(nCols);
                    sortCriteria[0] = "None";//multiselect checkbox

                    for (var i = 1; i < nCols; i++)
                    {
                        var c = cells[i];
                        var sort;
                        switch (c.innerText)
                        {
                           case 'Network Address':
                              sort = 'IP_Address';
                              break;
                           case 'Slave Number':
                              sort = 'Number';
                              break;
                           default:
                              sort = "String";
                              break;
                        }//switch
                        sortCriteria[i] = sort;
                    }//for

                    sortableTable = new SortableTable(table, table, sortCriteria);
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
        
        function selectAll0(chkbox)
                {
                    selectAll(chkbox.checked);
                    var topchkbox = document.getElementById("selectAll1");
                    if (topchkbox != chkbox && topchkbox.checked != chkbox.checked)
                        topchkbox.checked = chkbox.checked;

                    var bottomchkbox = document.getElementById("selectAll2");
                    if (bottomchkbox != chkbox && bottomchkbox.checked != chkbox.checked)
                        bottomchkbox.checked = chkbox.checked;
                }
                ]]>

                function onLoad()
                {
                    initSelection('resultsTable');
                    document.getElementsByName('Addresses.itemcount')[0].value = totalItems;
                    onRowCheck(false);
                    setSortableTable('resultsTable');

                    <xsl:if test="$ShowPreflightInfo and not($SwapNode)">initPreflightControls();</xsl:if>
                }
                <xsl:if test="$SwapNode">
                    var OldIP = '<xsl:value-of select="/TpMachineQueryResponse/OldIP"/>';
                    var Path = '<xsl:value-of select="/TpMachineQueryResponse/Path"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    singleSelect = true;
                    function onSwapNode()          
                    {
                        var table = document.getElementById('resultsTable');
                        var row = table.rows.item(lastClicked);
                        var checkbox = row.cells.item(0).children[0];
                        var cbValue = checkbox.value;
                        var NewIP = cbValue.substring(0, cbValue.indexOf(':'));
                    
                        if (confirm('Swap ' + OldIP + ' with ' + NewIP + ' ?'))
                        {
                            var s = "/WsTopology/TpSwapNode?Cluster=" + ClusterName;
                            s += "&OldIP=" + OldIP;
                            s += "&NewIP=" + NewIP;
                            s += "&Path=" + Path;
                            document.forms['listitems'].action = s;
                            return true;
                        }
                        return false;
                    }
                    ]]></xsl:text>
                </xsl:if>
            </script>
            <xsl:if test="$ShowPreflightInfo and not($SwapNode)">
                <xsl:call-template name="GenerateScriptForPreflightControls"/>
            </xsl:if>
        </head>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
            <form id="listitems" action="/ws_machine/GetMachineInfo" method="post">
                <xsl:if test="$SwapNode">
                    <xsl:attribute name="onsubmit">return onSwapNode()</xsl:attribute>
                </xsl:if>
                <input type="hidden" name="Path" value="{/TpMachineQueryResponse/Path}"/>
                <input type="hidden" name="Cluster" value="{$clusterName}"/>
                <input type="hidden" name="Addresses.itemcount" value=""/>
                <h3>
                    <xsl:choose>
                        <xsl:when test="$SwapNode">Select a spare node to swap</xsl:when>
                        <xsl:when test="/TpMachineQueryResponse/Type='THORSPARENODES'">Thor Slaves</xsl:when>
                        <xsl:when test="/TpMachineQueryResponse/Type='ALLSERVICES'">System Service Nodes</xsl:when>
                        <xsl:when test="/TpMachineQueryResponse/Type='AVAILABLEMACHINES'">Available Nodes</xsl:when>
                        <xsl:when test="$clusterName!=''">
                            <xsl:choose>
                                <xsl:when test="$clusterType='ROXIEMACHINES'">Roxie </xsl:when>
                                <xsl:when test="$clusterType='THORMACHINES'">Thor </xsl:when>
                                <xsl:when test="$clusterType='HOLEMACHINES'">Hole </xsl:when>
                            </xsl:choose>
                            <xsl:text disable-output-escaping="yes">Cluster '</xsl:text>
                            <xsl:value-of select="$clusterName"/>
                            <xsl:text disable-output-escaping="yes">'</xsl:text>
                        </xsl:when>
                        <xsl:otherwise>Nodes</xsl:otherwise>
                    </xsl:choose>
                </h3>
                <table class="sort-table" id="resultsTable">
                    <colgroup>
                        <col width="5"/>
                        <col width="145" align="left"/>
            <xsl:if test="/TpMachineQueryResponse/HasThorSpareProcess/text()='1' and /TpMachineQueryResponse/Type='THORMACHINES'">
              <col width="100"/>
            </xsl:if>
            <col width="150"/>
                        <col width="150"/>
                        <col width="100"/>
                        <col width="100"/>
                        <col width="100"/>
                    </colgroup>
                    <thead>
                        <tr>
                            <th>
                                <xsl:if test="TpMachine[2] and not($SwapNode)">
                                    <xsl:attribute name="id">selectAll1</xsl:attribute>
                                    <input type="checkbox" id="selectAll1" title="Select or deselect all machines" onclick="selectAll0(this)">
                                        <xsl:if test="not($SwapNode)">
                                            <xsl:attribute name="checked">true</xsl:attribute>
                                        </xsl:if>
                                    </input>
                                </xsl:if>
                            </th>
                            <th align="center">Name</th>
                                <xsl:if test="/TpMachineQueryResponse/HasThorSpareProcess/text()='1' and /TpMachineQueryResponse/Type='THORMACHINES'">
                                    <th>Action</th>
                                </xsl:if>
                            <th>Network Address</th>
                            <th>Component</th>
                            <xsl:if test="$numSlaveNodes > 0">
                                <th>Slave Number</th>
                            </xsl:if>
                            <th>Domain</th>
                            <th>Platform</th>
                        </tr>
                    </thead>
                    <tbody>
                        <xsl:apply-templates select="TpMachine"/>
                    </tbody>
                </table>
                <xsl:if test="TpMachine[2] and not($SwapNode)">
                    <table class="select-all">
                        <tr>
                            <th id="selectAll2">
                                <input type="checkbox" id="selectAll2" title="Select or deselect all machines" onclick="selectAll0(this)">
                  <xsl:if test="not($SwapNode)">
                    <xsl:attribute name="checked">true</xsl:attribute>
                  </xsl:if>
                </input>
                            </th>
                            <th align="left" colspan="6">Select All / None</th>
                        </tr>
                    </table>
                </xsl:if>
                <xsl:choose>
                    <xsl:when test="$SwapNode">
                        <input type="submit" value="Submit" id="submitBtn" disabled="true"/>
                        <input type="button" value="Cancel" name="Cancel" onclick="javascript:history.go(-1)"/>
                    </xsl:when>
                    <xsl:when test="$ShowPreflightInfo">
                        <xsl:call-template name="ShowPreflightControls">
                            <xsl:with-param name="clusterType" select="../Type"/>
              <xsl:with-param name="addProcessesToFilter" select="$addProcessesToFilter"/>
              <xsl:with-param name="enableSNMP" select="$enableSNMP"/>
            </xsl:call-template>
                    </xsl:when>
                </xsl:choose>
            </form>
        </body>
    </xsl:template>
    <xsl:template match="TpMachine">
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
                    <xsl:when test="Type='RoxieServerProcess'">Roxie Server</xsl:when>
                    <xsl:when test="Type='DropZone'">Drop Zone</xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="Type"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <td>
                <xsl:if test="$ShowPreflightInfo">
                    <xsl:choose>
                        <xsl:when test="not($SwapNode)">
                            <input type="checkbox" name="Addresses.{position()-1}" value="{Netaddress}|{ConfigNetaddress}:{Type}:{$clusterName}:{OS}:{translate(Directory, ':', '$')}:{ProcessNumber}" onclick="return clicked(this, event)">
                                <xsl:attribute name="checked">true</xsl:attribute>
                            </input>
                        </xsl:when>
                        <xsl:otherwise>
                            <input type="checkbox" name="Addresses.{position()-1}" value="{Netaddress}:{Type}:{$clusterName}:{OS}:{translate(Directory, ':', '$')}:{ProcessNumber}" onclick="return clicked(this, event)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:if>
            </td>
            <td>
                <a style="padding-right:2" href="">
                    <xsl:variable name="url">
                        <!--xsl:text>/esp/iframe/WsTopology/TpGetComponentFile?FileType=log&amp;NetAddress=</xsl:text>
                        <xsl:value-of select="concat(Netaddress, '&amp;CompType=', Type, '&amp;OsType=', OS, '&amp;Directory=')"/-->
                        <xsl:text>/WsTopology/TpGetComponentFile%3fFileType%3dlog%26NetAddress%3d</xsl:text>
                        <xsl:value-of select="concat(Netaddress, '%26CompType%3d', Type, '%26OsType%3d', OS, '%26Directory%3d')"/>
                    </xsl:variable>
                    <xsl:variable name="pageCaption">
                        <!--xsl:text>&amp;esp_iframe_title=Log file for </xsl:text-->
                        <xsl:text>esp_iframe_title=Log file for </xsl:text>
                        <xsl:value-of select="concat($displayType, ' [', Netaddress, '] of ')"/>
                        <xsl:variable name="clusterType" select="$clusterType"/>
                        <xsl:text disable-output-escaping="yes"> cluster '</xsl:text>
                        <xsl:value-of select="$clusterName"/>
                        <xsl:text disable-output-escaping="yes">'</xsl:text>
                    </xsl:variable>
                    <xsl:attribute name="onclick">
                        <xsl:text disable-output-escaping="yes">return popup('</xsl:text>
                        <xsl:value-of select="Netaddress"/>
                        <xsl:text disable-output-escaping="yes">', '</xsl:text>
                        <xsl:value-of select="translate(/TpMachineQueryResponse/LogDirectory, '\', '/')"/>
                        <xsl:text disable-output-escaping="yes">', '</xsl:text>
                        <xsl:value-of select="$url"/>
                        <xsl:text disable-output-escaping="yes">', "</xsl:text>
                        <xsl:value-of select="$pageCaption"/>
                        <xsl:text disable-output-escaping="yes">",</xsl:text>
                        <xsl:value-of select="OS"/>
                        <xsl:text>);</xsl:text>
                    </xsl:attribute>
                    <img border="0" src="/esp/files_/img/base.gif" alt="View log file..." width="19" height="16"/>
                </a>            
                <xsl:value-of select="Name"/>
            </td>
      <xsl:if test="/TpMachineQueryResponse/HasThorSpareProcess/text()='1' and /TpMachineQueryResponse/Type='THORMACHINES'">
        <td>
                  <xsl:if test="Type='ThorSlaveProcess'">
                      <a href="/WsTopology/TpMachineQuery?Type=THORSPARENODES&amp;Cluster={$clusterName}&amp;OldIP={Netaddress}&amp;Path={/TpMachineQueryResponse/Path}" 
                          title="Swap node...">
                          Swap Node
                      </a>
                  </xsl:if>
              </td>
      </xsl:if>
      <td>
        <xsl:value-of select="Netaddress"/>
      </td>
            <td>
            <xsl:value-of select="$displayType"/>
            </td>
            <xsl:if test="$numSlaveNodes > 0">
                <td>
                    <xsl:if test="Type='ThorSlaveProcess'">
                        <xsl:value-of select="ProcessNumber"/>
                    </xsl:if>
                </td>
            </xsl:if>
            <td>
                <xsl:value-of select="Domain"/>
            </td>
            <td>
                <xsl:choose>
                    <xsl:when test="OS=0">Windows</xsl:when>
                    <xsl:when test="OS=1">Solaris</xsl:when>
                    <xsl:when test="OS=2">Linux</xsl:when>
                    <xsl:otherwise>Unknown</xsl:otherwise>
                </xsl:choose>
            </td>
        </tr>
    </xsl:template>
</xsl:stylesheet>
