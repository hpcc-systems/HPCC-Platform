<?xml version="1.0" encoding="utf-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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
   <!--!ENTITY filePathEntity "/development/esp/files"-->
   <!--!ENTITY xsltPathEntity "d:/development/esp/services"-->
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
   <xsl:output method="html"/>
   <xsl:include href="&xsltPathEntity;/ws_machine/preflightControls.xslt"/>
  
   <xsl:variable name="reqInfo" select="/GetTargetClusterInfoResponse/RequestInfo"/>
   <xsl:variable name="clusterName" select="string($reqInfo/Cluster)"/>
   <xsl:variable name="clusterType" select="string($reqInfo/ClusterType)"/>
   <xsl:variable name="memThresholdType" select="number($reqInfo/MemThresholdType)"/>
   <xsl:variable name="diskThresholdType" select="number($reqInfo/DiskThresholdType)"/>
   
   <xsl:variable name="memThreshold"><!-- from 0 to 100 (%)-->
      <xsl:choose>
         <xsl:when test="$reqInfo/MemThreshold">
            <xsl:value-of select="$reqInfo/MemThreshold"/>
         </xsl:when>
         <xsl:otherwise>20</xsl:otherwise>
      </xsl:choose>
   </xsl:variable>
   
   <xsl:variable name="diskThreshold"><!-- from 0 to 100 (%)-->
      <xsl:choose>
         <xsl:when test="$reqInfo/DiskThreshold">
            <xsl:value-of select="$reqInfo/DiskThreshold"/>
         </xsl:when>
         <xsl:otherwise>20</xsl:otherwise>
      </xsl:choose>
   </xsl:variable>
   
   <xsl:variable name="cpuThreshold"><!-- from 0 to 100 (%)-->
      <xsl:choose>
         <xsl:when test="$reqInfo/CpuThreshold">
            <xsl:value-of select="$reqInfo/CpuThreshold"/>
         </xsl:when>
         <xsl:otherwise>90</xsl:otherwise>
      </xsl:choose>
   </xsl:variable>

   <xsl:variable name="autoRefresh" select="$reqInfo/AutoRefresh"/>
   <xsl:variable name="numColumns" select="count(/GetTargetClusterInfoResponse/Columns/Item)"/>
   <xsl:variable name="countTargetClusters" select="count(/GetTargetClusterInfoResponse/TargetClusterInfoList/TargetClusterInfo)"/>

    <xsl:template match="/GetTargetClusterInfoResponse">
      <html>
        <head>
            <!--meta http-equiv="refresh" content="{60*$autoRefresh}"/-->
            <!--meta http-equiv="reload" content="10"/-->
            <title>Machine Information</title>
            <xsl:if test="TargetClusterInfoList">
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
                  var allowReloadPage = true;
                  var sortableTable = null;
                  var fromTargetClusterPage = true;
                  var clusterChecked=0;

                  function onLoad()
                  {
                    if (countTCs > 0)
                      setReloadFunction('reloadPage');

                    document.getElementsByName('TargetClusters.itemcount')[0].value = countTCs;
                    initSelection('resultsTable');
                    initPreflightControls();

                    var selObj = document.getElementById("methodObj");
                    if (selObj.length > 1)
                    {
                      for(var i=1; selObj.length; i++)
                      {
                          selObj.remove(i);
                      }
                    }

                    var inputs = document.getElementById("listitems").getElementsByTagName("input");
                    for(var i=0, len=inputs.length; i<len; i++)
                    {
                      if(inputs[i].name.match(/^TargetClusters.\d+$/))
                      {
                          if (inputs[i].checked)
                              clusterChecked++;
                      }
                    }
                    document.forms[0].submitBtn.disabled = clusterChecked == 0;
                    if (countTCs > 1)
                    {
                      document.getElementById( 'TargetClusters.All' ).checked = clusterChecked==countTCs;
                      document.getElementById( 'TargetClusters.All2' ).checked = clusterChecked==countTCs;
                    }

                    var table = document.getElementById('resultsTable');
                    if (table)
                    {
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
                          case 'Location': 
                            sort = 'IP_Address'; 
                            break;
                          case 'Type':      
                          case 'Processes': 
                          case 'Processes Down':
                          case 'Condition':
                          case 'State':
                            sort = 'String'; 
                            break;
                          case 'Up Time':
                          case 'Computer Up Time': 
                            sort = 'TimePeriod'; 
                            break;
                          default:
                            sort = "Percentage";
                            break;
                        }//switch
                        sortCriteria[i] = sort;
                      }//for

                      sortableTable = new SortableTable(table, table, sortCriteria);

                      var toolarray = [];
                      for(i=0;i<table.rows.length;i++) 
                      {
                        for (var j=0;j<table.rows[i].cells.length;j++) 
                        {
                          var cell =table.rows[i].cells[j]
                          if (cell.id.length>0 && cell.title.length>0) {
                            toolarray.push(cell.id);
                          }
                        }
                      }

                      var ttA = new YAHOO.widget.Tooltip("ttA", { 
                        context:toolarray,
                        effect:{effect:YAHOO.widget.ContainerEffect.FADE,duration:0.20}
                      });
                    }//if table
                  }

                  function onRowCheck(checked)
                  {
                    document.forms[0].submitBtn.disabled = checkedCount == 0;
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
                      //saveState();
                    }
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
            </xsl:if>
          </head>
          <body class="yui-skin-sam"  onload="nof5();onLoad();">
          <form id="listitems" action="/ws_machine/GetTargetClusterInfo" method="post">
            <input type="hidden" name="Path" value="{$reqInfo/Path}"/>
            <input type="hidden" name="Cluster" value="{$clusterName}"/>
            <input type="hidden" name="TargetClusters.itemcount" value=""/>
               <xsl:choose>
                  <xsl:when test="Exceptions">
                     <h1><xsl:value-of select="Exceptions"/></h1>
                  </xsl:when>
                  <xsl:when test="not(TargetClusterInfoList)">
                     <h2>No target clusters selected!</h2>
                  </xsl:when>
                  <xsl:otherwise>
                      <center>
                      <table>
                        <tbody>
                           <tr>
                              <th align="left">
                              <h3>
                                     <xsl:choose>                       
                                    <xsl:when test="$clusterType='THORSPARENODES'">Thor Slaves</xsl:when>
                                    <xsl:when test="$clusterType='ALLSERVICES'">System Service Nodes</xsl:when>
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
                                    <xsl:otherwise>Machine Information</xsl:otherwise>
                                 </xsl:choose>
                              </h3>
                              </th>
                           </tr>
                           <tr>
                              <td>
                                <xsl:if test="TargetClusterInfoList/TargetClusterInfo[2]">
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
                                <xsl:for-each select="TargetClusterInfoList/TargetClusterInfo">
                                  <xsl:call-template name="show-cluster">
                                    <xsl:with-param name="type" select="Type"/>
                                    <xsl:with-param name="name" select="Name"/>
                                  </xsl:call-template>
                                </xsl:for-each>
                                <xsl:if test="TargetClusterInfoList/TargetClusterInfo[2]">
                                      <table cellpadding="0" width="100%">
                                          <tr>
                                              <th id="selectAll2" width="1%" style="padding-left:4px">
                                                  <input type="checkbox" id="TargetClusters.All2" name="TargetClusters.ALL2"
                                                         title="Select all target clusters" onclick="return clickTCCheckbox('', '', this);"></input>
                                              </th>
                                              <th colspan="5" align="left">Select All / None</th>
                                          </tr>
                                      </table>
                                </xsl:if>
                                <b>Fetched: </b>
                                <xsl:value-of select="TimeStamp"/>
                                <br/>
                                <br/>
                                <xsl:call-template name="ShowPreflightControls">
                                   <xsl:with-param name="getProcessorInfo" select="boolean(RequestInfo/GetProcessorInfo=1)"/>
                                   <xsl:with-param name="getSoftwareInfo" select="boolean(RequestInfo/GetSoftwareInfo=1)"/>
                                   <xsl:with-param name="getStorageInfo" select="boolean(RequestInfo/GetStorageInfo=1)"/>
                                   <xsl:with-param name="localFileSystemsOnly" select="boolean(RequestInfo/LocalFileSystemsOnly=1)"/>
                                   <xsl:with-param name="applyProcessFilter" select="boolean(RequestInfo/ApplyProcessFilter=1)"/>
                                   <xsl:with-param name="addProcessesToFilter" select="RequestInfo/AddProcessesToFilter"/>
                                   <xsl:with-param name="securityString" select="RequestInfo/SecurityString"/>
                                   <xsl:with-param name="command" select="RequestInfo/Command"/>
                                   <xsl:with-param name="waitUntilDone" select="1"/>
                                   <xsl:with-param name="autoRefresh" select="$autoRefresh"/>
                                   <xsl:with-param name="clusterType" select="$clusterType"/>
                                   <xsl:with-param name="targetCluster" select="1"/>                                </xsl:call-template>
                              </td>
                           </tr>
                        </tbody>
                     </table>                      
                     </center>            
                  </xsl:otherwise>
               </xsl:choose>
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
          <input type="checkbox" id="TargetClusters.{count(preceding::TargetClusterInfo)}" name="TargetClusters.{count(preceding::TargetClusterInfo)}" checked="1"
                                value="{$type}:{$name}" title="Select this target cluster" onclick="return clickTCCheckbox('{$type}', '{$name}', this);"></input>
        </td>
        <td align="left" width="20">
          <a href="javascript:showDetails('{$type}', '{$name}');">
            <img id="view_details_{$name}" border="0" src="&filePathEntity;/img/keyfile.png" align="middle"/>
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
        <td colspan="6" align="left" style="padding-left=30px">
          <span id="div_{$name}_{position()}" style="display:inline;visibility:visible">
            <xsl:apply-templates select="Processes"/>
          </span>
        </td>
      </tr>
    </table>
  </xsl:template>

  <xsl:template match="Processes">
      <!--xsl:variable name="order" select="../RequestInfo/SortBy/text()"/-->
      <table class="blueline" border="2" frame="box" rules="groups">
         <thead>
            <tr bgcolor="#C0C0C0">
               <th>Location</th>
               <th>Component</th>
               <xsl:choose>
                  <xsl:when test="../../../Columns/Item">
                     <xsl:for-each select="../../../Columns/Item[text()='Condition']">
                        <th align="center">Condition</th>
                     </xsl:for-each>
                     <xsl:for-each select="../../../Columns/Item[text()='State']">
                        <th align="center">State</th>
                     </xsl:for-each>
                     <xsl:for-each select="../../../Columns/Item[text()='UpTime']">
                        <th align="center">Up Time</th>
                     </xsl:for-each>
                     <!--process Processes first-->
                     <xsl:for-each select="../../../Columns/Item[text()='Processes']">
                        <th align="center"><xsl:value-of select="."/>
                           <xsl:if test="text()='Processes' and $reqInfo/ApplyProcessFilter=1"> Down</xsl:if>
                        </th>
                     </xsl:for-each>
                     <!--process disk storage next-->
                     <xsl:for-each select="../../../Columns/Item[text()!='Processes' and text()!='Up Time' and not(contains(text(), 'Memory')) and not(starts-with(text(), 'CPU')) and text()!='State' and text()!='Condition' and text()!='UpTime' and text()!='Swap']">
                        <th align="center"><xsl:value-of select="."/></th>
                     </xsl:for-each>
                     <!--process physical memory and swap next-->      
                     <xsl:for-each select="../../../Columns/Item[text() = 'Physical Memory' or text()='Swap']">
                        <th align="center"><xsl:value-of select="."/></th>
                     </xsl:for-each>
                     <!--process CPU Load next -->      
                     <xsl:for-each select="../../../Columns/Item[starts-with(text(), 'CPU') and contains(text(), 'Load')]">
                        <th align="center"><xsl:value-of select="."/></th>
                     </xsl:for-each>
                     <!--process Up Time next -->
                     <xsl:for-each select="../../../Columns/Item[text()='Up Time']">
                        <th align="center">Computer Up Time</th>
                     </xsl:for-each>
                  </xsl:when>
                  <xsl:otherwise>
                     <th>Description</th>
                  </xsl:otherwise>
               </xsl:choose>
            </tr>
         </thead>
         <tbody>
            <!--xsl:choose>
               <xsl:when test="$order!=''">
                  <xsl:apply-templates select="MachineInfoEx">
                     <xsl:sort select="dyn:evaluate($order)"/>
                  </xsl:apply-templates>
               </xsl:when>
               <xsl:otherwise>
                  <xsl:apply-templates select="MachineInfoEx"/>
               </xsl:otherwise>
            </xsl:choose-->
            <xsl:apply-templates select="MachineInfoEx"/>
         </tbody>
      </table>
   </xsl:template>
   
   <xsl:template match="MachineInfoEx">
   
      <tr id="{position()}" onmouseenter="this.oldClass=this.className; this.className='hilite';" onmouseleave="this.className=this.oldClass">
         <xsl:choose>
            <xsl:when test="position() mod 2">
               <xsl:attribute name="class">odd</xsl:attribute>
            </xsl:when>
            <xsl:otherwise>
               <xsl:attribute name="class">even</xsl:attribute>
            </xsl:otherwise>
         </xsl:choose>   
         <td align="left">
               <xsl:choose>
               <xsl:when test="$clusterType='THORMACHINES' and ProcessType='ThorSlaveProcess' and OS=0">
                  <a href="/WsTopology/TpMachineQuery?Type=THORSPARENODES&amp;Cluster={$clusterName}&amp;OldIP={Address}&amp;Path={$reqInfo/Path}" title="Swap node..."><xsl:value-of select="Address"/></a>
               </xsl:when>               
               <xsl:otherwise><xsl:value-of select="Address"/></xsl:otherwise>
            </xsl:choose>
             <br/>
                <xsl:variable name="len" select="string-length(ComponentPath)"/>
                <xsl:value-of select="substring(ComponentPath, 1, number($len)-1)"/>
         </td>
         <td>
            <xsl:value-of select="DisplayType"/>
            <xsl:if test="string(ComponentName)!=''">
                <br/>
                <xsl:choose>
                    <xsl:when test="ProcessType='ThorSlaveProcess'">
                        <xsl:value-of select="concat('[', ComponentName, ', ', ProcessNumber, ']')"/>
                    </xsl:when>
                    <xsl:when test="ProcessType!='ThorMasterProcess' and ProcessType!='RoxieServerProcess'">
                        <xsl:value-of select="concat('[', ComponentName, ']')"/>
                    </xsl:when>
                </xsl:choose>
            </xsl:if>
         </td>
         <xsl:choose>
            <xsl:when test="UpTime/text()">
                  <xsl:variable name="cond" select="ComponentInfo/Condition"/>
                 <xsl:if test="../../../../Columns/Item[text()='Condition']">
                            <td id="condition_{position()}">
                                <xsl:if test="$cond='0' or $cond='4' or $cond='5' or $cond='6'">
                                    <xsl:attribute name="bgcolor">#FF8800</xsl:attribute>
                                </xsl:if>
                          <xsl:choose>
                                    <xsl:when test="$cond='0'">Unknown</xsl:when>
                                    <xsl:when test="$cond='1'">Normal</xsl:when>
                                    <xsl:when test="$cond='2'">Warning</xsl:when>
                                    <xsl:when test="$cond='3'">Minor</xsl:when>
                                    <xsl:when test="$cond='4'">Major</xsl:when>
                                    <xsl:when test="$cond='5'">Critical</xsl:when>
                                    <xsl:when test="$cond='6'">Fatal</xsl:when>
                                    <xsl:when test="$cond='7'">-</xsl:when>
                    <xsl:when test="$cond='-1'">
                      <xsl:attribute name="title">Failed to retrieve information.  Please check configuration.</xsl:attribute>
                    </xsl:when>
                                </xsl:choose>
                            </td>
                 </xsl:if>
                 <xsl:if test="../../../../Columns/Item[text()='State']">
                            <td id="state_{position()}">
                            <xsl:variable name="state" select="ComponentInfo/State"/>
                              <xsl:choose>
                                        <xsl:when test="$state='0'"><xsl:attribute name="bgcolor">#FF8800</xsl:attribute>Unknown</xsl:when>
                                        <xsl:when test="$state='1'">Starting</xsl:when>
                                        <xsl:when test="$state='2'">Stopping</xsl:when>
                                        <xsl:when test="$state='3'">Suspended</xsl:when>
                                        <xsl:when test="$state='4'">Recycling</xsl:when>
                                        <xsl:when test="$state='5'">Ready</xsl:when>
                                        <xsl:when test="$state='6'">Busy</xsl:when>
                                        <xsl:when test="$state='7'">-</xsl:when>
                                        <xsl:when test="$cond='-1'">
                      <xsl:attribute name="title">Failed to retrieve information.  Please check configuration.</xsl:attribute>
                                            <xsl:text>N/A</xsl:text>
                                        </xsl:when>
                                    </xsl:choose>
                            </td>
                   </xsl:if>
                   <xsl:if test="../../../../Columns/Item[text()='UpTime']">                    
                        <td id="uptime_{position()}">
                            <xsl:choose>
                                    <xsl:when test="$cond='-1' or $cond='7'">
                    <xsl:attribute name="title">Failed to retrieve information.  Please check configuration.</xsl:attribute>
                                        <xsl:text>N/A</xsl:text>
                                    </xsl:when>
                                    <xsl:otherwise>
                                    <xsl:value-of select="ComponentInfo/UpTime"/>
                                    </xsl:otherwise>
                                </xsl:choose>
                        </td>
                   </xsl:if>
               <xsl:call-template name="showMachineInfo"/>
            </xsl:when>
            <xsl:otherwise>
               <td colspan="{$numColumns}"  bgcolor="#FF8800"><xsl:value-of select="Description"/></td>
            </xsl:otherwise>
         </xsl:choose>            
      </tr>
   </xsl:template>

    <!--process software information first-->
   <xsl:template name="showMachineInfo">
      <xsl:variable name="storageInfo" select="Storage/StorageInfo"/>
      <xsl:variable name="machineNode" select="."/>

      <xsl:choose>
         <xsl:when test="Running/SWRunInfo[1]">
            <td align="center" id="process_{position()}">
               <xsl:if test="$reqInfo/ApplyProcessFilter=1">
                  <xsl:attribute name="bgcolor">FF8800</xsl:attribute>
               </xsl:if>
               <xsl:choose>
                  <xsl:when test="count(Running/SWRunInfo) &gt; 1">
                     <xsl:variable name="processNames">
                          <xsl:variable name="total" select="count(Running/SWRunInfo)"/>
                          <xsl:choose>
                                            <xsl:when test="$total > 30">
                                                <xsl:text disable-output-escaping="yes">&lt;table&gt;&lt;tr&gt;&lt;td noWrap valign="top"&gt;</xsl:text>
                                     <xsl:apply-templates select="Running/SWRunInfo" mode="findProcessNames">
                                                  <xsl:with-param name="total" select="$total"/>
                                     </xsl:apply-templates>
                                                <xsl:text disable-output-escaping="yes">&lt;/td&gt;&lt;/tr&gt;&lt;/table&gt;</xsl:text>
                                            </xsl:when>
                                            <xsl:otherwise>
                                 <xsl:apply-templates select="Running/SWRunInfo" mode="findProcessNames">
                                              <xsl:with-param name="total" select="$total"/>
                                 </xsl:apply-templates>
                                            </xsl:otherwise>
                                        </xsl:choose>
                     </xsl:variable>
                     <xsl:variable name="caption">Processes
                        <xsl:if test="$reqInfo/ApplyProcessFilter=1"> Down</xsl:if>
                     </xsl:variable>
                    <xsl:attribute name="title">
                      <xsl:value-of select="$caption"/>,<xsl:value-of select="$processNames"/>
                    </xsl:attribute>
                     <xsl:value-of select="Running/SWRunInfo[1]/Name"/><xsl:text>...</xsl:text>
                  </xsl:when>
                  <xsl:otherwise>
                     <xsl:value-of select="Running/SWRunInfo[1]/Name"/>
                  </xsl:otherwise>
               </xsl:choose>
            </td>
         </xsl:when>
         <xsl:when test="$reqInfo/GetSoftwareInfo=1 and $reqInfo/ApplyProcessFilter=1">
            <td align="center">-</td>
         </xsl:when>
      </xsl:choose>      
      
      <xsl:variable name="OS" select="number(OS)"/>
      <!--process disk storage next-->
     <xsl:variable name="rowpos" select="position()"/>
     
     <xsl:for-each select="/GetTargetClusterInfoResponse/Columns/Item">
          <xsl:variable name="text" select="text()"/>
          <xsl:if test="$text!='Processes' and $text!='Up Time' and $text!='State' and $text!='Condition' and $text!='UpTime' and not(contains($text, 'Memory')) and not(starts-with($text, 'CPU')) and $text!='Swap'">
             <xsl:variable name="storageNode" select="$storageInfo[($OS!=0 and Description=$text) or ($OS=0 and starts-with(Description,$text))]"/>
             <xsl:choose>
                <xsl:when test="$storageNode">
                   <xsl:call-template name="showMemoryOrDiskSpace">
                      <xsl:with-param name="memNode" select="$storageNode"/>
                   <xsl:with-param name="parentRow" select="$rowpos" />
                   </xsl:call-template>
                </xsl:when>
                <xsl:otherwise>
                   <td>
                      <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                   </td>
                </xsl:otherwise>
             </xsl:choose>
          </xsl:if>
      </xsl:for-each>      
             
      <!--process Physical memory / Swap next -->      
      <xsl:for-each select="/GetTargetClusterInfoResponse/Columns/Item[text() = 'Physical Memory' or text()='Swap']">
      <!--save:  and not(starts-with(text(), 'CPU') and contains(substring-after(text(), 'CPU'), 'Load'))-->
         <xsl:variable name="label" select="text()"/>
         <xsl:variable name="storageNode" select="$storageInfo[Description=$label]"/>
         <xsl:choose>
            <xsl:when test="$storageNode">
               <xsl:call-template name="showMemoryOrDiskSpace">
                  <xsl:with-param name="memNode" select="$storageNode"/>
                  <xsl:with-param name="parentRow" select="$rowpos" />
               </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
               <td>
                  <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
               </td>
            </xsl:otherwise>
         </xsl:choose>
      </xsl:for-each>      
             
      <!--process CPU load next -->      
      <xsl:for-each select="/GetTargetClusterInfoResponse/Columns/Item[starts-with(text(), 'CPU') and contains(text(), 'Load')]">
      <!--save:  and not(starts-with(text(), 'CPU') and contains(substring-after(text(), 'CPU'), 'Load'))-->
         <xsl:variable name="label" select="text()"/>
         <xsl:choose>
            <xsl:when test="starts-with($label, 'CPU') and contains($label, 'Load')">
               <!--find CPU number -->
               <td align="center">
                  <xsl:variable name="cpuNum" select="substring-before(substring-after($label, 'CPU '), ' Load')"/>
                  <xsl:variable name="cpuNodeNum">
                     <xsl:choose>
                        <xsl:when test="not($cpuNum)">1</xsl:when>
                        <xsl:otherwise><xsl:value-of select="number($cpuNum)"/></xsl:otherwise>
                     </xsl:choose>
                  </xsl:variable>
                  <xsl:variable name="cpuNode" select="$machineNode/Processors/ProcessorInfo[position()=$cpuNodeNum]"/>
                  <xsl:choose>
                     <xsl:when test="$cpuNode">
                        <xsl:if test="not($cpuThreshold = 0) and $cpuNode/Load &gt; $cpuThreshold">
                           <xsl:attribute name="bgcolor">FF8800</xsl:attribute>
                        </xsl:if>
                        <xsl:value-of select="$cpuNode/Load"/><xsl:text> %</xsl:text>
                     </xsl:when>
                     <xsl:otherwise>
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                     </xsl:otherwise>
                  </xsl:choose>
               </td>
            </xsl:when>
            <xsl:otherwise>
               <td>
                  <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
               </td>
            </xsl:otherwise>
         </xsl:choose>
      </xsl:for-each>      
      <td><xsl:value-of select="UpTime"/></td>
   </xsl:template>
   
      
   <xsl:template name="showMemoryOrDiskSpace">
     <xsl:param name="memNode"/>
     <xsl:param name="parentRow" />
      <xsl:variable name="threshold">
         <xsl:choose>
            <xsl:when test="contains($memNode/Description, 'Memory')"><xsl:value-of select="$memThreshold"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="$diskThreshold"/></xsl:otherwise>
         </xsl:choose>
      </xsl:variable>
      <xsl:variable name="thresholdType">
         <xsl:choose>
            <xsl:when test="contains($memNode/Description, 'Memory')"><xsl:value-of select="$memThresholdType"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="$diskThresholdType"/></xsl:otherwise>
         </xsl:choose>
      </xsl:variable>
            
      <td id="{$memNode/Description}_row{$parentRow}_{position()}" align="center">
        <xsl:attribute name="title">
          <xsl:value-of select="$memNode/Description"/>
          <xsl:text disable-output-escaping="yes"><![CDATA[ <br /> ]]></xsl:text>
          <xsl:value-of select="$memNode/Available"/><xsl:text disable-output-escaping="yes"> MB Avail</xsl:text>
          <xsl:text disable-output-escaping="yes"><![CDATA[ <br /> ]]></xsl:text>
          <xsl:value-of select="$memNode/Total"/> MB Total
        </xsl:attribute>
        <xsl:choose>
          <xsl:when test="$memNode/Total=0">N/A</xsl:when>
          <xsl:otherwise>
            <xsl:if test="$threshold != 0">
              <xsl:choose>
                <xsl:when test="$thresholdType=0 and $memNode/PercentAvail &lt; $threshold">
                  <xsl:attribute name="bgcolor">FF8800</xsl:attribute>
                </xsl:when>
                <xsl:when test="$thresholdType=1 and $memNode/Available &lt; $threshold">
                  <xsl:attribute name="bgcolor">FF8800</xsl:attribute>
                </xsl:when>
              </xsl:choose>
            </xsl:if>
            <xsl:value-of select="$memNode/PercentAvail"/>%
          </xsl:otherwise>
        </xsl:choose>
      </td>            
   </xsl:template>
  
   <xsl:template match="Running/SWRunInfo" mode="findProcessNames">
    <xsl:param name="total"/>
    <xsl:variable name="newColumn" select="$total>30 and (position() mod 30)=0"/>
    <xsl:variable name="processInfo">
            <xsl:value-of select="Name"/>
            <xsl:if test="Instances &gt; 1"> (<xsl:value-of select="Instances"/>)</xsl:if>
            <xsl:text disable-output-escaping="yes">&lt;br/&gt;</xsl:text>
    </xsl:variable>
        <xsl:value-of select="$processInfo"/>
        <xsl:if test="$newColumn">
            <xsl:text disable-output-escaping="yes">&lt;/td&gt;</xsl:text>
            <xsl:text disable-output-escaping="yes">&lt;td noWrap valign="top"&gt;</xsl:text>
        </xsl:if>
   </xsl:template>
  
</xsl:stylesheet>
