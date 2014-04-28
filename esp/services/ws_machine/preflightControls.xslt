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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template name="GenerateScriptForPreflightControls">
   <script type="text/javascript">
      var autoRefreshVal=<xsl:value-of select="$autoRefresh"/>;
      <xsl:text disable-output-escaping="yes"><![CDATA[
         //############ begin auto reload script ######################
         var reloadTimer = null;
         var reloadTimeout = 0;
         var formSubmitted = false;
         
         // This function gets called when the window has completely loaded.
         // It starts the reload timer with a default time value.
         
         function initPreflightControls() {
            //var autoRefreshEdit = document.forms['listitems'].all['AutoRefresh'];
            var autoRefreshEdit = document.getElementById('edAutoRefresh');
            autoRefreshVal = autoRefreshEdit.value;
            if (autoRefreshVal > 0)            
               setReloadTimeout(autoRefreshVal); // Pass a default value
            enableAutoRefresh(autoRefreshVal > 0);
               
            //there seems to be a problem that if the user comes back to this page
            //then the radio button selection and the arguments section (span)
            //get out of sync so resync now
            var selectObj = document.getElementById('methodObj');
            var method = selectObj.options[ selectObj.selectedIndex ].value;
            showPreflightControl(method);
            var cbGetSoftwareInfo = document.getElementById("GetSoftwareInfo");
            onGetSoftwareInfo(cbGetSoftwareInfo);            
            onGetStorageInfo(document.getElementById("GetStorageInfo"));
         }
         
         function reloadPage() {
             document.forms['listitems'].submit();
         }

         function setReloadTimeout(mins) {
             if (reloadTimeout != mins && allowReloadPage) {
                if (reloadTimer) {              
                   clearTimeout(reloadTimer);
                   reloadTimer = null;
                }               
                if (mins > 0)
                    reloadTimer = setTimeout("reloadPage()", Math.ceil(parseFloat(mins) * 60 * 1000));
                reloadTimeout = mins;
             }
         }

         function enableAutoRefresh(enable) {
            //var autoRefreshEdit = document.forms['listitems'].all['AutoRefresh'];
            var autoRefreshEdit = document.getElementById('edAutoRefresh');
            if (enable) {
               if (autoRefreshVal == 0)
                  autoRefreshVal = 1;
               autoRefreshEdit.value = autoRefreshVal;
               autoRefreshEdit.disabled = false;
               setReloadTimeout(autoRefreshVal);
            }
            else {
               autoRefreshVal = autoRefreshEdit.value;
               autoRefreshEdit.value = 0;
               autoRefreshEdit.disabled = true
               setReloadTimeout(0);
            }
         }
         //############ end of auto reload script ######################
         function showPreflightControl(name) 
         {
          if (!fromTargetClusterPage)
          {
             if ((name=='Start') || (name=='Stop'))
                method = name;
             else
                document.forms['listitems'].action = '/ws_machine/' + name;

             if (name != 'GetMachineInfo')
                document.getElementById('GetMachineInfo').style.display="none";

             if (name != 'GetMetrics' && document.getElementById('GetMetrics') != undefined)
                document.getElementById('GetMetrics').style.display="none";

             if (name != 'RemoteExec')
                document.getElementById('RemoteExec').style.display="none";
                  
             if (name != 'Start' && name != 'Stop')
                document.getElementById('StartStop').style.display="none";

             document.getElementById('AutoRefreshSection').style.display= name == "GetMachineInfo" ? "block" : "none";
             if (name!='Start' && name!='Stop')
                document.getElementById(name).style.display='block';
             else
                document.getElementById('StartStop').style.display='block';

             method = name;
          }
          else
          {
              method = "GetMachineInfo";
              document.forms['listitems'].action = '/ws_machine/GetTargetClusterInfo';

              selectObj = document.getElementById( 'methodObj' );
              if (selectObj.selectedIndex == 0)
              {
                if (clusterChecked > 0)
                {
                  document.forms[0].submitBtn.disabled = false;
                }
                else
                {
                  document.forms[0].submitBtn.disabled = true;
                }
              }
            }
         } 

         function onGetStorageInfo(cb)
         {
            document.getElementById("LocalFileSystemsOnly").disabled = !cb.checked;
         }
         
         function onGetSoftwareInfo(cb)
         {
            var disable = !cb.checked;
            var cbApplyFilters = document.getElementById("ApplyProcessFilter");
            cbApplyFilters.disabled = disable;
            
            var editAddProcessesToFilter = document.getElementById("AddProcessesToFilter");
            editAddProcessesToFilter.disabled = !cbApplyFilters.checked || disable;
         }

         function onApplyProcessFilter(cb)
         {
            var disable = !cb.checked;
            var editAddProcessesToFilter = document.getElementById("AddProcessesToFilter");
            editAddProcessesToFilter.disabled = disable;
         }
         
         var os;
         function ensureSameOS(cb)
         {
            //cb.value is of the form IP:ProcessType:compName:OS:Dir
            //
            var parts = cb.value.split(':');
            var thisOS = parts[3];
      
            if (os == -1)
            {
               os = thisOS;
               return true;
            }
            else
               return os == thisOS; 
         }
         
         var excludedServers = [];
         var excludedCheckboxes = [];
         
         function ensureStartable(cb)
         {
            //cb.value is of the form IP:ProcessType:OS:Dir
            //
            var parts = cb.value.split(':');
            var compType = parts[1];
               
               excludedCheckboxes.push(cb);
            if (compType == 'DaliServerProcess')
                    excludedServers.push('Dali servers');
            else if (compType == 'DKCSlaveProcess')
                    excludedServers.push('DKC slaves');
            else if (compType == 'DropZone')
                    excludedServers.push('Drop Zones');
            //else if (compType == 'EclAgentProcess')
                //  excludedServers.push('ECL agents');
            else if (compType == 'MySQLProcess')
                    excludedServers.push('MySQL servers');              
            else if (compType == 'LDAPServerProcess')
                    excludedServers.push('LDAP servers');            
                else
                    excludedCheckboxes.pop(cb);//OK to start/stop       
            return true;
         }
         
                     function checkSelected(o)
                     {
                        if (o.tagName=='INPUT' && o.id!='All2'  && o.id!='All1')
                        {
              totalItems++;
              if (o.checked)
              {
                              ensureStartable(o);
                checkedCount++;
              }
                        }

                        var ch=o.children;
                        if (ch)
                            for (var i in ch)
                                checkSelected(ch[i]);
                         return;
                     }

         function onClickSubmit()
         {
            if (formSubmitted)
                return false;
            formSubmitted = true;
            return formSubmitted;
         }
         
         function adjustThresholdValue(namePrefix, thresholdType)
         {
              if (thresholdType == 0)
              {                 
                    var input = document.getElementsByName( namePrefix + 'Threshold' )[0];
                    if (input.value > 100)
                        input.value = 20; 
              }
         }
         function adjustThresholdType(namePrefix, threshold)
         {
                if (threshold > 100)
                {
                    var select = document.getElementsByName( namePrefix + 'ThresholdType' )[0];
                    select.selectedIndex = 1; //select MB
                    }
         }
         var method = 'GetMachineInfo';
        ]]>
      </xsl:text>
   </script>
</xsl:template>

 <xsl:template name="ShowPreflightControls">
    <xsl:param name="method" select="'GetMachineInfo'"/>
    <xsl:param name="getProcessorInfo" select="1"/>
    <xsl:param name="getSoftwareInfo" select="1"/>
    <xsl:param name="getStorageInfo" select="1"/>
    <xsl:param name="localFileSystemsOnly" select="1"/>
    <xsl:param name="applyProcessFilter" select="1"/>
    <xsl:param name="addProcessesToFilter"/>
    <xsl:param name="enableSNMP"/>
    <xsl:param name="username"/>
    <xsl:param name="password"/>
    <xsl:param name="securityString"/>
    <xsl:param name="command"/>
    <xsl:param name="waitUntilDone" select="1"/>
    <xsl:param name="clusterType" select="'ROXIEMACHINES'"/>
    <xsl:param name="targetCluster" select="0"/>

   <input type="hidden" name="ClusterType" value="{$clusterType}"/>
        <input type="hidden" name="UserName" value="{$username}"/>
    <input type="hidden" name="Password" value="{$password}"/>
    <input type="hidden" id="SecurityString0" name="SecurityString" value="{$securityString}"/>
   <input type="hidden" name="enableSNMP" value="{$enableSNMP}"/>
   <input type="hidden" name="AutoRefresh" value="{$autoRefresh}"/>
   <input type="hidden" name="MemThreshold" value="{$memThreshold}"/>
   <input type="hidden" name="DiskThreshold" value="{$diskThreshold}"/>
   <input type="hidden" name="CpuThreshold" value="{$cpuThreshold}"/>
   <input type="hidden" name="MemThresholdType" value="{$memThresholdType}"/>
   <input type="hidden" name="DiskThresholdType" value="{$diskThresholdType}"/>
   <div>
         <table border="0">
                <tr>
                    <th width="80" align="left"><xsl:value-of select="$hpccStrings/st[@id='Action']"/>:</th>
                    <td align="left">
                        <select id="methodObj" onchange="showPreflightControl(options[selectedIndex].value)">
                            <option value="GetMachineInfo">
                                <xsl:if test="$method='GetMachineInfo'">
                                    <xsl:attribute name="selected">true</xsl:attribute>
                                </xsl:if>
                                <xsl:value-of select="$hpccStrings/st[@id='MachineInfo']"/>
                            </option>
                            <xsl:if test="($clusterType = 'ROXIEMACHINES') and ($targetCluster = 0)">
                                <option value="GetMetrics">
                                    <xsl:if test="$method='GetMetrics'">
                                        <xsl:attribute name="selected">true</xsl:attribute>
                                    </xsl:if>
                                    <xsl:value-of select="$hpccStrings/st[@id='GetRoxieMetrics']"/>
                                </option>
                            </xsl:if>
                        </select>
                    </td>
                </tr>
            </table>
        </div>
        <div id="GetMachineInfo">
            <xsl:if test="$method != 'GetMachineInfo'">
                <xsl:attribute name="style">display:none</xsl:attribute>
            </xsl:if>
            <table border="0">
                <tr>
                    <td>
                        <input type="checkbox" name="GetProcessorInfo" align="left" value="1">
                            <xsl:if test="boolean($getProcessorInfo)">
                                <xsl:attribute name="checked"/>
                            </xsl:if>
                        </input><xsl:value-of select="$hpccStrings/st[@id='GetProcessorInformation']"/></td>
                    <td width="8"/>
                    <td><xsl:value-of select="$hpccStrings/st[@id='WarnIfCPUUsageIsOver']"/> </td>
                    <td align="left">
                        <input type="text" name="CpuThreshold" size="2" value="{$cpuThreshold}" maxlength="3"/> %</td>
                </tr>
                <tr>
                    <td>
                        <input type="checkbox" id="GetStorageInfo" name="GetStorageInfo" align="left" value="1" onclick="onGetStorageInfo(this)">
                            <xsl:if test="$getStorageInfo">
                                <xsl:attribute name="checked"/>
                            </xsl:if>
                        </input><xsl:value-of select="$hpccStrings/st[@id='GetStorageInformation']"/>
                     </td>
                    <td width="8"/>
                    <td><xsl:value-of select="$hpccStrings/st[@id='WarnIfAvailableMemoryIsUnder']"/> </td>
                    <td align="left">
                        <input type="text" name="MemThreshold" size="2" value="{$memThreshold}" maxlength="6" onchange="adjustThresholdType('Mem', value)"/>
                        <select name="MemThresholdType" onchange="adjustThresholdValue('Mem', options[selectedIndex].value)">
                            <option value="0">
                                <xsl:if test="$memThresholdType=0">
                                    <xsl:attribute name="selected">true</xsl:attribute>
                                </xsl:if>
                                <xsl:text> %</xsl:text>
                            </option>
                            <option value="1">
                                <xsl:if test="$memThresholdType=1">
                                    <xsl:attribute name="selected">true</xsl:attribute>
                                </xsl:if>
                                <xsl:text>MB</xsl:text>
                            </option>
                        </select>
                    </td>
                </tr>
                <tr>
                    <td col="2" align="center">
                        <input type="checkbox" id="LocalFileSystemsOnly" name="LocalFileSystemsOnly" value="1">
                            <xsl:if test="$localFileSystemsOnly">
                                <xsl:attribute name="checked"/>
                            </xsl:if>
                        </input><xsl:value-of select="$hpccStrings/st[@id='LocalFileSystemsOnly']"/>
                    </td>
                </tr>
                <tr>
                    <td valign="top">
                        <input type="checkbox" id="GetSoftwareInfo" name="GetSoftwareInfo" align="left" value="1" onclick="onGetSoftwareInfo(this)">
                            <xsl:if test="$getSoftwareInfo">
                                <xsl:attribute name="checked"/>
                            </xsl:if>
                        </input><xsl:value-of select="$hpccStrings/st[@id='GetSoftwareInformation']"/></td>
                    <td width="8"/>
                    <td><xsl:value-of select="$hpccStrings/st[@id='WarnIfAvailableDiskSpaceIsUnder']"/> </td>
                    <td align="left">
                        <input type="text" name="DiskThreshold" size="2" value="{$diskThreshold}" maxlength="6" onchange="adjustThresholdType('Disk', value)"/>
                        <select name="DiskThresholdType" onchange="adjustThresholdValue('Disk', options[selectedIndex].value)">
                            <option value="0">
                                <xsl:if test="$diskThresholdType=0">
                                    <xsl:attribute name="selected">true</xsl:attribute>
                                </xsl:if>
                                <xsl:text> %</xsl:text>
                            </option>
                            <option value="1">
                                <xsl:if test="$diskThresholdType=1">
                                    <xsl:attribute name="selected">true</xsl:attribute>
                                </xsl:if>
                                <xsl:text>MB</xsl:text>
                            </option>
                        </select>
                    </td>
                </tr>
                <tr>
                    <td>
                        <input type="checkbox" name="ApplyProcessFilter" id="ApplyProcessFilter" align="left" value="1" onclick="onApplyProcessFilter(this)">
                            <xsl:if test="$applyProcessFilter">
                                <xsl:attribute name="checked"/>
                            </xsl:if>
                        </input><xsl:value-of select="$hpccStrings/st[@id='ShowProcessesUsingFilter']"/></td>
                    <td/>
          <xsl:choose>
            <xsl:when test="$enableSNMP != 0">
              <td>
                <xsl:value-of select="$hpccStrings/st[@id='SecurityString']"/>:<input type="password" id="SecurityString" name="SecurityString" size="17" value="{$securityString}"/>
              </td>
              <td/>
            </xsl:when>
          </xsl:choose>
            </tr>
                <tr>
                    <td colspan="2"><xsl:value-of select="$hpccStrings/st[@id='AdditionalProcessesToFilter']"/>: </td>
                    <td><input type="text" name="AddProcessesToFilter" id="AddProcessesToFilter" value="{$addProcessesToFilter}" size="35"/>
                    </td>
                </tr>
                <tr id="AutoRefreshSection">
                    <td>
                        <input type="checkbox" id="cbAutoRefresh" name="cbAutoRefresh" onclick="enableAutoRefresh(this.checked)">
                            <xsl:if test="$autoRefresh &gt; 0">
                                <xsl:attribute name="checked">true</xsl:attribute>
                            </xsl:if>
                        </input><xsl:value-of select="$hpccStrings/st[@id='AutoRefreshEvery']"/> <input type="text" id="edAutoRefresh" name="AutoRefresh" size="2" value="{$autoRefresh}" onblur="setReloadTimeout(this.value)">
                        <xsl:if test="$autoRefresh = 0">
                            <xsl:attribute name="disabled">true</xsl:attribute>
                        </xsl:if>
                        </input> <xsl:value-of select="$hpccStrings/st[@id='Mins']"/>
                    </td>
                </tr>
            </table>
        </div>
        <xsl:if test="$clusterType='ROXIEMACHINES'">
            <div id="GetMetrics">
                <xsl:if test="$method != 'GetMetrics'">
                    <xsl:attribute name="style">display:none</xsl:attribute>
                </xsl:if>
            </div>
        </xsl:if>
    <input type="submit" value="{$hpccStrings/st[@id='Submit']}" id="submitBtn" onclick="return onClickSubmit()"/>
</xsl:template>

</xsl:stylesheet>
