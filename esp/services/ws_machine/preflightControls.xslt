<?xml version="1.0" encoding="utf-8"?>
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

            var c = document.forms['listitems'].all;
            if (name != 'GetMachineInfo')
               c['GetMachineInfo'].style.display="none";
               
            if (name != 'GetMetrics' && c['GetMetrics'] != undefined)
               c['GetMetrics'].style.display="none";
            
            if (name != 'RemoteExec')
               c['RemoteExec'].style.display="none";
                  
            if (name != 'Start' && name != 'Stop')
               c['StartStop'].style.display="none";
                  
            c['AutoRefreshSection'].style.display= name == "GetMachineInfo" ? "block" : "none";
            if (name!='Start' && name!='Stop')
               c[name].style.display='block';
                    else
               c['StartStop'].style.display='block';

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
            if (method == 'RemoteExec')
            {
               os = -1;//unknown
               var rc = processSelectedItems(ensureSameOS);
               if (!rc)
                  alert('Please select machines of the same platform type for remote execution!');
               return rc;
            }
            else if (method == 'GetMachineInfo' || method == 'GetMetrics' || method == 'GetTargetClusterInfo')
            {
               var index = method == 'GetMachineInfo' ? 1 : 2; //in fact, nothing is needed here
               //var src = document.getElementById("SecurityString"+index);
               //var dest= document.getElementById("SecurityString0");
               //dest.value = src.value;
            }
            else //start or stop
            {
                      var op = method.toLowerCase();
                 excludedServers.length = 0;
                 excludedCheckboxes.length = 0;
              //processSelectedItems(ensureStartable);
              totalItems = 0;
              checkedCount = 0;
              checkSelected(document.forms['listitems']);;
              var rc;
              if (excludedServers.length > 0)
              {
                var bAllChecked = checkedCount == totalItems;
                              var s = excludedServers.join(', ') + ' cannot be ' + op + 'ed!';
                              alert(s);
                              var n = excludedCheckboxes.length;
                              for (var i=0; i<n; i++)
                                  excludedCheckboxes[i].checked = false;
                                
                              ///countItems();//updates checkedCount and totalItems                     
                              ///rowsChecked = checkedCount > 0;
                              ///if (bAllChecked)
                                ///  checkSelectAllCheckBoxes(false);
                              document.forms[0].submitBtn.disabled = checkedCount == 0;
                              rc = false;
              }
              else
              {
                 rc = confirm('Are you sure you would like to ' + op + ' the selected server(s)?');
                 if (rc)
                      document.forms[0].action = '/ws_machine/StartStopBegin?Stop=' + (op=='start' ? '0':'1');
              }
            }
            if (rc)
                    formSubmitted = true;
            return rc;
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
                    <th width="80" align="left">Action:</th>
                    <td align="left">
                        <select id="methodObj" onchange="showPreflightControl(options[selectedIndex].value)">
                            <option value="GetMachineInfo">
                                <xsl:if test="$method='GetMachineInfo'">
                                    <xsl:attribute name="selected">true</xsl:attribute>
                                </xsl:if>
                                <xsl:text>Machine Information</xsl:text>
                            </option>
                            <xsl:if test="($clusterType = 'ROXIEMACHINES') and ($targetCluster = 0)">
                                <option value="GetMetrics">
                                    <xsl:if test="$method='GetMetrics'">
                                        <xsl:attribute name="selected">true</xsl:attribute>
                                    </xsl:if>
                                    <xsl:text>Get Roxie Metrics</xsl:text>
                                </option>
                            </xsl:if>
              <!--option value="RemoteExec">
                                <xsl:if test="$method='RemoteExec'">
                                    <xsl:attribute name="selected">true</xsl:attribute>
                                </xsl:if>
                                <xsl:text>Remote Execution</xsl:text>
                            </option-->
                            <xsl:if test="$enableSNMP != 0 and $clusterType='' and ($targetCluster = 0)">
                                <option value="Start">
                                    <xsl:if test="$method='Start'">
                                        <xsl:attribute name="selected">true</xsl:attribute>
                                    </xsl:if>
                                    <xsl:text>Start Server(s)</xsl:text>
                                </option>
                                <option value="Stop">
                                    <xsl:if test="$method='Stop'">
                                        <xsl:attribute name="selected">true</xsl:attribute>
                                    </xsl:if>
                                    <xsl:text>Stop Server(s)</xsl:text>
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
                        </input>Get processor information</td>
                    <td width="8"/>
                    <td>Warn if CPU usage is over </td>
                    <td align="left">
                        <input type="text" name="CpuThreshold" size="2" value="{$cpuThreshold}" maxlength="3"/> %</td>
                </tr>
                <tr>
                    <td>
                        <input type="checkbox" name="GetStorageInfo" align="left" value="1">
                            <xsl:if test="$getStorageInfo">
                                <xsl:attribute name="checked"/>
                            </xsl:if>
                        </input>Get storage information
                     </td>
                    <td width="8"/>
                    <td>Warn if available memory is under </td>
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
                    <td valign="top">
                        <input type="checkbox" id="GetSoftwareInfo" name="GetSoftwareInfo" align="left" value="1" onclick="onGetSoftwareInfo(this)">
                            <xsl:if test="$getSoftwareInfo">
                                <xsl:attribute name="checked"/>
                            </xsl:if>
                        </input>Get software information</td>
                    <td width="8"/>
                    <td>Warn if available disk space is under </td>
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
                        </input>Show processes using filter</td>
                    <td/>
          <xsl:choose>
            <xsl:when test="$enableSNMP != 0">
              <td>
                Security String:<input type="password" id="SecurityString" name="SecurityString" size="17" value="{$securityString}"/>
              </td>
              <td/>
            </xsl:when>
            <!--xsl:otherwise>
              <td>
                User Name:<input type="text" name="UserName" size="17" value="{$username}"/>
              </td>
              <td>
                Password:<input type="password" name="Password" size="17" value="{$password}"/>
              </td>
            </xsl:otherwise-->
          </xsl:choose>
            </tr>
                <tr>
                    <td colspan="2">Additional processes to filter: </td>
                    <td><input type="text" name="AddProcessesToFilter" id="AddProcessesToFilter" value="{$addProcessesToFilter}" size="35"/>
                    </td>
                </tr>
                <tr id="AutoRefreshSection">
                    <td>
                        <input type="checkbox" id="cbAutoRefresh" name="cbAutoRefresh" onclick="enableAutoRefresh(this.checked)">
                            <xsl:if test="$autoRefresh &gt; 0">
                                <xsl:attribute name="checked">true</xsl:attribute>
                            </xsl:if>
                        </input>Auto Refresh every <input type="text" id="edAutoRefresh" name="AutoRefresh" size="2" value="{$autoRefresh}" onblur="setReloadTimeout(this.value)">
                        <xsl:if test="$autoRefresh = 0">
                            <xsl:attribute name="disabled">true</xsl:attribute>
                        </xsl:if>
                        </input> mins.
                    </td>
                </tr>
            </table>
        </div>
        <xsl:if test="$clusterType='ROXIEMACHINES'">
            <div id="GetMetrics">
                <xsl:if test="$method != 'GetMetrics'">
                    <xsl:attribute name="style">display:none</xsl:attribute>
                </xsl:if>
                <!--table border="0">
          <xsl:if test="$enableSNMP != 0">
            <tr>
              <td>Security String:</td>
              <td>
                <input type="password" id="SecurityString2" name="SecurityString2" size="15" value="{$securityString}"/>
              </td>
            </tr>
          </xsl:if>
                    <tr>
                        <td/>
                    </tr>
                </table-->
            </div>
        </xsl:if>
        <div id="RemoteExec">
            <xsl:if test="$method != 'RemoteExec'">
                <xsl:attribute name="style">display:none</xsl:attribute>
            </xsl:if>
            <table border="0">          
                <tr>
                    <td colspan="2">
                        <table border="0" cellpadding="0">
                            <tr>
                                <td width="80">Command: </td>
                                <td><input type="text" name="Command" size="30" value="{$command}"/></td>
                            </tr>
                            <tr>
                                <td>User Name: </td>
                                <td><input type="text" id="Key3" name="Key3" size="20" value=""/></td>
                            </tr>
                            <tr>
                                <td>Password: </td>
                                <td><input type="password" id="Key3" name="Key4" size="20" value=""/></td>
                            </tr>
                            <tr>
                                <td colspan="2">
                                    <input type="checkbox" name="Wait" align="left">
                                        <xsl:if test="$waitUntilDone">
                                            <xsl:attribute name="checked"/>
                                        </xsl:if> Wait until done</input>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
        </div>
        <div id="StartStop">
            <xsl:if test="$method != 'Start' and $method != 'Stop'">
                <xsl:attribute name="style">display:none</xsl:attribute>
            </xsl:if>
      <xsl:if test="$enableSNMP != 0">
        <table border="0">          
                  <tr>
                      <td colspan="2">
                          <table border="0" cellpadding="0">
                              <tr>
                                  <td>User Name: </td>
                                  <td><input type="text" id="Key1" name="Key1" size="20" value=""/></td>
                              </tr>
                              <tr>
                                  <td>Password: </td>
                                  <td><input type="password" id="Key2" name="Key2" size="20" value=""/></td>
                              </tr>
                          </table>
                      </td>
                  </tr>
              </table>
      </xsl:if>
    </div>
    <input type="submit" value="Submit" id="submitBtn" onclick="return onClickSubmit()"/>
</xsl:template>

</xsl:stylesheet>
