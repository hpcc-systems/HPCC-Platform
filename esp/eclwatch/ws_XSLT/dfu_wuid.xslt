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
    <xsl:variable name="wuid" select="/*/result/ID"/>
    <xsl:variable name="srcformat" select="/*/result/SourceFormat"/>
    <xsl:variable name="command" select="/*/result/Command"/>
    <xsl:variable name="rowtag" select="/*/result/RowTag"/>
    <xsl:variable name="state" select="/*/result/State"/>
    <xsl:variable name="archived" select="/*/result/Archived"/>
    <xsl:variable name="autoRefresh" select="/*/AutoRefresh"/>

    <xsl:template match="/*">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script type="text/javascript">
             var autoRefreshVal=<xsl:value-of select="$autoRefresh"/>;
                var wuid='<xsl:value-of select="$wuid"/>';
                var rowtag = '<xsl:value-of select="$rowtag"/>';
                
                <xsl:text disable-output-escaping="yes"><![CDATA[
                    var reloadTimer = null;
                    var reloadTimeout = 0;
         
                     // This function gets called when the window has completely loaded.
                     // It starts the reload timer with a default time value.
         
                     function onLoad()
                     {
                        //var autoRefreshEdit = document.forms['protect'].all['AutoRefresh'];
                        //autoRefreshVal = autoRefreshEdit.value;
                        if (autoRefreshVal > 0)            
                           setReloadTimeout(autoRefreshVal); // Pass a default value
                        //enableAutoRefresh(autoRefreshVal > 0);
                     }               
                             
                     function setReloadTimeout(mins) {
                         if (reloadTimeout != mins) {
                            if (reloadTimer) {              
                               clearTimeout(reloadTimer);
                               reloadTimer = null;
                            }               
                            if (mins > 0)
                                reloadTimer = setTimeout("reloadPage()", Math.ceil(parseFloat(mins) * 60 * 1000));
                            reloadTimeout = mins;
                         }
                     }

                     /*function enableAutoRefresh(enable) {
                        var autoRefreshEdit = document.forms[0].all['AutoRefresh'];
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
                     }*/
         
                     function reloadPage() {
                         //document.forms[0].submit();
                         document.location.href='/FileSpray/GetDFUWorkunit?wuid='+wuid;
                     }
                    function launch(url)
                    {
                        document.location.href = url.href;
                    }                         
                    function submitaction(button, command, srcformat)
                    {
                        var value = button.value;
            var form = document.forms['form2'];
                        var rc = false;//do not submit the form
                        if (value == "Abort")
                        {
                            if (confirm("Are you sure you want to abort the workunit?"))
                            {
                                form.action = "/FileSpray/AbortDFUWorkunit";
                                rc = true;
                            }
                        }
                        else if (value == "Resubmit")
                        {
                            form.action = "/FileSpray/SubmitDFUWorkunit";
                            rc = true;
                        }
                        else if (command == 6 || command == 7 || command == 1)
                        {
                            rc = true;
                            if (command == 6 && srcformat == 0)
                                form.action = "/FileSpray/SprayFixedInput";
                            else if (command == 6 && rowtag.length == 0)
                                form.action = "/FileSpray/SprayVariableInput?submethod=csv";
                            else if (command == 6)
                                form.action = "/FileSpray/SprayVariableInput?submethod=xml";
                            else if (command == 7)
                                form.action = "/FileSpray/DesprayInput";
                            else if (command == 1)
                                form.action = "/FileSpray/copyInput";
                            else
                                rc = false;                 
                        }
                        return rc;
                    }                         
          function getWUXML(url) {
              document.location.href = url;
          }                     
                ]]></xsl:text>
                </script>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title><xsl:value-of select="$wuid"/></title>
                <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
                <!--style type="text/css">
                    table {}
                    table.workunit tr { padding:2 0; }
                    table.workunit th { text-align:left; vertical-align:top; }
                    table.workunit td { padding:2 2 2 12; }
                        .sbutton { width: 7em; font: 10pt arial , helvetica, sans-serif; }
                        .running { font-weight : bolder; }
                </style-->
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <h3>DFU Workunit Details</h3>
                <xsl:choose>
                    <xsl:when test="number($archived)">
                        <form id="protect" action="/FileSpray/DFUWorkunitsAction" method="post">
                            <table class="workunit">
                                <colgroup>
                                    <col width="19%" align="left"/>
                                    <col width="1%"/>
                                    <col width="80%"/>
                                </colgroup>
                                <xsl:apply-templates/>
                                <tr>
                                    <td colspan="2"/>
                                    <td align="left">
                                        <input type="hidden" name="wuids_i1" value="{$wuid}"/>
                                        <input type="submit" value="Restore" name="Type" class="sbutton"/>
                                    </td>
                                </tr>
                            </table>
                        </form>
                    </xsl:when>
                    <xsl:otherwise>
                        <form id="protect" action="/FileSpray/UpdateDFUWorkunit?wu.ID={$wuid}" method="post">
                            <table class="workunit">
                                <colgroup>
                                    <col width="19%" align="left"/>
                                    <col width="1%"/>
                                    <col width="80%"/>
                                </colgroup>
                                <xsl:apply-templates/>
                                <tr>
                                    <td colspan="2"/>
                                    <td align="left">
                                        <input type="hidden" name="wuid" value="{$wuid}"/>
                                        <input type="hidden" name="StateOrig" value="{$state}"/>
                                        <input type="hidden" name="ClusterOrig" value="{/*/result/ClusterName}"/>
                                        <input type="hidden" name="isProtectedOrig" value="{/*/result/isProtected}"/>
                                        <input type="hidden" name="JobNameOrig" value="{/*/result/JobName}"/>
                                        <input type="submit" name="Type" value="Save" class="sbutton"/>
                                        <input type="reset" name="Type" value="Reset" class="sbutton"/>
                                        <!--input type="submit" name="Type" value="Reset" class="sbutton"/-->
                                    </td>
                                </tr>
                            </table>
                        </form>
                        <br/>
                        <!--form id="protect" action="/FileSpray/UpdateDFUWorkunit?wu.ID={$wuid}" method="post"-->
                        <form id="form2" method="POST">
                            <input type="hidden" name="wuid" value="{$wuid}"/>
                            <table class="workunit">
                                <tr>
                                    <td align="left">
                                    <xsl:choose>
                                        <xsl:when test="$state!='4' and $state!='5' and $state!='6'">
                                            <input type="submit" value="Abort" class="sbutton" onclick="return submitaction(this, '{$command}', '{$srcformat}')"/>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <input type="submit" value="Abort" class="sbutton" disabled="true"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:choose>
                                        <xsl:when test="string-length(result/Queue) and $state!='2' and $state!='3' and $state!='7' and $state!='8'">
                                            <input type="submit" value="Resubmit" class="sbutton" onclick="return submitaction(this, '{$command}', '{$srcformat}')"/>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <input type="submit" value="Resubmit" class="sbutton" disabled="true"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:choose>
                                        <xsl:when test="$command = 6 or $command = 7 or $command = 1">
                                            <input type="submit" value="Modify" class="sbutton" onclick="return submitaction(this, '{$command}', '{$srcformat}')"/>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <input type="submit" value="Modify" class="sbutton" disabled="true"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    </td>
                                </tr>
                            </table>
                        </form>
                    </xsl:otherwise>
                </xsl:choose>
                <br/>                             
            </body>
        </html>
    </xsl:template> 

    <xsl:template match="result">
        <xsl:apply-templates select="ID"/>
        <xsl:apply-templates select="ClusterName"/>
        <xsl:apply-templates select="JobName"/>
        <xsl:apply-templates select="Queue"/>
        <xsl:apply-templates select="User"/>
        <xsl:apply-templates select="isProtected"/>
        <xsl:choose>
            <xsl:when test="number($archived)">
                <xsl:apply-templates select="CommandMessage"/>
            </xsl:when>
            <xsl:otherwise>         
                <xsl:apply-templates select="Command"/>
            </xsl:otherwise>            
        </xsl:choose>
        <xsl:apply-templates select="TimeStarted"/>
        <xsl:apply-templates select="TimeStopped"/>
        <xsl:apply-templates select="PercentDone"/>
        <xsl:apply-templates select="SecsLeft"/>
        <xsl:apply-templates select="ProgressMessage"/>
        <xsl:apply-templates select="SummaryMessage"/>
        <xsl:choose>
            <xsl:when test="number($archived)">
                <xsl:apply-templates select="StateMessage"/>
            </xsl:when>
            <xsl:otherwise>         
                <xsl:apply-templates select="State"/>
            </xsl:otherwise>            
        </xsl:choose>
        <xsl:apply-templates select="SourceLogicalName"/>
        <xsl:apply-templates select="SourceIP"/>
        <xsl:apply-templates select="SourceFilePath"/>
        <xsl:apply-templates select="SourceDali"/>
        <xsl:apply-templates select="SourceRecordSize"/>
        <xsl:apply-templates select="SourceFormat"/>
        <xsl:apply-templates select="RowTag"/>
        <xsl:apply-templates select="SourceNumParts"/>
        <xsl:apply-templates select="SourceDirectory"/>
        <xsl:apply-templates select="DestLogicalName"/>
        <xsl:apply-templates select="DestGroupName"/>
        <xsl:apply-templates select="DestDirectory"/>
        <xsl:apply-templates select="DestIP"/>
        <xsl:apply-templates select="DestFilePath"/>
        <xsl:apply-templates select="DestFormat"/>
        <xsl:apply-templates select="DestNumParts"/>
        <xsl:apply-templates select="DestRecordSize"/>
        <xsl:apply-templates select="MonitorEventName"/>
        <xsl:apply-templates select="MonitorSub"/>
        <xsl:apply-templates select="MonitorShotLimit"/>
        <xsl:apply-templates select="Overwrite"/>
        <xsl:apply-templates select="Replicate"/>
        <xsl:apply-templates select="Compress"/>
    </xsl:template>

    <xsl:template match="result1">
            <tr>
                <th><xsl:apply-templates select="." mode="caption"/></th><th>:</th>
                <td><xsl:apply-templates select="."/></td>
            </tr>
    </xsl:template>

    <xsl:template match="ID">
        <tr>
            <th>ID</th><th>:</th>
            <td>
        <xsl:variable name="href0">
          <xsl:text>/esp/iframe?esp_iframe_title=DFU Workunit XML - </xsl:text>
          <xsl:value-of select="$wuid"/>
          <xsl:text>&amp;inner=/FileSpray/DFUWUFile%3fWuid%3d</xsl:text>
          <xsl:value-of select="$wuid"/>
        </xsl:variable>
                <xsl:choose>
                    <xsl:when test="number($archived)">
                        <xsl:value-of select="$wuid"/>
                    </xsl:when>
                    <xsl:otherwise>         
                        <a href="javascript:getWUXML('{$href0}')" onclick="launch(this)">
                            <xsl:value-of select="$wuid"/>
                        </a>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="PercentDone">
        <tr>
            <th>PercentDone</th><th>:</th>
            <td>
            <a href="/FileSpray/GetDFUProgress?wuid={$wuid}" onclick="launch(this)">View Progress</a>
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="isProtected">
        <tr>
            <th>Protected</th><th>:</th>
            <td>
            <input type="checkbox" name="wu.isProtected"> <xsl:if test="number(.)"><xsl:attribute name="checked"/></xsl:if></input>   
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="State">
        <tr>
            <th>State</th><th>:</th>
            <td>
            <select size="1" name="wu.State">
            <option value="{text()}">
            <xsl:choose>
                <xsl:when test="text()='0'">unknown</xsl:when>
                <xsl:when test="text()='1'">scheduled</xsl:when>
                <xsl:when test="text()='2'">queued</xsl:when>
                <xsl:when test="text()='3'">started</xsl:when>
                <xsl:when test="text()='4'">aborted</xsl:when>
                <xsl:when test="text()='5'">failed</xsl:when>
                <xsl:when test="text()='6'">finished</xsl:when>
                <xsl:when test="text()='7'">monitoring</xsl:when>
                <xsl:when test="text()='8'">aborting</xsl:when>
            </xsl:choose>
            </option>
            <xsl:if test="text()!='5'">
            <option value="5">failed</option>
            </xsl:if>
            </select> 
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="SourceFormat|DestFormat">
        <tr>
            <th><xsl:value-of select="name()"/></th><th>:</th>
            <td>
            <xsl:choose>
            <xsl:when test=". = 0">FIXED</xsl:when>
            <xsl:when test=". = 1">CSV</xsl:when>
            <xsl:when test=". = 2">UTF8</xsl:when>
            <xsl:when test=". = 3">UTF8N</xsl:when>
            <xsl:when test=". = 4">UTF16</xsl:when>
            <xsl:when test=". = 5">UTF16LE</xsl:when>
            <xsl:when test=". = 6">UTF16BE</xsl:when>
            <xsl:when test=". = 7">UTF32</xsl:when>
            <xsl:when test=". = 8">UTF32LE</xsl:when>
            <xsl:when test=". = 9">UTF32BE</xsl:when>
            <xsl:when test=". = 10">VARIABLE</xsl:when>
            <xsl:when test=". = 11">RECFMVB</xsl:when>
            <xsl:when test=". = 12">RECFMV</xsl:when>
            <xsl:when test=". = 13">VARIABLEBIGENDIAN</xsl:when>
            <xsl:otherwise>UNKNOWN</xsl:otherwise>
            </xsl:choose>                                
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="JobName">
        <tr>
            <th>JobName</th><th>:</th>
            <td>
            <input type="text" name="wu.JobName" value="{.}" size="40"/>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="ClusterName">
        <tr>
            <th>ClusterName</th><th>:</th>
            <td>
            <input type="text" name="wu.ClusterName" value="{.}" size="40" editable="false"/>
            </td>
        </tr>
    </xsl:template>
        
    <xsl:template match="Command">
        <tr>
            <th>Command</th><th>:</th>
            <td>
            <xsl:choose>
            <!-- translate commands 0-12 to strings in order of probability of occurrence for optimization -->
            <xsl:when test=". = 6">Spray</xsl:when><!--DFUcmd_import-->
            <xsl:when test=". = 7">Despray</xsl:when><!--DFUcmd_export-->
            <xsl:when test=". = 1">Copy</xsl:when><!--DFUcmd_copy-->
            <xsl:when test=". = 2">Delete</xsl:when><!--DFUcmd_remove-->
            <xsl:when test=". = 3">Move</xsl:when><!--DFUcmd_move-->
            <xsl:when test=". = 5">Replicate</xsl:when><!--DFUcmd_replicate-->
            <xsl:when test=". = 4">Rename</xsl:when><!--DFUcmd_rename-->
            <xsl:when test=". = 8">Add</xsl:when><!--DFUcmd_add-->
            <xsl:when test=". = 9">Transfer</xsl:when><!--DFUcmd_transfer-->
            <xsl:when test=". = 10">DKC</xsl:when><!--DFUcmd_dkc-->
            <xsl:when test=". = 11">Save Map</xsl:when><!--DFUcmd_savemap-->
            <xsl:when test=". = 12">Add Group</xsl:when><!--DFUcmd_addgroup-->
            <xsl:when test=". = 13">Server</xsl:when><!--DFUcmd_server-->
            <xsl:when test=". = 14">Monitor</xsl:when><!--DFUcmd_server-->
            <xsl:when test=". = 15">Copy Merge</xsl:when><!--DFUcmd_server-->
            <xsl:when test=". = 16">Supercopy</xsl:when><!--DFUcmd_server-->
            <xsl:when test=". = 0"></xsl:when><!--DFUcmd_none-->
            <xsl:otherwise></xsl:otherwise>
            </xsl:choose>                                
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="SourceLogicalName">
        <tr>
            <th>SourceLogicalName</th><th>:</th>
            <td>
                <xsl:choose>
                    <xsl:when test="number($archived)">
                        <xsl:value-of select="."/>
                    </xsl:when>
                    <xsl:when test="string-length(../SourceDali) = 0">
                        <a href="/WsDfu/DFUInfo?Name={.}" onclick="launch(this)">
                            <xsl:value-of select="."/>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="."/>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="DestLogicalName">
        <tr>
            <th>DestLogicalName</th><th>:</th>
            <td>
                <xsl:choose>
                    <xsl:when test="number($archived)">
                        <xsl:value-of select="."/>
                    </xsl:when>
                    <xsl:otherwise>
                        <a href="/WsDfu/DFUInfo?Name={.}" onclick="launch(this)">
                            <xsl:value-of select="."/>
                        </a>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="*">
        <tr>
            <th><xsl:value-of select="name()"/></th><th>:</th>
            <td>
            <xsl:value-of select="."/>
            </td>
        </tr>       
    </xsl:template>
    
    <!--RM: removed apparently orphan code below-->
    <!--xsl:template name="makeAbortSubmitForm">
        <table class="workunit">
            <tr>
                <td>
                    <form id="form3" action="/FileSpray/AbortDFUWorkunit" method="post">
                        <input type="hidden" name="wuid" value="{$wuid}"/>
                        <input type="submit" name="Abort" value="Abort" class="sbutton" onclick="return confirm('Abort workunit?')"/>
                    </form>
                </td>
                <td>
                    <xsl:if test="string-length(result/Queue)">
                        <form action="/FileSpray/SubmitDFUWorkunit" method="post">
                            <input type="hidden" name="wuid" value="{$wuid}"/>
                            <input type="submit" name="Resubmit" value="Resubmit" class="sbutton"/>
                        </form>
                    </xsl:if>
                </td>
                <xsl:if test="$command = 6 or $command = 7 or $command = 1">
                <td>
                    <form method="post">
                        <xsl:attribute name="action">
                            <xsl:text>/FileSpray/</xsl:text>
                            <xsl:choose>
                                <xsl:when test="$command = 6">
                                    <xsl:choose>
                                        <xsl:when test="$srcformat = 0">SprayFixedInput</xsl:when>
                                        <xsl:when test="string-length($rowtag) = 0">SprayVariableInput</xsl:when>
                                        <xsl:otherwise>SprayVariableInput</xsl:otherwise>
                                    </xsl:choose>
                                </xsl:when>
                                <xsl:when test="$command = 7">DesprayInput</xsl:when>
                                <xsl:when test="$command = 1">copyInput</xsl:when>
                            </xsl:choose>
                        </xsl:attribute>
                        <input type="hidden" name="wuid" value="{$wuid}"/>
                        <input type="submit" name="Modify" value="Modify" class="sbutton"/>
                        <xsl:if test="$command=6">
                            <input type="hidden" name="submethod">
                                <xsl:attribute name="value">
                                    <xsl:choose>
                                        <xsl:when test="string-length($rowtag) = 0">csv</xsl:when>
                                        <xsl:otherwise>xml</xsl:otherwise>
                                    </xsl:choose>
                                </xsl:attribute>
                            </input>
                        </xsl:if>
                    </form>
                </td>
                </xsl:if>
            </tr>
        </table>
    </xsl:template-->

    <xsl:template match="text()">
        <xsl:value-of select="."/>
    </xsl:template>

    <xsl:template match="comment()"/>
     
</xsl:stylesheet>
