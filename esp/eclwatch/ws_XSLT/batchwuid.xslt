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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html"/>
    <xsl:variable name="wuid" select="BatchWUInfoResponse/Workunit/Wuid"/>
   <xsl:variable name="wuid0" select="BatchWUGraphTimingResponse/Workunit/Wuid"/>
    <xsl:variable name="autoRefresh" select="BatchWUInfoResponse/AutoRefresh"/>
    <xsl:include href="/esp/xslt/lib.xslt"/>

    <xsl:template match="Workunit">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title><xsl:value-of select="$wuid"/></title>
        <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script type="text/javascript" src="files_/scripts/tooltip.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
                <script type="text/javascript">
                    var autoRefreshTimer=<xsl:value-of select="$autoRefresh"/>; //minute
                    var wid='<xsl:value-of select="$wuid"/>';
                    var dorefresh = true;
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        var reloadTimer = null;
                        var reloadTimeout = 0;

                        // This function gets called when the window has completely loaded.
                        // It starts the reload timer with a default time value.         
                        function onLoad()
                        {
                            if (autoRefreshTimer > 0)            
                            {
                                setReloadTimeout(autoRefreshTimer); // Pass a default value
                                var checkbox = document.getElementById("AutoRefreshBtn");
                                if (checkbox != NaN)
                                checkbox.checked = true;

                                var selection = document.getElementById("AutoRefreshTimer");
                                if (selection != NaN)
                                {
                                    for (i=0; i < selection.length; i++)
                                    {
                                        if (selection.options[i].value == autoRefreshTimer)
                                        {
                                            selection.options[i].selected="selected";
                                        }
                                    }
                                }
                            }
                            else
                            {
                                dorefresh = false;
                            }
                        }               

                        function setReloadTimeout(mins) 
                        {
                            if (reloadTimeout != mins) 
                            {
                                if (reloadTimer) 
                                {              
                                    clearTimeout(reloadTimer);
                                    reloadTimer = null;
                                }               
                                if (mins > 0)
                                    reloadTimer = setTimeout("reloadPage()", Math.ceil(parseFloat(mins) * 60 * 1000));
                                reloadTimeout = mins;
                            }
                        }

                        function setAutoRefresh(obj)
                        {
                            if (obj.checked)
                            {
                                var selection = document.getElementById("AutoRefreshTimer");
                                if (selection != NaN)
                                {
                                    setReloadTimeout(selection.options[selection.selectedIndex].value);
                                }
                            }
                            else if (reloadTimer) 
                            {              
                                clearTimeout(reloadTimer);
                                reloadTimer = null;
                            }               
                        }

                        function reloadPage() 
                        {
                            var url='/WsBatchWorkunits/BatchWUInfo?Wuid='+wid;
                            var checkbox = document.getElementById("AutoRefreshBtn");
                            if (checkbox != NaN && checkbox.checked)
                            {
                                url=url+'&amp;AutoRefresh='+reloadTimeout;
                            }

                            document.location.href=url;
                        }

                        function launch(url)
                        {
                            document.location.href=url;
                        } 
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam" onload="nof5();onLoad()">
                <table class="workunit0">
                    <colgroup>
                        <col width="250" align="left"/>
                        <col width="120" align="left"/>
                        <col width="100" align="left"/>
                        <col width="120" align="right"/>
                    </colgroup>
                    <tr>
                        <th>Workunit Details</th>
                        <!--td onmouseover="EnterContent('ToolTip','','Turn on/off Auto Refresh'); Activate();" onmouseout="deActivate();">
                            <input type="image" id="refresh" value="refresh" onclick="TurnRefresh()"/>
                        </td>
                        <td style="visibility:hidden">
                        <div id="ToolTip"/>
                        </td-->
                        <td>
                            <input type="checkbox"  id="AutoRefreshBtn" value="Auto Refresh" onclick="setAutoRefresh(this);"/> Auto Refresh
                        </td>
                        <td>
                            <select size="1" id="AutoRefreshTimer" onchange="setReloadTimeout(options[selectedIndex].value);">
                                <option value="1" selected="selected">1</option>
                                <option value="2">2</option>
                                <option value="4">4</option>
                                <option value="16">16</option>
                                <option value="256">256</option>
                            </select> mintes
                        </td>
                        <td>
                            <input type="button" class="sbutton" value="RefreshNow" onclick="reloadPage();"/>
                        </td>
                    </tr>
                </table>
                <form id="workunit" method="post">
                    <table class="workunit">
                        <colgroup>
                            <col width="20%"/>
                            <col width="80%"/>
                        </colgroup>
                        <tr>
                            <th>WUID:</th>
                            <td>
                                <!--a href="/esp/iframe?esp_iframe_title=ECL Workunit XML - {$wuid}&amp;inner=/WsBatchWorkunits/BatchWUFile%3fWuid%3d{$wuid}%26Type%3dXML"
                                   onclick="launch(this)">
                                    <xsl:value-of select="$wuid"/>
                                </a-->
                                <a href="javascript:launch('/WsWorkunits/WUFile?Wuid={$wuid}&amp;Type=XML')">
                                    <xsl:value-of select="$wuid"/>
                                </a>
                            </td>
                        </tr>
                        <tr>
                            <th>SDS User ID:</th>
                            <td>
                                <xsl:value-of select="Owner"/>
                     </td>
                        </tr>
                        <tr>
                            <th>Input File:</th>
                            <td>
                                <xsl:value-of select="InputFileName"/>
                     </td>
                        </tr>
                        <tr>
                            <th>Output File:</th>
                            <td>
                                <xsl:value-of select="OutputFileName"/>
                     </td>
                        </tr>
                        <tr>
                            <th>Activity:</th>
                            <td>
                                <xsl:value-of select="Activity"/>
                     </td>
                        </tr>
                        <!--tr>
                            <th>Process Description:</th>
                            <td>
                                <xsl:value-of select="ProcessDescription"/>
                     </td>
                        </tr-->
                        <tr>
                            <th>Priority:</th>
                            <td>
                                <xsl:value-of select="Priority"/>
                     </td>
                        </tr>
                        <tr>
                            <th>Status:</th>
                            <td>
                                <xsl:value-of select="Status"/>
                     </td>
                        </tr>
                        <tr>
                        </tr>
                        <tr>
                            <td colspan="3">
                                <table class="sort-table" id="tbl_id1">
                                    <tr>
                                        <th>Login ID</th>
                                        <th>Customer ID</th>
                                        <th>Customer Name</th>
                                        <th>Accurint Job Number</th>
                                        <th>Gateway Type</th>
                                        <th>Machine</th>
                                    </tr>
                                    <tr>
                                   <td>
                                            <xsl:value-of select="LoginID"/>
                                        </td>
                                   <td>
                                            <xsl:value-of select="CustomerID"/>
                                        </td>
                                   <td>
                                            <xsl:value-of select="CustomerName"/>
                                        </td>
                                   <td>
                                            <xsl:value-of select="AccurintJobNumber"/>
                                        </td>
                                   <!--td>
                                            <xsl:value-of select="GWType"/>
                                        </td-->
                                   <td>
                                            <xsl:value-of select="Machine"/>
                                        </td>
                                    </tr>
                                </table>
                     </td>
                        </tr>
                        <tr>
                        </tr>
                        <tr>
                            <td colspan="3">
                                <table class="sort-table" id="tbl_count1">
                                    <tr>
                                        <th>Input File Size</th>
                                        <th>Input Rec Count</th>
                                        <th>Output Rec Count</th>
                                    </tr>
                                    <tr>
                                   <td>
                                            <xsl:value-of select="InputFileSize"/>
                                        </td>
                                   <td>
                                            <xsl:value-of select="InputRecCount"/>
                                        </td>
                                   <td>
                                            <xsl:value-of select="OutputRecCount"/>
                                        </td>
                                    </tr>
                                </table>
                     </td>
                        </tr>
                        <tr>
                        </tr>
                        <tr>
                            <td colspan="3">
                                <table class="sort-table" id="tbl_time2">
                                    <tr>
                                        <th>Time Created</th>
                                        <th>Time Started</th>
                                        <!--th>Time Uploaded</th-->
                                        <th>Time Completed</th>
                                        <th>Run Time</th>
                                        <th>Max Run Time</th>
                                        <!--th>Upload Wait</th-->
                                    </tr>
                                    <tr>
                                        <td>
                                            <xsl:value-of select="TimeCreated"/>
                                        </td>
                                        <td>
                                            <xsl:value-of select="TimeStarted"/>
                                        </td>
                                        <!--td>
                                            <xsl:value-of select="TimeUploaded"/>
                                        </td-->
                                        <td>
                                            <xsl:value-of select="TimeCompleted"/>
                                        </td>
                                   <td>
                                            <xsl:value-of select="RunTime"/>
                                        </td>
                                   <td>
                                            <xsl:value-of select="MaxRunTime"/>
                                        </td>
                                   <!--td>
                                            <xsl:value-of select="UploadWait"/>
                                        </td-->
                                    </tr>
                                </table>
                     </td>
                        </tr>
                        <xsl:if test="count(Graphs/ECLGraph)">
                            <xsl:choose>
                                <xsl:when test="string-length(HaveSubGraphTimings)">
                                    <tr>
                                        <td align="left" style="padding:0;margin:0"><b>Graphs:</b></td>
                                        <td><a href="/WsBatchWorkunits/BatchWUGraphTiming?Wuid={$wuid}" onclick="launch(this)">Sub Graph Timings</a>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td></td>
                                        <td>
                                            <table class="list">
                                                <xsl:apply-templates select="Graphs"/>
                                            </table>
                                        </td>
                                    </tr>
                                </xsl:when>
                                <xsl:otherwise>
                                    <tr>
                                        <th>Graphs:</th>
                                        <td>
                                            <table class="list">
                                                <xsl:apply-templates select="Graphs"/>
                                            </table>
                                        </td>
                                    </tr>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:if>
                        <xsl:if test="count(Exceptions/ECLException[Severity='Error'])">
                            <tr>
                                <th>Errors:</th>
                                <td>
                                    <table>
                                        <xsl:apply-templates select="Exceptions/ECLException[Severity='Error']"/>
                                    </table>
                                </td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="count(Exceptions/ECLException[Severity='Warning'])">
                            <tr>
                                <th>Warnings:</th>
                                <td>
                                    <table>
                                        <xsl:apply-templates select="Exceptions/ECLException[Severity='Warning']"/>
                                    </table>
                                </td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="count(Exceptions/ECLException[Severity='Info'])">
                            <tr>
                                <th>Info:</th>
                                <td>
                                    <table>
                                        <xsl:apply-templates select="Exceptions/ECLException[Severity='Info']"/>
                                    </table>
                                </td>
                            </tr>
                        </xsl:if>
                    </table>
                </form>
                <table>
                    <tr>
                        <td>
                            <form action="/WsBatchWorkunits/BatchWUAbort?UrlRedirectTo=/WsBatchWorkunits/BatchWUInfo" method="post">
                                <xsl:variable name="ScheduledAborting">
                                    <xsl:choose>
                                        <xsl:when test="Status='scheduled' and number(Aborting)">1</xsl:when>
                                        <xsl:otherwise>0</xsl:otherwise>
                                    </xsl:choose>   
                                </xsl:variable>
                                <input type="hidden" name="Wuids_i1" value="{$wuid}"/>
                                <input type="submit" name="ActionType" value="Abort" class="sbutton" title="Abort workunit" onclick="return confirm('Abort workunit?')">
                                    <xsl:if test="Status='aborting' or Status='aborted' or Status='failed' or Status='completed' or $ScheduledAborting!=0">
                                        <xsl:attribute name="disabled">disabled</xsl:attribute>
                                    </xsl:if>
                                </input>
                            </form>
                        </td>
                        <xsl:if test="string-length(Queue)">
                            <td>
                                <form action="/WsBatchWorkunits/BatchWUResubmit" method="post">
                                    <input type="hidden" name="Queue" value="{Queue}"/>
                                    <input type="hidden" name="Wuids_i1" value="{$wuid}"/>
                                    <input type="submit" name="Resubmit" value="Resubmit" class="sbutton" title="Resubmit workunit">
                                        <xsl:if test="Status!='aborted' and Status!='failed' and Status!='completed' and Status!='archived'">
                                            <xsl:attribute name="disabled">disabled</xsl:attribute>
                                        </xsl:if>
                                    </input>
                                </form>
                            </td>
                        </xsl:if>
                    </tr>
                </table>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="ECLGraph">
        <tr>
            <xsl:choose>
                <xsl:when test="number(Running)">
                    <xsl:attribute name="class">running</xsl:attribute>
                </xsl:when>
                <xsl:when test="number(Complete)">
                </xsl:when>
                <xsl:when test="number(Failed)">
                    <xsl:attribute name="class">red</xsl:attribute>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:attribute name="class">grey</xsl:attribute>
                </xsl:otherwise>
            </xsl:choose>
            <td>
                <a href="/WsWorkunits/WUGVCGraphInfo?Wuid={$wuid}&amp;Name={Name}&amp;BatchWU=1" onclick="launch(this)">
                    <xsl:value-of select="Name"/>
                    <xsl:if test="number(RunningId)"> (<xsl:value-of select="RunningId"/>)</xsl:if>
                </a>
            </td>
            <td>
                <a href="/WsWorkunits/WUGraphInfo?Wuid={$wuid}&amp;Name={Name}&amp;BatchWU=1" onclick="launch(this)">
                    <xsl:value-of select="Name"/>_SVG
                    <xsl:if test="number(RunningId)"> (<xsl:value-of select="RunningId"/>)</xsl:if>
                </a>
            </td>
            <!--td>
                <a href="/WsWorkunits/WUGraphInfo?Wuid={$wuid}&amp;Name={Name}&amp;BatchWU=1" onclick="launch(this)">
                    <xsl:value-of select="Label"/>
                </a>
            </td-->
        </tr> 
    </xsl:template>

    <xsl:template match="ECLException">
        <tr>
            <th>
                <xsl:value-of select="Source"/>
            </th>
            <td>
                <xsl:variable name="pos">
                    <xsl:value-of select="FileName"/>
                    <xsl:if test="number(LineNo) and number(Column)">
                        (<xsl:value-of select="LineNo"/>,<xsl:value-of select="Column"/>)
                    </xsl:if>
                </xsl:variable>

                <xsl:if test="string-length($pos)">
                    <xsl:value-of select="$pos"/>:
                </xsl:if>

                <xsl:value-of select="Code"/>:
                <xsl:value-of select="Message"/>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="BatchWUGraphTimingResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title><xsl:value-of select="$wuid0"/></title>
                <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script type="text/javascript" src="files_/scripts/tooltip.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
                <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <script type="text/javascript">
                <xsl:text disable-output-escaping="yes"><![CDATA[
                        // This function gets called when the window has completely loaded.
                        // It starts the reload timer with a default time value.
         
                     function onLoad()
                     {
                        initSelection('resultsTable');
                     }               
                       
                     function ChangeHeader(o1, headerid)
                     {
                        if (headerid%2)
                        {
                            o1.bgColor = '#CCCCCC';
                        }
                        else
                        {
                            o1.bgColor = '#F0F0FF';
                        }
                     }

                     function launch(link)
                     {
                        document.location.href=link.href;
                     }        
               ]]></xsl:text>
            </script>
            </head>
            <body class="yui-skin-sam" onload="onLoad()">
                <h3>Graph Timings for : 
                    <a href="/WsBatchWorkunits/BatchWUInfo?Wuid={$wuid0}" onclick="launch(this)">
                       <xsl:value-of select="$wuid0"/>
                    </a>
                </h3>
                <table class="sort-table" id="resultsTable">
                    <colgroup>
                       <col/>
                       <col class="number"/>
                       <col class="number"/>
                       <col class="number"/>
                       <col class="number"/>
                    </colgroup>
                    <thead>
                        <tr>
                            <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 0)" onmouseout="ChangeHeader(this, 1)">ID</th>
                            <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 0)" onmouseout="ChangeHeader(this, 1)">Graph #</th>
                            <th align="center">Sub Graph #</th>
                            <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 0)" onmouseout="ChangeHeader(this, 1)">Minutes</th>
                            <th align="center" style="cursor:pointer" onmouseover="ChangeHeader(this, 0)" onmouseout="ChangeHeader(this, 1)">Milliseconds</th>
                        </tr>
                    </thead>
                    <tbody>
                        <xsl:apply-templates select="Workunit/TimingData/ECLTimingData"/>
                    </tbody>
                </table>
            </body>
        </html>
    </xsl:template>

   <xsl:template match="Workunit/TimingData/ECLTimingData">
      <tr>
         <td align="left">
            <a href="/WsWorkunits/WUGraphInfo?Wuid={$wuid0}&amp;Name=graph{GraphNum}&amp;GID={GID}&amp;BatchWU=1" onclick="launch(this)">
            <xsl:value-of select="GID"/></a>
         </td>
         <td align="left">
            <xsl:value-of select="GraphNum"/>
         </td>
         <td align="left">
            <xsl:value-of select="SubGraphNum"/>
         </td>
         <td align="left">
            <xsl:value-of select="Min"/>
         </td>
         <td align="left">
            <xsl:value-of select="MS"/>
         </td>
      </tr>
    </xsl:template>

   <xsl:template match="text()|comment()"/>

</xsl:stylesheet>
