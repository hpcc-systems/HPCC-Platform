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
   <xsl:variable name="archived" select="/WUQueryResponse/Type"/>
   <xsl:variable name="owner" select="/WUQueryResponse/Owner"/>
   <xsl:variable name="cluster" select="/WUQueryResponse/Cluster"/>
   <xsl:variable name="roxiecluster" select="/WUQueryResponse/RoxieCluster"/>
   <xsl:variable name="state" select="/WUQueryResponse/State"/>
   <xsl:variable name="totalThorTime" select="/WUQueryResponse/TotalThorTime"/>
   <xsl:variable name="pagesize" select="/WUQueryResponse/PageSize"/>
   <xsl:variable name="pagestartfrom" select="/WUQueryResponse/PageStartFrom"/>
   <xsl:variable name="start" select="/WUQueryResponse/StartDate"/>
   <xsl:variable name="end" select="/WUQueryResponse/EndDate"/>
   <xsl:variable name="firstpage" select="/WUQueryResponse/First"/>
   <xsl:variable name="prevpage" select="/WUQueryResponse/PrevPage"/>
   <xsl:variable name="nextpage" select="/WUQueryResponse/NextPage"/>
   <xsl:variable name="lastpage" select="/WUQueryResponse/LastPage"/>
   <xsl:variable name="num" select="/WUQueryResponse/NumWUs"/>
   <xsl:variable name="sortby" select="/WUQueryResponse/Sortby"/>
   <xsl:variable name="descending" select="/WUQueryResponse/Descending"/>
   <xsl:variable name="filters" select="/WUQueryResponse/Filters"/>
   <xsl:variable name="basicquery" select="/WUQueryResponse/BasicQuery"/>
   <xsl:template match="WUQueryResponse">
      <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
         <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Workunits</title>
            <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
           <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
           <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
           <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
           <script type="text/javascript" src="files_/scripts/sortabletable.js">&#160;</script>
           <script language="JavaScript1.2" src="files_/scripts/multiselect.js">&#160;</script>
           <script language="JavaScript1.2">
               var currentFilters='<xsl:value-of select="$filters"/>';
               var archivedStr='<xsl:value-of select="$archived"/>';
               var basicQuery='<xsl:value-of select="$basicquery"/>';
               <xsl:text disable-output-escaping="yes"><![CDATA[
                     var protectedChecked = 0;
                     var unprotectedChecked = 0;
                     var scheduleChecked = 0;
                     var descheduleChecked = 0;
                     var noscheduleChecked = 0;
                     function checkSelected(o)
                     {
                        if (o.tagName=='INPUT' && o.id!='All2'  && o.id!='All1'  && o.checked)
                        {
                            if (document.getElementById(o.value))
                            {
                                protectedChecked++;
                            }
                            else
                            {
                                unprotectedChecked++;
                            }

                            var v1 = o.value + "_S";
                            var v2 = o.value + "_D";
                            var v1v2 = false;
                            if (document.getElementById(v1))
                            {
                                scheduleChecked++;
                                v1v2 = true;
                            }
                            if (document.getElementById(v2))
                            {
                                descheduleChecked++;
                                v1v2 = true;
                            }
                            if (v1v2 == false)
                                noscheduleChecked++;

                            return;
                        }

                        var ch=o.children;
                        if (ch)
                            for (var i in ch)
                                checkSelected(ch[i]);
                         return;
                     }
                     function onRowCheck(checked)
           {
                        if (document.getElementById("deleteBtn"))
                            document.getElementById("deleteBtn").disabled = true;
                        if (document.getElementById("protectBtn"))
                            document.getElementById("protectBtn").disabled = true;
                        if (document.getElementById("unprotectBtn"))
                            document.getElementById("unprotectBtn").disabled = true;
                        if (document.getElementById("restoreBtn"))
                            document.getElementById("restoreBtn").disabled = true;
                        if (document.getElementById("scheduleBtn"))
                            document.getElementById("scheduleBtn").disabled = true;
                        if (document.getElementById("descheduleBtn"))
                            document.getElementById("descheduleBtn").disabled = true;
                        if (document.getElementById("changeStateBtn"))
                            document.getElementById("changeStateBtn").disabled = true;

            protectedChecked = 0;
                        unprotectedChecked = 0;
            scheduleChecked = 0;
                        descheduleChecked = 0;
                        noscheduleChecked = 0;
                        checkSelected(document.forms['listitems']);
                        if (protectedChecked + unprotectedChecked > 0)
                        {
                            document.getElementById("changeStateBtn").disabled = false;
                        }

                        if (protectedChecked > 0 && unprotectedChecked == 0)
                        {
                            document.getElementById("unprotectBtn").disabled = false;
                        }
                        else if (unprotectedChecked > 0 && protectedChecked == 0)
                        {
                            if (document.getElementById("deleteBtn"))
                                document.getElementById("deleteBtn").disabled = false;
                            if (document.getElementById("protectBtn"))
                                document.getElementById("protectBtn").disabled = false;
                            if ((archivedStr != '') && document.getElementById("restoreBtn"))
                                document.getElementById("restoreBtn").disabled = false;
                        }
                        if (noscheduleChecked == 0)
                        {
                            if (scheduleChecked > 0 && descheduleChecked == 0)
                            {
                                document.getElementById("scheduleBtn").disabled = false;
                            }
                            if (descheduleChecked > 0 && scheduleChecked == 0)
                            {
                                document.getElementById("descheduleBtn").disabled = false;
                            }
                        }
           }     
                     function headerClicked(headername, descending)
                     {
                        var url;
                        if (currentFilters.length > 0)
                            url='/WsWorkunits/WUQuery?'+currentFilters+'&Sortby='+headername+'&Descending='+descending;
                        else
                            url='/WsWorkunits/WUQuery?Sortby='+headername+'&Descending='+descending;
                        
                        document.location.href=url;
                     }                
                     function getNewPage()
                     {
                        var startFrom = document.getElementById("PageStartFrom").value;
                        var pageEndAt = document.getElementById("PageEndAt").value;
                        if (startFrom < 1)
                            startFrom = 1;
                        if ((pageEndAt < 1) || (pageEndAt - startFrom < 0))
                            pageEndAt = 100;
                        var size = pageEndAt - startFrom + 1;
                        startFrom -= 1;
                        if (basicQuery.length > 0)
                            document.location.href = '/WsWorkunits/WUQuery?'+ basicQuery + '&PageStartFrom='+startFrom+'&PageSize='+size;
                        else
                            document.location.href = '/WsWorkunits/WUQuery?PageStartFrom='+startFrom+'&PageSize='+size;

                        return false;
                     }             
                     function selectAll0(chkbox)
                     {
                        selectAll(chkbox.checked);
                        var topchkbox = document.getElementById("All1");
                        if (topchkbox != chkbox && topchkbox.checked != chkbox.checked)
                            topchkbox.checked = chkbox.checked;

                        var bottomchkbox = document.getElementById("All2");
                        if (bottomchkbox != chkbox && bottomchkbox.checked != chkbox.checked)
                            bottomchkbox.checked = chkbox.checked;
                     }

                     function onLoad()
                     {
                        initSelection('resultsTable');
                        //var table = document.getElementById('resultsTable');
                        //if (table)
                        //   sortableTable = new SortableTable(table, table, ["None", "None", "String", "String", "String", "String", "String",]);
                     }       
                     //var sortableTable = null;
               ]]></xsl:text>
            </script>
         </head>
        <body class="yui-skin-sam" onload="nof5();onLoad()">
          <h3>ECL Workunits</h3>
          <xsl:if test="$num > 0">
                <form id="newpageform" onsubmit="return getNewPage()" method="post">
                    <tr><th>
                        <xsl:if test="not(string-length($archived))">Total <b><xsl:value-of select="/WUQueryResponse/NumWUs"/></b> workunits.</xsl:if>
                        Current page starts from:</th>
                    <td><input type="text" id="PageStartFrom" name="PageStartFrom" value="{/WUQueryResponse/PageStartFrom}" size="10"/></td>
                    <th> to:</th>
                    <td><input type="text" id="PageEndAt" name="PageEndAt" value="{/WUQueryResponse/PageEndAt}" size="10"/></td>
                    <td><input type="submit" class="sbutton" value="Submit"/></td></tr>
                </form>
          </xsl:if>
            <form id="listitems" action="/WsWorkunits/WUAction/?{$filters}&amp;PageSize={$pagesize}&amp;CurrentPage={$pagestartfrom - 1}&amp;Sortby={$sortby}&amp;Descending={$descending}" method="post">
               <xsl:choose>
                  <xsl:when test="Workunits/ECLWorkunit[1]">
                     <table class="sort-table" id="resultsTable">
                        <colgroup>
                           <col width="5"/>
                           <col width="5"/>
                           <col width="300" align="left"/>
                           <col width="100"/>
                           <col width="300"/>
                           <col width="100"/>
                           <col width="100"/>
                           <col width="100"/>
                        </colgroup>
                        <thead>
                        <tr>
                           <th>
                              <xsl:if test="Workunits/ECLWorkunit[2]">
                                 <xsl:attribute name="id">selectAll1</xsl:attribute>
                                 <input type="checkbox" id="All1" title="Select or deselect all workunits" onclick="selectAll0(this)"/>
                              </xsl:if>
                           </th>
                                    <xsl:choose>
                                    <xsl:when test="$sortby='Protected' and $descending &lt; 1">
                                        <th title="Protected" onclick="headerClicked('Protected', 1)">
                                            <img src="/esp/files_/img/locked.gif" width="11" height="13"></img><img src="/esp/files_/img/upsimple.png" width="10" height="10"></img>
                                        </th>
                                    </xsl:when>
                                    <xsl:when test="$sortby='Protected'">
                                        <th title="Protected" onclick="headerClicked('Protected', 0)">
                                            <img src="/esp/files_/img/locked.gif" width="11" height="13"></img><img src="/esp/files_/img/downsimple.png" width="10" height="10"></img>
                                        </th>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <th title="Protected" onclick="headerClicked('Protected', 0)">
                                            <img src="/esp/files_/img/locked.gif" width="11" height="13"></img>
                                        </th>
                                    </xsl:otherwise>
                                    </xsl:choose>
                           <xsl:choose>
                            <xsl:when test="(string-length($archived))">
                                <th>WUID</th><th>Owner</th><th>Job Name</th><th>Cluster</th><th>State</th>
                            </xsl:when>
                            <xsl:otherwise>
                            <xsl:choose>
                               <xsl:when test="$sortby='WUID' and $descending &lt; 1">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 1)">WUID<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:when test="$sortby='WUID'">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 0)">WUID<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <!--xsl:when test="string-length($sortby)">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 0)">WUID</th>
                               </xsl:when-->
                               <xsl:otherwise>
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 0)">WUID</th>
                               </xsl:otherwise>
                           </xsl:choose>
                           <xsl:choose>
                               <xsl:when test="$sortby='Owner' and $descending &lt; 1">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Owner', 1)">Owner<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:when test="$sortby='Owner'">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Owner', 0)">Owner<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:otherwise>
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Owner', 0)">Owner</th>
                               </xsl:otherwise>
                           </xsl:choose>
                           <xsl:choose>
                               <xsl:when test="$sortby='Jobname' and $descending &lt; 1">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Jobname', 1)">Job Name<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:when test="$sortby='Jobname'">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Jobname', 0)">Job Name<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:otherwise>
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Jobname', 0)">Job Name</th>
                               </xsl:otherwise>
                           </xsl:choose>
                           <xsl:choose>
                               <xsl:when test="$sortby='Cluster' and $descending &lt; 1">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Cluster', 1)">Cluster<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:when test="$sortby='Cluster'">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Cluster', 0)">Cluster<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:otherwise>
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Cluster', 0)">Cluster</th>
                               </xsl:otherwise>
                           </xsl:choose>
                           <xsl:choose>
                               <xsl:when test="$sortby='RoxieCluster' and $descending &lt; 1">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('RoxieCluster', 1)">RoxieCluster<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:when test="$sortby='RoxieCluster'">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('RoxieCluster', 0)">RoxieCluster<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:otherwise>
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('RoxieCluster', 0)">RoxieCluster</th>
                               </xsl:otherwise>
                           </xsl:choose>
                           <!--th>RoxieCluster</th-->
                           <xsl:choose>
                               <xsl:when test="$sortby='State' and $descending &lt; 1">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('State', 1)">State<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:when test="$sortby='State'">
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('State', 0)">State<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                               </xsl:when>
                               <xsl:otherwise>
                                  <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('State', 0)">State</th>
                               </xsl:otherwise>
                           </xsl:choose>
                <xsl:choose>
                  <xsl:when test="$sortby='ThorTime' and $descending &lt; 1">
                    <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ThorTime', 1)">
                      Total Thor Time<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img>
                    </th>
                  </xsl:when>
                  <xsl:when test="$sortby='ThorTime'">
                    <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ThorTime', 0)">
                      Total Thor Time<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img>
                    </th>
                  </xsl:when>
                  <xsl:otherwise>
                    <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ThorTime', 0)">Total Thor Time</th>
                  </xsl:otherwise>
                </xsl:choose>
                           </xsl:otherwise>
                           </xsl:choose>
                        </tr>
                        </thead>
                        <tbody>
                        <xsl:apply-templates/>
                        </tbody>
                        </table>
                         
                        <xsl:if test="Workunits/ECLWorkunit[2]">
                         <table class="select-all">
                           <tr>
                              <th id="selectAll2">
                                 <input type="checkbox"  id="All2" title="Select or deselect all workunits" onclick="selectAll0(this)"/>
                              </th>
                              <th>Select All / None</th>
                           </tr>
                           </table>
                        </xsl:if>

                        <xsl:choose>
                            <xsl:when test="string-length($basicquery)">
                                <xsl:if test="$firstpage &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$basicquery}&amp;PageSize={$pagesize}')">First</a>&#160;
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$basicquery}&amp;PageStartFrom={$prevpage}&amp;PageSize={$pagesize}')">Prev</a>&#160;
                                </xsl:if>
                                <xsl:if test="$nextpage &gt; -1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$basicquery}&amp;PageStartFrom={$nextpage}&amp;PageSize={$pagesize}')">Next</a>&#160;
                                    <xsl:if test="not(string-length($archived))">
                                        <a href="javascript:go('/WsWorkunits/WUQuery?{$basicquery}&amp;PageStartFrom={$lastpage}&amp;PageSize={$pagesize}')">Last</a>&#160;
                                    </xsl:if>
                                </xsl:if>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:if test="$firstpage &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?PageSize={$pagesize}')">First</a>&#160;
                                    <a href="javascript:go('/WsWorkunits/WUQuery?PageStartFrom={$prevpage}&amp;PageSize={$pagesize}')">Prev</a>&#160;
                                </xsl:if>
                                <xsl:if test="$nextpage &gt; -1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?PageStartFrom={$nextpage}&amp;PageSize={$pagesize}')">Next</a>&#160;
                                    <xsl:if test="not(string-length($archived))">
                                        <a href="javascript:go('/WsWorkunits/WUQuery?PageStartFrom={$lastpage}&amp;PageSize={$pagesize}')">Last</a>&#160;
                                    </xsl:if>
                                </xsl:if>
                            </xsl:otherwise>
                        </xsl:choose>
                        
                        <table style="margin:10 0 0 0" id="btnTable" name="btnTable">
                        <tr>
                           <xsl:choose>
                                <xsl:when test="string-length($archived)">
                                   <td>
                                      <input type="submit" class="sbutton" name="ActionType" id="restoreBtn" value="Restore" disabled="true"/>
                                   </td>
                                </xsl:when>
                                <xsl:otherwise>
                                   <td>
                                      <input type="submit" class="sbutton" name="ActionType" id="deleteBtn" value="Delete" disabled="true" onclick="return confirm('Delete selected workunits?')"/>
                                   </td>
                                   <td width="10"/>
                                   <td>
                                      <input type="submit" class="sbutton" name="ActionType" id="protectBtn" value="Protect" disabled="true"/>
                                   </td>
                                   <td width="10"/>
                                   <td>
                                      <input type="submit" class="sbutton" name="ActionType" id="unprotectBtn" value="Unprotect" disabled="true"/>
                                   </td>
                                </xsl:otherwise>
                            </xsl:choose>
                           <td width="10"/>
                           <td>
                              <input type="submit" class="sbutton" name="ActionType" id="scheduleBtn" value="Reschedule" disabled="true"/>
                           </td>
                           <td width="10"/>
                           <td>
                              <input type="submit" class="sbutton" name="ActionType" id="descheduleBtn" value="Deschedule" disabled="true"/>
                           </td>
                           <td width="10"/>
                           <td>
                              <input type="submit" class="sbutton" name="ActionType" id="changeStateBtn" value="SetToFailed" disabled="true"/>
                           </td>
                        </tr>
                     </table>
                  </xsl:when>
                  <xsl:otherwise>
                            <xsl:choose>
                                <xsl:when test="$num > 0">
                                    Access to workunit is denied.<br/>
                                </xsl:when>
                                <xsl:otherwise>
                                    No workunits found.<br/>
                                </xsl:otherwise>
                            </xsl:choose>
                    </xsl:otherwise>
               </xsl:choose>
            </form>
         </body>
      </html>
   </xsl:template>
   
   
   <xsl:template match="ECLWorkunit">
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
         <td>
            <input type="checkbox" name="Wuids_i{position()}" value="{Wuid}" onclick="clicked(this, event)"/>
         </td>
         <td>
            <xsl:if test="Protected=1">
                <img border="0" alt="protected" src="/esp/files_/img/locked.gif" width="11" height="11"/>
                <input type="hidden" id="{Wuid}"/>
            </xsl:if>
            <xsl:if test="number(EventSchedule) = 1">
                <input type="hidden" id="{Wuid}_S"/>
            </xsl:if>
            <xsl:if test="number(EventSchedule) = 2">
                <input type="hidden" id="{Wuid}_D"/>
            </xsl:if>
         </td>
         <td>
            <xsl:choose>
                <xsl:when test="not(string-length($archived))">
                    <a href="javascript:go('/esp/files/stub.htm?Widget=WUDetailsWidget&amp;Wuid={Wuid}')">
                       <xsl:value-of select="Wuid"/>
                    </a>
                </xsl:when>
                <xsl:otherwise>
                    <a href="javascript:go('/WsWorkunits/WUInfo?Wuid={Wuid}&amp;{$basicquery}&amp;IncludeExceptions=0&amp;IncludeGraphs=0&amp;IncludeSourceFiles=0&amp;IncludeResults=0&amp;IncludeVariables=0&amp;IncludeTimers=0&amp;IncludeDebugValues=0&amp;IncludeApplicationValues=0&amp;IncludeWorkflows&amp;SuppressResultSchemas=1')">
                        <xsl:value-of select="Wuid"/>
                    </a>
                </xsl:otherwise>
            </xsl:choose>
         </td>
         <td>
         <xsl:choose>
            <xsl:when test="string-length(Owner) and not(string-length($owner))">
                <xsl:choose>
                    <xsl:when test="string-length($filters)">
                        <xsl:choose>
                            <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;Owner={Owner}&amp;Sortby={$sortby}')">
                                    <xsl:value-of select="Owner"/></a>
                            </xsl:when>
                            <xsl:when test="string-length($sortby)">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;Owner={Owner}&amp;Sortby={$sortby}&amp;Descending=1')">
                                    <xsl:value-of select="Owner"/></a>
                            </xsl:when>
                            <xsl:otherwise>
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;Owner={Owner}')">
                                    <xsl:value-of select="Owner"/></a>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?Owner={Owner}&amp;Sortby={$sortby}')">
                                    <xsl:value-of select="Owner"/></a>
                            </xsl:when>
                            <xsl:when test="string-length($sortby)">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?Owner={Owner}&amp;Sortby={$sortby}&amp;Descending=1')">
                                    <xsl:value-of select="Owner"/></a>
                            </xsl:when>
                            <xsl:otherwise>
                                    <a href="javascript:go('/WsWorkunits/WUQuery?Owner={Owner}')">
                                    <xsl:value-of select="Owner"/></a>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                  <xsl:value-of select="Owner"/>
            </xsl:otherwise>
         </xsl:choose>
         </td>
         <td>
            <xsl:value-of select="substring(concat(substring(Jobname,1,40),'...'),1,string-length(Jobname))"/>
         </td>
         <td>
         <xsl:choose>
            <xsl:when test="string-length(Cluster) and not(string-length($cluster)) and not(string-length($archived))">
                <xsl:choose>
                    <xsl:when test="string-length($filters)">
                        <xsl:choose>
                            <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;Cluster={Cluster}&amp;Sortby={$sortby}')">
                                    <xsl:value-of select="Cluster"/></a>
                            </xsl:when>
                            <xsl:when test="string-length($sortby)">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;Cluster={Cluster}&amp;Sortby={$sortby}&amp;Descending=1')">
                                    <xsl:value-of select="Cluster"/></a>
                            </xsl:when>
                            <xsl:otherwise>
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;Cluster={Cluster}')">
                                    <xsl:value-of select="Cluster"/></a>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?Cluster={Cluster}&amp;Sortby={$sortby}')">
                                    <xsl:value-of select="Cluster"/></a>
                            </xsl:when>
                            <xsl:when test="string-length($sortby)">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?Cluster={Cluster}&amp;Sortby={$sortby}&amp;Descending=1')">
                                    <xsl:value-of select="Cluster"/></a>
                            </xsl:when>
                            <xsl:otherwise>
                                    <a href="javascript:go('/WsWorkunits/WUQuery?Cluster={Cluster}')">
                                    <xsl:value-of select="Cluster"/></a>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
               <xsl:value-of select="Cluster"/>
            </xsl:otherwise>
         </xsl:choose>
         </td>
         <xsl:if test="not(string-length($archived))">
         <td>
               <!--xsl:value-of select="RoxieCluster"/-->
         <xsl:choose>
            <xsl:when test="string-length(RoxieCluster) and not(string-length($roxiecluster)) and not(string-length($archived))">
                <xsl:choose>
                    <xsl:when test="string-length($filters)">
                        <xsl:choose>
                            <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;RoxieCluster={RoxieCluster}&amp;Sortby={$sortby}')">
                                    <xsl:value-of select="RoxieCluster"/></a>
                            </xsl:when>
                            <xsl:when test="string-length($sortby)">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;RoxieCluster={RoxieCluster}&amp;Sortby={$sortby}&amp;Descending=1')">
                                    <xsl:value-of select="RoxieCluster"/></a>
                            </xsl:when>
                            <xsl:otherwise>
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;RoxieCluster={RoxieCluster}')">
                                    <xsl:value-of select="RoxieCluster"/></a>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?RoxieCluster={RoxieCluster}&amp;Sortby={$sortby}')">
                                    <xsl:value-of select="RoxieCluster"/></a>
                            </xsl:when>
                            <xsl:when test="string-length($sortby)">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?RoxieCluster={RoxieCluster}&amp;Sortby={$sortby}&amp;Descending=1')">
                                    <xsl:value-of select="RoxieCluster"/></a>
                            </xsl:when>
                            <xsl:otherwise>
                                    <a href="javascript:go('/WsWorkunits/WUQuery?RoxieCluster={RoxieCluster}')">
                                    <xsl:value-of select="RoxieCluster"/></a>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
               <xsl:value-of select="RoxieCluster"/>
            </xsl:otherwise>
         </xsl:choose>
         </td>
         </xsl:if>
         <td>
         <xsl:choose>
           <xsl:when test="number(IsPausing)">
             Pausing
           </xsl:when>
            <xsl:when test="string-length(State) and not(string-length($state))">
                <xsl:choose>
                    <xsl:when test="string-length($filters)">
                        <xsl:choose>
                            <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;State={State}&amp;Sortby={$sortby}')">
                                    <xsl:choose>
                                        <xsl:when test="State='scheduled' and number(Aborting)">
                                            scheduled(aborting)
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:value-of select="State"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    </a>
                            </xsl:when>
                            <xsl:when test="string-length($sortby)">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;State={State}&amp;Sortby={$sortby}&amp;Descending=1')">
                                    <xsl:choose>
                                        <xsl:when test="State='scheduled' and number(Aborting)">
                                            scheduled(aborting)
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:value-of select="State"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    </a>
                            </xsl:when>
                            <xsl:otherwise>
                                    <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;State={State}')">
                                    <xsl:choose>
                                        <xsl:when test="State='scheduled' and number(Aborting)">
                                            scheduled(aborting)
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:value-of select="State"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    </a>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?State={State}&amp;Sortby={$sortby}')">
                                    <xsl:choose>
                                        <xsl:when test="State='scheduled' and number(Aborting)">
                                            scheduled(aborting)
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:value-of select="State"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    </a>
                            </xsl:when>
                            <xsl:when test="string-length($sortby)">
                                    <a href="javascript:go('/WsWorkunits/WUQuery?State={State}&amp;Sortby={$sortby}&amp;Descending=1')">
                                    <xsl:choose>
                                        <xsl:when test="State='scheduled' and number(Aborting)">
                                            scheduled(aborting)
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:value-of select="State"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    </a>
                            </xsl:when>
                            <xsl:otherwise>
                                    <a href="javascript:go('/WsWorkunits/WUQuery?State={State}')">
                                    <xsl:choose>
                                        <xsl:when test="State='scheduled' and number(Aborting)">
                                            scheduled(aborting)
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:value-of select="State"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    </a>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                <xsl:choose>
                    <xsl:when test="State='scheduled' and number(Aborting)">
                        scheduled(aborting)
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="State"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:otherwise>
         </xsl:choose>
         </td>
        <xsl:if test="not(string-length($archived))">
        <td>
          <xsl:choose>
            <xsl:when test="string-length(TotalThorTime) and not(string-length($totalThorTime)) and not(string-length($archived))">
              <xsl:choose>
                <xsl:when test="string-length($filters)">
                  <xsl:choose>
                    <xsl:when test="string-length($sortby) and $descending &lt; 1">
                      <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;TotalThorTime={TotalThorTime}&amp;Sortby={$sortby}')">
                        <xsl:value-of select="TotalThorTime"/>
                      </a>
                    </xsl:when>
                    <xsl:when test="string-length($sortby)">
                      <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;TotalThorTime={TotalThorTime}&amp;Sortby={$sortby}&amp;Descending=1')">
                        <xsl:value-of select="TotalThorTime"/>
                      </a>
                    </xsl:when>
                    <xsl:otherwise>
                      <a href="javascript:go('/WsWorkunits/WUQuery?{$filters}&amp;TotalThorTime={TotalThorTime}')">
                        <xsl:value-of select="TotalThorTime"/>
                      </a>
                    </xsl:otherwise>
                  </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:choose>
                    <xsl:when test="string-length($sortby) and $descending &lt; 1">
                      <a href="javascript:go('/WsWorkunits/WUQuery?TotalThorTime={TotalThorTime}&amp;Sortby={$sortby}')">
                        <xsl:value-of select="TotalThorTime"/>
                      </a>
                    </xsl:when>
                    <xsl:when test="string-length($sortby)">
                      <a href="javascript:go('/WsWorkunits/WUQuery?TotalThorTime={TotalThorTime}&amp;Sortby={$sortby}&amp;Descending=1')">
                        <xsl:value-of select="TotalThorTime"/>
                      </a>
                    </xsl:when>
                    <xsl:otherwise>
                      <a href="javascript:go('/WsWorkunits/WUQuery?TotalThorTime={TotalThorTime}')">
                        <xsl:value-of select="TotalThorTime"/>
                      </a>
                    </xsl:otherwise>
                  </xsl:choose>
                </xsl:otherwise>
              </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="TotalThorTime"/>
            </xsl:otherwise>
          </xsl:choose>
        </td>
        </xsl:if>
      </tr>
   </xsl:template>
   <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
