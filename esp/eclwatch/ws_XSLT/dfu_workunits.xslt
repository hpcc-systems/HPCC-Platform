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
    <xsl:variable name="archived" select="/GetDFUWorkunitsResponse/Type"/>
    <xsl:variable name="owner" select="/GetDFUWorkunitsResponse/Owner"/>
    <xsl:variable name="cluster" select="/GetDFUWorkunitsResponse/Cluster"/>
    <xsl:variable name="statereq" select="/GetDFUWorkunitsResponse/StateReq"/>
    <xsl:variable name="pagesize" select="/GetDFUWorkunitsResponse/PageSize"/>
    <xsl:variable name="firstpage" select="/GetDFUWorkunitsResponse/First"/>
    <xsl:variable name="prevpage" select="/GetDFUWorkunitsResponse/PrevPage"/>
    <xsl:variable name="nextpage" select="/GetDFUWorkunitsResponse/NextPage"/>
    <xsl:variable name="lastpage" select="/GetDFUWorkunitsResponse/LastPage"/>
    <xsl:variable name="sortby" select="/GetDFUWorkunitsResponse/Sortby"/>
    <xsl:variable name="descending" select="/GetDFUWorkunitsResponse/Descending"/>
    <xsl:variable name="num" select="/GetDFUWorkunitsResponse/NumWUs"/>
    <xsl:variable name="filters" select="/GetDFUWorkunitsResponse/Filters"/>
    <xsl:variable name="basicquery" select="/GetDFUWorkunitsResponse/BasicQuery"/>
    <xsl:template match="/GetDFUWorkunitsResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Workunits</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <script type="text/javascript" src="files_/scripts/sortabletable.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <script language="JavaScript1.2">
                    var currentFilters='<xsl:value-of select="$filters"/>';
                    var archived='<xsl:value-of select="$archived"/>';
                    var basicQuery='<xsl:value-of select="$basicquery"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                            var protectedChecked = 0;
                            var unprotectedChecked = 0;

                            function checkSelected(o)
                            {
                                if (o.tagName=='INPUT' && o.id!='All'  && o.id!='All1' && o.checked)
                                {
                                    if (document.getElementById(o.value))
                                    {
                                        protectedChecked++;
                                    }
                                    else
                                    {
                                        unprotectedChecked++;
                                    }
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
                                protectedChecked = 0;
                                unprotectedChecked = 0;
                                if (archived != 'archived workunits')
                                {
                                    document.getElementById("deleteBtn").disabled = true;
                                    document.getElementById("protectBtn").disabled = true;
                                    document.getElementById("unprotectBtn").disabled = true;
                                    document.getElementById("changeStateBtn").disabled = true;

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
                                        document.getElementById("deleteBtn").disabled = false;
                                        document.getElementById("protectBtn").disabled = false;
                                    }
                                }
                                else
                                {
                                    document.getElementById("restoreBtn").disabled = true;

                                    checkSelected(document.forms['listitems']);
                                    if (protectedChecked + unprotectedChecked > 0)
                                    {
                                        document.getElementById("restoreBtn").disabled = false;
                                    }
                                }
                            }       
                            function headerClicked(headername, descending)
                            {
                                var url;
                                if (currentFilters.length > 0)
                                    url='/FileSpray/GetDFUWorkunits?'+currentFilters+'&Sortby='+headername+'&Descending='+descending;
                                else
                                    url='/FileSpray/GetDFUWorkunits?Sortby='+headername+'&Descending='+descending;
                                
                                //alert(url);
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
                                    document.location.href = '/FileSpray/GetDFUWorkunits?'+ basicQuery + '&PageStartFrom='+startFrom+'&PageSize='+size;
                                else
                                    document.location.href = '/FileSpray/GetDFUWorkunits?PageStartFrom='+startFrom+'&PageSize='+size;

                                return false;
                            }             
                            function selectAll0(chkbox)
                            {
                                selectAll(chkbox.checked);
                                var topchkbox = document.getElementById("All1");
                                if (topchkbox != chkbox && topchkbox.checked != chkbox.checked)
                                    topchkbox.checked = chkbox.checked;

                                var bottomchkbox = document.getElementById("All");
                                if (bottomchkbox != chkbox && bottomchkbox.checked != chkbox.checked)
                                    bottomchkbox.checked = chkbox.checked;
                            }
              function onLoad()
                            {
                                initSelection('resultsTable');
                            }   
                            var sortableTable = null;
                ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <h3>DFU Workunits</h3>
        <xsl:choose>
          <xsl:when test="not(results/DFUWorkunit[1])">
            No DFU workunits found.
          </xsl:when>
          <xsl:otherwise>
            <form id="newpageform" onsubmit="return getNewPage()" method="post">
              <tr>
                <th>
                  <xsl:if test="not(string-length($archived))">Total <b><xsl:value-of select="/GetDFUWorkunitsResponse/NumWUs"/></b> workunits.</xsl:if>
                  Current page starts from:
                </th>
                <td>
                  <input type="text" id="PageStartFrom" name="PageStartFrom" value="{/GetDFUWorkunitsResponse/PageStartFrom}" size="10"/>
                </td>
                <th> to:</th>
                <td>
                  <input type="text" id="PageEndAt" name="PageEndAt" value="{/GetDFUWorkunitsResponse/PageEndAt}" size="10"/>
                </td>
                <td>
                  <input type="submit" class="sbutton" value="Submit"/>
                </td>
              </tr>
            </form>
            <form id="listitems" action="/FileSpray/DFUWorkunitsAction" method="post">
              <table class="sort-table" id="resultsTable">
                <colgroup>
                  <col align="left"/>
                  <col/>
                  <col/>
                  <col/>
                  <col/>
                  <col/>
                  <col/>
                  <col/>
                  <xsl:if test="$archived != 'archived workunits'">
                    <col class="number"/>
                  </xsl:if>
                </colgroup>
                <thead>
                  <tr>
                    <th>
                      <xsl:if test="results/DFUWorkunit[2]">
                        <xsl:attribute name="id">selectAll1</xsl:attribute>
                        <input type="checkbox" id="All1" value="0" title="Select or deselect all workunits" onclick="selectAll0(this)"/>
                      </xsl:if>
                    </th>
                    <xsl:choose>
                      <xsl:when test="$archived != 'archived workunits'">
                        <xsl:choose>
                          <xsl:when test="$sortby='Protected' and $descending &lt; 1">
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Protected', 1)">
                              <img src="/esp/files_/img/locked.gif" width="11" height="13"></img><img src="/esp/files_/img/upsimple.png" width="10" height="10"></img>
                            </th>
                          </xsl:when>
                          <xsl:when test="$sortby='Protected'">
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Protected', 0)">
                              <img src="/esp/files_/img/locked.gif" width="11" height="13"></img><img src="/esp/files_/img/downsimple.png" width="10" height="10"></img>
                            </th>
                          </xsl:when>
                          <xsl:otherwise>
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Protected', 0)">
                              <img src="/esp/files_/img/locked.gif" width="11" height="13"></img>
                            </th>
                          </xsl:otherwise>
                        </xsl:choose>
                        <xsl:choose>
                          <xsl:when test="$sortby='ID' and $descending &lt; 1">
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ID', 1)">ID<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                          </xsl:when>
                          <xsl:when test="$sortby='ID'">
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ID', 0)">ID<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                          </xsl:when>
                          <xsl:otherwise>
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('ID', 0)">ID</th>
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
                          <xsl:when test="$sortby='Type' and $descending &lt; 1">
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Type', 1)">Type<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                          </xsl:when>
                          <xsl:when test="$sortby='Type'">
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Type', 0)">Type<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                          </xsl:when>
                          <xsl:otherwise>
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Type', 0)">Type</th>
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
                          <xsl:when test="$sortby='PCTDone' and $descending &lt; 1">
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('PCTDone', 1)">% Done<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                          </xsl:when>
                          <xsl:when test="$sortby='PCTDone'">
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('PCTDone', 0)">% Done<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                          </xsl:when>
                          <xsl:otherwise>
                            <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('PCTDone', 0)">% Done</th>
                          </xsl:otherwise>
                        </xsl:choose>
                      </xsl:when>
                      <xsl:otherwise>
                        <th title="Protected">
                          <img src="/esp/files_/img/locked.gif" width="11" height="13"></img>
                        </th>
                        <th>ID</th><th>Job Name</th><th>Type</th><th>Owner</th><th>Cluster</th><th>State</th>
                      </xsl:otherwise>
                    </xsl:choose>
                  </tr>
                </thead>
                <tbody>
                  <xsl:apply-templates/>
                </tbody>
              </table>
              <xsl:if test="results/DFUWorkunit[2]">
                <table class="select-all">
                  <tr>
                    <th id="selectAll2">
                      <input  id="All" type="checkbox" title="Select or deselect all workunits" onclick="selectAll0(this)"/>
                    </th>
                    <th>Select All / None</th>
                  </tr>
                </table>
              </xsl:if>
              <xsl:choose>
                <xsl:when test="string-length($basicquery)">
                  <xsl:if test="$firstpage &lt; 1">
                    <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$basicquery}&amp;PageSize={$pagesize}')">First</a>
                    <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$basicquery}&amp;PageStartFrom={$prevpage}&amp;PageSize={$pagesize}')">Prev</a>
                  </xsl:if>
                  <xsl:if test="$nextpage &gt; -1">
                    <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$basicquery}&amp;PageStartFrom={$nextpage}&amp;PageSize={$pagesize}')">Next</a>
                    <xsl:if test="not(string-length($archived))">
                      <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$basicquery}&amp;PageStartFrom={$lastpage}&amp;PageSize={$pagesize}')">Last</a>
                    </xsl:if>
                  </xsl:if>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:if test="$firstpage &lt; 1">
                    <a href="javascript:go('/FileSpray/GetDFUWorkunits?PageSize={$pagesize}')">First</a>
                    <a href="javascript:go('/FileSpray/GetDFUWorkunits?PageStartFrom={$prevpage}&amp;PageSize={$pagesize}')">Prev</a>
                  </xsl:if>
                  <xsl:if test="$nextpage &gt; -1">
                    <a href="javascript:go('/FileSpray/GetDFUWorkunits?PageStartFrom={$nextpage}&amp;PageSize={$pagesize}')">Next</a>
                    <xsl:if test="not(string-length($archived))">
                      <a href="javascript:go('/FileSpray/GetDFUWorkunits?PageStartFrom={$lastpage}&amp;PageSize={$pagesize}')">Last</a>
                    </xsl:if>
                  </xsl:if>
                </xsl:otherwise>
              </xsl:choose>                         
              <br/>
              <xsl:choose>                          
                <xsl:when test="$archived != 'archived workunits'">
                  <input id="deleteBtn" type="submit" class="sbutton" name="Type" value="Delete" disabled="true" onclick="return confirm('Delete selected workunits?')"/>
                  <input id="protectBtn" type="submit" class="sbutton" name="Type" value="Protect" disabled="true"/>
                  <input id="unprotectBtn" type="submit" class="sbutton" name="Type" value="Unprotect" disabled="true"/>
                  <input id="changeStateBtn" type="submit" class="sbutton" name="Type" value="SetToFailed" disabled="true"/>
                </xsl:when>
                <xsl:otherwise>
                  <input id="restoreBtn" type="submit" class="sbutton" name="Type" value="Restore" disabled="true"/>
                </xsl:otherwise>
              </xsl:choose>                         
            </form>
          </xsl:otherwise>
        </xsl:choose>
      </body>
    </html>
  </xsl:template>
    <xsl:template match="DFUWorkunit">
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
                <input type="checkbox" name="wuids_i{position()}" value="{ID}" onclick="return clicked(this, event)"/>
            </td>
            <td>
                <xsl:if test="isProtected=1">
                    <img alt="protected" border="0" src="/esp/files_/img/locked.gif" width="11" height="11"/>
                    <input type="hidden" id="{ID}"/>
                </xsl:if>
            </td>
            <td>
                <a href="javascript:go('/esp/files/stub.htm?Widget=DFUWUDetailsWidget&amp;Wuid={ID}')">
                    <xsl:choose>
                        <xsl:when test="State=2 or State=3"><b><xsl:value-of select="ID"/></b></xsl:when>
                        <xsl:otherwise><xsl:value-of select="ID"/></xsl:otherwise>
                    </xsl:choose>
                </a>
            </td>
            <td>
                <xsl:value-of select="substring(concat(substring(JobName,1,40),'...'),1,string-length(JobName))"/>
            </td>
            <td>
                <xsl:choose>
                    <!-- translate commands 0-12 to strings in order of probability of occurrence for optimization -->
                    <xsl:when test="Command = 6">Spray</xsl:when><!--DFUcmd_import-->
                    <xsl:when test="Command = 7">Despray</xsl:when><!--DFUcmd_export-->
                    <xsl:when test="Command = 1">Copy</xsl:when><!--DFUcmd_copy-->
                    <xsl:when test="Command = 2">Delete</xsl:when><!--DFUcmd_remove-->
                    <xsl:when test="Command = 3">Move</xsl:when><!--DFUcmd_move-->
                    <xsl:when test="Command = 5">Replicate</xsl:when><!--DFUcmd_replicate-->
                    <xsl:when test="Command = 4">Rename</xsl:when><!--DFUcmd_rename-->
                    <xsl:when test="Command = 8">Add</xsl:when><!--DFUcmd_add-->
                    <xsl:when test="Command = 9">Transfer</xsl:when><!--DFUcmd_transfer-->
                    <xsl:when test="Command = 10">DKC</xsl:when><!--DFUcmd_dkc-->
                    <xsl:when test="Command = 11">Save Map</xsl:when><!--DFUcmd_savemap-->
                    <xsl:when test="Command = 12">Add Group</xsl:when><!--DFUcmd_addgroup-->
                    <xsl:when test="Command = 13">Server</xsl:when><!--DFUcmd_server-->
                    <xsl:when test="Command = 14">Monitor</xsl:when><!--DFUcmd_server-->
                    <xsl:when test="Command = 15">Copy Merge</xsl:when><!--DFUcmd_server-->
                    <xsl:when test="Command = 16">Supercopy</xsl:when><!--DFUcmd_server-->
                    <xsl:when test="Command = 0"></xsl:when><!--DFUcmd_none-->
                    <xsl:otherwise></xsl:otherwise>
                </xsl:choose>                                
            </td>
            <td>
            <xsl:choose>
                <xsl:when test="string-length(User) and not(string-length($owner))">
                    <xsl:choose>
                        <xsl:when test="string-length($filters)">
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$filters}&amp;Owner={User}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="User"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$filters}&amp;Owner={User}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="User"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$filters}&amp;Owner={User}')">
                                        <xsl:value-of select="User"/></a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?Owner={User}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="User"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?Owner={User}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="User"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?Owner={User}')">
                                        <xsl:value-of select="User"/></a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="User"/>
                </xsl:otherwise>
            </xsl:choose>
            </td>
            <td>
                <xsl:choose>
                    <xsl:when test="string-length(ClusterName) and not(string-length($cluster))">
                        <xsl:choose>
                            <xsl:when test="string-length($filters)">
                                <xsl:choose>
                                    <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                            <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$filters}&amp;Cluster={ClusterName}&amp;Sortby={$sortby}')">
                                            <xsl:value-of select="ClusterName"/></a>
                                    </xsl:when>
                                    <xsl:when test="string-length($sortby)">
                                            <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$filters}&amp;Cluster={ClusterName}&amp;Sortby={$sortby}&amp;Descending=1')">
                                            <xsl:value-of select="ClusterName"/></a>
                                    </xsl:when>
                                    <xsl:otherwise>
                                            <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$filters}&amp;Cluster={ClusterName}')">
                                            <xsl:value-of select="ClusterName"/></a>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:choose>
                                    <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                            <a href="javascript:go('/FileSpray/GetDFUWorkunits?Cluster={ClusterName}&amp;Sortby={$sortby}')">
                                            <xsl:value-of select="ClusterName"/></a>
                                    </xsl:when>
                                    <xsl:when test="string-length($sortby)">
                                            <a href="javascript:go('/FileSpray/GetDFUWorkunits?Cluster={ClusterName}&amp;Sortby={$sortby}&amp;Descending=1')">
                                            <xsl:value-of select="ClusterName"/></a>
                                    </xsl:when>
                                    <xsl:otherwise>
                                            <a href="javascript:go('/FileSpray/GetDFUWorkunits?Cluster={ClusterName}')">
                                            <xsl:value-of select="ClusterName"/></a>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="ClusterName"/>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
            <td>
            <xsl:variable name="state">
                <xsl:choose>
                    <xsl:when test="State=0">unknown</xsl:when>
                    <xsl:when test="State=1">scheduled</xsl:when>
                    <xsl:when test="State=2">queued</xsl:when>
                    <xsl:when test="State=3">started</xsl:when>
                    <xsl:when test="State=4">aborted</xsl:when>
                    <xsl:when test="State=5">failed</xsl:when>
                    <xsl:when test="State=6">finished</xsl:when>
                    <xsl:when test="State=7">monitoring</xsl:when>
          <xsl:when test="State=8">aborting</xsl:when>
        </xsl:choose>
            </xsl:variable>
            <xsl:choose>
                <xsl:when test="string-length(State) and not(string-length($statereq))">
                    <xsl:choose>
                        <xsl:when test="string-length($filters)">
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$filters}&amp;StateReq={$state}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="$state"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$filters}&amp;StateReq={$state}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="$state"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?{$filters}&amp;StateReq={$state}')">
                                        <xsl:value-of select="$state"/></a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?StateReq={$state}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="$state"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?StateReq={$state}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="$state"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:go('/FileSpray/GetDFUWorkunits?StateReq={$state}')">
                                        <xsl:value-of select="$state"/></a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="$state"/>
                </xsl:otherwise>
            </xsl:choose>
            </td>
            <xsl:if test="$archived != 'archived workunits'">
                <td>
                    <xsl:value-of select="PercentDone"/>
                </td>
            </xsl:if>
        </tr>
    </xsl:template>
    <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
