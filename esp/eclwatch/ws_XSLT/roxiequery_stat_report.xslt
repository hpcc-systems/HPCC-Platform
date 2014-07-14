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
    <xsl:variable name="lastpagefrom" select="/StatisticsReportResponse/LastPageFrom"/>
    <xsl:variable name="prevpagefrom" select="/StatisticsReportResponse/PrevPageFrom"/>
    <xsl:variable name="nextpagefrom" select="/StatisticsReportResponse/NextPageFrom"/>
    <xsl:variable name="sortby" select="/StatisticsReportResponse/Sortby"/>
    <xsl:variable name="descending" select="/StatisticsReportResponse/Descending"/>
    <xsl:variable name="basicquery" select="/StatisticsReportResponse/BasicQuery"/>
    <xsl:variable name="parametersforpaging" select="/StatisticsReportResponse/ParametersForPaging"/>
    <xsl:variable name="filters" select="/StatisticsReportResponse/Filters"/>

    <xsl:template match="/StatisticsReportResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Roxie Query</title>
        <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <script language="JavaScript1.2" src="/esp/files_/popup.js">
                    null
                </script>
                <script language="JavaScript1.2">
                    var basicQuery='<xsl:value-of select="$basicquery"/>';
                    var currentFilters='<xsl:value-of select="$filters"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        function getNewPage(type)
                        {
                            var startFrom = document.getElementById("PageStartFrom").value;
                            var pageEndAt = document.getElementById("PageEndAt").value;
                            if (!startFrom || startFrom < 1)
                                startFrom = 1;
                            if (!pageEndAt || pageEndAt < startFrom)
                                pageEndAt = 100;
                            var size = pageEndAt - startFrom + 1;

                            if (basicQuery.length > 0)
                                document.location.href = '/WsRoxieQuery/StatisticsReport?PageSize='+size+'&amp;'+basicQuery+'&amp;PageStartFrom='+startFrom;
                            else
                                document.location.href = '/WsRoxieQuery/StatisticsReport?PageStartFrom='+startFrom+'&amp;PageSize='+size;

                            return false;
                        }

                        function headerClicked(headername, descending)
                        {
                            document.location.href='/WsRoxieQuery/StatisticsReport?'+currentFilters+'&amp;Sortby='+headername+'&amp;Descending='+descending;
                        }                

                        function onLoad()
                        {
                            initSelection('resultsTable');
                        }                                 
                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <h3>Roxie Query Statistics Report: <xsl:value-of select="/StatisticsReportResponse/Description"/></h3>
                <xsl:choose>
                    <xsl:when test="(RoxieQueries/QueryStatisticsItem[1])">
                        <xsl:apply-templates select="RoxieQueries"/>
                    </xsl:when>
                    <xsl:when test="(RoxieQuerySummary/QueryStatisticsSummary[1])">
                        <xsl:apply-templates select="RoxieQuerySummary"/>
                    </xsl:when>
                    <xsl:when test="(RoxieExceptions/QueryStatisticsException[1])">
                        <xsl:apply-templates select="RoxieExceptions"/>
                    </xsl:when>
                    <xsl:otherwise>
                        No data found.
                    </xsl:otherwise>
                </xsl:choose>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="RoxieQuerySummary">
        <form id="newpageform" onsubmit="return getNewPage(0)" method="post">
            <tr><th>Total <b><xsl:value-of select="/StatisticsReportResponse/NumItems"/></b> items. Current page starts from:</th>
            <td><input type="text" id="PageStartFrom" name="PageStartFrom" value="{/StatisticsReportResponse/PageStartFrom}" size="10"/></td>
            <th> to:</th>
            <td><input type="text" id="PageEndAt" name="PageEndAt" value="{/StatisticsReportResponse/PageEndAt}" size="10"/></td>
            <td><input type="submit" class="sbutton" value="Submit"/></td></tr>
        </form>
        <table class="sort-table" id="resultsTable">
            <colgroup>
                <col width="100"/>
                <col width="100"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
            </colgroup>
            <thead>
            <tr class="grey">
               <xsl:choose>
                   <xsl:when test="$sortby='Time From' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time From', 1)">Time From<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Time From'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time From', 0)">Time From<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                    <xsl:when test="$sortby!=''">
                      <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time From', 0)">Time From</th>
                    </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time From', 1)">Time From</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Time To' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time To', 1)">Time To<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Time To'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time To', 0)">Time To<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                    <xsl:when test="$sortby!=''">
                      <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time To', 0)">Time To</th>
                    </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time To', 1)">Time To</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Query Count' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Query Count', 1)">Query Count<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Query Count'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Query Count', 0)">Query Count<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Query Count', 0)">Query Count</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Average Time' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Average Time', 1)">Average Time<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Average Time'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Average Time', 0)">Average Time<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Average Time', 0)">Average Time</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Max Time' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Max Time', 1)">Max Time<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Max Time'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Max Time', 0)">Max Time<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Max Time', 0)">Max Time</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Min Time' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Min Time', 1)">Min Time<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Min Time'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Min Time', 0)">Min Time<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Min Time', 0)">Min Time</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Average Memory' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Average Memory', 1)">Average Memory<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Average Memory'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Average Memory', 0)">Average Memory<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Average Memory', 0)">Average Memory</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Max Memory' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Max Memory', 1)">Max Memory<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Max Memory'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Max Memory', 0)">Max Memory<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Max Memory', 0)">Max Memory</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Min Memory' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Min Memory', 1)">Min Memory<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Min Memory'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Min Memory', 0)">Min Memory<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Min Memory', 0)">Min Memory</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Average Size' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Average Size', 1)">Average Result Size<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Average Size'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Average Size', 0)">Average Result Size<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Average Size', 0)">Average Result Size</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Max Size' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Max Size', 1)">Max Result Size<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Max Size'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Max Size', 0)">Max Result Size<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Max Size', 0)">Max Result Size</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Min Size' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Min Size', 1)">Min Result Size<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Min Size'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Min Size', 0)">Min Result Size<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Min Size', 0)">Min Result Size</th>
                   </xsl:otherwise>
               </xsl:choose>
            </tr>
            </thead>
            <tbody>
            <xsl:apply-templates select="QueryStatisticsSummary"/>
         </tbody>
        </table>
        <table>
            <tr>
                <xsl:if test="$prevpagefrom &gt; 0">
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}')">First</a></td>
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}&amp;PageStartFrom={$prevpagefrom}')">Prev</a></td>
                </xsl:if>
                <xsl:if test="$nextpagefrom &gt; 0">
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}&amp;PageStartFrom={$nextpagefrom}')">Next</a></td>
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}&amp;PageStartFrom={$lastpagefrom}')">Last</a></td>
                </xsl:if>
            </tr>
        </table>
    </xsl:template>
    
    <xsl:template match="QueryStatisticsSummary">
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
            <td align="left">
                <xsl:value-of select="StartTime"/>
            </td>
            <td align="left">
                <xsl:value-of select="EndTime"/>
            </td>
            <td>
                <xsl:value-of select="QueryCount"/>
            </td>
            <td>
                <xsl:value-of select="TimeTakenAverage"/>
            </td>
            <td>
                <xsl:value-of select="TimeTakenMax"/>
            </td>
            <td>
                <xsl:value-of select="TimeTakenMin"/>
            </td>
            <td>
                <xsl:value-of select="MemoryAverage"/>
            </td>
            <td>
                <xsl:value-of select="MemoryMax"/>
            </td>
            <td>
                <xsl:value-of select="MemoryMin"/>
            </td>
            <td>
                <xsl:value-of select="ResultSizeMax"/>
            </td>
            <td>
                <xsl:value-of select="ResultSizeAverage"/>
            </td>
            <td>
                <xsl:value-of select="ResultSizeMin"/>
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="RoxieQueries">
        <form id="newpageform" onsubmit="return getNewPage(0)" method="post">
            <tr><th>Total <b><xsl:value-of select="/StatisticsReportResponse/NumItems"/></b> items. Current page starts from:</th>
            <td><input type="text" id="PageStartFrom" name="PageStartFrom" value="{/StatisticsReportResponse/PageStartFrom}" size="10"/></td>
            <th> to:</th>
            <td><input type="text" id="PageEndAt" name="PageEndAt" value="{/StatisticsReportResponse/PageEndAt}" size="10"/></td>
            <td><input type="submit" class="sbutton" value="Submit"/></td></tr>
        </form>
        <table class="sort-table" id="resultsTable">
            <colgroup>
                <col width="100"/>
                <col width="100"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
                <col width="100" class="number"/>
            </colgroup>
            <thead>
            <tr class="grey">
               <xsl:choose>
                   <xsl:when test="$sortby='Receive Time' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Receive Time', 1)">Receive Time<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Receive Time'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Receive Time', 0)">Receive Time<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                    <xsl:when test="$sortby!=''">
                      <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Receive Time', 0)">Receive Time</th>
                    </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Receive Time', 1)">Receive Time</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Query Name' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Query Name', 1)">Query Name<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Query Name'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Query Name', 0)">Query Name<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Query Name', 0)">Query Name</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Time Taken' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time Taken', 1)">Time Taken<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Time Taken'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time Taken', 0)">Time Taken<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Time Taken', 0)">Time Taken</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Memory' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Memory', 1)">Memory<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Memory'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Memory', 0)">Memory<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Memory', 0)">Memory</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Priority' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Priority', 1)">Priority<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Priority'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Priority', 0)">Priority<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Priority', 0)">Priority</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Result Size' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Result Size', 1)">Result Size<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Result Size'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Result Size', 0)">Result Size<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Result Size', 0)">Result Size</th>
                   </xsl:otherwise>
               </xsl:choose>
            </tr>
            </thead>
            <tbody>
            <xsl:apply-templates select="QueryStatisticsItem"/>
         </tbody>
        </table>
        <table>
            <tr>
                <xsl:if test="$prevpagefrom &gt; 0">
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}')">First</a></td>
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}&amp;PageStartFrom={$prevpagefrom}')">Prev</a></td>
                </xsl:if>
                <xsl:if test="$nextpagefrom &gt; 0">
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}&amp;PageStartFrom={$nextpagefrom}')">Next</a></td>
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}&amp;PageStartFrom={$lastpagefrom}')">Last</a></td>
                </xsl:if>
            </tr>
        </table>
    </xsl:template>
    
    <xsl:template match="QueryStatisticsItem">
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
            <td align="left">
                <a href="javascript:go('/WsRoxieQuery/StatisticsReportQueryDetail?ID={ID}')">
                    <xsl:value-of select="ReceiveTime"/>
                </a>
            </td>
            <td align="left">
                <xsl:value-of select="QueryName"/>
            </td>
            <td>
                <xsl:value-of select="TimeTaken"/>
            </td>
            <td>
                <xsl:value-of select="Memory"/>
            </td>
            <td>
                <xsl:value-of select="Priority"/>
            </td>
            <td>
                <xsl:value-of select="ResultSize"/>
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="RoxieExceptions">
        <form id="newpageform" onsubmit="return getNewPage(1)" method="post">
            <tr><th>Total <b><xsl:value-of select="/StatisticsReportResponse/NumItems"/></b> files. Current page starts from:</th>
            <td><input type="text" id="PageStartFrom" name="PageStartFrom" value="{/StatisticsReportResponse/PageStartFrom}" size="10"/></td>
            <th> to:</th>
            <td><input type="text" id="PageEndAt" name="PageEndAt" value="{/StatisticsReportResponse/PageEndAt}" size="10"/></td>
            <td><input type="submit" class="sbutton" value="Submit"/></td></tr>
        </form>
        <table class="sort-table" id="resultsTable">
            <colgroup>
                <col width="100"/>
                <col width="100"/>
                <col width="100"/>
                <col width="100" class="number"/>
            </colgroup>
            <thead>
            <tr class="grey">
               <xsl:choose>
                   <xsl:when test="$sortby='Receive Time' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Receive Time', 1)">Receive Time<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Receive Time'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Receive Time', 0)">Receive Time<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                    <xsl:when test="$sortby!=''">
                      <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Receive Time', 0)">Receive Time</th>
                    </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Receive Time', 1)">Receive Time</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Roxie IP' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Roxie IP', 1)">Roxie IP<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Roxie IP'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Roxie IP', 0)">Roxie IP<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Roxie IP', 0)">Roxie IP</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Error Class' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Error Class', 1)">Error Class<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Error Class'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Error Class', 0)">Error Class<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Error Class', 0)">Error Class</th>
                   </xsl:otherwise>
               </xsl:choose>
               <xsl:choose>
                   <xsl:when test="$sortby='Error Code' and $descending &lt; 1">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Error Code', 1)">Error Code<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:when test="$sortby='Error Code'">
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Error Code', 0)">Error Code<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                   </xsl:when>
                   <xsl:otherwise>
                      <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Error Code', 0)">Error Code</th>
                   </xsl:otherwise>
               </xsl:choose>
            </tr>
            </thead>
            <tbody>
            <xsl:apply-templates select="QueryStatisticsException"/>
         </tbody>
        </table>
        <table>
            <tr>
                <xsl:if test="$prevpagefrom &gt; 0">
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}')">First</a></td>
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}&amp;PageStartFrom={$prevpagefrom}')">Prev</a></td>
                </xsl:if>
                <xsl:if test="$nextpagefrom &gt; 0">
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}&amp;PageStartFrom={$nextpagefrom}')">Next</a></td>
                    <td><a href="javascript:go('/WsRoxieQuery/StatisticsReport?{$parametersforpaging}&amp;PageStartFrom={$lastpagefrom}')">Last</a></td>
                </xsl:if>
            </tr>
        </table>
    </xsl:template>
    
    <xsl:template match="QueryStatisticsException">
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
            <td align="left">
                <a href="javascript:go('/WsRoxieQuery/StatisticsReportExceptionDetail?ID={ID}')">
                    <xsl:value-of select="ReceiveTime"/>
                </a>
            </td>
            <td>
                <xsl:value-of select="RoxieIP"/>
            </td>
            <td align="left">
                <xsl:value-of select="ErrorClass"/>
            </td>
            <td>
                <xsl:value-of select="ErrorCode"/>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
