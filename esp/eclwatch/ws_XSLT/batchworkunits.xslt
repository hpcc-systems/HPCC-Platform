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
    <xsl:variable name="owner" select="/BatchWUQueryResponse/Owner"/>
    <xsl:variable name="loginid" select="/BatchWUQueryResponse/LoginID"/>
   <xsl:variable name="customername" select="/BatchWUQueryResponse/CustomerName"/>
   <xsl:variable name="priority" select="/BatchWUQueryResponse/Priority"/>
   <xsl:variable name="timeuploaded" select="/BatchWUQueryResponse/TimeUploaded"/>
   <xsl:variable name="status" select="/BatchWUQueryResponse/Status"/>
   <xsl:variable name="pagesize" select="/BatchWUQueryResponse/PageSize"/>
   <xsl:variable name="pagestartfrom" select="/BatchWUQueryResponse/PageStartFrom"/>
   <xsl:variable name="firstpage" select="/BatchWUQueryResponse/First"/>
   <xsl:variable name="prevpage" select="/BatchWUQueryResponse/PrevPage"/>
   <xsl:variable name="nextpage" select="/BatchWUQueryResponse/NextPage"/>
   <xsl:variable name="lastpage" select="/BatchWUQueryResponse/LastPage"/>
   <xsl:variable name="num" select="/BatchWUQueryResponse/NumWUs"/>
   <xsl:variable name="sortby" select="/BatchWUQueryResponse/Sortby"/>
   <xsl:variable name="descending" select="/BatchWUQueryResponse/Descending"/>
   <xsl:variable name="filters" select="/BatchWUQueryResponse/Filters"/>
   <xsl:variable name="basicquery" select="/BatchWUQueryResponse/BasicQuery"/>
   <xsl:variable name="columncount" select="/BatchWUQueryResponse/ColumnCount"/>
   <xsl:variable name="autorefreshtimer" select="/BatchWUQueryResponse/AutoRefreshTimer"/>
 
   <xsl:template match="BatchWUQueryResponse">
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
        <style type="text/css">
                    .menu1 {
                        MARGIN:         0px 5px 0px 0px; WIDTH: 12px; HEIGHT: 14px
                    }
                    .workunits-table {
                        background:     Window;
                        border:         0;
                        border-bottom:  #777 2px solid;
                        color:          WindowText;
                        font:           10pt arial, helvetica, sans-serif;
                        padding:        2px 5px 2px 5px;
                        text-align:     center;
                    }
                    .workunits-table thead {
                        background:     lightgrey;
                    };
                    .workunits-table th {
                        border:         #777 1px solid;
                        cursor:         default;
                        padding:        2px 5px 2px 5px;
                    }
                    .workunits-table td {
                        padding:        2px 5px 2px 5px;
                        border:         lightgrey 1px solid;
                    }
                </style>
                <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <script type="text/javascript" src="files_/scripts/tooltip.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <script language="JavaScript1.2">
                    var currentFilters='<xsl:value-of select="$filters"/>';
                    var basicQuery='<xsl:value-of select="$basicquery"/>';
                    var autoRefreshTimer = '<xsl:value-of select="$autorefreshtimer"/>'; //minute
                    var pageSize='<xsl:value-of select="$pagesize"/>';
                    var pageStartFrom='<xsl:value-of select="$pagestartfrom"/>';
                    var columnCount='<xsl:value-of select="$columncount"/>';
                    var showcolumns = '';
            
                    var reloadTimer = null;
                    var reloadTimeout = 0;

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

                        function checkSelected0(o)
                        {
                            if (o.tagName=='INPUT' && o.id!='All2'  && o.id!='All1'  && o.checked)
                            {
                                unprotectedChecked++;
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
                            if (document.getElementById("killJobBtn"))
                            document.getElementById("killJobBtn").disabled = true;

                            unprotectedChecked = 0;
                            checkSelected0(document.forms['listitems']);

                            if (unprotectedChecked > 0)
                            {
                                document.getElementById("killJobBtn").disabled = false;
                            }
                        }   
                                  
                        function headerClicked(headername, descending)
                        {
                            var url='/WsBatchWorkunits/BatchWUQuery?';
                            if (currentFilters.length > 0)
                                url=url+currentFilters+'&Sortby='+headername+'&Descending='+descending;
                            else
                                url=url+'Sortby='+headername+'&Descending='+descending;

                            url=url+'&ListColumns='+showcolumns;

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

                            var url='/WsBatchWorkunits/BatchWUQuery?';
                            if (basicQuery.length > 0)
                                url=url+ basicQuery + '&amp;PageStartFrom='+startFrom+'&amp;PageSize='+size;
                            else
                                url=url+'PageStartFrom='+startFrom+'&amp;PageSize='+size;

                            url=url+'&amp;ListColumns='+showcolumns;

                            document.location.href=url;
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

                        function getElementByTagName(node, tag)
                        {
                            for (var child = node.firstChild; child; child=child.nextSibling)
                                if (child.tagName == tag)
                                    return child;
                            return null;
                        }

                        function menuHandler(cmd, skipCheckBoxUpdate)
                        { 
                            var index = cmd.substring(5); //skip past "_col_"
                            var table     = document.forms['listitems'].all['resultsTable'];
                            if (table != NaN)
                            {
                                var colGroup  = getElementByTagName(table, 'COLGROUP');
                                var col = colGroup.children[index];
                                var show = col.style && col.style.display && col.style.display == 'none' ; //show if hidden at present
                                col.style.display = show ? 'block' : 'none';
                            }

                            showcolumns = '';
                            var table1     = document.forms['listColumns'].all['listColumnTable'];
                            if (table1 != NaN)
                            {
                                for (i= 0; i< columnCount; i++)
                                {
                                    var id = 'ShowColumns_i' + (i); //no checkbox exists for column 1 
                                    var checkbox = table1.all[id];                 
                                    if (checkbox.checked)
                                        showcolumns = showcolumns + '/' + i;
                                }
                                //alert(showcolumns);
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

                        function reloadPage() 
                        {
                            var pageFrom = pageStartFrom -1;
                            var url='/WsBatchWorkunits/BatchWUQuery?';
                            if (basicQuery.length > 0)
                                url=url+ basicQuery + '&amp;PageStartFrom='+pageFrom+'&amp;PageSize='+pageSize;
                            else
                                url=url+'PageStartFrom='+pageFrom+'&amp;PageSize='+pageSize;

                            url=url+'&amp;ListColumns='+showcolumns;

                            var checkbox = document.getElementById("AutoRefresh");
                            if (checkbox != NaN && checkbox.checked)
                            {
                                url=url+'&amp;AutoRefreshTimer='+reloadTimeout;
                            }

                            document.location.href=url;
                        }

                        function onLoad()
                        {
                            //initSelection('resultsTable');

                            showcolumns = '';
                            var form0 = document.forms['listitems'];
                            var form1 = document.forms['listColumns'];
                            if (form0 != NaN && form1 != NaN)
                            {
                                var table     = form0.all['resultsTable'];
                                var table1     = form1.all['listColumnTable'];
                                if (table != NaN && table1 != NaN)
                                {
                                    var colGroup  = getElementByTagName(table, 'COLGROUP');
                                    for (i= 0; i< columnCount; i++)
                                    {
                                        var id = 'ShowColumns_i' + (i); //no checkbox exists for column 1 
                                        var checkbox = table1.all[id];                 
                                        if (checkbox.checked)
                                            showcolumns = showcolumns + '/' + i;
                                        else
                                        {
                                            var col = colGroup.children[i];
                                            col.style.display = 'none';
                                        }
                                    }
                                }
                            }

                            if (autoRefreshTimer > 0)            
                            {
                                setReloadTimeout(autoRefreshTimer); // Pass a default value
                                var checkbox = document.getElementById("AutoRefresh");
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

                        function goplus(url)
                        {
                            url=url+'&amp;ListColumns='+showcolumns;
                            document.location.href=url;
                        }                     
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam" onload="nof5();onLoad()">
                <form id="refreshform" method="post">
                    <table id="refreshTable">
                        <colgroup>
                            <col width="250" align="left"/>
                            <col width="120" align="left"/>
                            <col width="100" align="left"/>
                            <col width="120" align="right"/>
                        </colgroup>
                        <tr>
                            <td><b>Batch Workunits</b>
                            </td>
                            <td>
                                <input type="checkbox"  id="AutoRefresh" value="Auto Refresh" onclick="setAutoRefresh(this);"/> Auto Refresh
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
                </form>

                <form id="newpageform" onsubmit="return getNewPage()" method="post">
                    <tr><th>
                        Total <b><xsl:value-of select="/BatchWUQueryResponse/NumWUs"/></b> workunits.
                        Current page starts from:</th>
                    <td><input type="text" id="PageStartFrom" name="PageStartFrom" value="{/BatchWUQueryResponse/PageStartFrom}" size="10"/></td>
                    <th> to:</th>
                    <td><input type="text" id="PageEndAt" name="PageEndAt" value="{/BatchWUQueryResponse/PageEndAt}" size="10"/></td>
                    <td><input type="submit" class="sbutton" value="Submit"/></td></tr>
                </form>
            
                <xsl:choose>
                    <xsl:when test="BatchWorkunits/BatchWorkunit[1]">
                        <form id="listitems" action="/WsBatchWorkunits/WUAction/?{$filters}&amp;PageSize={$pagesize}&amp;CurrentPage={$pagestartfrom - 1}&amp;Sortby={$sortby}&amp;Descending={$descending}" method="post">
                             <table class="workunits-table" id="resultsTable" cellspacing="0">
                                <colgroup>
                                    <col width="150" align="left"/>
                                    <col width="100"/>
                                    <col width="100"/>
                                    <col width="100"/>
                                    <col width="200"/>
                                    <col width="60"/>
                                    <col width="100"/>
                                    <!--col width="100"/-->
                                    <col width="60"/>
                                </colgroup>
                                <thead>
                                <tr>
                                    <xsl:choose>
                                        <xsl:when test="$sortby='WUID' and $descending &lt; 1">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 1)">WUID<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:when test="$sortby='WUID'">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 0)">WUID<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:otherwise>
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('WUID', 0)">WUID</th>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:choose>
                                        <xsl:when test="$sortby='Owner' and $descending &lt; 1">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Owner', 1)">SDSUserID<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:when test="$sortby='Owner'">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Owner', 0)">SDSUserID<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:otherwise>
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Owner', 0)">SDSUserID</th>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:choose>
                                        <xsl:when test="$sortby='LoginID' and $descending &lt; 1">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('LoginID', 1)">LoginID<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:when test="$sortby='LoginID'">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('LoginID', 0)">LoginID<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:otherwise>
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('LoginID', 0)">LoginID</th>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:choose>
                                        <xsl:when test="$sortby='CustomerName' and $descending &lt; 1">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('CustomerName', 1)">CustomerName<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:when test="$sortby='CustomerName'">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('CustomerName', 0)">CustomerName<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:otherwise>
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('CustomerName', 0)">CustomerName</th>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:choose>
                                        <xsl:when test="$sortby='Priority' and $descending &lt; 1">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Priority', 1)">Priority<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:when test="$sortby='Priority'">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Priority', 0)">Priority<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:otherwise>
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Priority', 0)">Priority</th>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:choose>
                                        <xsl:when test="$sortby='InputRecCount' and $descending &lt; 1">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('InputRecCount', 1)">InputRecordCount<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:when test="$sortby='InputRecCount'">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('InputRecCount', 0)">InputRecordCount<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:otherwise>
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('InputRecCount', 0)">InputRecordCount</th>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <!--xsl:choose>
                                        <xsl:when test="$sortby='TimeUploaded' and $descending &lt; 1">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('TimeUploaded', 1)">TimeUploaded<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:when test="$sortby='TimeUploaded'">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('TimeUploaded', 0)">TimeUploaded<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:otherwise>
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('TimeUploaded', 0)">TimeUploaded</th>
                                        </xsl:otherwise>
                                    </xsl:choose-->
                                    <xsl:choose>
                                        <xsl:when test="$sortby='TimeCompleted' and $descending &lt; 1">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('TimeCompleted', 1)">TimeCompleted<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:when test="$sortby='TimeCompleted'">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('TimeCompleted', 0)">TimeCompleted<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:otherwise>
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('TimeCompleted', 0)">TimeCompleted</th>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:choose>
                                        <xsl:when test="$sortby='Status' and $descending &lt; 1">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Status', 1)">Status<img src="/esp/files_/img/upsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:when test="$sortby='Status'">
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Status', 0)">Status<img src="/esp/files_/img/downsimple.png" width="10" height="10"></img></th>
                                        </xsl:when>
                                        <xsl:otherwise>
                                          <th style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Status', 0)">Status</th>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </tr>
                                </thead>
                                <tbody>
                                    <xsl:apply-templates/>
                                </tbody>
                            </table>
         
                            <!--xsl:if test="BatchWorkunits/BatchWorkunit[2]">
                                <table class="select-all">
                                <tr>
                                  <th id="selectAll2">
                                     <input type="checkbox"  id="All2" title="Select or deselect all workunits" onclick="selectAll0(this)"/>
                                  </th>
                                  <th>Select All / None</th>
                                </tr>
                                </table>
                            </xsl:if-->

                            <xsl:choose>
                                <xsl:when test="string-length($basicquery)">
                                    <xsl:if test="$firstpage &lt; 1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$basicquery}&amp;PageSize={$pagesize}')">First</a>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$basicquery}&amp;PageStartFrom={$prevpage}&amp;PageSize={$pagesize}')">Prev</a>
                                    </xsl:if>
                                    <xsl:if test="$nextpage &gt; -1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$basicquery}&amp;PageStartFrom={$nextpage}&amp;PageSize={$pagesize}')">Next</a>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$basicquery}&amp;PageStartFrom={$lastpage}&amp;PageSize={$pagesize}')">Last</a>
                                    </xsl:if>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:if test="$firstpage &lt; 1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?PageSize={$pagesize}')">First</a>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?PageStartFrom={$prevpage}&amp;PageSize={$pagesize}')">Prev</a>
                                    </xsl:if>
                                    <xsl:if test="$nextpage &gt; -1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?PageStartFrom={$nextpage}&amp;PageSize={$pagesize}')">Next</a>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?PageStartFrom={$lastpage}&amp;PageSize={$pagesize}')">Last</a>
                                    </xsl:if>
                                </xsl:otherwise>
                            </xsl:choose>

                            <!--table style="margin:10 0 0 0" id="btnTable" name="btnTable">
                                <tr>
                                    <td>
                                      <input type="submit" class="sbutton" name="ActionType" id="killJobBtn" value="KillJob" disabled="true"/>
                                    </td>
                                    <td width="10"/>
                                </tr>
                            </table-->
                        </form>

                        <form id = "listColumns">
                            <table id="listColumnTable" width="100%">
                                <tr/>
                                <tr>
                                    <td>
                                        <b>View Columns:</b>
                                    </td>
                                </tr>
                                <tr/>
                                <tr>
                                    <xsl:apply-templates select="ListColumns/ListColumn" mode="createCheckboxes"/>
                                </tr>
                            </table>
                        </form>
                    </xsl:when>
                    <xsl:otherwise>
                        No workunits found.<br/>
                    </xsl:otherwise>
                </xsl:choose>
            </body>
        </html>
   </xsl:template>

    <xsl:template match="BatchWorkunit">
        <tr BGCOLOR="#AAEFAA">
            <td>
                <a href="javascript:goplus('/WsBatchWorkunits/BatchWUInfo?Wuid={Wuid}&amp;{$basicquery}')">
                    <xsl:value-of select="Wuid"/>
                </a>
            </td>
            <td>
            <xsl:choose>
                <xsl:when test="string-length(Owner) and not(string-length($owner))">
                    <xsl:choose>
                        <xsl:when test="string-length($filters)">
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;Owner={Owner}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="Owner"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;Owner={Owner}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="Owner"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;Owner={Owner}')">
                                        <xsl:value-of select="Owner"/></a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?Owner={Owner}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="Owner"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?Owner={Owner}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="Owner"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?Owner={Owner}')">
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
            <xsl:choose>
                <xsl:when test="string-length(LoginID) and not(string-length($loginid))">
                    <xsl:choose>
                        <xsl:when test="string-length($filters)">
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;LoginID={LoginID}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="LoginID"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;LoginID={LoginID}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="LoginID"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;LoginID={LoginID}')">
                                        <xsl:value-of select="LoginID"/></a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?LoginID={LoginID}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="LoginID"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?LoginID={LoginID}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="LoginID"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?LoginID={LoginID}')">
                                        <xsl:value-of select="LoginID"/></a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                      <xsl:value-of select="LoginID"/>
                </xsl:otherwise>
             </xsl:choose>
             </td>
            <td>
            <xsl:choose>
                <xsl:when test="string-length(CustomerName) and not(string-length($customername))">
                    <xsl:choose>
                        <xsl:when test="string-length($filters)">
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;CustomerName={CustomerName}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="CustomerName"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;CustomerName={CustomerName}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="CustomerName"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;CustomerName={CustomerName}')">
                                        <xsl:value-of select="CustomerName"/></a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:choose>
                                <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?CustomerName={CustomerName}&amp;Sortby={$sortby}')">
                                        <xsl:value-of select="CustomerName"/></a>
                                </xsl:when>
                                <xsl:when test="string-length($sortby)">
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?CustomerName={CustomerName}&amp;Sortby={$sortby}&amp;Descending=1')">
                                        <xsl:value-of select="CustomerName"/></a>
                                </xsl:when>
                                <xsl:otherwise>
                                        <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?CustomerName={CustomerName}')">
                                        <xsl:value-of select="CustomerName"/></a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                      <xsl:value-of select="CustomerName"/>
                </xsl:otherwise>
             </xsl:choose>
            </td>
            <td>
                <xsl:value-of select="Priority"/>
            </td>
            <td>
                <xsl:value-of select="InputRecCount"/>
            </td>
            <!--td>
                <xsl:value-of select="TimeUploaded"/>
            </td-->
            <td>
                <xsl:value-of select="TimeCompleted"/>
            </td>
            <td>
                 <xsl:choose>
                        <xsl:when test="string-length(Status) and not(string-length($status))">
                        <xsl:choose>
                            <xsl:when test="string-length($filters)">
                                <xsl:choose>
                                    <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                            <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;Status={Status}&amp;Sortby={$sortby}')">
                                            <xsl:choose>
                                                <xsl:when test="Status='scheduled' and number(Aborting)">
                                                    scheduled(aborting)
                                                </xsl:when>
                                                <xsl:otherwise>
                                                    <xsl:value-of select="Status"/>
                                                </xsl:otherwise>
                                            </xsl:choose>
                                            </a>
                                    </xsl:when>
                                    <xsl:when test="string-length($sortby)">
                                            <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;Status={Status}&amp;Sortby={$sortby}&amp;Descending=1')">
                                            <xsl:choose>
                                                <xsl:when test="Status='scheduled' and number(Aborting)">
                                                    scheduled(aborting)
                                                </xsl:when>
                                                <xsl:otherwise>
                                                    <xsl:value-of select="Status"/>
                                                </xsl:otherwise>
                                            </xsl:choose>
                                            </a>
                                    </xsl:when>
                                    <xsl:otherwise>
                                            <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?{$filters}&amp;Status={Status}')">
                                            <xsl:choose>
                                                <xsl:when test="Status='scheduled' and number(Aborting)">
                                                    scheduled(aborting)
                                                </xsl:when>
                                                <xsl:otherwise>
                                                    <xsl:value-of select="Status"/>
                                                </xsl:otherwise>
                                            </xsl:choose>
                                            </a>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:choose>
                                    <xsl:when test="string-length($sortby) and $descending &lt; 1">
                                            <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?Status={Status}&amp;Sortby={$sortby}')">
                                            <xsl:choose>
                                                <xsl:when test="Status='scheduled' and number(Aborting)">
                                                    scheduled(aborting)
                                                </xsl:when>
                                                <xsl:otherwise>
                                                    <xsl:value-of select="Status"/>
                                                </xsl:otherwise>
                                            </xsl:choose>
                                            </a>
                                    </xsl:when>
                                    <xsl:when test="string-length($sortby)">
                                            <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?Status={Status}&amp;Sortby={$sortby}&amp;Descending=1')">
                                            <xsl:choose>
                                                <xsl:when test="Status='scheduled' and number(Aborting)">
                                                    scheduled(aborting)
                                                </xsl:when>
                                                <xsl:otherwise>
                                                    <xsl:value-of select="Status"/>
                                                </xsl:otherwise>
                                            </xsl:choose>
                                            </a>
                                    </xsl:when>
                                    <xsl:otherwise>
                                            <a href="javascript:goplus('/WsBatchWorkunits/BatchWUQuery?Status={Status}')">
                                            <xsl:choose>
                                                <xsl:when test="Status='scheduled' and number(Aborting)">
                                                    scheduled(aborting)
                                                </xsl:when>
                                                <xsl:otherwise>
                                                    <xsl:value-of select="Status"/>
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
                            <xsl:when test="Status='scheduled' and number(Aborting)">
                                scheduled(aborting)
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="Status"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
        </tr>
   </xsl:template>

   <xsl:template match="ListColumn" mode="createCheckboxes">
      <xsl:variable name="index" select="position()-1"/>
      <xsl:if test="$index mod 5 = 0 and $index > 0">
         <xsl:text disable-output-escaping="yes">&lt;/tr&gt;&lt;tr&gt;</xsl:text>
      </xsl:if>
      <td>
         <xsl:choose>
            <xsl:when test="ColumnSize=1">
               <input type="checkbox" id="ShowColumns_i{$index}" name="ShowColumns_i{$index}" 
                   value="" onclick="menuHandler('_col_{$index}', true);" checked = "0">
                 <xsl:value-of select="ColumnLabel"/>
                 </input>
            </xsl:when>
            <xsl:otherwise>
               <input type="checkbox" id="ShowColumns_i{$index}" name="ShowColumns_i{$index}" 
                   value="" onclick="menuHandler('_col_{$index}', true);">
                 <xsl:value-of select="ColumnLabel"/>
                 </input>
            </xsl:otherwise>
         </xsl:choose>
      </td>
    </xsl:template>

   <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
