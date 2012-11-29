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
    <xsl:variable name="owner" select="/DFUQueryResponse/Owner"/>
    <xsl:variable name="cluster" select="/DFUQueryResponse/Prefix"/>
    <xsl:variable name="clustername" select="/DFUQueryResponse/ClusterName"/>
    
    <xsl:variable name="logicalname" select="/DFUQueryResponse/LogicalName"/>
    <xsl:variable name="descriptionfilter" select="/DFUQueryResponse/Description"/>
    <xsl:variable name="startdate" select="/DFUQueryResponse/StartDate"/>
    <xsl:variable name="enddate" select="/DFUQueryResponse/EndDate"/>
    <xsl:variable name="filetype" select="/DFUQueryResponse/FileType"/>
    <xsl:variable name="filesizefrom" select="/DFUQueryResponse/FileSizeFrom"/>
    <xsl:variable name="filesizeto" select="/DFUQueryResponse/FileSizeTo"/>
    <xsl:variable name="firstn" select="/DFUQueryResponse/FirstN"/>
    <xsl:variable name="firstntype" select="/DFUQueryResponse/FirstNType"/>

    <xsl:variable name="num" select="/DFUQueryResponse/NumFiles"/>
    <xsl:variable name="pagesize" select="/DFUQueryResponse/PageSize"/>
    <xsl:variable name="lastpagefrom" select="/DFUQueryResponse/LastPageFrom"/>
    <xsl:variable name="prevpagefrom" select="/DFUQueryResponse/PrevPageFrom"/>
    <xsl:variable name="nextpagefrom" select="/DFUQueryResponse/NextPageFrom"/>
    <xsl:variable name="sortby" select="/DFUQueryResponse/Sortby"/>
    <xsl:variable name="descending" select="/DFUQueryResponse/Descending"/>
    <xsl:variable name="filters" select="/DFUQueryResponse/Filters"/>
    <xsl:variable name="parametersforpaging" select="/DFUQueryResponse/ParametersForPaging"/>
    <xsl:variable name="basicquery" select="/DFUQueryResponse/BasicQuery"/>

    <xsl:template match="/DFUQueryResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <xsl:text disable-output-escaping="yes"><![CDATA[
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
          <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
                    <link type="text/css" rel="styleSheet" href="/esp/files/css/sortabletable.css"/>
           <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
          <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
          <script language="JavaScript1.2" src="/esp/files/scripts/multiselect.js"></script>
        ]]></xsl:text>

          <script language="JavaScript1.2" id="menuhandlers">
                    var owner = '<xsl:value-of select="$owner"/>';;
                    var cluster = '<xsl:value-of select="$cluster"/>';;
                    var clusterName = '<xsl:value-of select="$clustername"/>';;
                    var logicalName = '<xsl:value-of select="$logicalname"/>';;
                    var descriptionFilter = '<xsl:value-of select="$descriptionfilter"/>';;
                    var startDate = '<xsl:value-of select="$startdate"/>';;
                    var endDate = '<xsl:value-of select="$enddate"/>';;
                    var fileType = '<xsl:value-of select="$filetype"/>';;
                    var fileSizeFrom = '<xsl:value-of select="$filesizefrom"/>';;
                    var fileSizeTo = '<xsl:value-of select="$filesizeto"/>';;
                    var pageSize = '<xsl:value-of select="$pagesize"/>';;
                    var sortBy = '<xsl:value-of select="$sortby"/>';;
                    var descending = '<xsl:value-of select="$descending"/>';;
                    //var firstN = '<xsl:value-of select="$firstn"/>';;
                    //var firstNType = '<xsl:value-of select="$firstntype"/>';;

            var oMenu;

            <xsl:text disable-output-escaping="yes"><![CDATA[
          function DFUFilePopup(query, filename, cluster, replicate, roxiecluster, browsedata, PosId) {
            function detailsDFUFile() {
              document.location.href='/esp/files/stub.htm?Widget=LFDetailsWidget&Name=' + escape(filename) + '&Cluster=' + cluster;
                      }
                        function browseDFUData() {
                          document.location.href='/WsDfu/DFUGetDataColumns?OpenLogicalName='+filename;
                        }
                        function searchDFUData() {
                          if (roxiecluster != "0") {
                                  document.location.href='/WsDfu/DFUSearchData?OpenLogicalName='+filename+ '&ClusterType=roxie&Cluster=' + cluster;
                            } else {
                                  document.location.href='/WsDfu/DFUSearchData?OpenLogicalName='+filename+ '&ClusterType=thor&Cluster=' + cluster + '&RoxieSelections=0';
              }
            }
            function replicateDFUFile() {
              document.location.href='/FileSpray/Replicate?sourceLogicalName='+filename;
            }
            function copyDFUFile() {
              document.location.href='/FileSpray/CopyInput?sourceLogicalName='+filename;
            }
            function renameDFUFile() {
              document.location.href='/FileSpray/RenameInput?sourceLogicalName='+filename;
            }
            function desprayDFUFile()
            {
              document.location.href='/FileSpray/DesprayInput?sourceLogicalName='+filename;
            }
            function showRoxieQueries()
            {
              //document.location.href='/WsRoxieQuery/QueriesAction?Type=ListQueries&Cluster='+cluster+'&LogicalName='+filename;
              document.location.href='/WsSMC/DisabledInThisVersion?form_';
            }
            var xypos = YAHOO.util.Dom.getXY('mn' + PosId);
            if (oMenu) {
              oMenu.destroy();
            }              
            oMenu = new YAHOO.widget.Menu("logicalfilecontextmenu", {position: "dynamic", xy: xypos} );
            oMenu.clearContent();

            oMenu.addItems([
            { text: "Details", onclick: { fn: detailsDFUFile } },
            { text: "Copy", onclick: { fn: copyDFUFile } },
            { text: "Rename", onclick: { fn: renameDFUFile } }
            ]);

            if (browsedata != 0) {
            oMenu.addItems([
            { text: "View Data File", onclick: { fn: searchDFUData } }
            ]);
            }

            if (replicate != 0) {
            oMenu.addItems([
            { text: "Replicate", onclick: { fn: replicateDFUFile } }
            ]);
            }

            if (roxiecluster != 0) {
            oMenu.addItems([
            { text: "ShowQuery", onclick: { fn: showRoxieQueries } }
            ]);
            }
            else {
            oMenu.addItems([
            { text: "Despray", onclick: { fn: desprayDFUFile } }
            ]);
            }

            //showPopup(menu,(window.event ? window.event.screenX : 0),  (window.event ? window.event.screenY : 0));
            oMenu.render("dfulogicalfilemenu");
            oMenu.show();
            return false;

            }
          ]]></xsl:text>
          </script>

                    <script language="JavaScript1.2">
                        var title=new Array()
                        title[0]='<ins>Logical Name</ins>';
                        title[1]='Logical Name';
                        title[2]='<ins>Parts</ins>';
                        title[3]='Parts';
                        title[4]='<ins>Size</ins>';
                        title[5]='Size';
                        title[6]='<ins>Records</ins>';
                        title[7]='Records';
                        title[8]='<ins>Modified</ins>';
                        title[9]='Modified';
                        title[10]='<ins>Owner</ins>';
                        title[11]='Owner';
                        title[12]='<ins>Cluster</ins>';
                        title[13]='Cluster';
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

                            //o1.innerHTML=title[headerid];
                        }
                    </script>
                    <script language="JavaScript1.2">
                         <xsl:text disable-output-escaping="yes"><![CDATA[
                                 function doQuery(type, value)
                                 {
                                    var numParam = 0;
                                    var url = '/WsDfu/DFUQuery?';
                                    if (startDate)
                                    {
                                        url += 'StartDate=' + startDate;
                                        numParam++;
                                    }
                                    if (endDate)
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'EndDate=' + endDate;
                                        numParam++;
                                    }
                                    if (pageSize)
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'PageSize=' + pageSize;
                                        numParam++;
                                    }
                                    if (sortBy)
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'Sortby=' + sortBy;
                                        numParam++;
                                    }
                                    if (descending > 0)
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'Descending=' + descending;
                                        numParam++;
                                    }
                                    if (fileType)
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'FileType=' + fileType;
                                        numParam++;
                                    }
                                    if (fileSizeFrom > -1)
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'FileSizeFrom=' + fileSizeFrom;
                                        numParam++;
                                    }
                                    if (fileSizeTo > -1)
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'FileSizeTo=' + fileSizeTo;
                                        numParam++;
                                    }
                                    /*if (firstN)
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'FirstN=' + firstN;
                                        numParam++;
                                    }
                                    if (firstNType)
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'FirstNType=' + firstNType;
                                        numParam++;
                                    }*/
                                    if (type != 1)
                                    {
                                        if (logicalName)
                                        {
                                            if (numParam > 0)
                                                url += '&';
                                            url += 'LogicalName=' + logicalName;
                                            numParam++;
                                        }
                                        if (cluster)
                                        {
                                            if (numParam > 0)
                                                url += '&';
                                            url += 'Prefix=' + cluster;
                                            numParam++;
                                        }
                                        if (descriptionFilter)
                                        {
                                            if (numParam > 0)
                                                url += '&';
                                            url += 'Description=' + descriptionFilter;
                                            numParam++;
                                        }
                                    }
                                    else
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'Prefix=' + value;
                                        numParam++;
                                    }

                                    if (type != 2)
                                    {
                                        if (owner)
                                        {
                                            if (numParam > 0)
                                                url += '&';
                                            url += 'Owner=' + owner;
                                            numParam++;
                                        }
                                    }
                                    else
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'Owner=' + value;
                                        numParam++;
                                    }
                                    if (type != 3)
                                    {
                                        if (clusterName)
                                        {
                                            if (numParam > 0)
                                                url += '&';
                                            url += 'ClusterName=' + clusterName;
                                            numParam++;
                                        }
                                    }
                                    else
                                    {
                                        if (numParam > 0)
                                            url += '&';
                                        url += 'ClusterName=' + value;
                                        numParam++;
                                    }
                                    document.location.href=url;
                                 }
                    ]]></xsl:text>
                    </script>
                    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                    <title>DFU</title>
        <script language="JavaScript1.2">
                    var currentFilters='<xsl:value-of select="$filters"/>';
                    var basicQuery='<xsl:value-of select="$basicquery"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                            var roxieFileChecked = 0;
                            var nonRoxieFileChecked = 0;
                            
                            function checkSelected(o)
                            {
                                if (o.tagName=='INPUT' && o.id!='All'  && o.id!='All1' && o.checked)
                                {
                                    if (document.getElementById(o.value))
                                    {
                                        roxieFileChecked++;
                                    }
                                    else
                                    {
                                        nonRoxieFileChecked++;
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
                                roxieFileChecked = 0;
                                nonRoxieFileChecked = 0;

                                checkSelected(document.forms['listitems']);

                                document.getElementById("deleteBtn").disabled = (roxieFileChecked + nonRoxieFileChecked) == 0;
                                document.getElementById("addTSBtn").disabled = (roxieFileChecked + nonRoxieFileChecked) == 0;
                                /*document.getElementById("RoxieQueryBtn").disabled = true;

                                if (roxieFileChecked > 0 && nonRoxieFileChecked == 0)
                                {
                                    document.getElementById("RoxieQueryBtn").disabled = false;
                                }*/
                            }                            
                            
                            function getSelected(o)
                            {
                                if (o.tagName=='INPUT')
                                {
                                    var val = o.value;
                                    var pt = val.indexOf("@");
                                    if (pt > 0)
                                        val = val.substring(0, pt);
                                    return o.checked ? '\n'+val : '';
                                }
                            
                                var s='';
                                var ch=o.children;
                                if (ch)
                                    for (var i in ch)
                                    s=s+getSelected(ch[i]);
                                 return s;
                            }
                          
                            function headerClicked(headername, descending)
                            {
                                document.location.href='/WsDfu/DFUQuery?'+currentFilters+'&Sortby='+headername+'&Descending='+descending;
                            }                
                            
                            function onLoad()
                            {
                                initSelection('resultsTable');
                                //var table = document.getElementById('resultsTable');
                                //if (table)
                                //  sortableTable = new SortableTable(table, table, ["None", "None", "String", "Number", "NumberWithCommas", "NumberWithCommas", "String", "String"]);
                            }        
                          
                            function getNewPage()
                            {
                                var startFrom = document.getElementById("PageStartFrom").value;
                                var pageEndAt = document.getElementById("PageEndAt").value;
                                if (!startFrom || startFrom < 1)
                                    startFrom = 1;
                                if (!pageEndAt || pageEndAt < startFrom)
                                    pageEndAt = 100;
                                var size = pageEndAt - startFrom + 1;

                                if (basicQuery.length > 0)
                                    document.location.href = '/WsDfu/DFUQuery?PageSize='+size+'&'+basicQuery+'&PageStartFrom='+startFrom;
                                else
                                    document.location.href = '/WsDfu/DFUQuery?PageStartFrom='+startFrom+'&PageSize='+size;

                                return false;
                            }

                            /*function onSubmit(o, subaction)
                            {
                                alert("url");
                                if (o.id != 'deleteBtn' || confirm('Delete selected workunits?'))
                                {
                                    document.forms['listitems'].action = '/FileSpray/' + subaction;
                                    return true;
                                }
                                return false;
                            }
                            //var sortableTable = null;*/
                    ]]></xsl:text>
                </script>
            </head>
            <body onload="nof5();onLoad()" class="yui-skin-sam">
                <h3>Logical Files <xsl:if test="Prefix/text()"> on cluster <xsl:value-of select="Prefix"/></xsl:if></h3>
                <xsl:choose>
                    <xsl:when test="not(DFULogicalFiles/DFULogicalFile[1])">
                        No files found.
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates/>
                    </xsl:otherwise>
                </xsl:choose>
        <div id="dfulogicalfilemenu" />
      </body>
        </html>
    </xsl:template>
    

    <xsl:template match="DFULogicalFiles">
        <form id="newpageform" onsubmit="return getNewPage()" method="post">
            <tr><th>Total <b><xsl:value-of select="/DFUQueryResponse/NumFiles"/></b> files. Current page starts from:</th>
            <td><input type="text" id="PageStartFrom" name="PageStartFrom" value="{/DFUQueryResponse/PageStartFrom}" size="10"/></td>
            <th> to:</th>
            <td><input type="text" id="PageEndAt" name="PageEndAt" value="{/DFUQueryResponse/PageEndAt}" size="10"/></td>
            <td><input type="submit" class="sbutton" value="Submit"/></td></tr>
        </form>
        <form id="listitems" action="/WsDFU/DFUArrayAction" method="post">
            <xsl:choose>
                <xsl:when test="$basicquery!=''">
          <input type="hidden" id="BackToPage" name="BackToPage" value="/WsDfu/DFUQuery?PageSize={$pagesize}&amp;{$basicquery}&amp;PageStartFrom={/DFUQueryResponse/PageStartFrom}">&#160;</input> 
                    <!--input type="hidden" id="BackToPage" name="BackToPage" value="/WsDfu/DFUQuery{$pagesize}&amp;{/DFUQueryResponse/PageStartFrom}"/-->
                </xsl:when>
                <xsl:otherwise>
                    <input type="hidden" id="BackToPage" name="BackToPage" value="/WsDfu/DFUQuery?PageSize={$pagesize}&amp;PageStartFrom={/DFUQueryResponse/PageStartFrom}">&#160;</input>
          <!--input type="hidden" id="BackToPage" name="BackToPage" value="/WsDfu/DFUQuery{$pagesize}&amp;{/DFUQueryResponse/PageStartFrom}"/-->
                </xsl:otherwise>
            </xsl:choose>
            <table class="sort-table" id="resultsTable">
                <colgroup>
                    <col/>
                    <col/>
                    <col/>
                    <col/>
                    <col/>
                    <col class="number"/>
                    <col class="number"/>
                    <col/>
                    <col/>
                    <col/>
                    <col class="number"/>
                </colgroup>
                <thead>
                <tr>
                    <th>
            &#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;&#160;
                        <!--xsl:if test="$num > 1">
                            <xsl:attribute name="id">selectAll1</xsl:attribute>
                            <input type="checkbox" id="All1" title="Select or deselect all logical files" onclick="selectAll(this.checked)"/>
                        </xsl:if-->
                    </th>
                    <th><img src="/esp/files/img/zip.gif" title="Compressed"></img></th>
                    <th><img src="/esp/files/img/keyfile.png" title="Indexed"></img></th>
                    <xsl:choose>
                       <xsl:when test="$sortby='Name' and $descending &lt; 1">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Name', 1)">Logical Name<img src="/esp/files/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Name'">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Name', 0)">Logical Name<img src="/esp/files/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby!=''">
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Name', 0)">Logical Name</th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="left" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Name', 1)">Logical Name</th>
                       </xsl:otherwise>
                   </xsl:choose>
                   <xsl:choose>
                       <xsl:when test="$sortby='Description' and $descending &lt; 1">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Description', 1)">Description<img src="/esp/files/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Description'">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Description', 0)">Description<img src="/esp/files/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Description', 0)">Description</th>
                       </xsl:otherwise>
                   </xsl:choose>
                   <xsl:choose>
                       <xsl:when test="$sortby='Size' and $descending &lt; 1">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Size', 1)">Size<img src="/esp/files/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Size'">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Size', 0)">Size<img src="/esp/files/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Size', 1)">Size</th>
                       </xsl:otherwise>
                   </xsl:choose>
                   <xsl:choose>
                       <xsl:when test="$sortby='Records' and $descending &lt; 1">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Records', 1)">Records<img src="/esp/files/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Records'">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Records', 0)">Records<img src="/esp/files/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Records', 1)">Records</th>
                       </xsl:otherwise>
                   </xsl:choose>
                   <xsl:choose>
                       <xsl:when test="$sortby='Modified' and $descending &lt; 1">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Modified', 1)">Modified (UTC/GMT)<img src="/esp/files/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Modified'">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Modified', 0)">Modified (UTC/GMT)<img src="/esp/files/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Modified', 1)">Modified (UTC/GMT)</th>
                       </xsl:otherwise>
                   </xsl:choose>
                   <xsl:choose>
                       <xsl:when test="$sortby='Owner' and $descending &lt; 1">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Owner', 1)">Owner<img src="/esp/files/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Owner'">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Owner', 0)">Owner<img src="/esp/files/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Owner', 0)">Owner</th>
                       </xsl:otherwise>
                   </xsl:choose>
                   <xsl:choose>
                       <xsl:when test="$sortby='Cluster' and $descending &lt; 1">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Cluster', 1)">Cluster<img src="/esp/files/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Cluster'">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Cluster', 0)">Cluster<img src="/esp/files/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Cluster', 0)">Cluster</th>
                       </xsl:otherwise>
                   </xsl:choose>
                   <xsl:choose>
                       <xsl:when test="$sortby='Parts' and $descending &lt; 1">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Parts', 1)">Parts<img src="/esp/files/img/upsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:when test="$sortby='Parts'">
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Parts', 0)">Parts<img src="/esp/files/img/downsimple.png" width="10" height="10"></img></th>
                       </xsl:when>
                       <xsl:otherwise>
                          <th align="center" style="cursor:pointer" onmouseover="bgColor='#FFFFFF'" onmouseout="bgColor='#CCCCCC'" onclick="headerClicked('Parts', 0)">Parts</th>
                       </xsl:otherwise>
                   </xsl:choose>
                </tr>
                </thead>
                <tbody>
                <xsl:apply-templates select="DFULogicalFile"/>
                </tbody>
            </table>
      <table>
            <tr>
            <xsl:if test="$prevpagefrom &gt; 0">
                <td><a href="javascript:go('/WsDfu/DFUQuery?{$parametersforpaging}')">First</a></td>
                <td><a href="javascript:go('/WsDfu/DFUQuery?{$parametersforpaging}&amp;PageStartFrom={$prevpagefrom}')">Prev</a></td>
            </xsl:if>
            <xsl:if test="$nextpagefrom &gt; 0">
                <td><a href="javascript:go('/WsDfu/DFUQuery?{$parametersforpaging}&amp;PageStartFrom={$nextpagefrom}')">Next</a></td>
                <td><a href="javascript:go('/WsDfu/DFUQuery?{$parametersforpaging}&amp;PageStartFrom={$lastpagefrom}')">Last</a></td>
            </xsl:if>
            </tr>
      </table>  
            <br/>
            <table id="btnTable">
                <colgroup>
                    <col span="4"/>
                </colgroup>
                <tr>
                    <td>
                        <input type="button" class="sbutton" value="Clear" onclick="selectAll(false)"/>
                    </td>
                    <td>
                        <input type="submit" class="sbutton" id="deleteBtn" name="Type" value="Delete" disabled="true" onclick="return confirm('Are you sure you want to delete the following file(s) ?\n\n'+getSelected(document.forms['listitems']).substring(1,1000))"/>
                    </td>
                    <!--td>
                        <input type="submit" class="sbutton" id="RoxieQueryBtn"  name="Type" value="ShowRoxieQueries" disabled="true" />
                    </td-->
                    <td>
                        <input type="submit" class="sbutton" id="addTSBtn" name="Type" value="Add To Superfile" disabled="true" />
                    </td>
                </tr>
            </table>
        </form>
    </xsl:template>
    
    <xsl:template match="DFULogicalFile">
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
            <xsl:variable name="info_query">
                <xsl:value-of select="Name"/>
                <xsl:choose>
                    <xsl:when test="string-length(ClusterName)">&amp;Cluster=<xsl:value-of select="ClusterName"/></xsl:when>
                </xsl:choose>
            </xsl:variable>
      <td>
        <xsl:if test="FromRoxieCluster=1">
          <input type="hidden" id="{Name}@{ClusterName}"/>
        </xsl:if>
        <input type="checkbox" name="LogicalFiles_i{position()}" value="{Name}@{ClusterName}" onclick="return clicked(this, event)"/>
          <xsl:variable name="popup">return DFUFilePopup('<xsl:value-of select="$info_query"/>', '<xsl:value-of select="Name"/>', '<xsl:value-of select="ClusterName"/>', '<xsl:value-of select="Replicate"/>', '<xsl:value-of select="FromRoxieCluster"/>', '<xsl:value-of select="BrowseData"/>', '<xsl:value-of select="position()"/>')</xsl:variable>
          <xsl:variable name="oncontextmenu">
            <xsl:value-of select="$popup"/>
          </xsl:variable>
          <img id="mn{position()}" class="menu1" src="/esp/files/img/menu1.png" onclick="{$popup}"></img>
          <a href="javascript:go('/esp/files/stub.htm?Widget=LFDetailsWidget&amp;Cluster={ClusterName}&amp;Name={Name}')">Details</a>
      </td>
            <td>
              <xsl:if test="isZipfile=1">
                <img border="0" src="/esp/files/img/zip.gif" title="Compressed" width="16" height="16"/>
              </xsl:if>
            </td>
            <td>
              <xsl:if test="IsKeyFile=1">
                <img border="0" src="/esp/files/img/keyfile.png" title="Indexed" width="16" height="16"/>
              </xsl:if>
            </td>
            <td align="left">
              <xsl:choose>
              <xsl:when test="isSuperfile=1">
                <I>
                <b>
                <xsl:choose>
                    <xsl:when test="string-length(Prefix) and not(string-length($cluster)) and not(string-length($logicalname))">
                        <a href="javascript:doQuery(1, '{Prefix}')">
                            <xsl:value-of select="Prefix"/>
                        </a>
                        <xsl:value-of select="substring(Name,1+string-length(Prefix))"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="Name"/>
                    </xsl:otherwise>
                </xsl:choose>
                </b>
                </I>
              </xsl:when>
              <xsl:otherwise>
                <xsl:choose>
                    <xsl:when test="string-length(Prefix) and not(string-length($cluster)) and not(string-length($logicalname))">
                        <a href="javascript:doQuery(1, '{Prefix}')">
                            <xsl:value-of select="Prefix"/>
                        </a>
                        <xsl:value-of select="substring(Name,1+string-length(Prefix))"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="Name"/>
                    </xsl:otherwise>
                </xsl:choose>
              </xsl:otherwise>
              </xsl:choose>
            </td>
            <td>
                    <xsl:value-of select="Description"/>
            </td>
            <td>
                    <xsl:value-of select="Totalsize"/>
            </td>
            <td>
                    <xsl:value-of select="RecordCount"/>
            </td>
            <td nowrap="nowrap" align="center">
                    <xsl:value-of select="Modified"/>
            </td>
            <td>
                <xsl:choose>
                    <xsl:when test="string-length(Owner) and not(string-length($owner))">
                        <a href="javascript:doQuery(2, '{Owner}')">
                            <xsl:value-of select="Owner"/>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="Owner"/>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
            <td>
                <xsl:choose>
                    <xsl:when test="string-length(ClusterName) and not(string-length($clustername))">
                        <a href="javascript:doQuery(3, '{ClusterName}')">
                            <xsl:value-of select="ClusterName"/>
                        </a>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="ClusterName"/>
                    </xsl:otherwise>
                </xsl:choose>
            </td>
            <td>
                    <xsl:value-of select="Parts"/>
            </td>
        </tr>
    </xsl:template>
    
    <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
