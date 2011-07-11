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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xalan="http://xml.apache.org/xalan" exclude-result-prefixes="xalan">
    <xsl:param name="pageSize" select="/DFUBrowseDataResponse/PageSize"/>
    <xsl:param name="rowStart" select="/DFUBrowseDataResponse/Start"/>
    <xsl:param name="rowCount" select="/DFUBrowseDataResponse/Count"/>
    <xsl:param name="rowStart0" select="/DFUBrowseDataResponse/StartForGoback"/>
    <xsl:param name="rowCount0" select="/DFUBrowseDataResponse/CountForGoback"/>
    <xsl:param name="columnCount" select="/DFUBrowseDataResponse/ColumnCount"/>
    <xsl:param name="Total0" select="/DFUBrowseDataResponse/Total"/>
    <xsl:param name="filterBy0" select="/DFUBrowseDataResponse/FilterForGoBack"/>
    <xsl:param name="logicalName0" select="/DFUBrowseDataResponse/LogicalName"/>
    <xsl:param name="chooseFile" select="/DFUBrowseDataResponse/ChooseFile"/>
    
    <xsl:variable name="debug" select="0"/>
    <xsl:variable name="stage1Only" select="0"/><!--for debugging: produce intermediate nodeset only-->
    <xsl:variable name="stage2Only" select="0"/><!--for debugging: process intermediate nodeset when fed as input-->
    <xsl:variable name="filePath">
        <xsl:choose>
            <xsl:when test="$debug">c:/development/bin/debug/files</xsl:when>
            <xsl:otherwise>/esp/files_</xsl:otherwise>
        </xsl:choose>
    </xsl:variable>

    <xsl:output method="html"/><!--change this to xml for stage1Only-->
    <xsl:include href="/esp/xslt/result_lib2.xslt"/>
    
    <xsl:template match="/">
        <xsl:choose>
            <xsl:when test="$debug and $stage1Only">
                <xsl:apply-templates select="DFUBrowseDataResponse/Result"/>
            </xsl:when>
            <xsl:when test="$debug and $stage2Only">
                <html>
          <body class="yui-skin-sam">
                        <table class="results" cellspacing="0" frame="box" rules="all">
                            <xsl:call-template name="show-row">
                                <xsl:with-param name="nodes" select="."/>
                                <xsl:with-param name="level" select="1"/>
                            </xsl:call-template>
                        </table>
                    </body>
                </html>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    
    <xsl:template match="/DFUBrowseDataResponse">
        <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Result <xsl:value-of select="Name"/>
                </title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <style type="text/css">
                   .menu1 {
                    MARGIN: 0px 5px 0px 0px; WIDTH: 12px; HEIGHT: 14px
                   }
                   .results-table {
                      border-top: #777 2px solid; 
                      border-bottom: #777 2px solid; 
                      font: 9pt arial, helvetica, sans-serif;
                      padding:1px;
                      text-align:center;
                        border:             double solid #0066CC; 
                    }
                   .results-table thead {
                      background:lightgrey;
                   };
                   .results-table th {
                      border:             1 solid #AACCDD; 
                   }
                   .results-table td {
                      border:             1 solid #AACCDD; 
                   }
                </style>
                    <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                    </script>
                <script language="JavaScript1.2">
                    var start0='<xsl:value-of select="$rowStart0"/>';
                    var count0='<xsl:value-of select="$rowCount0"/>';
                    var total0='<xsl:value-of select="$Total0"/>';
                    var name0='<xsl:value-of select="$logicalName0"/>';
                    var filterby0='<xsl:value-of select="$filterBy0"/>';
                    var columncount='<xsl:value-of select="$columnCount"/>';
                    var choosefile='<xsl:value-of select="$chooseFile"/>';
                    var rbid = 0;
                    
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    function onRBChanged(id)
                    {
                        rbid = id;
                    }

                    function get_data()
                    {
                        var start = 0;
                        var count = 1;
                        if (rbid > 1)
                        {
                            start = document.getElementById('_start').value - 1;
                            count = document.getElementById('_count').value - 0;
                        }
                        else if (rbid > 0) //last n records
                        {
                            count = document.getElementById('last_n').value - 0;
                            start = total0 - count;
                        }
                        else //first n records
                        {
                            count = document.getElementById('first_n').value - 0;
                            start = 0;
                        }

                        if(count>=10000)
                        {
                            alert('Count must be less than 10000');
                            return false;
                        }
                        if(0>start || start>=total0)
                        {
                            alert('Start must be between 1 and ' + total0);
                            return false;
                        }

                        var showcolumns = '';
                        for (i= 0; i< columncount; i++)
                        {
                            var id = 'ShowColumns_i' + (i); //no checkbox exists for column 1 
                            var form = document.forms['control'];                      
                            var checkbox = form.all[id];                       
                            if (checkbox.checked)
                                showcolumns = showcolumns + '/' + i;
                        }
                        
                        var url = "/WsDFU/DFUBrowseData?ChooseFile=" + choosefile + "&LogicalName=" + name0 + 
                                "&Start=" + start + "&Count=" + count +
                                "&StartForGoback=" + start0 + "&CountForGoback=" + count0;
                        if (showcolumns.length > 0)
                            url = url + "&ShowColumns=" + showcolumns;
                    
                        if (filterby0)
                            url = url + "&FilterBy=" + escape(filterby0);
                    
                        document.location.href=url;
                        return true;
                    }

                    function go_back()
                    {
                        var showcolumns = '';
                        for (i= 0; i< columncount; i++)
                        {
                            var id = 'ShowColumns_i' + (i); //no checkbox exists for column 1 
                            var form = document.forms['control'];                      
                            var checkbox = form.all[id];                       
                            if (checkbox.checked)
                                showcolumns = showcolumns + '/' + i;
                        }
                        
                        var end0 = start0 + count0;
                        var url = "/WsDfu/DFUGetDataColumns?ChooseFile=" + choosefile + "&OpenLogicalName=" + name0 + 
                                "&StartIndex=" + start0 + "&EndIndex=" + end0;
                        if (showcolumns.length > 0)
                            url = url + "&ShowColumns=" + showcolumns;
                    
                        if (filterby0)
                            url = url + "&FilterBy=" + escape(filterby0);
                    
                        document.location.href=url;
                        return true;
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
                        var table     = document.forms['data'].all['dataset_table'];
                        if (table != NaN)
                        {
                            var colGroup  = getElementByTagName(table, 'COLGROUP');
                            var col = colGroup.children[index];
                            var show = col.style && col.style.display && col.style.display == 'none' ; //show if hidden at present
                            col.style.display = show ? 'block' : 'none';

                            if (skipCheckBoxUpdate == null)
                            {
                                var id = 'ShowColumns_i' + (index - 1); //no checkbox exists for column 1 
                                var checkbox = document.forms['control'].all[id];                      
                                checkbox.checked = show;
                                clicked(checkbox);
                            }
                        }
                    }

                    function onRowCheck(checked)
                    {
                        var table     = document.forms['data'].all['dataset_table'];
                        if (table != NaN)
                        {
                            var colGroup  = getElementByTagName(table, 'COLGROUP');
                            for (i=1; i<=columncount; i++)
                            {
                                var col = colGroup.children[i];
                                    col.style.display = checked ? 'block' : 'none';

                            }
                        }
                    }

                    function onLoad()
                    {
                        initSelection('viewTable', true);
                    }        
                    ]]></xsl:text>
            </script>
        </head>
                <xsl:variable name="From" select="Start+1"/>
        <xsl:variable name="To" select="Start+Count"/>
    <body class="yui-skin-sam" onload="nof5();onLoad()">
                <form method="post" onsubmit="return go_back()">
                    <h3>
                        <xsl:if test="string-length(Wuid)">
                        Wuid:<a href="/WsWorkunits/WUInfo?Wuid={Wuid}">
                                <xsl:value-of select="Wuid"/>
                            </a>
                        </xsl:if>
                        <xsl:if test="string-length(LogicalName)">
                            File:<xsl:value-of select="LogicalName"/>
                        </xsl:if>
                    </h3>
                    <input id="backBtn" type="submit" value="Back to Column Filter Page" />
                </form>
                <form>
                    <h4>
                        <xsl:call-template name="id2string">
                            <xsl:with-param name="toconvert" select="Name"/>
                        </xsl:call-template>
                        
                        <xsl:if test="string-length(Wuid)">
                            <input type="hidden" name="Wuid" value="{Wuid}"/>
                            <input type="hidden" name="Sequence" value="{Sequence}"/>
                        </xsl:if>
                        <xsl:if test="string-length(LogicalName)">
                            <input type="hidden" name="LogicalName" value="{LogicalName}"/>
                        </xsl:if>
                        <input type="hidden" name="Start" id="Start" value="{Start}"/>
                        <input type="hidden" name="Count" id="Count" value="{Count}"/>
                        <input type="hidden" name="FilterBy" id="Count" value="{FilterBy}"/>
                        <!--xsl:if test="Total > 0">
                            <xsl:if test="Total!=9223372036854775807">Total: <xsl:value-of select="Total"/> rows; 
                            display from <xsl:value-of select="$From"/> to <xsl:value-of select="$To"/>; </xsl:if>
                        </xsl:if-->
                        <xsl:if test="string-length(FilterBy)">
                            Filters: <xsl:value-of select="FilterBy"/>
                        </xsl:if>
                    </h4>
                </form>
                <!--form id="control" method="post" onsubmit="return go_back()"-->
                <form>
                    <table>
                        <tr>
                            <td>View Records:
                                <input type="radio" name="ShowDataRB" value="FirstNRB" checked="checked" onclick="onRBChanged(0)"/>Get first
                                <input id="first_n" size="5" type="text" value=""/>
                                <xsl:text>  </xsl:text>
                                <input type="radio" name="ShowDataRB" value="LastNRB" onclick="onRBChanged(1)"/>
                                Get last <input id="last_n" size="5" type="text" value=""/>
                                <xsl:text>  </xsl:text>
                                <input type="radio" name="ShowDataRB" value="RangeRB" onclick="onRBChanged(2)"/>
                                Start from <input id="_start" size="5" type="text" value=""/>
                                Count <input id="_count" size="5" type="text" value=""/>
                                <xsl:text>  </xsl:text>
                                <input type="button" name="Go" value="Submit" onclick="return get_data();"/>
                            </td>
                        </tr>
                    </table>
                </form>
                <form id="control">
                    <table>
                        <tr>
                            <td>
                                <b>View Columns:</b>
                                <input id="CheckALL" type="button" value="Check All" onclick="selectAll(1)"/>
                                <input id="ClearAll" type="button" value="Clear All"  onclick="selectAll(0)"/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <table id="viewTable" width="100%">
                                   <tr/>
                                   <tr>
                                      <xsl:apply-templates select="ColumnsHidden/ColumnHidden" mode="createCheckboxes"/>
                                   </tr>
                                </table>
                                <!--xsl:if test="count(ColumInformation/ColumInfo) > 2">
                                   <table>
                                      <tr>
                                         <th id="selectAll1" colspan="5">
                                            <input type="checkbox" title="Select or deselect all" onclick="selectAll(this.checked)">Select all / none</input>
                                         </th>
                                      </tr>
                                   </table>
                                </xsl:if-->
                            </td>
                        </tr>
                    </table>
                </form>
                <form id="data">
                    <xsl:apply-templates select="Result"/>
                    <h4>
                        <xsl:if test="Total > 0">
                            <xsl:if test="Total!=9223372036854775807">Total: <xsl:value-of select="Total"/> rows; 
                            display from <xsl:value-of select="$From"/> to <xsl:value-of select="$To"/>; </xsl:if>
                        </xsl:if>
                        <br/>                           
                    </h4>
                </form>
            </body>
        </html>
    </xsl:template>
    
    <xsl:template match="ColumnHidden" mode="createCheckboxes">
      <xsl:variable name="index" select="position()-1"/>
      <xsl:if test="$index mod 5 = 0 and $index > 0">
         <xsl:text disable-output-escaping="yes">&lt;/tr&gt;&lt;tr&gt;</xsl:text>
      </xsl:if>
      <td>
         <xsl:choose>
            <xsl:when test="ColumnSize=1">
               <input type="checkbox" id="ShowColumns_i{$index}" name="ShowColumns_i{$index}" 
                   value="" onclick="menuHandler('_col_{$index+1}', true);" checked = "0">
                 <xsl:value-of select="ColumnLabel"/>
                 </input>
            </xsl:when>
            <xsl:otherwise>
               <input type="checkbox" id="ShowColumns_i{$index}" name="ShowColumns_i{$index}" 
                   value="" onclick="menuHandler('_col_{$index+1}', true);">
                 <xsl:value-of select="ColumnLabel"/>
                 </input>
            </xsl:otherwise>
         </xsl:choose>
      </td>
    </xsl:template>
   
   <xsl:template match="node()|@*" mode="createCheckboxes"/>


</xsl:stylesheet>
