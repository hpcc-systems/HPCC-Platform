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
    <xsl:import href="/esp/xslt/result_lib.xslt"/>
    <xsl:output method="html"/><!--change this to xml for stage1Only-->
    <xsl:param name="pageSize" select="/WUResultResponse/Requested"/>
    <xsl:param name="rowStart" select="/WUResultResponse/Start"/>
    <xsl:param name="rowCount" select="/WUResultResponse/Count"/>
    
    <xsl:variable name="debug" select="0"/>
    <xsl:variable name="stage1Only" select="0"/><!--for debugging: produce intermediate nodeset only-->
    <xsl:variable name="stage2Only" select="0"/><!--for debugging: process intermediate nodeset when fed as input-->
    <xsl:variable name="filePath">
        <xsl:choose>
            <xsl:when test="$debug">c:/development/bin/debug/files</xsl:when>
            <xsl:otherwise>/esp/files_</xsl:otherwise>
        </xsl:choose>
    </xsl:variable>

    
    <xsl:template match="/">
        <xsl:choose>
            <xsl:when test="$debug and $stage1Only">
                <xsl:apply-templates select="WUResultResponse/Result"/>
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
    
    
    <xsl:template match="/WUResultResponse">
        <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Result <xsl:value-of select="Name"/>
                </title>
        <xsl:if test="(string-length(Format) &lt; 1) or (Format != 'xls')">
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
        <link rel="stylesheet" type="text/css" href="{$filePath}/default.css"/>
        </xsl:if>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script language="JavaScript1.2">
                    function get_xls(all)
                    {
                        var start=document.getElementById("Start").value-0,
                        count=document.getElementById("Count").value-0;
                        
                        <xsl:text disable-output-escaping="yes">
                            var ref='/WsWorkunits/WUResultBin?Format=xls&amp;';
                        </xsl:text>
                        <xsl:if test="string-length(Wuid)">
              ref += 'Wuid=<xsl:value-of select="Wuid"/>
                            <xsl:text disable-output-escaping="yes">&amp;</xsl:text>Sequence=<xsl:value-of select="Sequence"/>';
                        </xsl:if>
                        <xsl:if test="string-length(LogicalName)">
              ref += 'LogicalName=<xsl:value-of select="LogicalName"/>';
                        </xsl:if>
            if (!all)
            {
                          <xsl:text disable-output-escaping="yes">
                              ref += '&amp;Count='+count+'&amp;Start='+start;
                          </xsl:text>
            }
            document.location.href=ref;
                    }
                    
                    function show_results(start,count)
                    {
                        document.getElementById("Count").value=count;
                        document.getElementById("Start").value=start;
                        return true;
                    }
                    function submit_form()
                    {
                        var start=document.getElementById("Start").value-0,
                        count=document.getElementById("Count").value-0;
                        if(count>=10000)
                        {
                            alert('Count must be less than 10000');
                            return false;
                        }
                        if(0>start || start>=<xsl:value-of select="Total"/>)
                        {
                            alert('Start must be between 1 and <xsl:value-of select="Total"/>');
                            return false;
                        }
                        return true;
                    }
            </script>
        </head>
        <body class="yui-skin-sam" onload="nof5();">
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
                <form method="get" onsubmit="return submit_form()" action="/WsWorkunits/WUResult">
                    <h4>
                        <xsl:call-template name="id2string">
                            <xsl:with-param name="toconvert" select="Name"/>
                        </xsl:call-template>
                        <xsl:if test="Total!=9223372036854775807">: total <xsl:value-of select="Total"/> rows </xsl:if>
                        <a href="javascript:get_xls(1)">(.xls)</a>
                    <xsl:if test="Total > 0"> -- current display: from row <xsl:value-of select="$rowStart+1"/> to row <xsl:value-of select="$rowStart+$rowCount"/> <a href="javascript:get_xls(0)">(.xls)</a>
          </xsl:if>
                        <xsl:if test="string-length(Wuid)">
                            <input type="hidden" name="Wuid" value="{Wuid}"/>
                            <input type="hidden" name="Sequence" value="{Sequence}"/>
                        </xsl:if>
                        <xsl:if test="string-length(LogicalName)">
                            <input type="hidden" name="LogicalName" value="{LogicalName}"/>
                        </xsl:if>
            <xsl:if test="string-length(Cluster)">
              <input type="hidden" name="Cluster" value="{Cluster}"/>
            </xsl:if>
                        <input type="hidden" name="Start" id="Start" value="{Start}"/>
                        <input type="hidden" name="Count" id="Count" value="{Count}"/>
                    <xsl:if test="Total > 0">
                        <br/><br/>
                        Start <input id="_start" size="5" type="text" value="{1 + Start}"/>
                        Count <input id="_count" size="5" type="text" value="{Requested}"/>
                        <xsl:text>  </xsl:text>
                        <input type="submit" name="Go" value="Submit" onclick="return show_results(parseInt(document.getElementById('_start').value)-1,parseInt(document.getElementById('_count').value))"/>
                    </xsl:if>
                    </h4>
                    <xsl:if test="Total > Count">
                        <input type="submit" value="&lt;&lt;" onclick="return show_results(Math.max({Start - $pageSize},0),{$pageSize})">
                            <xsl:if test="not(Start>0)">
                                <xsl:attribute name="disabled">disabled</xsl:attribute>
                            </xsl:if>
                        </input>
                        <xsl:text> </xsl:text>
                        <input type="submit" value=">>" onclick="return show_results({Start + $pageSize},{$pageSize})">
                            <xsl:if test="not(Total>Start + $pageSize)">
                                <xsl:attribute name="disabled">disabled</xsl:attribute>
                            </xsl:if>
                        </input>
                        <br/>
                    </xsl:if>
                    <xsl:apply-templates select="Result"/>
                    <xsl:if test="Total > Count">
                        <input type="submit" value="&lt;&lt;" onclick="return show_results(Math.max({Start - $pageSize},0),{$pageSize})">
                            <xsl:if test="not(Start>0)">
                                <xsl:attribute name="disabled">disabled</xsl:attribute>
                            </xsl:if>
                        </input>
                        <xsl:text> </xsl:text>
                        <input type="submit" value=">>" onclick="return show_results({Start + $pageSize},{$pageSize})">
                            <xsl:if test="not(Total>Start + $pageSize)">
                                <xsl:attribute name="disabled">disabled</xsl:attribute>
                            </xsl:if>
                        </input>
                        <br/>
                    </xsl:if>
                </form>
            </body>
        </html>
    </xsl:template>
    
    
    <xsl:template match="/WUResultExcel">
        <html>
            <head>
                <!--link type="text/css" rel="StyleSheet" href="files_/css/list.css"/-->
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Result <xsl:value-of select="AttrName"/>
                </title>
        <!--link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <style type="text/css">
                table.results 
                { 
                     font: 9pt arial; 
                }
                </style-->
            </head>
      <body class="yui-skin-sam">
                <xsl:apply-templates select="Result"/>
            </body>
        </html>
    </xsl:template>
    
</xsl:stylesheet>
