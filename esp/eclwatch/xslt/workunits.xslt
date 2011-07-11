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
    <xsl:param name="__querystring"/>
    <xsl:param name="query" select="concat(substring('&amp;',1+number(not($__querystring))),$__querystring)"/>
    <xsl:param name="owner"/>
    <xsl:param name="cluster"/>
    <xsl:param name="state"/>
    <xsl:param name="description"/>
    <xsl:template match="Workunits">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Workunits</title>
            <style type="text/css">
                body { background-color: #white;}
                table.list { border-collapse: collapse; border: double solid #777; font: 10pt arial, helvetica, sans-serif; }
                table.list th, table.list td { border: 1 solid #777; padding:2px; }
                .grey { background-color: #ddd;}
                .number { text-align:right; }
                .sbutton { width: 7em; font: 10pt arial , helvetica, sans-serif; }
            </style>
            <script language="JavaScript1.2">
            function selectAll(o,select)
            {   
                if(o.tagName=='INPUT')
                {
                    o.checked=select ? true : false;
                    return;
                }
                var ch=o.children;
                if(ch)
                    for(var i in ch)
                        selectAll(ch[i],select);
            }
            </script>
          </head>
          <body>
            <form id="listitems" action="/wuid{substring('?',1+number(not($__querystring)))}{$__querystring}" method="post">
                <table class="list">
                    <colgroup>
                       <col width="20"/>
                       <col width="300"/>
                       <col width="100"/>
                       <col width="300"/>
                       <col width="100"/>
                       <col width="100"/>
                    </colgroup>
                    <tr class="grey"><th></th><th>WUID</th><th>Owner</th><th>Job Name</th><th>Cluster</th><th>State</th></tr>
                    <xsl:apply-templates>
                        <xsl:sort select="Wuid" data-type="text" order = "descending"/>
                    </xsl:apply-templates>
                </table>

                <table style="margin:20 0 0 0">
                <colgroup>
                   <col span="4" width="120"/>
                </colgroup>
                <tr>
                    <td><input type="button" class="sbutton" value="Select All" onclick="selectAll(document.forms['listitems'],1)"/></td>
                    <td><input type="button" class="sbutton" value="Select None" onclick="selectAll(document.forms['listitems'],0)"/></td>
                    <td></td>
                    <td><input type="submit" class="sbutton" name="wuid_delete" value="Delete" onclick="return confirm('Delete selected workunits?')"/></td>
                </tr>
                </table>
            </form>
          </body> 
        </html>
    </xsl:template>

    <xsl:template match="Workunit">
        <tr>
        <xsl:if test="(position() mod 2)=0"><xsl:attribute name="class">grey</xsl:attribute></xsl:if>
        <td><xsl:if test="Protected=0"><input type="checkbox" name="check_{Wuid}"/></xsl:if></td>
        <td><a href="/wuid/{Wuid}"><xsl:value-of select="Wuid"/></a></td>
        <xsl:choose>
            <xsl:when test="string-length(Owner) and not(string-length($owner))"> 
                <td><a href="/wuid?owner={Owner}{$query}"><xsl:value-of select="Owner"/></a></td>
            </xsl:when>
            <xsl:otherwise>
                <td><xsl:value-of select="Owner"/></td>
            </xsl:otherwise>
        </xsl:choose>
        <td><xsl:value-of select="substring(concat(substring(JobName,1,40),'...'),1,string-length(JobName))"/></td>
        <xsl:choose>
            <xsl:when test="string-length(Cluster) and not(string-length($cluster))"> 
                <td><a href="/wuid?cluster={Cluster}{$query}"><xsl:value-of select="Cluster"/></a></td>
            </xsl:when>
            <xsl:otherwise>
                <td><xsl:value-of select="Cluster"/></td>
            </xsl:otherwise>
        </xsl:choose>
        <xsl:choose>
            <xsl:when test="string-length(State) and not(string-length($state))"> 
                <td><a href="/wuid?state={State}{$query}"><xsl:value-of select="State"/></a></td>
            </xsl:when>
            <xsl:otherwise>
                <td><xsl:value-of select="State"/></td>
            </xsl:otherwise>
        </xsl:choose>
        </tr>
    </xsl:template>
</xsl:stylesheet>
