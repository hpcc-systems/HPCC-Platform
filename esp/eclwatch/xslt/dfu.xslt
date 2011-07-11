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
    <xsl:param name="cluster"/>
    <xsl:template match="Dfu">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>DFU</title>
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
            function getSelected(o)
            {
                if(o.tagName=='INPUT')
                {
                    return o.checked ? ', '+o.name.substring(6) : '';
                }
                var s='';
                var ch=o.children;
                if(ch)
                    for(var i in ch)
                        s=s+getSelected(ch[i]);
                return s;
            }

            </script>
          </head>
          <body>
            <form id="listitems" action="/dfu{substring('?',1+number(not($__querystring)))}{$__querystring}" method="post">
                <table class="list">
                    <colgroup>
                       <col width="20"/>
                       <col width="400"/>
                       <col width="400"/>
                       <col width="30" class="number"/>
                       <col width="50" class="number"/>
                    </colgroup>
                    <tr class="grey"><th></th><th>Logical Name</th><th>Directory</th><th>Parts</th><th>Size</th></tr>
                    <xsl:apply-templates select="File">
                        <xsl:sort select="Name"/>
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
                    <td><input type="submit" class="sbutton" name="dfu_compress" value="Compress"/></td>
                    <td><input type="submit" class="sbutton" name="dfu_delete" value="Delete" onclick="return confirm('Delete these files:'+getSelected(document.forms['listitems']).substring(1,5000)+' ?')"/></td>
                </tr>
                </table>
            </form>
          </body> 
        </html>
    </xsl:template>

    <xsl:template match="File">
        <tr>
        <xsl:if test="(position() mod 2)=0"><xsl:attribute name="class">grey</xsl:attribute></xsl:if>
        <td><input type="checkbox" name="check_{Name}"/></td>
        <td>
        <xsl:choose>
            <xsl:when test="string-length(Cluster) and not(string-length($cluster))"> 
                <a href="/dfu?cluster={Cluster}{$query}"><xsl:value-of select="Cluster"/></a><xsl:value-of select="substring(Name,1+string-length(Cluster))"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="Name"/>
            </xsl:otherwise>
        </xsl:choose>
        </td>
        <td><xsl:value-of select="Dir"/></td>
        <td><a href="/dfu/{Name}"><xsl:value-of select="Parts"/></a></td>
        <td><a href="/dfu/{Name}"><xsl:value-of select="Size"/></a></td>
        </tr>
    </xsl:template>
</xsl:stylesheet>
