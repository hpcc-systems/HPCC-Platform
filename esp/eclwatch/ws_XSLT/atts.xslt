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

<!DOCTYPE xsl:stylesheet [
    <!--define the HTML non-breaking space:-->
    <!ENTITY nbsp "<xsl:text disable-output-escaping='yes'>&amp;nbsp;</xsl:text>">
]>
<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:param name="wuid" select="''"/>
<xsl:param name="graphName" select="''"/>

<xsl:output method="xhtml"/>
<xsl:include href="/esp/xslt/lib.xslt"/>
    

<xsl:template match="/">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <style type="text/css">
            body    { margin:0; }
            </style>
            <script defer="defer">
                function popups_loaded()
                {
                    var top = window.top;
                    if (!top.show_popup || !top.activepopup)
                        return;
                    top.show_popup(null, top.activepopup);
                }
            </script>
        </head>
        <body class="yui-skin-sam" onload="nof5();popups_loaded()">     
            <xsl:apply-templates select="//node|//edge[att]"/>
        </body>
    </html>
</xsl:template>


<xsl:template match="graph">
    <xsl:apply-templates/>
</xsl:template>


<xsl:template match="node|edge">
    <xsl:if test="att[string-length(@name) and not(starts-with(@name, '_'))]">
        <div id="popup_{@id}" style="overflow:auto">
            <table id="tab" style="font:menu">
                <colgroup>
                    <col align="left" valign="top"/>
                </colgroup>
                <tr id="captionRow" style="display:none">
                    <th><xsl:value-of select="name()"/></th>
                    <td align="right">
                        <a href="" onclick="javascript:parent.hide_popup()">
                            <img border="0" src="../esp/files/img/close_wnd.gif"/>
                        </a>
                    </td>
                </tr>
                <tr id="wuRow" style="display:none">
                    <th>workunit</th>
                    <td><xsl:value-of select="$wuid"/></td>
                </tr>
                <tr id="graphNameRow" style="display:none">
                    <th>graph</th>
                    <td>
                        <xsl:value-of select="$graphName"/>
                    </td>
                </tr>
                <tr>
                    <xsl:if test="name()='edge'">
                        <xsl:attribute name="id">idRow</xsl:attribute>
                        <xsl:attribute name="style">display:none</xsl:attribute>
                    </xsl:if>
                    <th>id</th>
                    <td>
                        <xsl:value-of select="@id"/>
                    </td>
                </tr>
                <xsl:apply-templates select="att[string-length(@name) and not(starts-with(@name, '_'))]"/>
            </table>
        </div>
    </xsl:if>
</xsl:template>


<xsl:template match="att">
    <tr>
        <th>
            <xsl:value-of select="@name"/>
        </th>
        <xsl:choose>
            <xsl:when test="Dataset[1]">
                <td>
                    <xsl:apply-templates select="Dataset"/>
                </td>
            </xsl:when>
            <xsl:when test="@name='count' or @name='max' or @name='min'">
                <td>
                    <xsl:call-template name="comma_separated">
                        <xsl:with-param name="value" select="@value"/>
                    </xsl:call-template>
                </td>
            </xsl:when>
            <xsl:otherwise>
                <td>
                    <xsl:value-of select="@value"/>
                </td>
            </xsl:otherwise>
        </xsl:choose>
    </tr>
</xsl:template>


<xsl:template match="Dataset">
    <table style="font:menu;" border="1" cellspacing="0">
        <thead>
            <tr>
                <xsl:for-each select="Group[1]/Row[1]/*">
                    <th>
                        <xsl:value-of select="name()"/>
                    </th>
                </xsl:for-each>
            </tr>
        </thead>
        <xsl:apply-templates select="Group"/>
    </table>
</xsl:template>


<xsl:template match="Group">
    <tbody>
        <xsl:apply-templates select="Row"/>
    </tbody>
</xsl:template>


<xsl:template match="Row">
    <tr>
        <xsl:apply-templates select="*"/>
    </tr>
</xsl:template>


<xsl:template match="*">
    <td>
        <xsl:choose>
            <xsl:when test=".!=''">
                <xsl:value-of select="."/>
            </xsl:when>
            <xsl:otherwise>&nbsp;</xsl:otherwise>
        </xsl:choose>
    </td>
</xsl:template>

</xsl:stylesheet>
